package no.basis.integrationplatform.camel;

import jakarta.enterprise.context.ApplicationScoped;
import no.basis.integrationplatform.camel.step.StepOutcomeFailure;
import org.apache.camel.AsyncCallback;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.direct.DirectEndpoint;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.processor.SendProcessor;
import org.apache.camel.support.AsyncProcessorSupport;
import org.apache.camel.support.service.ServiceHelper;

/**
 * Publishes an exhausted managed Kafka input to {@code basis.message-store.dlq-uri} and marks
 * it parked only after the endpoint accepted it. Any other exchange, such as a request a route
 * sends on an exchange of its own, fails back to its caller with its own failure. The endpoint must report a failed publish as an error:
 * a non-transactional Kafka producer that waits for the broker, which Camel's default
 * {@code requestRequiredAcks=all} does, or the application's own {@code direct:} route. A
 * Kafka DLQ always receives one record per message. A failed publish leaves the exchange
 * failed and the message not parked, so its Kafka offset is not committed.
 */
@ApplicationScoped
public class PlatformDeadLetterRoute extends RouteBuilder {
    /** Route id of the adapter's dead-letter route. Its publish is not a replayable step. */
    public static final String ROUTE_ID = "bip-confirmed-dead-letter";

    /** Creates the dead-letter route. */
    public PlatformDeadLetterRoute() {
    }

    @Override
    public void configure() {
        from(PlatformRouteConfiguration.DLQ_URI).routeId(ROUTE_ID).errorHandler(noErrorHandler())
                .process(new Publisher(getContext()));
    }

    /**
     * Resolves the configured endpoint and refuses one whose success does not mean the
     * record was accepted. A Kafka DLQ gets its own endpoint that sends the body as one
     * record: with Camel's default {@code useIterator=true}, a collection body would be sent
     * as one record per element, and an empty one as none.
     */
    static Endpoint confirmingEndpoint(CamelContext context, String uri) {
        // An expression such as ${header.name} can evaluate to no endpoint at all.
        if (uri == null || uri.isBlank() || uri.contains("${") || uri.contains("$simple{")) {
            throw new IllegalArgumentException("basis.message-store.dlq-uri must name one fixed endpoint;"
                    + " an empty value or an expression such as ${header.name} is not accepted");
        }
        Endpoint endpoint = context.getEndpoint(uri);
        if (endpoint instanceof KafkaEndpoint) {
            // Check and publish through one endpoint of its own, made from the component's
            // settings now: the registered endpoint may have been created before them.
            KafkaEndpoint kafka = (KafkaEndpoint) context.getCamelContextExtension().getPrototypeEndpoint(uri);
            var configuration = kafka.getConfiguration();
            Object acks = configuration.getAdditionalProperties()
                    .getOrDefault("acks", configuration.getRequestRequiredAcks());
            if (acks != null && "0".equals(acks.toString().trim())) {
                throw new IllegalArgumentException("basis.message-store.dlq-uri must wait for the broker;"
                        + " with acks=0 a publish is never confirmed");
            }
            // A transactional producer commits after the exchange completes, which is after
            // the adapter has recorded the outcome and committed the input.
            if (configuration.isTransacted()
                    || (configuration.getTransactionalId() != null && !configuration.getTransactionalId().isBlank())
                    || configuration.getAdditionalProperties().containsKey("transactional.id")) {
                throw new IllegalArgumentException("basis.message-store.dlq-uri must not be transactional;"
                        + " its transaction commits only after the input offset");
            }
            configuration.setUseIterator(false);
            return kafka;
        }
        if (endpoint instanceof DirectEndpoint direct) {
            if (!direct.isFailIfNoConsumers()) {
                throw new IllegalArgumentException("basis.message-store.dlq-uri must not set failIfNoConsumers=false;"
                        + " a direct: endpoint without a consumer would then drop the record");
            }
            return endpoint;
        }
        throw new IllegalArgumentException("basis.message-store.dlq-uri must be a kafka: endpoint or the"
                + " application's own direct: route, whose failures show whether the record was parked");
    }

    /**
     * Sends to the configured endpoint and marks the message parked in the send's own
     * completion, so nothing in a {@code direct:} parking route, such as {@code stop()}, can
     * skip the mark.
     */
    static final class Publisher extends AsyncProcessorSupport {
        private final CamelContext context;
        private volatile Endpoint endpoint;
        private volatile SendProcessor send;

        Publisher(CamelContext context) {
            this.context = context;
        }

        @Override
        protected void doStart() {
            // Checked when the route starts, once the components are configured.
            endpoint = confirmingEndpoint(context, context.resolvePropertyPlaceholders(PlatformRouteConfiguration.DLQ_ENDPOINT));
        }

        @Override
        protected void doStop() {
            ServiceHelper.stopService(send);
            send = null;
            if (endpoint instanceof KafkaEndpoint) {
                // Its own endpoint, not registered in the context.
                ServiceHelper.stopService(endpoint);
            }
        }

        @Override
        public boolean process(Exchange exchange, AsyncCallback callback) {
            if (!managedKafkaInput(exchange)) {
                // As in 0.3.6, only a managed Kafka input is parked. A request on an exchange of its
                // own, or an input from another source, fails back to its caller with its own
                // failure, without a parked record or the parking ERROR: the caller owns recovery.
                Throwable original = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
                Throwable unrecorded = StepOutcomeFailure.of(exchange);
                exchange.setException(original != null ? original : unrecorded != null ? unrecorded
                        : new IllegalStateException("Only a managed Kafka input is parked in the DLQ;"
                                + " the failure goes back to the caller"));
                callback.done(true);
                return true;
            }
            try {
                PlatformRouteConfiguration.prepareDlq(exchange);
            } catch (RuntimeException e) {
                exchange.setException(e);
                callback.done(true);
                return true;
            }
            Throwable unrecorded = StepOutcomeFailure.of(exchange);
            if (unrecorded != null) {
                // The input is consumed again once step outcomes can be recorded; a parked copy would repeat it.
                exchange.setException(new IllegalStateException("Not parked: a step outcome was not checked or "
                        + "recorded, so the Kafka offset is not committed and the input is consumed again", unrecorded));
                callback.done(true);
                return true;
            }
            SendProcessor processor;
            try {
                processor = send();
            } catch (Exception e) {
                exchange.setException(e);
                callback.done(true);
                return true;
            }
            return processor.process(exchange, doneSync -> {
                if (exchange.getException() == null) {
                    exchange.setProperty(PlatformExchangeProperties.DLQ_PARKED, true);
                }
                callback.done(doneSync);
            });
        }

        private static boolean managedKafkaInput(Exchange exchange) {
            return exchange.getProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT) != null
                    || exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT) != null;
        }

        // The producer starts when parking is first needed, so an application with no
        // Kafka input does not need a broker merely to start this route.
        private SendProcessor send() {
            SendProcessor current = send;
            if (current == null) {
                synchronized (this) {
                    current = send;
                    if (current == null) {
                        current = new SendProcessor(endpoint);
                        ServiceHelper.startService(current);
                        send = current;
                    }
                }
            }
            return current;
        }
    }
}
