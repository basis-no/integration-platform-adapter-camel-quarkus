package no.basis.integrationplatform.camel;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.AsyncCallback;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.direct.DirectEndpoint;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.processor.SendProcessor;
import org.apache.camel.support.AsyncProcessorSupport;
import org.apache.camel.support.service.ServiceHelper;

/**
 * Publishes an exhausted exchange to {@code basis.message-store.dlq-uri} and marks it parked
 * only after the endpoint accepted it. The endpoint must report a failed publish as an error:
 * a Kafka producer that waits for the broker, which Camel's default
 * {@code requestRequiredAcks=all} does, or the application's own {@code direct:} route. A
 * failed publish leaves the exchange failed and the message not parked, so its Kafka offset
 * is not committed.
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
     * record was accepted.
     */
    static Endpoint confirmingEndpoint(CamelContext context, String uri) {
        // An expression such as ${header.name} can evaluate to no endpoint at all.
        if (uri == null || uri.isBlank() || uri.contains("${") || uri.contains("$simple{")) {
            throw new IllegalArgumentException("basis.message-store.dlq-uri must name one fixed endpoint;"
                    + " an empty value or an expression such as ${header.name} is not accepted");
        }
        Endpoint endpoint = context.getEndpoint(uri);
        if (endpoint instanceof KafkaEndpoint kafka) {
            var configuration = kafka.getConfiguration();
            Object acks = configuration.getAdditionalProperties()
                    .getOrDefault("acks", configuration.getRequestRequiredAcks());
            if (acks != null && "0".equals(acks.toString().trim())) {
                throw new IllegalArgumentException("basis.message-store.dlq-uri must wait for the broker;"
                        + " with acks=0 a publish is never confirmed");
            }
            return endpoint;
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
        }

        @Override
        public boolean process(Exchange exchange, AsyncCallback callback) {
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
