package no.basis.integrationplatform.camel;

import jakarta.enterprise.context.ApplicationScoped;
import no.basis.integrationplatform.camel.step.StepOutcomeFailure;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.model.ToDynamicDefinition;

/** Confirms parking only after the configured dead-letter endpoint succeeds. */
@ApplicationScoped
public class PlatformDeadLetterRoute extends RouteBuilder {
    @Override
    public void configure() {
        var route = from(PlatformRouteConfiguration.DLQ_URI).routeId("bip-confirmed-dead-letter")
                .errorHandler(noErrorHandler())
                .process(exchange -> {
                    PlatformRouteConfiguration.prepareDlq(exchange);
                    // A handler that restored the original message also restored a replay's redo list.
                    exchange.getMessage().removeHeader(PlatformHeaders.REDO_STEPS);
                    if (exchange.getProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT) == null
                            && exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT) == null) {
                        Throwable failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
                        if (failure instanceof Exception original) {
                            // The caller owns recovery; retain the receiver's type and diagnostics.
                            throw original;
                        }
                        throw new IllegalStateException("DLQ parking requires a managed Kafka input; "
                                + "internal request failure must propagate to its caller",
                                failure);
                    }
                    Throwable unrecorded = StepOutcomeFailure.of(exchange);
                    if (unrecorded != null) {
                        // The input is consumed again once step outcomes can be recorded; a parked copy would repeat it.
                        throw new IllegalStateException("Not parked: a step outcome was not checked or recorded, "
                                + "so the Kafka offset is not committed and the input is consumed again", unrecorded);
                    }
                });
        // Resolve the producer when parking is needed, so an application with
        // no Kafka input does not need a broker merely to start this route.
        route.addOutput(new ToDynamicDefinition("{{basis.message-store.dlq-uri:kafka:bip.message.dlq}}")
                .allowOptimisedComponents(false));
        route.process(exchange -> exchange.setProperty(PlatformExchangeProperties.DLQ_PARKED, true));
    }
}
