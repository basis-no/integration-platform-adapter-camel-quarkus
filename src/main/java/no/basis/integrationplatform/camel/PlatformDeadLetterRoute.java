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
    private static final String CAUGHT_BEFORE_MASKING = "basis.messageStore.caughtBeforeMasking";

    @Override
    public void configure() {
        var route = from(PlatformRouteConfiguration.DLQ_URI).routeId("bip-confirmed-dead-letter")
                .errorHandler(noErrorHandler())
                .process(exchange -> {
                    try {
                        PlatformRouteConfiguration.prepareDlq(exchange);
                    } catch (RuntimeException e) {
                        maskCaughtUntilParked(exchange);
                        throw e;
                    }
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
                    // From here a failure leaves the input unparked, and Camel's fallback error
                    // handler logs the failure the route caught.
                    maskCaughtUntilParked(exchange);
                    Throwable unrecorded = StepOutcomeFailure.of(exchange);
                    if (unrecorded != null) {
                        // The input is consumed again once step outcomes can be recorded; a parked copy would repeat it.
                        throw new IllegalStateException("Not parked: a step outcome was not checked or recorded, "
                                + "so the Kafka offset is not committed and the input is consumed again",
                                ErrorTextMasking.forLog(unrecorded));
                    }
                });
        // Resolve the producer when parking is needed, so an application with
        // no Kafka input does not need a broker merely to start this route.
        route.addOutput(new ToDynamicDefinition("{{basis.message-store.dlq-uri:kafka:bip.message.dlq}}")
                .allowOptimisedComponents(false));
        route.process(exchange -> {
            restoreCaught(exchange);
            exchange.setProperty(PlatformExchangeProperties.DLQ_PARKED, true);
        });
    }

    /**
     * When parking fails, Camel's fallback error handler logs the failure the route caught,
     * whose text may name a receiver's URI with credentials: it gets the masked copy until the
     * message is parked, after which the original is put back.
     */
    private static void maskCaughtUntilParked(Exchange exchange) {
        Throwable caught = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        Throwable masked = ErrorTextMasking.forLog(caught);
        if (masked != caught) {
            exchange.setProperty(CAUGHT_BEFORE_MASKING, caught);
            exchange.setProperty(Exchange.EXCEPTION_CAUGHT, masked);
        }
    }

    private static void restoreCaught(Exchange exchange) {
        Throwable original = exchange.getProperty(CAUGHT_BEFORE_MASKING, Throwable.class);
        if (original != null) {
            exchange.removeProperty(CAUGHT_BEFORE_MASKING);
            exchange.setProperty(Exchange.EXCEPTION_CAUGHT, original);
        }
    }
}
