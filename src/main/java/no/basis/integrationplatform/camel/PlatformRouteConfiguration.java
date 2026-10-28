package no.basis.integrationplatform.camel;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

@ApplicationScoped
public class PlatformRouteConfiguration extends RouteConfigurationBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(PlatformRouteConfiguration.class);
    public static final String SLOW_BACKEND = "ip-slow-backend";
    public static final String NO_RETRY = "ip-no-retry";
    static final String DLQ_URI = "direct:bip-confirmed-dead-letter";

    @Override
    public void configuration() {
        routeConfiguration()
                .errorHandler(deadLetterChannel(DLQ_URI)
                        .deadLetterHandleNewException(false)
                        .onRedelivery(this::captureRedeliveryDiagnostics)
                        .onPrepareFailure(this::prepareDlq)
                        .maximumRedeliveries(3)
                        .redeliveryDelay(1000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2));

        routeConfiguration(SLOW_BACKEND)
                .errorHandler(deadLetterChannel(DLQ_URI)
                        .deadLetterHandleNewException(false)
                        .onRedelivery(this::captureRedeliveryDiagnostics)
                        .onPrepareFailure(this::prepareDlq)
                        .maximumRedeliveries(6)
                        .redeliveryDelay(5000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2));

        routeConfiguration(NO_RETRY)
                .errorHandler(deadLetterChannel(DLQ_URI)
                        .deadLetterHandleNewException(false)
                        .onRedelivery(this::captureRedeliveryDiagnostics)
                        .onPrepareFailure(this::prepareDlq)
                        .maximumRedeliveries(0));
    }

    /**
     * Contract §5: in-process retry diagnostics are reported via
     * auto_redelivered / auto_redelivery_count / auto_redelivery_max. Camel's
     * RedeliveryErrorHandler removes the redelivery headers from the message
     * before delivering an exhausted exchange to the dead letter channel, so
     * the values must be captured while a redelivery attempt is in flight —
     * onRedelivery runs before each attempt with the headers present. On the
     * retry-then-success path the headers survive to the end of the exchange;
     * these properties make the DLQ path report the same truth.
     */
    void captureRedeliveryDiagnostics(Exchange exchange) {
        Integer counter = exchange.getMessage().getHeader(Exchange.REDELIVERY_COUNTER, Integer.class);
        if (counter != null) {
            exchange.setProperty(PlatformExchangeProperties.AUTO_REDELIVERED, true);
            exchange.setProperty(PlatformExchangeProperties.AUTO_REDELIVERY_COUNT, counter);
        }
        Integer max = exchange.getMessage().getHeader(Exchange.REDELIVERY_MAX_COUNTER, Integer.class);
        if (max != null) {
            exchange.setProperty(PlatformExchangeProperties.AUTO_REDELIVERY_MAX, max);
        }
    }

    void prepareDlq(Exchange exchange) {
        exchange.setProperty(PlatformExchangeProperties.DLQ_PARKED, false);

        // The DLQ header keeps only the message; operators need the stack trace to diagnose it.
        Throwable failure = failure(exchange);
        if (failure != null) {
            LOG.error("Message {} failed after its configured retries and is being sent to the dead letter queue",
                    exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), failure);
        }

        // Contract §8: the DLQ record must carry the ORIGINAL consumed payload,
        // not a mid-transformation body. Restore it from the capture made at
        // route start when the payload was capturable; metadata_only /
        // uncapturable bodies are parked as-is.
        byte[] originalPayload = exchange.getProperty(PlatformExchangeProperties.PAYLOAD_BYTES, byte[].class);
        if (originalPayload != null) {
            exchange.getMessage().setBody(originalPayload);
        }

        exchange.getMessage().setHeader(PlatformHeaders.ERROR, failureMessage(exchange));
        exchange.getMessage().setHeader(PlatformHeaders.DLQ_AT, Instant.now().toString());
    }

    private String failureMessage(Exchange exchange) {
        Throwable failure = failure(exchange);
        if (failure == null) {
            return "unknown failure";
        }
        return failure.getMessage() == null ? failure.getClass().getName() : failure.getMessage();
    }

    private Throwable failure(Exchange exchange) {
        Throwable failure = exchange.getException();
        return failure != null ? failure : exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
    }
}
