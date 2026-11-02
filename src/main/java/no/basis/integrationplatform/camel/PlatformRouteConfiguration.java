package no.basis.integrationplatform.camel;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@ApplicationScoped
public class PlatformRouteConfiguration extends RouteConfigurationBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(PlatformRouteConfiguration.class);
    public static final String SLOW_BACKEND = "ip-slow-backend";
    public static final String NO_RETRY = "ip-no-retry";
    static final String DLQ_URI = "direct:bip-confirmed-dead-letter";
    static final String MASKED = "xxxxxx";
    private static final Set<String> CREDENTIAL_HEADERS = Set.of(
            "authorization", "proxy-authorization", "cookie", "set-cookie", "www-authenticate", "proxy-authenticate");
    private static final List<String> CREDENTIAL_PARTS = List.of(
            "secret", "token", "password", "passwd", "credential", "session", "signature",
            "apikey", "api-key", "api_key", "subscription-key", "private-key", "auth");

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
     * In-process retry diagnostics are reported via
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

        // The parked record is the consumed input. A route may have set another id for a
        // send to another integration and failed before restoring it; park and journal the
        // failure under the input's id, not the downstream one.
        String inputMessageId = exchange.getProperty(PlatformExchangeProperties.INPUT_MESSAGE_ID, String.class);
        if (inputMessageId != null && !inputMessageId.isBlank()) {
            exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, inputMessageId);
        }

        // The DLQ header keeps only the message; operators need the stack trace to diagnose it.
        Throwable failure = failure(exchange);
        if (failure != null) {
            LOG.error("Message {} failed after its configured retries and is being sent to the dead letter queue",
                    exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), failure);
        }

        // The DLQ record must carry the ORIGINAL consumed payload, not a body the route
        // transformed or changed in place. Restore it from the capture made at route start:
        // the replay copy, or the parking copy when replay storage does not keep the payload.
        // A body type that cannot be captured is parked as the route left it.
        Object originalPayload = exchange.getProperty(PlatformExchangeProperties.PAYLOAD_BYTES, byte[].class);
        if (originalPayload == null) {
            originalPayload = exchange.getProperty(PlatformExchangeProperties.PARKING_PAYLOAD);
        }
        if (originalPayload instanceof byte[] bytes) {
            // The parking endpoint gets its own array, so the capture stays the input for replay storage.
            exchange.getMessage().setBody(bytes.clone());
        } else if (originalPayload != null) {
            exchange.getMessage().setBody(originalPayload);
        }

        maskCredentialHeaders(exchange);
        exchange.getMessage().setHeader(PlatformHeaders.ERROR, failureMessage(exchange));
        exchange.getMessage().setHeader(PlatformHeaders.DLQ_AT, Instant.now().toString());
    }

    /**
     * The Kafka DLQ producer forwards every message header. Request headers a route set
     * for a receiver, and response headers copied back from a successful call, can carry
     * credentials and session cookies. Keep the names for diagnosis, mask the values.
     */
    static void maskCredentialHeaders(Exchange exchange) {
        for (String name : List.copyOf(exchange.getMessage().getHeaders().keySet())) {
            if (isCredentialHeader(name)) {
                exchange.getMessage().setHeader(name, MASKED);
            }
        }
    }

    static boolean isCredentialHeader(String name) {
        String lower = name.toLowerCase(Locale.ROOT);
        if (lower.startsWith("x-bip-") || lower.equals("idempotency-key")) {
            return false;
        }
        // Names ending in -key or _key (x-functions-key, x-access-key, x-client-key) carry keys too.
        return CREDENTIAL_HEADERS.contains(lower) || CREDENTIAL_PARTS.stream().anyMatch(lower::contains)
                || lower.endsWith("-key") || lower.endsWith("_key");
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
