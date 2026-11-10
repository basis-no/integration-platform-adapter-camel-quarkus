package no.basis.integrationplatform.camel;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.step.StepOutcomeFailure;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteConfigurationBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Set;
import java.util.Locale;
import java.util.List;

/** Shared retry, DLQ, and source-propagation route configurations. */
@ApplicationScoped
public class PlatformRouteConfiguration extends RouteConfigurationBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(PlatformRouteConfiguration.class);
    static final String MASKED = "xxxxxx";
    private static final Set<String> CREDENTIAL_HEADERS = Set.of(
            "authorization", "proxy-authorization", "cookie", "set-cookie", "www-authenticate", "proxy-authenticate");
    private static final List<String> CREDENTIAL_PARTS = List.of(
            "secret", "token", "password", "passwd", "credential", "session", "signature",
            "apikey", "api-key", "api_key", "subscription-key", "private-key", "auth");
    /** Named profile for slow external backends. */
    public static final String SLOW_BACKEND = "ip-slow-backend";
    // The slow-backend waits run on the Kafka consumer thread: 5, 10, 20, 20, 20 and 20 seconds,
    // 95 seconds in all, leave most of the 300-second max.poll.interval.ms to the attempts themselves.
    // Camel's default 60-second cap gave 195 seconds of waiting.
    static final int SLOW_BACKEND_REDELIVERIES = 6;
    static final long SLOW_BACKEND_FIRST_DELAY_MS = 5_000;
    static final long SLOW_BACKEND_MAX_DELAY_MS = 20_000;
    /** Named profile disabling automatic redelivery. */
    public static final String NO_RETRY = "ip-no-retry";
    /** Named source-to-Kafka profile that propagates exhausted failure. */
    public static final String INGRESS_NORMALIZATION = "bip-ingress-normalization";
    /** The dead-letter channel's endpoint: the adapter's route that confirms parking. */
    static final String DLQ_URI = "direct:" + PlatformDeadLetterRoute.ROUTE_ID;
    /** The configured parking endpoint that the dead-letter route publishes to. */
    static final String DLQ_ENDPOINT = "{{basis.message-store.dlq-uri:kafka:bip.message.dlq}}";
    private final MessageFailureMode failureMode;

    /** Creates the default Kafka DLQ configuration. */
    public PlatformRouteConfiguration() {
        this(MessageFailureMode.DLQ);
    }

    /**
     * Creates route configurations for the selected terminal failure mode.
     *
     * @param failureMode configured adapter failure mode
     */
    @Inject
    public PlatformRouteConfiguration(
            @ConfigProperty(name = "basis.message-store.failure-mode", defaultValue = "dlq") String failureMode) {
        this(MessageFailureMode.fromConfig(failureMode));
    }

    PlatformRouteConfiguration(MessageFailureMode failureMode) {
        this.failureMode = failureMode;
    }

    @Override
    public void configuration() {
        HttpClientRetries.disableByDefault(getContext());
        if (failureMode == MessageFailureMode.RECORD_FAILED) {
            recordFailedConfigurations();
            return;
        }

        routeConfiguration()
                .errorHandler(deadLetterChannel(DLQ_URI)
                        .deadLetterHandleNewException(false)
                        .onExceptionOccurred(new HttpRetryAfter(HttpRetryAfter.policy(3, 1000, null)))
                        .onRedelivery(this::timedRedelivery)
                        .maximumRedeliveries(3)
                        .redeliveryDelay(1000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2));

        routeConfiguration(SLOW_BACKEND)
                .errorHandler(deadLetterChannel(DLQ_URI)
                        .deadLetterHandleNewException(false)
                        .onExceptionOccurred(new HttpRetryAfter(HttpRetryAfter.policy(
                                SLOW_BACKEND_REDELIVERIES, SLOW_BACKEND_FIRST_DELAY_MS, SLOW_BACKEND_MAX_DELAY_MS)))
                        .onRedelivery(this::timedRedelivery)
                        .maximumRedeliveries(SLOW_BACKEND_REDELIVERIES)
                        .redeliveryDelay(SLOW_BACKEND_FIRST_DELAY_MS)
                        .useExponentialBackOff()
                        .backOffMultiplier(2)
                        .maximumRedeliveryDelay(SLOW_BACKEND_MAX_DELAY_MS));

        routeConfiguration(NO_RETRY)
                .errorHandler(deadLetterChannel(DLQ_URI)
                        .deadLetterHandleNewException(false)
                        .onRedelivery(this::captureRedeliveryDiagnostics)
                        .maximumRedeliveries(0));

        ingressNormalizationConfiguration();
    }

    private void recordFailedConfigurations() {
        routeConfiguration()
                .errorHandler(defaultErrorHandler()
                        .onRedelivery(this::captureRedeliveryDiagnostics)
                        .maximumRedeliveries(3)
                        .redeliveryDelay(1000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2));

        routeConfiguration(SLOW_BACKEND)
                .errorHandler(defaultErrorHandler()
                        .onRedelivery(this::captureRedeliveryDiagnostics)
                        .maximumRedeliveries(SLOW_BACKEND_REDELIVERIES)
                        .redeliveryDelay(SLOW_BACKEND_FIRST_DELAY_MS)
                        .useExponentialBackOff()
                        .backOffMultiplier(2)
                        .maximumRedeliveryDelay(SLOW_BACKEND_MAX_DELAY_MS));

        routeConfiguration(NO_RETRY)
                .errorHandler(defaultErrorHandler()
                        .onRedelivery(this::captureRedeliveryDiagnostics)
                        .maximumRedeliveries(0));

        ingressNormalizationConfiguration();
    }

    private void ingressNormalizationConfiguration() {
        // A source-to-Kafka normalization failure must propagate back to the
        // source consumer so that files are not moved and queue messages are
        // not acknowledged before the durable Kafka boundary is reached.
        routeConfiguration(INGRESS_NORMALIZATION)
                .errorHandler(defaultErrorHandler()
                        .onRedelivery(this::captureRedeliveryDiagnostics)
                        .maximumRedeliveries(3)
                        .redeliveryDelay(1000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2));
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

    /** Runs before a redelivery whose delay the adapter may have timed by {@code Retry-After}. */
    void timedRedelivery(Exchange exchange) {
        HttpRetryAfter.afterTiming(exchange);
        captureRedeliveryDiagnostics(exchange);
    }

    /**
     * Prepares an exhausted exchange for parking. The message counts as parked only once
     * {@link PlatformDeadLetterRoute} has published it; until then, and when the publish fails,
     * it is not. The parking route runs it on the exchange the dead letter channel sends it,
     * not onPrepareFailure: Camel runs that also before an application's own onException
     * handler, which must see and send the message as the route left it.
     */
    static void prepareDlq(Exchange exchange) {
        exchange.setProperty(PlatformExchangeProperties.DLQ_PARKED, false);

        // The parked record is the consumed input. A route may have set another id for a
        // send to another integration and failed before restoring it; park and journal the
        // failure under the input's id, not the downstream one.
        String inputMessageId = exchange.getProperty(PlatformExchangeProperties.INPUT_MESSAGE_ID, String.class);
        if (inputMessageId != null && !inputMessageId.isBlank()) {
            exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, inputMessageId);
        }

        Throwable failure = exchange.getException();
        if (failure == null) {
            failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        }
        if (failure != null && StepOutcomeFailure.of(exchange) != null) {
            LOG.error("Message {} exhausted retries and is not parked: a step outcome was not checked or recorded",
                    exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), failure);
        } else if (failure != null) {
            LOG.error("Message {} exhausted retries and is being parked in the DLQ",
                    exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), failure);
        }

        // The DLQ record must carry the ORIGINAL consumed payload, not a body the route
        // transformed or changed in place. Restore it from the capture made at route start:
        // the claim check of an object payload, the replay copy, or the parking copy when
        // replay storage does not keep the payload. A body type that cannot be captured is
        // parked as the route left it. The parking endpoint gets its own array, so the
        // capture stays the input for replay storage.
        byte[] claimCheck = exchange.getProperty(PlatformExchangeProperties.PAYLOAD_CLAIM_CHECK, byte[].class);
        Object originalPayload = exchange.getProperty(PlatformExchangeProperties.PAYLOAD_BYTES, byte[].class);
        if (originalPayload == null) {
            originalPayload = exchange.getProperty(PlatformExchangeProperties.PARKING_PAYLOAD);
        }
        if (claimCheck != null) {
            exchange.getMessage().setBody(claimCheck.clone());
            exchange.getMessage().setHeader(PlatformHeaders.TRANSPORT_CONTENT_TYPE,
                    no.basis.integrationplatform.camel.payload.PayloadReference.CONTENT_TYPE);
        } else if (originalPayload instanceof byte[] bytes) {
            exchange.getMessage().setBody(bytes.clone());
        } else if (originalPayload != null) {
            exchange.getMessage().setBody(originalPayload);
        }

        maskCredentialHeaders(exchange);
        // A handler that restored the original message also restored a replay's redo list.
        exchange.getMessage().removeHeader(PlatformHeaders.REDO_STEPS);
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
        return CREDENTIAL_HEADERS.contains(lower) || CREDENTIAL_PARTS.stream().anyMatch(lower::contains)
                || lower.endsWith("-key") || lower.endsWith("_key");
    }

    private static String failureMessage(Exchange exchange) {
        Throwable failure = exchange.getException();
        if (failure == null) {
            failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        }
        if (failure == null) {
            return "unknown failure";
        }
        return failure.getMessage() == null ? failure.getClass().getName() : failure.getMessage();
    }
}
