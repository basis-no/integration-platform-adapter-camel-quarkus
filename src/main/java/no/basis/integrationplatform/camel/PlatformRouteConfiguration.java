package no.basis.integrationplatform.camel;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteConfigurationBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/** Shared retry, DLQ, and source-propagation route configurations. */
@ApplicationScoped
public class PlatformRouteConfiguration extends RouteConfigurationBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(PlatformRouteConfiguration.class);
    /** Named profile for slow external backends. */
    public static final String SLOW_BACKEND = "ip-slow-backend";
    /** Named profile disabling automatic redelivery. */
    public static final String NO_RETRY = "ip-no-retry";
    /** Named source-to-Kafka profile that propagates exhausted failure. */
    public static final String INGRESS_NORMALIZATION = "bip-ingress-normalization";
    private static final String DLQ_URI = "{{basis.message-store.dlq-uri:kafka:bip.message.dlq}}";
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
        if (failureMode == MessageFailureMode.RECORD_FAILED) {
            recordFailedConfigurations();
            return;
        }

        routeConfiguration()
                .errorHandler(deadLetterChannel(DLQ_URI)
                        .onRedelivery(this::captureRedeliveryDiagnostics)
                        .onPrepareFailure(this::markDlqParked)
                        .maximumRedeliveries(3)
                        .redeliveryDelay(1000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2));

        routeConfiguration(SLOW_BACKEND)
                .errorHandler(deadLetterChannel(DLQ_URI)
                        .onRedelivery(this::captureRedeliveryDiagnostics)
                        .onPrepareFailure(this::markDlqParked)
                        .maximumRedeliveries(6)
                        .redeliveryDelay(5000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2));

        routeConfiguration(NO_RETRY)
                .errorHandler(deadLetterChannel(DLQ_URI)
                        .onRedelivery(this::captureRedeliveryDiagnostics)
                        .onPrepareFailure(this::markDlqParked)
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
                        .maximumRedeliveries(6)
                        .redeliveryDelay(5000)
                        .useExponentialBackOff()
                        .backOffMultiplier(2));

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

    void markDlqParked(Exchange exchange) {
        exchange.setProperty(PlatformExchangeProperties.DLQ_PARKED, true);

        Throwable failure = exchange.getException();
        if (failure == null) {
            failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        }
        if (failure != null) {
            LOG.error("Message {} exhausted retries and is being parked in the DLQ",
                    exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), failure);
        }

        // Contract §8: the DLQ record must carry the ORIGINAL consumed payload,
        // not a mid-transformation body. Restore it from the capture made at
        // route start when the payload was capturable; metadata_only /
        // uncapturable bodies are parked as-is.
        byte[] claimCheck = exchange.getProperty(PlatformExchangeProperties.PAYLOAD_CLAIM_CHECK, byte[].class);
        byte[] originalPayload = exchange.getProperty(PlatformExchangeProperties.PAYLOAD_BYTES, byte[].class);
        if (claimCheck != null) {
            exchange.getMessage().setBody(claimCheck);
            exchange.getMessage().setHeader(PlatformHeaders.TRANSPORT_CONTENT_TYPE,
                    no.basis.integrationplatform.camel.payload.PayloadReference.CONTENT_TYPE);
        } else if (originalPayload != null) {
            exchange.getMessage().setBody(originalPayload);
        }

        exchange.getMessage().setHeader(PlatformHeaders.ERROR, failureMessage(exchange));
        exchange.getMessage().setHeader(PlatformHeaders.DLQ_AT, Instant.now().toString());
    }

    private String failureMessage(Exchange exchange) {
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
