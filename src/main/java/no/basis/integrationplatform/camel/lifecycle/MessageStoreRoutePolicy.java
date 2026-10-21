package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import no.basis.integrationplatform.camel.traffic.TrafficSignals;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.support.RoutePolicySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class MessageStoreRoutePolicy extends RoutePolicySupport {
    private static final Logger LOG = LoggerFactory.getLogger(MessageStoreRoutePolicy.class);

    private final MessageStoreSink sink;
    private TrafficSignals trafficSignals;
    private final String integrationName;
    private final PayloadCapturePolicy payloadPolicy;

    public MessageStoreRoutePolicy(MessageStoreSink sink, String integrationName) {
        this(sink, integrationName, PayloadCapturePolicy.defaultPolicy());
    }

    public MessageStoreRoutePolicy(MessageStoreSink sink, String integrationName, PayloadCapturePolicy payloadPolicy) {
        this.sink = sink;
        this.integrationName = integrationName;
        this.payloadPolicy = payloadPolicy;
    }

    void setTrafficSignals(TrafficSignals trafficSignals) {
        this.trafficSignals = trafficSignals;
    }

    @Override
    public void onExchangeBegin(Route route, Exchange exchange) {
        ensureMessageId(exchange);
        captureKafkaManualCommit(exchange);
        capturePayloadMetadata(exchange);
        if (trafficSignals != null) trafficSignals.recordAdmitted(exchange);
    }

    @Override
    public void onExchangeDone(Route route, Exchange exchange) {
        MessageAttemptResult result = result(route, exchange);

        try {
            sink.record(result);
        } catch (Exception e) {
            fail(exchange, "Failed to record message outcome; Kafka offset was not committed", e);
            return;
        }
        if (result.status() == MessageStatus.FAILED) {
            fail(exchange, "Processing failed without confirmed DLQ parking; Kafka offset was not committed", null);
            return;
        }
        try {
            commitKafkaOffsetIfPresent(exchange);
        } catch (Exception e) {
            fail(exchange, "Message outcome was recorded but Kafka offset commit failed", e);
        }
    }

    private void fail(Exchange exchange, String message, Exception cause) {
        var failure = new IllegalStateException(message, cause);
        // Camel logs/ignores exceptions thrown by route-policy completion. Keep
        // the exchange failed as well, so the Kafka consumer can seek/retry.
        exchange.setException(failure);
        throw failure;
    }

    protected MessageAttemptResult result(Route route, Exchange exchange) {
        boolean dlqParked = dlqParked(exchange);
        Throwable failure = failure(exchange, dlqParked);
        MessageStatus status = status(dlqParked, failure);

        return new MessageAttemptResult(
                schemaVersion(exchange),
                ensureMessageId(exchange),
                exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID, String.class),
                exchange.getMessage().getHeader(PlatformHeaders.CONTENT_TYPE, String.class),
                exchange.getMessage().getHeader(PlatformHeaders.SOURCE_SYSTEM, String.class),
                exchange.getMessage().getHeader(PlatformHeaders.TARGET_SYSTEM, String.class),
                payloadStorage(exchange),
                payloadSizeBytes(exchange),
                payloadBytes(exchange),
                payloadExpiresAt(exchange),
                payloadDeletedAt(exchange),
                replayable(exchange),
                replayDisabledReason(exchange),
                integrationName,
                route.getRouteId(),
                attempt(exchange),
                autoRedelivered(exchange),
                autoRedeliveryCount(exchange),
                autoRedeliveryMax(exchange),
                Instant.now(),
                status,
                steps(exchange),
                errorMessage(failure));
    }

    private MessageStatus status(boolean dlqParked, Throwable failure) {
        if (dlqParked) {
            return MessageStatus.DLQ;
        }
        return failure == null ? MessageStatus.COMPLETED : MessageStatus.FAILED;
    }

    @SuppressWarnings("unchecked")
    private List<StepResult> steps(Exchange exchange) {
        List<StepResult> steps = exchange.getProperty(PlatformExchangeProperties.STEPS, List.class);
        return steps == null ? List.of() : List.copyOf(steps);
    }

    private void commitKafkaOffsetIfPresent(Exchange exchange) {
        KafkaManualCommit manualCommit = exchange.getProperty(
                PlatformExchangeProperties.KAFKA_MANUAL_COMMIT,
                KafkaManualCommit.class);
        if (manualCommit == null) {
            manualCommit = exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        }
        if (manualCommit == null) {
            return;
        }

        manualCommit.commit();
        LOG.debug("Committed Kafka offset for message {}", exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
    }

    private void captureKafkaManualCommit(Exchange exchange) {
        KafkaManualCommit manualCommit = exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        if (manualCommit != null) {
            exchange.setProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT, manualCommit);
        }
    }

    private void capturePayloadMetadata(Exchange exchange) {
        Object body = exchange.getMessage().getBody();
        byte[] bytes = payloadBytes(body);
        Long sizeBytes = bytes == null ? payloadSizeBytes(body) : Long.valueOf(bytes.length);
        if (sizeBytes != null) {
            exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, sizeBytes);
        }

        if (!payloadPolicy.replayEnabled()) {
            metadataOnly(exchange, "replay disabled by policy");
            return;
        }
        if (payloadPolicy.mode() == PayloadMode.METADATA_ONLY) {
            metadataOnly(exchange, "payload storage mode is metadata-only");
            return;
        }
        if (bytes == null) {
            metadataOnly(exchange, "payload body type is not safely capturable");
            return;
        }
        if (bytes.length > payloadPolicy.maxBytes()) {
            metadataOnly(exchange, "payload exceeds max inline size");
            return;
        }

        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, PayloadMode.POSTGRES_INLINE.storageValue());
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_BYTES, bytes);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT,
                Instant.now().plus(payloadPolicy.retention()));
        exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, true);
    }

    private void metadataOnly(Exchange exchange, String reason) {
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, PayloadMode.METADATA_ONLY.storageValue());
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_BYTES);
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT);
        exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, false);
        exchange.setProperty(PlatformExchangeProperties.REPLAY_DISABLED_REASON, reason);
    }

    private Long payloadSizeBytes(Object body) {
        if (body == null) {
            return 0L;
        }
        if (body instanceof byte[] bytes) {
            return (long) bytes.length;
        }
        if (body instanceof String text) {
            return (long) text.getBytes(StandardCharsets.UTF_8).length;
        }
        return null;
    }

    private byte[] payloadBytes(Object body) {
        if (body == null) {
            return new byte[0];
        }
        if (body instanceof byte[] bytes) {
            return bytes;
        }
        if (body instanceof String text) {
            return text.getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }

    private String ensureMessageId(Exchange exchange) {
        String messageId = exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
        if (messageId != null && !messageId.isBlank()) {
            return messageId;
        }

        messageId = UUID.randomUUID().toString();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, messageId);
        return messageId;
    }

    private int schemaVersion(Exchange exchange) {
        Integer version = exchange.getMessage().getHeader(PlatformHeaders.SCHEMA_VERSION, Integer.class);
        return version == null ? 1 : version;
    }

    private int attempt(Exchange exchange) {
        Integer attempt = exchange.getMessage().getHeader(PlatformHeaders.ATTEMPT, Integer.class);
        return attempt == null || attempt < 1 ? 1 : attempt;
    }

    // Contract §5 retry diagnostics. The redelivery headers survive on the
    // retry-then-success path but are stripped by Camel's error handler before
    // a dead-letter delivery, so the properties captured during redelivery
    // (PlatformRouteConfiguration#captureRedeliveryDiagnostics) take precedence.

    private boolean autoRedelivered(Exchange exchange) {
        if (Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERED, Boolean.class))) {
            return true;
        }
        Boolean redelivered = exchange.getMessage().getHeader(Exchange.REDELIVERED, Boolean.class);
        Integer count = autoRedeliveryCount(exchange);
        return Boolean.TRUE.equals(redelivered) || (count != null && count > 0);
    }

    private Integer autoRedeliveryCount(Exchange exchange) {
        Integer captured = exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERY_COUNT, Integer.class);
        if (captured != null) {
            return captured;
        }
        return exchange.getMessage().getHeader(Exchange.REDELIVERY_COUNTER, Integer.class);
    }

    private Integer autoRedeliveryMax(Exchange exchange) {
        Integer captured = exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERY_MAX, Integer.class);
        if (captured != null) {
            return captured;
        }
        return exchange.getMessage().getHeader(Exchange.REDELIVERY_MAX_COUNTER, Integer.class);
    }

    private String payloadStorage(Exchange exchange) {
        String storage = exchange.getProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, String.class);
        return storage == null || storage.isBlank() ? "metadata_only" : storage;
    }

    private Long payloadSizeBytes(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, Long.class);
    }

    private byte[] payloadBytes(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_BYTES, byte[].class);
    }

    private Instant payloadExpiresAt(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT, Instant.class);
    }

    private Instant payloadDeletedAt(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_DELETED_AT, Instant.class);
    }

    private boolean replayable(Exchange exchange) {
        return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.REPLAYABLE, Boolean.class));
    }

    private String replayDisabledReason(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.REPLAY_DISABLED_REASON, String.class);
    }

    private Throwable failure(Exchange exchange, boolean includeCaughtException) {
        if (exchange.getException() != null) {
            return exchange.getException();
        }
        if (includeCaughtException || exchange.isFailed()) {
            return exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        }
        return null;
    }

    private boolean dlqParked(Exchange exchange) {
        return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.DLQ_PARKED, Boolean.class));
    }

    private String errorMessage(Throwable failure) {
        if (failure == null) {
            return null;
        }
        return failure.getMessage() == null ? failure.getClass().getName() : failure.getMessage();
    }
}
