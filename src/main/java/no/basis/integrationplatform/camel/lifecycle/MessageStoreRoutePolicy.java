package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
import no.basis.integrationplatform.camel.payload.PayloadReference;
import no.basis.integrationplatform.camel.payload.PayloadStorageException;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.support.RoutePolicySupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/** Records terminal lifecycle outcomes and owns the Kafka acknowledgement boundary. */
public class MessageStoreRoutePolicy extends RoutePolicySupport {
    private static final Logger LOG = LoggerFactory.getLogger(MessageStoreRoutePolicy.class);

    private final MessageStoreSink sink;
    private final String integrationName;
    private final PayloadCapturePolicy payloadPolicy;
    private final MessageBoundaryKind boundaryKind;
    private final boolean kafkaCommitEnabled;
    private final PayloadObjectStore objectStore;

    /**
     * Creates a Kafka policy using the default payload policy.
     *
     * @param sink terminal outcome sink
     * @param integrationName stable integration name
     */
    public MessageStoreRoutePolicy(MessageStoreSink sink, String integrationName) {
        this(sink, integrationName, PayloadCapturePolicy.defaultPolicy(), MessageBoundaryKind.KAFKA, true,
                PayloadObjectStore.disabled());
    }

    /**
     * Creates a Kafka policy using an explicit payload policy.
     *
     * @param sink terminal outcome sink
     * @param integrationName stable integration name
     * @param payloadPolicy payload capture and replay policy
     */
    public MessageStoreRoutePolicy(MessageStoreSink sink, String integrationName, PayloadCapturePolicy payloadPolicy) {
        this(sink, integrationName, payloadPolicy, MessageBoundaryKind.KAFKA, true, PayloadObjectStore.disabled());
    }

    MessageStoreRoutePolicy(
            MessageStoreSink sink,
            String integrationName,
            PayloadCapturePolicy payloadPolicy,
            MessageBoundaryKind boundaryKind) {
        this(sink, integrationName, payloadPolicy, boundaryKind, true, PayloadObjectStore.disabled());
    }

    MessageStoreRoutePolicy(
            MessageStoreSink sink,
            String integrationName,
            PayloadCapturePolicy payloadPolicy,
            MessageBoundaryKind boundaryKind,
            boolean kafkaCommitEnabled) {
        this(sink, integrationName, payloadPolicy, boundaryKind, kafkaCommitEnabled, PayloadObjectStore.disabled());
    }

    MessageStoreRoutePolicy(
            MessageStoreSink sink,
            String integrationName,
            PayloadCapturePolicy payloadPolicy,
            MessageBoundaryKind boundaryKind,
            boolean kafkaCommitEnabled,
            PayloadObjectStore objectStore) {
        this.sink = sink;
        this.integrationName = integrationName;
        this.payloadPolicy = payloadPolicy;
        this.boundaryKind = boundaryKind;
        this.kafkaCommitEnabled = kafkaCommitEnabled;
        this.objectStore = objectStore;
    }

    @Override
    public void onExchangeBegin(Route route, Exchange exchange) {
        ensureMessageId(exchange);
        if (boundaryKind == MessageBoundaryKind.KAFKA) {
            captureKafkaManualCommit(exchange);
        }
        capturePayloadMetadata(exchange);
    }

    @Override
    public void onExchangeDone(Route route, Exchange exchange) {
        if (recordingSuppressed(exchange)) {
            commitKafkaOffsetIfPresent(exchange);
            LOG.debug("Skipped message-store recording for an ineligible exchange on route {}", route.getRouteId());
            return;
        }

        MessageAttemptResult result = result(route, exchange);

        try {
            sink.record(result);
            commitKafkaOffsetIfPresent(exchange);
        } catch (Exception e) {
            String consequence = boundaryKind == MessageBoundaryKind.KAFKA
                    ? "Kafka offset was not committed"
                    : "source response was not completed";
            IllegalStateException failure = new IllegalStateException(
                    "Failed to record message outcome; " + consequence, e);

            // Camel logs and otherwise ignores exceptions thrown by
            // RoutePolicy#onExchangeDone. Mark the exchange failed as well so
            // source consumers (notably platform-http) cannot return success.
            exchange.setException(failure);
            if (boundaryKind == MessageBoundaryKind.HTTP) {
                exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
                exchange.getMessage().setHeader(Exchange.CONTENT_TYPE, "text/plain");
                exchange.getMessage().setBody("Message outcome could not be recorded");
            }
            throw failure;
        }
    }

    private MessageAttemptResult result(Route route, Exchange exchange) {
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
                payloadRef(exchange),
                payloadSha256(exchange),
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
        if (boundaryKind != MessageBoundaryKind.KAFKA || !kafkaCommitEnabled) {
            return;
        }
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
        if (isPayloadReference(exchange)) {
            resolvePayloadReference(exchange, body);
            return;
        }

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
        if (bytes == null && body instanceof InputStream input && payloadPolicy.mode() == PayloadMode.TIERED) {
            captureStream(exchange, input);
            return;
        }
        if (bytes == null) {
            metadataOnly(exchange, "payload body type is not safely capturable");
            return;
        }
        if (bytes.length > payloadPolicy.inlineThresholdBytes() && payloadPolicy.mode() == PayloadMode.TIERED) {
            captureObject(exchange, new ByteArrayInputStream(bytes), (long) bytes.length);
            return;
        }
        if (bytes.length > payloadPolicy.inlineThresholdBytes()) {
            metadataOnly(exchange, "payload exceeds max inline size");
            return;
        }

        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, PayloadMode.POSTGRES_INLINE.storageValue());
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_BYTES, bytes);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SHA256, sha256(bytes));
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT,
                Instant.now().plus(payloadPolicy.retention()));
        exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, true);
    }

    private void metadataOnly(Exchange exchange, String reason) {
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, PayloadMode.METADATA_ONLY.storageValue());
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_BYTES);
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_REF);
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_SHA256);
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT);
        exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, false);
        exchange.setProperty(PlatformExchangeProperties.REPLAY_DISABLED_REASON, reason);
    }

    private void captureStream(Exchange exchange, InputStream input) {
        try {
            int probeSize = Math.toIntExact(payloadPolicy.inlineThresholdBytes() + 1);
            byte[] probe = input.readNBytes(probeSize);
            if (probe.length <= payloadPolicy.inlineThresholdBytes()) {
                exchange.getMessage().setBody(probe);
                exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, (long) probe.length);
                exchange.setProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, PayloadMode.POSTGRES_INLINE.storageValue());
                exchange.setProperty(PlatformExchangeProperties.PAYLOAD_BYTES, probe);
                exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SHA256, sha256(probe));
                exchange.setProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT,
                        Instant.now().plus(payloadPolicy.retention()));
                exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, true);
                return;
            }
            InputStream completePayload = new SequenceInputStream(new ByteArrayInputStream(probe), input);
            PayloadReference reference = captureObject(exchange, completePayload, null);
            exchange.getMessage().setBody(objectStore.open(reference));
        } catch (PayloadStorageException e) {
            throw e;
        } catch (Exception e) {
            throw new PayloadStorageException("Failed to capture streaming payload", e);
        }
    }

    private PayloadReference captureObject(Exchange exchange, InputStream input, Long declaredSize) {
        Instant expiresAt = Instant.now().plus(payloadPolicy.retention());
        PayloadReference reference = objectStore.store(
                integrationName,
                ensureMessageId(exchange),
                input,
                declaredSize,
                exchange.getMessage().getHeader(PlatformHeaders.CONTENT_TYPE, String.class),
                expiresAt,
                payloadPolicy.objectMaxBytes());
        applyPayloadReference(exchange, reference);
        return reference;
    }

    private void resolvePayloadReference(Exchange exchange, Object body) {
        PayloadReference reference = PayloadReference.fromJson(body);
        validateReferenceHeaders(exchange, reference);
        if (reference.expiryInstant().isBefore(Instant.now())) {
            throw new PayloadStorageException("Payload reference has expired: " + reference.ref());
        }
        objectStore.verify(reference);
        applyPayloadReference(exchange, reference);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_CLAIM_CHECK, reference.toJsonBytes());
        exchange.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, reference.contentType());
        exchange.getMessage().setBody(objectStore.open(reference));
    }

    private void applyPayloadReference(Exchange exchange, PayloadReference reference) {
        byte[] claimCheck = reference.toJsonBytes();
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_STORAGE, PayloadReference.STORAGE);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SIZE_BYTES, reference.sizeBytes());
        exchange.removeProperty(PlatformExchangeProperties.PAYLOAD_BYTES);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_REF, reference.ref());
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_SHA256, reference.sha256());
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_CLAIM_CHECK, claimCheck);
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_EXPIRES_AT, reference.expiryInstant());
        exchange.setProperty(PlatformExchangeProperties.REPLAYABLE, true);
        exchange.removeProperty(PlatformExchangeProperties.REPLAY_DISABLED_REASON);
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_STORAGE, PayloadReference.STORAGE);
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_REF, reference.ref());
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_SIZE, Long.toString(reference.sizeBytes()));
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_SHA256, reference.sha256());
    }

    private boolean isPayloadReference(Exchange exchange) {
        String storage = exchange.getMessage().getHeader(PlatformHeaders.PAYLOAD_STORAGE, String.class);
        String contentType = exchange.getMessage().getHeader(PlatformHeaders.TRANSPORT_CONTENT_TYPE, String.class);
        return PayloadReference.STORAGE.equals(storage) || PayloadReference.CONTENT_TYPE.equals(contentType);
    }

    private void validateReferenceHeaders(Exchange exchange, PayloadReference reference) {
        matchesHeader(exchange, PlatformHeaders.PAYLOAD_STORAGE, reference.storage());
        matchesHeader(exchange, PlatformHeaders.PAYLOAD_REF, reference.ref());
        matchesHeader(exchange, PlatformHeaders.PAYLOAD_SIZE, Long.toString(reference.sizeBytes()));
        matchesHeader(exchange, PlatformHeaders.PAYLOAD_SHA256, reference.sha256());
    }

    private void matchesHeader(Exchange exchange, String name, String expected) {
        String actual = exchange.getMessage().getHeader(name, String.class);
        if (actual != null && !actual.equals(expected)) {
            throw new PayloadStorageException("Payload-reference header " + name + " does not match its document");
        }
    }

    private String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception e) {
            throw new PayloadStorageException("Failed to calculate payload checksum", e);
        }
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

    private String payloadRef(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_REF, String.class);
    }

    private String payloadSha256(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.PAYLOAD_SHA256, String.class);
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
        Throwable caught = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        if (includeCaughtException || exchange.isFailed() || boundaryKind != MessageBoundaryKind.KAFKA) {
            if (caught != null) {
                return caught;
            }
        }
        if (boundaryKind == MessageBoundaryKind.HTTP) {
            Integer responseCode = exchange.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE, Integer.class);
            if (responseCode != null && responseCode >= 400) {
                return new IllegalStateException("HTTP response status " + responseCode);
            }
        }
        return null;
    }

    private boolean dlqParked(Exchange exchange) {
        return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.DLQ_PARKED, Boolean.class));
    }

    private boolean recordingSuppressed(Exchange exchange) {
        Boolean recordMessage = exchange.getProperty(PlatformExchangeProperties.RECORD_MESSAGE, Boolean.class);
        return Boolean.FALSE.equals(recordMessage);
    }

    private String errorMessage(Throwable failure) {
        if (failure == null) {
            return null;
        }
        return failure.getMessage() == null ? failure.getClass().getName() : failure.getMessage();
    }
}
