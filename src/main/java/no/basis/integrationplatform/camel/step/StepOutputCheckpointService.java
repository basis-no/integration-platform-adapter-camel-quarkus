package no.basis.integrationplatform.camel.step;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.lifecycle.PayloadCapturePolicy;
import no.basis.integrationplatform.camel.lifecycle.PayloadMode;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
import no.basis.integrationplatform.camel.payload.PayloadReference;
import org.apache.camel.Exchange;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Captures and restores durable outputs for safely skipped replay steps. */
@ApplicationScoped
public class StepOutputCheckpointService {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final PayloadObjectStore objectStore;
    private final PayloadCapturePolicy policy;
    private final String integrationName;
    private final Set<String> headerNames;
    private final Set<String> propertyNames;

    /**
     * Creates the checkpoint service from adapter payload-storage configuration.
     *
     * @param objectStore configured object store
     * @param integrationName integration identity used in object keys
     * @param payloadMode payload storage mode
     * @param inlineThresholdBytes optional maximum inline checkpoint size
     * @param legacyPayloadMaxBytes deprecated inline-threshold alias
     * @param objectMaxBytes maximum object-backed checkpoint size
     * @param retentionDays immutable checkpoint retention
     * @param headerNames comma-separated response headers to preserve
     * @param propertyNames comma-separated exchange properties to preserve
     */
    @Inject
    public StepOutputCheckpointService(
            PayloadObjectStore objectStore,
            @ConfigProperty(name = "quarkus.application.name", defaultValue = "unknown-integration")
                    String integrationName,
            @ConfigProperty(name = "basis.message-store.payload.mode", defaultValue = "postgres-inline")
                    String payloadMode,
            @ConfigProperty(name = "basis.message-store.payload.inline-threshold-bytes")
                    Optional<Long> inlineThresholdBytes,
            @ConfigProperty(name = "basis.message-store.payload.max-bytes")
                    Optional<Long> legacyPayloadMaxBytes,
            @ConfigProperty(name = "basis.message-store.payload.object-max-bytes",
                    defaultValue = "" + PayloadCapturePolicy.DEFAULT_OBJECT_MAX_BYTES)
                    long objectMaxBytes,
            @ConfigProperty(name = "basis.message-store.payload.retention-days", defaultValue = "30")
                    long retentionDays,
            @ConfigProperty(name = "basis.message-store.step.output-checkpoint.header-names")
                    Optional<String> headerNames,
            @ConfigProperty(name = "basis.message-store.step.output-checkpoint.property-names")
                    Optional<String> propertyNames) {
        this(
                objectStore,
                PayloadCapturePolicy.fromConfig(
                        payloadMode,
                        resolveInlineThreshold(inlineThresholdBytes, legacyPayloadMaxBytes),
                        objectMaxBytes,
                        retentionDays,
                        true),
                integrationName,
                headerNames.orElse(""),
                propertyNames.orElse(""));
    }

    static long resolveInlineThreshold(Optional<Long> configured, Optional<Long> legacy) {
        return configured.orElseGet(
                () -> legacy.orElse(PayloadCapturePolicy.DEFAULT_INLINE_THRESHOLD_BYTES));
    }

    StepOutputCheckpointService(
            PayloadObjectStore objectStore,
            PayloadCapturePolicy policy,
            String integrationName,
            String headerNames,
            String propertyNames) {
        this.objectStore = objectStore;
        this.policy = policy;
        this.integrationName = integrationName;
        this.headerNames = parseNames(headerNames);
        this.propertyNames = parseNames(propertyNames);
    }

    void ensureCaptureAvailable(String stepId) {
        if (policy.mode() == PayloadMode.METADATA_ONLY) {
            throw unavailable(stepId, "payload storage mode is metadata-only");
        }
    }

    StepOutputCheckpoint capture(Exchange exchange, String messageId, String stepId) {
        ensureCaptureAvailable(stepId);
        try {
            String contentType = exchange.getMessage().getHeader(PlatformHeaders.CONTENT_TYPE, String.class);
            EncodedBody body = encodeBody(exchange.getMessage().getBody(), contentType);
            Instant expiresAt = Instant.now().plus(policy.retention());
            Map<String, Object> headers = selectedHeaders(exchange);
            Map<String, Object> properties = selectedProperties(exchange);

            if (body.stream() != null) {
                return captureStream(exchange, messageId, stepId, body, expiresAt, headers, properties);
            }
            return captureBytes(messageId, stepId, body, expiresAt, headers, properties);
        } catch (StepOutputCheckpointException e) {
            throw e;
        } catch (Exception e) {
            throw new StepOutputCheckpointException(
                    "Failed to capture output checkpoint for step '" + stepId + "'", e);
        }
    }

    void restore(Exchange exchange, String stepId, StepOutputCheckpoint checkpoint) {
        if (checkpoint.expiresAt() == null || checkpoint.expiresAt().isBefore(Instant.now())) {
            throw unavailable(stepId, "the stored checkpoint has expired");
        }
        try {
            Object body = restoredBody(checkpoint);
            exchange.getMessage().setBody(body);
            if (checkpoint.contentType() != null) {
                exchange.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, checkpoint.contentType());
            }
            checkpoint.headers().forEach((name, value) -> exchange.getMessage().setHeader(name, value));
            checkpoint.properties().forEach(exchange::setProperty);
        } catch (StepOutputCheckpointException e) {
            throw unavailable(stepId, "the stored checkpoint is missing or corrupt", e);
        } catch (Exception e) {
            throw unavailable(stepId, "the stored checkpoint is missing or corrupt", e);
        }
    }

    private StepOutputCheckpoint captureBytes(
            String messageId,
            String stepId,
            EncodedBody body,
            Instant expiresAt,
            Map<String, Object> headers,
            Map<String, Object> properties) {
        byte[] bytes = body.bytes();
        if (bytes.length <= policy.inlineThresholdBytes()) {
            return checkpoint(
                    StepOutputCheckpoint.POSTGRES_INLINE,
                    bytes,
                    null,
                    sha256(bytes),
                    body.encoding(),
                    body.contentType(),
                    expiresAt,
                    headers,
                    properties);
        }
        if (policy.mode() != PayloadMode.TIERED) {
            throw unavailable(stepId,
                    "the output is larger than the Postgres inline threshold and tiered S3 storage is disabled");
        }
        PayloadReference reference = objectStore.store(
                integrationName,
                messageId + ":step-output:" + stepId,
                new ByteArrayInputStream(bytes),
                (long) bytes.length,
                body.contentType(),
                expiresAt,
                policy.objectMaxBytes());
        return checkpoint(reference, body.encoding(), headers, properties);
    }

    private StepOutputCheckpoint captureStream(
            Exchange exchange,
            String messageId,
            String stepId,
            EncodedBody body,
            Instant expiresAt,
            Map<String, Object> headers,
            Map<String, Object> properties) throws Exception {
        int probeSize = Math.toIntExact(policy.inlineThresholdBytes() + 1);
        byte[] probe = body.stream().readNBytes(probeSize);
        if (probe.length <= policy.inlineThresholdBytes()) {
            exchange.getMessage().setBody(probe);
            return checkpoint(
                    StepOutputCheckpoint.POSTGRES_INLINE,
                    probe,
                    null,
                    sha256(probe),
                    StepOutputCheckpoint.BYTES_BODY,
                    body.contentType(),
                    expiresAt,
                    headers,
                    properties);
        }
        if (policy.mode() != PayloadMode.TIERED) {
            throw unavailable(stepId,
                    "the streaming output is larger than the Postgres inline threshold and tiered S3 storage is disabled");
        }
        InputStream complete = new SequenceInputStream(new ByteArrayInputStream(probe), body.stream());
        PayloadReference reference = objectStore.store(
                integrationName,
                messageId + ":step-output:" + stepId,
                complete,
                null,
                body.contentType(),
                expiresAt,
                policy.objectMaxBytes());
        exchange.getMessage().setBody(objectStore.open(reference));
        return checkpoint(reference, StepOutputCheckpoint.BYTES_BODY, headers, properties);
    }

    private StepOutputCheckpoint checkpoint(
            PayloadReference reference,
            String encoding,
            Map<String, Object> headers,
            Map<String, Object> properties) {
        return new StepOutputCheckpoint(
                StepOutputCheckpoint.S3_OBJECT,
                null,
                reference.ref(),
                reference.sizeBytes(),
                reference.sha256(),
                encoding,
                reference.contentType(),
                reference.expiryInstant(),
                headers,
                properties);
    }

    private StepOutputCheckpoint checkpoint(
            String storage,
            byte[] bytes,
            String bodyRef,
            String sha256,
            String encoding,
            String contentType,
            Instant expiresAt,
            Map<String, Object> headers,
            Map<String, Object> properties) {
        return new StepOutputCheckpoint(
                storage, bytes, bodyRef, bytes.length, sha256, encoding, contentType, expiresAt, headers, properties);
    }

    private Object restoredBody(StepOutputCheckpoint checkpoint) throws Exception {
        byte[] bytes;
        if (StepOutputCheckpoint.POSTGRES_INLINE.equals(checkpoint.storage())) {
            bytes = checkpoint.bodyBytes();
            if (bytes == null || bytes.length != checkpoint.bodySizeBytes()
                    || !sha256(bytes).equals(checkpoint.bodySha256())) {
                throw new StepOutputCheckpointException("Inline checkpoint checksum mismatch");
            }
        } else if (StepOutputCheckpoint.S3_OBJECT.equals(checkpoint.storage())) {
            PayloadReference reference = new PayloadReference(
                    1,
                    PayloadReference.STORAGE,
                    checkpoint.bodyRef(),
                    checkpoint.bodySizeBytes(),
                    checkpoint.bodySha256(),
                    checkpoint.contentType(),
                    checkpoint.expiresAt().toString());
            objectStore.verify(reference);
            return objectStore.open(reference);
        } else {
            throw new StepOutputCheckpointException("Unsupported checkpoint storage: " + checkpoint.storage());
        }

        return switch (checkpoint.bodyEncoding()) {
            case StepOutputCheckpoint.NULL_BODY -> null;
            case StepOutputCheckpoint.BYTES_BODY -> bytes;
            case StepOutputCheckpoint.UTF8_BODY -> new String(bytes, StandardCharsets.UTF_8);
            case StepOutputCheckpoint.JSON_BODY -> JSON.readValue(bytes, Object.class);
            default -> throw new StepOutputCheckpointException(
                    "Unsupported checkpoint body encoding: " + checkpoint.bodyEncoding());
        };
    }

    private EncodedBody encodeBody(Object body, String contentType) throws Exception {
        if (body == null) {
            return new EncodedBody(new byte[0], null, StepOutputCheckpoint.NULL_BODY, contentType);
        }
        if (body instanceof byte[] bytes) {
            return new EncodedBody(bytes.clone(), null, StepOutputCheckpoint.BYTES_BODY, contentType);
        }
        if (body instanceof String value) {
            return new EncodedBody(value.getBytes(StandardCharsets.UTF_8), null, StepOutputCheckpoint.UTF8_BODY, contentType);
        }
        if (body instanceof InputStream stream) {
            return new EncodedBody(null, stream, StepOutputCheckpoint.BYTES_BODY, contentType);
        }
        return new EncodedBody(JSON.writeValueAsBytes(body), null, StepOutputCheckpoint.JSON_BODY, contentType);
    }

    private Map<String, Object> selectedHeaders(Exchange exchange) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (String name : headerNames) {
            Object value = exchange.getMessage().getHeader(name);
            if (value != null) {
                values.put(name, jsonCompatible(value, "header", name));
            }
        }
        return Map.copyOf(values);
    }

    private Map<String, Object> selectedProperties(Exchange exchange) {
        Map<String, Object> values = new LinkedHashMap<>();
        for (String name : propertyNames) {
            Object value = exchange.getProperty(name);
            if (value != null) {
                values.put(name, jsonCompatible(value, "property", name));
            }
        }
        return Map.copyOf(values);
    }

    private Object jsonCompatible(Object value, String kind, String name) {
        try {
            return JSON.convertValue(value, Object.class);
        } catch (Exception e) {
            throw new StepOutputCheckpointException(
                    "Configured output checkpoint " + kind + " '" + name + "' is not JSON-compatible", e);
        }
    }

    private Set<String> parseNames(String configuredNames) {
        if (configuredNames == null || configuredNames.isBlank()) {
            return Set.of();
        }
        Set<String> names = new LinkedHashSet<>();
        for (String value : configuredNames.split(",")) {
            if (!value.isBlank()) {
                names.add(value.trim());
            }
        }
        return Set.copyOf(names);
    }

    private String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private StepOutputCheckpointException unavailable(String stepId, String reason) {
        return unavailable(stepId, reason, null);
    }

    private StepOutputCheckpointException unavailable(String stepId, String reason, Throwable cause) {
        String message = "Cannot safely replay output-producing step '" + stepId + "': " + reason
                + ". Restore the checkpoint or request explicit redo for this step only when the receiver "
                + "honors its stable idempotency key.";
        return cause == null ? new StepOutputCheckpointException(message)
                : new StepOutputCheckpointException(message, cause);
    }

    private record EncodedBody(byte[] bytes, InputStream stream, String encoding, String contentType) {
    }
}
