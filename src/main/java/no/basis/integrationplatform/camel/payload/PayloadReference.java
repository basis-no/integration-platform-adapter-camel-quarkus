package no.basis.integrationplatform.camel.payload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Claim-check document describing one immutable S3-compatible payload object.
 *
 * @param schemaVersion claim-check schema version
 * @param storage storage discriminator
 * @param ref immutable {@code s3://} object reference
 * @param sizeBytes payload size
 * @param sha256 lowercase SHA-256 checksum
 * @param contentType original payload content type
 * @param expiresAt ISO-8601 immutable expiry
 */
@JsonIgnoreProperties(ignoreUnknown = false)
public record PayloadReference(
        int schemaVersion,
        String storage,
        String ref,
        long sizeBytes,
        String sha256,
        String contentType,
        String expiresAt) {
    /** Media type used for Kafka claim-check records. */
    public static final String CONTENT_TYPE = "application/vnd.bip.payload-reference+json";
    /** Storage discriminator used by the message-store read model. */
    public static final String STORAGE = "s3_object";
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Validates the complete immutable reference. */
    public PayloadReference {
        if (schemaVersion != 1) {
            throw new IllegalArgumentException("Unsupported payload-reference schemaVersion: " + schemaVersion);
        }
        if (!STORAGE.equals(storage)) {
            throw new IllegalArgumentException("Unsupported payload-reference storage: " + storage);
        }
        if (ref == null || !ref.startsWith("s3://")) {
            throw new IllegalArgumentException("Payload reference must use s3://");
        }
        if (sizeBytes < 0) {
            throw new IllegalArgumentException("Payload reference sizeBytes must be >= 0");
        }
        if (sha256 == null || !sha256.matches("[a-f0-9]{64}")) {
            throw new IllegalArgumentException("Payload reference sha256 must be lowercase hexadecimal");
        }
        try {
            Instant.parse(expiresAt);
        } catch (Exception e) {
            throw new IllegalArgumentException("Payload reference expiresAt must be an ISO-8601 instant", e);
        }
    }

    /**
     * Returns the parsed expiry.
     *
     * @return parsed immutable expiry
     */
    public Instant expiryInstant() {
        return Instant.parse(expiresAt);
    }

    /**
     * Serializes this reference.
     *
     * @return serialized claim-check JSON
     */
    public byte[] toJsonBytes() {
        try {
            return JSON.writeValueAsBytes(this);
        } catch (Exception e) {
            throw new PayloadStorageException("Failed to serialize payload reference", e);
        }
    }

    /**
     * Parses and validates a claim-check body.
     *
     * @param body JSON bytes or string
     * @return validated payload reference
     * @throws PayloadStorageException when malformed
     */
    public static PayloadReference fromJson(Object body) {
        try {
            byte[] bytes = switch (body) {
                case byte[] value -> value;
                case String value -> value.getBytes(StandardCharsets.UTF_8);
                default -> throw new IllegalArgumentException(
                        "Payload-reference body must be byte[] or String, not "
                                + (body == null ? "null" : body.getClass().getName()));
            };
            return JSON.readValue(bytes, PayloadReference.class);
        } catch (PayloadStorageException e) {
            throw e;
        } catch (Exception e) {
            throw new PayloadStorageException("Invalid payload-reference document", e);
        }
    }
}
