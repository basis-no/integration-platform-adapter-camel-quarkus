package no.basis.integrationplatform.camel.step;

import java.time.Instant;
import java.util.Map;

/**
 * Durable output required to continue a replay after an already-delivered side effect.
 *
 * @param storage payload storage discriminator
 * @param bodyBytes inline encoded response body, when applicable
 * @param bodyRef immutable object reference, when applicable
 * @param bodySizeBytes encoded body size
 * @param bodySha256 SHA-256 checksum of the encoded body
 * @param bodyEncoding body representation used for restoration
 * @param contentType response content type
 * @param expiresAt immutable checkpoint expiry
 * @param headers selected response headers required downstream
 * @param properties selected exchange properties required downstream
 */
public record StepOutputCheckpoint(
        String storage,
        byte[] bodyBytes,
        String bodyRef,
        long bodySizeBytes,
        String bodySha256,
        String bodyEncoding,
        String contentType,
        Instant expiresAt,
        Map<String, Object> headers,
        Map<String, Object> properties) {
    /** Storage discriminator for checkpoint bytes held in Postgres. */
    public static final String POSTGRES_INLINE = "postgres_inline";
    /** Storage discriminator for checkpoint bytes held in S3-compatible storage. */
    public static final String S3_OBJECT = "s3_object";
    /** Body encoding for a null Camel message body. */
    public static final String NULL_BODY = "null";
    /** Body encoding for raw bytes and streams. */
    public static final String BYTES_BODY = "bytes";
    /** Body encoding for a UTF-8 string. */
    public static final String UTF8_BODY = "utf8";
    /** Body encoding for an object serialized as JSON. */
    public static final String JSON_BODY = "json";

    /** Normalizes mutable inputs into defensive immutable copies. */
    public StepOutputCheckpoint {
        bodyBytes = bodyBytes == null ? null : bodyBytes.clone();
        headers = headers == null ? Map.of() : Map.copyOf(headers);
        properties = properties == null ? Map.of() : Map.copyOf(properties);
    }

    /**
     * Returns a defensive copy of inline body bytes.
     *
     * @return copied body bytes
     */
    @Override
    public byte[] bodyBytes() {
        return bodyBytes == null ? null : bodyBytes.clone();
    }
}
