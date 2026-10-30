package no.basis.integrationplatform.camel.payload;

import java.io.InputStream;
import java.time.Instant;

/** Immutable S3-compatible storage abstraction for replay payloads and step outputs. */
public interface PayloadObjectStore {
    /**
     * Streams and stores one payload with integrity metadata.
     *
     * @param integration integration name
     * @param messageId stable message or checkpoint identity
     * @param payload payload stream
     * @param declaredSize optional known size
     * @param contentType original content type
     * @param expiresAt immutable expiry
     * @param maxBytes maximum accepted bytes
     * @return immutable stored payload reference
     */
    PayloadReference store(
            String integration,
            String messageId,
            InputStream payload,
            Long declaredSize,
            String contentType,
            Instant expiresAt,
            long maxBytes);

    /**
     * Opens a verified payload stream.
     *
     * @param reference immutable payload reference
     * @return payload stream
     */
    InputStream open(PayloadReference reference);

    /**
     * Verifies object existence and reference metadata without returning the body.
     *
     * @param reference immutable payload reference
     */
    void verify(PayloadReference reference);

    /**
     * Returns a fail-closed object store for modes where object storage is unavailable.
     *
     * @return disabled store
     */
    static PayloadObjectStore disabled() {
        return new PayloadObjectStore() {
            private PayloadStorageException unavailable() {
                return new PayloadStorageException("Tiered payload storage requires an S3-compatible object store");
            }

            @Override
            public PayloadReference store(String integration, String messageId, InputStream payload,
                                          Long declaredSize, String contentType, Instant expiresAt, long maxBytes) {
                throw unavailable();
            }

            @Override
            public InputStream open(PayloadReference reference) {
                throw unavailable();
            }

            @Override
            public void verify(PayloadReference reference) {
                throw unavailable();
            }
        };
    }
}
