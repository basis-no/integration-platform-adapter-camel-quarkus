package no.basis.integrationplatform.camel.keyvalue;

import java.time.Instant;

/**
 * A live key-value store key as listed, without its value.
 *
 * @param key state key
 * @param version version of this physical row, starting at 1 again after deletion and recreation
 * @param expiresAt when the key expires, or {@code null} when it does not
 * @param updatedAt time of the last write
 * @param updatedBy {@code runtime} for adapter writes, {@code operator:<subject>} for a person's write
 */
public record KeyValueKey(
        String key,
        long version,
        Instant expiresAt,
        Instant updatedAt,
        String updatedBy) {
}
