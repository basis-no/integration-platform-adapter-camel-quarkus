package no.basis.integrationplatform.camel.keyvalue;

import com.fasterxml.jackson.databind.JsonNode;

import java.time.Instant;

/**
 * A live key-value store key's value and last write.
 *
 * @param value JSON document
 * @param version version of this physical row, starting at 1 again after deletion and recreation
 * @param expiresAt when the key expires, or {@code null} when it does not
 * @param updatedAt time of the last write
 * @param updatedBy {@code runtime} for adapter writes, {@code operator:<subject>} for a person's write
 */
public record KeyValueEntry(
        JsonNode value,
        long version,
        Instant expiresAt,
        Instant updatedAt,
        String updatedBy) {
}
