package no.basis.integrationplatform.camel.keyvalue;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.camel.spi.IdempotentRepository;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Small key-value state of this integration in its environment's Message Store.
 *
 * <p>Use it for polling cursors, markers such as "a full re-sync already ran",
 * keys of input already seen and small buffers, never for payloads, secrets or
 * business records. Keys belong to the integration identity the adapter records
 * in {@code bip_message_log.integration}. A key is 1-256 characters without
 * control characters; {@code /} groups keys by convention. A value is a JSON
 * document of at most 64 KiB. A version belongs to a physical database row: it
 * starts at 1 and increases on writes while that row remains. An expired key is
 * absent to operations; rewriting it before cleanup continues its version.
 * After expiry cleanup or explicit deletion removes the row, recreation starts
 * at 1. A stale version can therefore match a new row. Numeric compare-and-set
 * is not a lease or fencing token across deletion. The insertion threshold is
 * 10 000 live keys; concurrent inserts may overshoot because the canonical
 * trigger counts per transaction. It is not a strict serialized quota.</p>
 *
 * <p>Every call is one single-row statement in its own transaction, never part
 * of a step outcome or a message's status. A message that fails after a state
 * write and is redelivered or resent writes it again, so make state writes safe
 * to repeat. Replicas that advance the same key use
 * {@link #compareAndSet(String, long, JsonNode, Duration)}.</p>
 *
 * <p>A key or value outside the limits, or a new key beyond the live-key limit,
 * throws {@link KeyValueLimitException}. An unavailable store, or a
 * Message Store without key-value store (before contract 1.2), throws
 * {@link KeyValueStoreException}.</p>
 */
public interface KeyValueStore {
    /**
     * Reads a live key.
     *
     * @param key state key
     * @return the entry, or empty when the key is absent or expired
     */
    Optional<KeyValueEntry> get(String key);

    /**
     * Creates or replaces a key without expiry, clearing any earlier expiry.
     *
     * @param key state key
     * @param value JSON document
     * @return the new version
     */
    default long put(String key, JsonNode value) {
        return put(key, value, null);
    }

    /**
     * Creates or replaces a key.
     *
     * @param key state key
     * @param value JSON document
     * @param ttl time to live from now, or {@code null} for no expiry
     * @return the new version
     */
    long put(String key, JsonNode value, Duration ttl);

    /**
     * Replaces a live key at the expected version without expiry, clearing any
     * earlier expiry.
     *
     * @param key state key
     * @param expectedVersion version the caller read
     * @param value JSON document
     * @return the new version, or empty on a conflict that changed nothing
     */
    default OptionalLong compareAndSet(String key, long expectedVersion, JsonNode value) {
        return compareAndSet(key, expectedVersion, value, null);
    }

    /**
     * Replaces a live key only if it is at the expected version. A missing,
     * expired or newer key is a conflict, not an error.
     *
     * @param key state key
     * @param expectedVersion version the caller read
     * @param value JSON document
     * @param ttl time to live from now, or {@code null} for no expiry
     * @return the new version, or empty on a conflict that changed nothing
     */
    OptionalLong compareAndSet(String key, long expectedVersion, JsonNode value, Duration ttl);

    /**
     * Creates a key without expiry unless a live key exists.
     *
     * @param key state key
     * @param value JSON document
     * @return whether the key was created
     */
    default boolean putIfAbsent(String key, JsonNode value) {
        return putIfAbsent(key, value, null);
    }

    /**
     * Creates a key unless a live key exists. An expired key starts a new life
     * and continues its version.
     *
     * @param key state key
     * @param value JSON document
     * @param ttl time to live from now, or {@code null} for no expiry
     * @return whether the key was created
     */
    boolean putIfAbsent(String key, JsonNode value, Duration ttl);

    /**
     * Removes a live key.
     *
     * @param key state key
     * @return whether a key was removed
     */
    boolean delete(String key);

    /**
     * Removes a live key only if it is at the expected version.
     *
     * @param key state key
     * @param expectedVersion version the caller read
     * @return whether a key was removed
     */
    boolean delete(String key, long expectedVersion);

    /**
     * Lists the first live keys with a prefix, in byte order.
     *
     * @param prefix key prefix, matched literally; empty lists every key
     * @param limit maximum number of keys, 1 to 1000
     * @return keys with their versions, without values
     */
    default List<KeyValueKey> list(String prefix, int limit) {
        return list(prefix, null, limit);
    }

    /**
     * Lists live keys with a prefix, in byte order, after a key from an earlier
     * page.
     *
     * @param prefix key prefix, matched literally; empty lists every key
     * @param after last key of the previous page, or {@code null} for the first page
     * @param limit maximum number of keys, 1 to 1000
     * @return keys with their versions, without values
     */
    List<KeyValueKey> list(String prefix, String after, int limit);

    /**
     * Creates a Camel idempotent repository that stores each message id it sees
     * as the key {@code <prefix>/<message id>} with a time-to-live.
     *
     * <p>It is sized for source-level deduplication, such as files or polled
     * records: every id seen within the time-to-live is a live key and counts
     * toward the 10 000 live keys of the integration.</p>
     *
     * @param prefix key prefix, without the trailing {@code /}
     * @param ttl how long a seen id is remembered; required
     * @return the repository
     */
    IdempotentRepository idempotentRepository(String prefix, Duration ttl);
}
