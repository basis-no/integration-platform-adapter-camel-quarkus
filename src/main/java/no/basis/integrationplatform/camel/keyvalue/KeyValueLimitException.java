package no.basis.integrationplatform.camel.keyvalue;

/**
 * Signals state the Message Store rejects: a key outside 1-256 characters or
 * with control characters, a value over 64 KiB, or a new key beyond the
 * integration's live-key limit.
 *
 * <p>Repeating the call fails the same way. It is deliberately not an
 * {@link KeyValueStoreException}, so a handler that retries an unavailable
 * store never retries a write the store will always reject.</p>
 */
public class KeyValueLimitException extends RuntimeException {
    /**
     * Creates a limit failure detected before the database.
     *
     * @param message failure description
     */
    public KeyValueLimitException(String message) {
        super(message);
    }

    /**
     * Creates a limit failure reported by the database.
     *
     * @param message failure description
     * @param cause database constraint failure
     */
    public KeyValueLimitException(String message, Throwable cause) {
        super(message, cause);
    }
}
