package no.basis.integrationplatform.camel.keyvalue;

/**
 * Signals that key-value store could not be read or written because the
 * Message Store is unavailable or does not provide key-value store.
 */
public class KeyValueStoreException extends RuntimeException {
    /**
     * Creates a key-value store failure.
     *
     * @param message failure description
     * @param cause underlying storage failure
     */
    public KeyValueStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
