package no.basis.integrationplatform.camel.payload;

/** Indicates that required replay payload capture, verification, or retrieval failed. */
public class PayloadStorageException extends RuntimeException {
    /**
     * Creates a payload-storage failure.
     *
     * @param message actionable failure description
     */
    public PayloadStorageException(String message) {
        super(message);
    }

    /**
     * Creates a payload-storage failure with its underlying cause.
     *
     * @param message actionable failure description
     * @param cause underlying storage failure
     */
    public PayloadStorageException(String message, Throwable cause) {
        super(message, cause);
    }
}
