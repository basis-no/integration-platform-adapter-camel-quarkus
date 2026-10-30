package no.basis.integrationplatform.camel.store;

/** Indicates that a terminal outcome could not be recorded durably. */
public class MessageStoreSinkException extends RuntimeException {
    /**
     * Creates a sink failure.
     *
     * @param message actionable failure description
     * @param cause underlying persistence or publication failure
     */
    public MessageStoreSinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
