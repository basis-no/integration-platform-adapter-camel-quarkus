package no.basis.integrationplatform.camel.store;

/** Indicates that a terminal lifecycle event cannot be serialized. */
public class MessageStatusEventJsonException extends RuntimeException {
    /**
     * Creates a serialization failure.
     *
     * @param message failure description
     * @param cause underlying JSON failure
     */
    public MessageStatusEventJsonException(String message, Throwable cause) {
        super(message, cause);
    }
}
