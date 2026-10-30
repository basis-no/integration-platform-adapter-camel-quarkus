package no.basis.integrationplatform.camel.replay;

/** Indicates that a replay request cannot be validated, audited, or published. */
public class MessageReplayException extends RuntimeException {
    /**
     * Creates a replay failure.
     *
     * @param message actionable failure description
     */
    public MessageReplayException(String message) {
        super(message);
    }

    /**
     * Creates a replay failure with its underlying cause.
     *
     * @param message actionable failure description
     * @param cause underlying failure
     */
    public MessageReplayException(String message, Throwable cause) {
        super(message, cause);
    }
}
