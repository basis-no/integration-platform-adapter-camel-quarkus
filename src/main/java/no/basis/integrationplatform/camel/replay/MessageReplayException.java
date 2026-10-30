package no.basis.integrationplatform.camel.replay;

public class MessageReplayException extends RuntimeException {
    public MessageReplayException(String message) {
        super(message);
    }

    public MessageReplayException(String message, Throwable cause) {
        super(message, cause);
    }
}
