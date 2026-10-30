package no.basis.integrationplatform.camel.replay;

/** Validates, audits, and republishes stored message payloads. */
public interface MessageReplayService {
    /**
     * Replays a message without optional operator context.
     *
     * @param messageId stable platform message id
     * @return successful replay publication
     * @throws MessageReplayException when replay is unavailable or publication fails
     */
    MessageReplayResult replay(String messageId);

    /**
     * Replays a message with audited operator context and optional redo steps.
     *
     * @param request replay request
     * @return successful replay publication
     * @throws MessageReplayException when replay is unavailable or publication fails
     */
    MessageReplayResult replay(MessageReplayRequest request);
}
