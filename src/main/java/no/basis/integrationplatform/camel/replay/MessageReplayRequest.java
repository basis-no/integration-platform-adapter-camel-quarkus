package no.basis.integrationplatform.camel.replay;

import java.util.List;

/**
 * Operator context for one audited replay request.
 *
 * @param messageId stable platform message id
 * @param requestedBy operator identity
 * @param reason audited replay reason
 * @param redoSteps side-effect step ids that must execute again
 */
public record MessageReplayRequest(
        String messageId,
        String requestedBy,
        String reason,
        List<String> redoSteps) {
    /** Creates an immutable request and normalizes a missing redo list. */
    public MessageReplayRequest {
        redoSteps = redoSteps == null ? List.of() : List.copyOf(redoSteps);
    }

    /**
     * Creates a replay request without optional operator context.
     *
     * @param messageId stable platform message id
     * @return replay request
     */
    public static MessageReplayRequest forMessage(String messageId) {
        return new MessageReplayRequest(messageId, null, null, List.of());
    }
}
