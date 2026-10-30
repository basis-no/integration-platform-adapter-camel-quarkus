package no.basis.integrationplatform.camel.replay;

import java.util.List;

public record MessageReplayRequest(
        String messageId,
        String requestedBy,
        String reason,
        List<String> redoSteps) {
    public MessageReplayRequest {
        redoSteps = redoSteps == null ? List.of() : List.copyOf(redoSteps);
    }

    public static MessageReplayRequest forMessage(String messageId) {
        return new MessageReplayRequest(messageId, null, null, List.of());
    }
}
