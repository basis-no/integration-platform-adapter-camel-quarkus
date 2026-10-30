package no.basis.integrationplatform.camel.replay;

public record MessageReplayResult(
        String replayId,
        String messageId,
        String integration,
        String topic,
        int attempt) {
}
