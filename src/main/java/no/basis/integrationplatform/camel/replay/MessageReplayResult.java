package no.basis.integrationplatform.camel.replay;

/**
 * Successful audited replay publication.
 *
 * @param replayId replay audit id
 * @param messageId stable message id
 * @param integration integration name
 * @param topic replay target topic
 * @param attempt new message attempt
 */
public record MessageReplayResult(
        String replayId,
        String messageId,
        String integration,
        String topic,
        int attempt) {
}
