package no.basis.integrationplatform.camel.replay;

public interface MessageReplayService {
    MessageReplayResult replay(String messageId);

    MessageReplayResult replay(MessageReplayRequest request);
}
