package no.basis.integrationplatform.camel.step;

import java.util.concurrent.atomic.AtomicReference;

/**
 * What a consumed record claims when it carries {@code x-bip-redo-steps}: that it is the
 * replay this adapter published for this message, integration, input topic and attempt, with
 * this redo list. The list is honoured only when the Message Store confirms that, and only
 * until the attempt has a recorded outcome. Copies of the exchange, such as split parts, share
 * one claim, so the store is asked once.
 */
public final class RedoClaim {
    private final String messageId;
    private final String integration;
    private final String topic;
    private final int attempt;
    private final String redoStepsJson;
    private final AtomicReference<Boolean> verdict = new AtomicReference<>();

    /**
     * Creates a claim.
     *
     * @param messageId the record's {@code x-bip-message-id}
     * @param integration this integration's name
     * @param topic the topic the record was consumed from
     * @param attempt the record's attempt
     * @param redoStepsJson the record's {@code x-bip-redo-steps}, a JSON array
     */
    public RedoClaim(String messageId, String integration, String topic, int attempt, String redoStepsJson) {
        this.messageId = messageId;
        this.integration = integration;
        this.topic = topic;
        this.attempt = attempt;
        this.redoStepsJson = redoStepsJson;
    }

    /** @return the record's message id */
    public String messageId() {
        return messageId;
    }

    /** @return this integration's name */
    public String integration() {
        return integration;
    }

    /** @return the topic the record was consumed from */
    public String topic() {
        return topic;
    }

    /** @return the record's attempt */
    public int attempt() {
        return attempt;
    }

    /** @return the record's redo list as a JSON array */
    public String redoStepsJson() {
        return redoStepsJson;
    }

    Boolean verdict() {
        return verdict.get();
    }

    void decide(boolean authorised) {
        verdict.compareAndSet(null, authorised);
    }
}
