package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.model.StepResult;

/**
 * Stores durable step outcomes used to avoid repeating already delivered side effects.
 *
 * <p>The adapter owns the contract, not the storage choice. A first production
 * implementation can write to the message-store Postgres database, while
 * tests and early demos can use the no-op implementation.</p>
 */
public interface StepOutcomeStore {
    boolean wasDelivered(String idempotencyKey);

    void recordDelivered(StepResult step);

    void recordFailed(StepResult step);

    /**
     * Whether a consumed record's redo list belongs to a replay this adapter published: a
     * replay audit row for the claim's message id, integration, target topic and attempt, with
     * the same redo list, that is requested or published, while that attempt has no recorded
     * outcome yet. A redo list is honoured only then, and so only once.
     *
     * @param claim what the consumed record claims
     * @return {@code false} for a store that cannot tell, so that no redo is honoured
     */
    default boolean isAuthorisedReplay(RedoClaim claim) {
        return false;
    }
}
