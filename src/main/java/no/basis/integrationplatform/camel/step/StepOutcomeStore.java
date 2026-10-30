package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.model.StepResult;

/**
 * Stores durable step outcomes used to avoid repeating already delivered side effects.
 *
 * <p>The adapter owns the contract, not the storage choice. A first production
 * implementation can write to the platform message-store Postgres database, while
 * tests and early demos can use the no-op implementation.</p>
 */
public interface StepOutcomeStore {
    boolean wasDelivered(String idempotencyKey);

    void recordDelivered(StepResult step);

    void recordFailed(StepResult step);
}
