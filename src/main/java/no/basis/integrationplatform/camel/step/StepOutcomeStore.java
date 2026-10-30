package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.model.StepResult;

import java.util.Optional;

/**
 * Stores durable step outcomes used to avoid repeating already delivered side effects.
 *
 * <p>The adapter owns the contract, not the storage choice. A first production
 * implementation can write to the platform message-store Postgres database, while
 * tests and early demos can use the no-op implementation.</p>
 */
public interface StepOutcomeStore {
    /**
     * Reports checkpoint support.
     *
     * @return whether the store can persist and restore output checkpoints
     */
    default boolean supportsOutputCheckpoints() {
        return false;
    }

    /**
     * Tests whether a side effect already completed durably.
     *
     * @param idempotencyKey stable side-effect identity
     * @return {@code true} when already delivered
     */
    boolean wasDelivered(String idempotencyKey);

    /**
     * Records a delivered one-way side effect.
     *
     * @param step delivered step
     */
    void recordDelivered(StepResult step);

    /**
     * Records a delivered side effect and its required output.
     *
     * @param step delivered step
     * @param checkpoint durable output checkpoint
     */
    default void recordDelivered(StepResult step, StepOutputCheckpoint checkpoint) {
        if (checkpoint != null) {
            throw new StepOutputCheckpointException(
                    "The configured step outcome store cannot persist output checkpoints");
        }
        recordDelivered(step);
    }

    /**
     * Reads output required when a delivered step is skipped.
     *
     * @param idempotencyKey stable side-effect identity
     * @return stored checkpoint, when available
     */
    default Optional<StepOutputCheckpoint> readCheckpoint(String idempotencyKey) {
        return Optional.empty();
    }

    /**
     * Records a failed side-effect observation.
     *
     * @param step failed step
     */
    void recordFailed(StepResult step);
}
