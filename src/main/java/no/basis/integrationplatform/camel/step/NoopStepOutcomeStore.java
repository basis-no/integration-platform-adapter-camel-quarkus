package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.model.StepResult;

public class NoopStepOutcomeStore implements StepOutcomeStore {
    @Override
    public boolean wasDelivered(String idempotencyKey) {
        return false;
    }

    @Override
    public void recordDelivered(StepResult step) {
        // No-op until a durable store, typically Postgres, is configured.
    }

    @Override
    public void recordFailed(StepResult step) {
        // No-op until a durable store, typically Postgres, is configured.
    }
}
