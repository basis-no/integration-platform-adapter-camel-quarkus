package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;

import java.time.Instant;
import java.util.List;

public record MessageStatusEvent(
        int schemaVersion,
        String messageId,
        String correlationId,
        String integration,
        int attempt,
        Instant occurredAt,
        MessageStatus status,
        List<Step> steps,
        String error) {

    public static MessageStatusEvent from(MessageAttemptResult result) {
        return new MessageStatusEvent(
                result.schemaVersion(),
                result.messageId(),
                result.correlationId(),
                result.integration(),
                result.attempt(),
                result.occurredAt(),
                result.status(),
                result.steps().stream().map(Step::from).toList(),
                result.error());
    }

    /**
     * Per-attempt step observation, contract §7 (DECISION D4). The serialized
     * shape must match {@code spec/schemas/steps-json.schema.json}:
     * step, status, idempotencyKey, endpointUri, targetRef, error, occurredAt.
     */
    public record Step(
            String step,
            StepStatus status,
            String idempotencyKey,
            String endpointUri,
            String targetRef,
            String error,
            Instant occurredAt) {

        private static Step from(StepResult result) {
            return new Step(
                    result.step(),
                    result.status(),
                    result.idempotencyKey(),
                    result.endpointUri(),
                    result.targetRef(),
                    result.error(),
                    result.occurredAt());
        }
    }
}
