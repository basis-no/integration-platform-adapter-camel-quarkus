package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;

import java.time.Instant;
import java.util.List;

/**
 * Runtime-neutral terminal lifecycle event emitted by the Kafka sink.
 *
 * @param schemaVersion event schema version
 * @param messageId stable platform message id
 * @param correlationId optional business correlation id
 * @param integration integration name
 * @param attempt platform attempt number
 * @param occurredAt terminal outcome time
 * @param status terminal message status
 * @param steps observed durable step outcomes
 * @param error terminal error, when present
 */
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

    /**
     * Converts the adapter's terminal result into the status-event contract.
     *
     * @param result terminal message attempt
     * @return equivalent lifecycle event
     */
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
     *
     * @param step stable step id
     * @param status step outcome
     * @param idempotencyKey stable side-effect identity
     * @param endpointUri intercepted endpoint URI
     * @param targetRef optional receiver reference
     * @param error step error, when present
     * @param occurredAt outcome time
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
