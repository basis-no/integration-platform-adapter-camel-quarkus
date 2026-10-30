package no.basis.integrationplatform.camel.model;

import java.time.Instant;

/**
 * Terminal outcome observed for one classified external step.
 *
 * @param step stable step identity
 * @param status terminal step status
 * @param idempotencyKey stable side-effect identity
 * @param endpointUri intercepted endpoint URI
 * @param targetRef receiver-specific reference, when known
 * @param error failure description, when applicable
 * @param occurredAt observation timestamp
 */
public record StepResult(
        String step,
        StepStatus status,
        String idempotencyKey,
        String endpointUri,
        String targetRef,
        String error,
        Instant occurredAt) {

    /**
     * Creates a delivered step result at the current time.
     *
     * @param step stable step identity
     * @param idempotencyKey stable side-effect identity
     * @param endpointUri intercepted endpoint URI
     * @return delivered result
     */
    public static StepResult delivered(String step, String idempotencyKey, String endpointUri) {
        return new StepResult(step, StepStatus.DELIVERED, idempotencyKey, endpointUri, null, null, Instant.now());
    }

    /**
     * Creates a failed step result at the current time.
     *
     * @param step stable step identity
     * @param idempotencyKey stable side-effect identity
     * @param endpointUri intercepted endpoint URI
     * @param error observed failure
     * @return failed result
     */
    public static StepResult failed(String step, String idempotencyKey, String endpointUri, Throwable error) {
        return new StepResult(step, StepStatus.FAILED, idempotencyKey, endpointUri, null, errorMessage(error), Instant.now());
    }

    /**
     * Creates a skipped step result at the current time.
     *
     * @param step stable step identity
     * @param idempotencyKey stable side-effect identity
     * @param endpointUri intercepted endpoint URI
     * @return skipped result
     */
    public static StepResult skipped(String step, String idempotencyKey, String endpointUri) {
        return new StepResult(step, StepStatus.SKIPPED, idempotencyKey, endpointUri, null, null, Instant.now());
    }

    private static String errorMessage(Throwable error) {
        if (error == null) {
            return null;
        }
        return error.getMessage() == null ? error.getClass().getName() : error.getMessage();
    }
}
