package no.basis.integrationplatform.camel.model;

import no.basis.integrationplatform.camel.ErrorTextMasking;
import java.time.Instant;

public record StepResult(
        String step,
        StepStatus status,
        String idempotencyKey,
        String endpointUri,
        String targetRef,
        String error,
        Instant occurredAt) {
    /** Masks the URIs in an error text however the result was built. */
    public StepResult {
        error = ErrorTextMasking.mask(error);
    }


    public static StepResult delivered(String step, String idempotencyKey, String endpointUri) {
        return new StepResult(step, StepStatus.DELIVERED, idempotencyKey, endpointUri, null, null, Instant.now());
    }

    public static StepResult failed(String step, String idempotencyKey, String endpointUri, Throwable error) {
        return new StepResult(step, StepStatus.FAILED, idempotencyKey, endpointUri, null, errorMessage(error), Instant.now());
    }

    public static StepResult skipped(String step, String idempotencyKey, String endpointUri) {
        return new StepResult(step, StepStatus.SKIPPED, idempotencyKey, endpointUri, null, null, Instant.now());
    }

    private static String errorMessage(Throwable error) {
        if (error == null) {
            return null;
        }
        // Stored in the step ledger and the attempt's steps: a receiver URI keeps no credential.
        // PostgreSQL text cannot hold a NUL; a failure's own text must not make its step unstorable.
        return ErrorTextMasking.message(error).replace('\u0000', '\uFFFD');
    }
}
