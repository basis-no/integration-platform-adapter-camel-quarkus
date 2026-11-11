package no.basis.integrationplatform.camel.model;

import no.basis.integrationplatform.camel.ErrorTextMasking;
import java.time.Instant;
import java.util.List;

public record MessageAttemptResult(
        int schemaVersion,
        String messageId,
        String correlationId,
        String contentType,
        String sourceSystem,
        String targetSystem,
        String payloadStorage,
        Long payloadSizeBytes,
        byte[] payloadBytes,
        Instant payloadExpiresAt,
        Instant payloadDeletedAt,
        boolean replayable,
        String replayDisabledReason,
        String integration,
        String routeId,
        int attempt,
        boolean autoRedelivered,
        Integer autoRedeliveryCount,
        Integer autoRedeliveryMax,
        Instant occurredAt,
        MessageStatus status,
        List<StepResult> steps,
        String error) {
    /** Masks the URIs in an error text however the result was built. */
    public MessageAttemptResult {
        error = ErrorTextMasking.mask(error);
    }


    public MessageAttemptResult(
            int schemaVersion,
            String messageId,
            String correlationId,
            String integration,
            String routeId,
            int attempt,
            Instant occurredAt,
            MessageStatus status,
            List<StepResult> steps,
            String error) {
        this(
                schemaVersion,
                messageId,
                correlationId,
                null,
                null,
                null,
                "metadata_only",
                null,
                null,
                null,
                null,
                false,
                "payload metadata only",
                integration,
                routeId,
                attempt,
                false,
                null,
                null,
                occurredAt,
                status,
                steps,
                error);
    }
}
