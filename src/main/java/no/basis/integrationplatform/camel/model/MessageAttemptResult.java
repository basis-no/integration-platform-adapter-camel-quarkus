package no.basis.integrationplatform.camel.model;

import java.time.Instant;
import java.util.List;

/**
 * Complete terminal message-attempt data passed to a {@code MessageStoreSink}.
 *
 * @param schemaVersion envelope schema version
 * @param messageId stable platform message id
 * @param correlationId optional business correlation id
 * @param contentType captured payload content type
 * @param sourceSystem optional source-system identity
 * @param targetSystem optional target-system identity
 * @param payloadStorage payload storage discriminator
 * @param payloadSizeBytes captured payload size
 * @param payloadBytes inline payload bytes
 * @param payloadRef immutable external payload reference
 * @param payloadSha256 payload SHA-256 checksum
 * @param payloadExpiresAt immutable payload expiry
 * @param payloadDeletedAt payload deletion time
 * @param replayable whether policy and storage allow replay
 * @param replayDisabledReason reason replay is unavailable
 * @param integration integration name
 * @param routeId Camel boundary route id
 * @param attempt platform attempt number
 * @param autoRedelivered whether Camel automatically redelivered the exchange
 * @param autoRedeliveryCount completed automatic redeliveries
 * @param autoRedeliveryMax configured maximum automatic redeliveries
 * @param occurredAt terminal outcome time
 * @param status terminal message status
 * @param steps observed durable step outcomes
 * @param error terminal error, when present
 */
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
        String payloadRef,
        String payloadSha256,
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

    /**
     * Creates a metadata-only result without automatic-redelivery diagnostics.
     *
     * @param schemaVersion envelope schema version
     * @param messageId stable platform message id
     * @param correlationId optional business correlation id
     * @param integration integration name
     * @param routeId Camel boundary route id
     * @param attempt platform attempt number
     * @param occurredAt terminal outcome time
     * @param status terminal message status
     * @param steps observed durable step outcomes
     * @param error terminal error, when present
     */
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

    /**
     * Creates an inline-payload result without an external payload reference.
     *
     * @param schemaVersion envelope schema version
     * @param messageId stable platform message id
     * @param correlationId optional business correlation id
     * @param contentType captured payload content type
     * @param sourceSystem optional source-system identity
     * @param targetSystem optional target-system identity
     * @param payloadStorage payload storage discriminator
     * @param payloadSizeBytes captured payload size
     * @param payloadBytes inline payload bytes
     * @param payloadExpiresAt immutable payload expiry
     * @param payloadDeletedAt payload deletion time
     * @param replayable whether policy and storage allow replay
     * @param replayDisabledReason reason replay is unavailable
     * @param integration integration name
     * @param routeId Camel boundary route id
     * @param attempt platform attempt number
     * @param autoRedelivered whether Camel automatically redelivered the exchange
     * @param autoRedeliveryCount completed automatic redeliveries
     * @param autoRedeliveryMax configured maximum automatic redeliveries
     * @param occurredAt terminal outcome time
     * @param status terminal message status
     * @param steps observed durable step outcomes
     * @param error terminal error, when present
     */
    public MessageAttemptResult(
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
        this(schemaVersion, messageId, correlationId, contentType, sourceSystem, targetSystem,
                payloadStorage, payloadSizeBytes, payloadBytes, null, null, payloadExpiresAt,
                payloadDeletedAt, replayable, replayDisabledReason, integration, routeId, attempt,
                autoRedelivered, autoRedeliveryCount, autoRedeliveryMax, occurredAt, status, steps, error);
    }
}
