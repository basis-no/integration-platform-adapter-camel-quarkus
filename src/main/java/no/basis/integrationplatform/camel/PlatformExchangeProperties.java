package no.basis.integrationplatform.camel;

public final class PlatformExchangeProperties {
    public static final String STEPS = "basis.messageStore.steps";
    public static final String CURRENT_STEP_ID = "basis.messageStore.currentStepId";
    public static final String CURRENT_IDEMPOTENCY_KEY = "basis.messageStore.currentIdempotencyKey";
    public static final String CURRENT_ENDPOINT_URI = "basis.messageStore.currentEndpointUri";
    public static final String STEP_SHOULD_SKIP = "basis.messageStore.stepShouldSkip";
    public static final String REDO_STEPS = "basis.messageStore.redoSteps";
    public static final String STEP_ALREADY_DELIVERED = "basis.messageStore.stepAlreadyDelivered";
    /** The consumed input's message id, captured when a recorded exchange begins. */
    public static final String INPUT_MESSAGE_ID = "basis.messageStore.inputMessageId";
    public static final String KAFKA_MANUAL_COMMIT = "basis.messageStore.kafkaManualCommit";
    public static final String DLQ_PARKED = "basis.messageStore.dlqParked";
    public static final String PAYLOAD_STORAGE = "basis.messageStore.payloadStorage";
    public static final String PAYLOAD_SIZE_BYTES = "basis.messageStore.payloadSizeBytes";
    public static final String PAYLOAD_BYTES = "basis.messageStore.payloadBytes";
    public static final String PAYLOAD_EXPIRES_AT = "basis.messageStore.payloadExpiresAt";
    public static final String PAYLOAD_DELETED_AT = "basis.messageStore.payloadDeletedAt";
    public static final String REPLAYABLE = "basis.messageStore.replayable";
    public static final String REPLAY_DISABLED_REASON = "basis.messageStore.replayDisabledReason";
    public static final String AUTO_REDELIVERED = "basis.messageStore.autoRedelivered";
    public static final String AUTO_REDELIVERY_COUNT = "basis.messageStore.autoRedeliveryCount";
    public static final String AUTO_REDELIVERY_MAX = "basis.messageStore.autoRedeliveryMax";

    private PlatformExchangeProperties() {
    }
}
