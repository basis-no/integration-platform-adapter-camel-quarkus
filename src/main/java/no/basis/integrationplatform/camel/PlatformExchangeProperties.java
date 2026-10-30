package no.basis.integrationplatform.camel;

/** Internal Camel exchange-property names shared by adapter policies and interceptors. */
public final class PlatformExchangeProperties {
    /** Collected per-attempt durable step outcomes. */
    public static final String STEPS = "basis.messageStore.steps";
    /** Current intercepted stable step id. */
    public static final String CURRENT_STEP_ID = "basis.messageStore.currentStepId";
    /** Current intercepted side-effect idempotency key. */
    public static final String CURRENT_IDEMPOTENCY_KEY = "basis.messageStore.currentIdempotencyKey";
    /** Current intercepted endpoint URI. */
    public static final String CURRENT_ENDPOINT_URI = "basis.messageStore.currentEndpointUri";
    /** Whether the current delivered step should be skipped. */
    public static final String STEP_SHOULD_SKIP = "basis.messageStore.stepShouldSkip";
    /** Parsed redo-step identities for the current attempt. */
    public static final String REDO_STEPS = "basis.messageStore.redoSteps";
    /** Whether the current step has a delivered ledger outcome. */
    public static final String STEP_ALREADY_DELIVERED = "basis.messageStore.stepAlreadyDelivered";
    /** Current replay-semantic step role. */
    public static final String CURRENT_STEP_ROLE = "basis.messageStore.currentStepRole";
    /** Idempotency key injected by the adapter on the current send. */
    public static final String INJECTED_IDEMPOTENCY_KEY = "basis.messageStore.injectedIdempotencyKey";
    /** Captured Kafka manual commit handle. */
    public static final String KAFKA_MANUAL_COMMIT = "basis.messageStore.kafkaManualCommit";
    /** Whether terminal failure was successfully parked in the DLQ. */
    public static final String DLQ_PARKED = "basis.messageStore.dlqParked";
    /** Captured payload storage discriminator. */
    public static final String PAYLOAD_STORAGE = "basis.messageStore.payloadStorage";
    /** Captured payload size in bytes. */
    public static final String PAYLOAD_SIZE_BYTES = "basis.messageStore.payloadSizeBytes";
    /** Captured inline payload bytes. */
    public static final String PAYLOAD_BYTES = "basis.messageStore.payloadBytes";
    /** Captured immutable external payload reference. */
    public static final String PAYLOAD_REF = "basis.messageStore.payloadRef";
    /** Captured payload SHA-256 checksum. */
    public static final String PAYLOAD_SHA256 = "basis.messageStore.payloadSha256";
    /** Compact claim-check bytes to publish instead of a large payload. */
    public static final String PAYLOAD_CLAIM_CHECK = "basis.messageStore.payloadClaimCheck";
    /** Captured payload expiry. */
    public static final String PAYLOAD_EXPIRES_AT = "basis.messageStore.payloadExpiresAt";
    /** Captured payload deletion timestamp. */
    public static final String PAYLOAD_DELETED_AT = "basis.messageStore.payloadDeletedAt";
    /** Whether the captured payload can be replayed. */
    public static final String REPLAYABLE = "basis.messageStore.replayable";
    /** Reason replay is unavailable. */
    public static final String REPLAY_DISABLED_REASON = "basis.messageStore.replayDisabledReason";
    /** Whether Camel automatically redelivered the exchange. */
    public static final String AUTO_REDELIVERED = "basis.messageStore.autoRedelivered";
    /** Number of completed Camel automatic redeliveries. */
    public static final String AUTO_REDELIVERY_COUNT = "basis.messageStore.autoRedeliveryCount";
    /** Configured maximum Camel automatic redeliveries. */
    public static final String AUTO_REDELIVERY_MAX = "basis.messageStore.autoRedeliveryMax";
    /** Exchange-level business-message recording eligibility flag. */
    public static final String RECORD_MESSAGE = "basis.messageStore.recordMessage";
    /** Whether step interception is suppressed for the current send. */
    public static final String STEP_TRACKING_SUPPRESSED = "basis.messageStore.stepTrackingSuppressed";
    /** Durable output checkpoint for a skipped output-producing step. */
    public static final String CURRENT_STEP_CHECKPOINT = "basis.messageStore.currentStepCheckpoint";

    private PlatformExchangeProperties() {
    }
}
