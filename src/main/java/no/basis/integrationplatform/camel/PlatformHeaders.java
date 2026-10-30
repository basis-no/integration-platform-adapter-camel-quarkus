package no.basis.integrationplatform.camel;

public final class PlatformHeaders {
    public static final String MESSAGE_ID = "x-bip-message-id";
    public static final String CORRELATION_ID = "x-bip-correlation-id";
    public static final String SCHEMA_VERSION = "x-bip-schema-version";
    public static final String CONTENT_TYPE = "x-bip-content-type";
    public static final String IDEMPOTENCY_KEY = "x-bip-idempotency-key";
    public static final String SOURCE_SYSTEM = "x-bip-source-system";
    public static final String TARGET_SYSTEM = "x-bip-target-system";
    public static final String REPLAY = "x-bip-replay";
    public static final String ATTEMPT = "x-bip-attempt";
    public static final String REDO_STEPS = "x-bip-redo-steps";
    public static final String ERROR = "x-bip-error";
    public static final String DLQ_AT = "x-bip-dlq-at";

    public static final String HTTP_IDEMPOTENCY_KEY = "Idempotency-Key";

    private PlatformHeaders() {
    }
}
