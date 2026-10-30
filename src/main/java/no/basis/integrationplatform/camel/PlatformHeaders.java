package no.basis.integrationplatform.camel;

/** Canonical BIP envelope and transport header names used by the Camel adapter. */
public final class PlatformHeaders {
    /** Stable message identity retained across replay attempts. */
    public static final String MESSAGE_ID = "x-bip-message-id";
    /** Optional business correlation identity. */
    public static final String CORRELATION_ID = "x-bip-correlation-id";
    /** Envelope schema version. */
    public static final String SCHEMA_VERSION = "x-bip-schema-version";
    /** Original business payload content type. */
    public static final String CONTENT_TYPE = "x-bip-content-type";
    /** Stable platform side-effect idempotency key. */
    public static final String IDEMPOTENCY_KEY = "x-bip-idempotency-key";
    /** Optional source-system identity. */
    public static final String SOURCE_SYSTEM = "x-bip-source-system";
    /** Stable source event identity used during non-Kafka normalization. */
    public static final String SOURCE_EVENT_ID = "x-bip-source-event-id";
    /** Optional target-system identity. */
    public static final String TARGET_SYSTEM = "x-bip-target-system";
    /** Flag indicating a manual platform replay. */
    public static final String REPLAY = "x-bip-replay";
    /** Platform attempt number. */
    public static final String ATTEMPT = "x-bip-attempt";
    /** JSON array of delivered steps an operator requests to re-execute. */
    public static final String REDO_STEPS = "x-bip-redo-steps";
    /** Terminal error attached to a DLQ record. */
    public static final String ERROR = "x-bip-error";
    /** RFC 3339 timestamp attached when a record is parked. */
    public static final String DLQ_AT = "x-bip-dlq-at";
    /** Payload storage discriminator carried by a claim check. */
    public static final String PAYLOAD_STORAGE = "x-bip-payload-storage";
    /** Immutable external payload reference. */
    public static final String PAYLOAD_REF = "x-bip-payload-ref";
    /** Declared payload size in bytes. */
    public static final String PAYLOAD_SIZE = "x-bip-payload-size";
    /** Lowercase payload SHA-256 checksum. */
    public static final String PAYLOAD_SHA256 = "x-bip-payload-sha256";
    /** Transport-level content type used to identify claim-check records. */
    public static final String TRANSPORT_CONTENT_TYPE = "content-type";

    /** Conventional HTTP receiver idempotency header. */
    public static final String HTTP_IDEMPOTENCY_KEY = "Idempotency-Key";

    private PlatformHeaders() {
    }
}
