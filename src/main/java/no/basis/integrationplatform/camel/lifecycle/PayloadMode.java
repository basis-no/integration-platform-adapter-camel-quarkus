package no.basis.integrationplatform.camel.lifecycle;

/** Adapter payload-capture and replay storage modes. */
public enum PayloadMode {
    /** Store small bytes inline and larger bytes in S3-compatible object storage. */
    TIERED("tiered", "tiered"),
    /** Store only payloads within the Postgres inline threshold. */
    POSTGRES_INLINE("postgres-inline", "postgres_inline"),
    /** Retain metadata without replay payload bytes. */
    METADATA_ONLY("metadata-only", "metadata_only");

    /** Comma-separated supported configuration values. */
    public static final String SUPPORTED_CONFIG_VALUES = "tiered, postgres-inline, metadata-only";

    private final String configValue;
    private final String storageValue;

    PayloadMode(String configValue, String storageValue) {
        this.configValue = configValue;
        this.storageValue = storageValue;
    }

    /**
     * Returns the storage discriminator.
     *
     * @return value persisted in the message-store read model
     */
    public String storageValue() {
        return storageValue;
    }

    /**
     * Parses a configured payload mode.
     *
     * @param value configured mode
     * @return parsed mode
     * @throws IllegalArgumentException when unsupported
     */
    public static PayloadMode fromConfig(String value) {
        if (value == null || value.isBlank()) {
            return POSTGRES_INLINE;
        }
        for (PayloadMode mode : values()) {
            if (mode.configValue.equals(value)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("Unsupported basis.message-store.payload.mode value: " + value
                + ". Supported values: " + SUPPORTED_CONFIG_VALUES);
    }
}
