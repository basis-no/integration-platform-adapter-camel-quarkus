package no.basis.integrationplatform.camel.lifecycle;

public enum PayloadMode {
    POSTGRES_INLINE("postgres-inline", "postgres_inline"),
    METADATA_ONLY("metadata-only", "metadata_only");

    public static final String SUPPORTED_CONFIG_VALUES = "postgres-inline, metadata-only";

    private final String configValue;
    private final String storageValue;

    PayloadMode(String configValue, String storageValue) {
        this.configValue = configValue;
        this.storageValue = storageValue;
    }

    public String storageValue() {
        return storageValue;
    }

    static PayloadMode fromConfig(String value) {
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
