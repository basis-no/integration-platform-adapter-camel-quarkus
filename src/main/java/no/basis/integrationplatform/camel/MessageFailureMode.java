package no.basis.integrationplatform.camel;

import java.util.Locale;

/** Determines how an exhausted delivery is finalized after adapter retries. */
public enum MessageFailureMode {
    /** Park exhausted Kafka delivery in the configured DLQ. */
    DLQ("dlq"),
    /** Record terminal failure and propagate it to a non-Kafka source. */
    RECORD_FAILED("record-failed");

    /** Comma-separated values accepted by adapter configuration. */
    public static final String SUPPORTED_CONFIG_VALUES = "dlq, record-failed";

    private final String configValue;

    MessageFailureMode(String configValue) {
        this.configValue = configValue;
    }

    /**
     * Parses an adapter configuration value.
     *
     * @param value configured failure mode
     * @return parsed mode
     * @throws IllegalArgumentException when the value is unsupported
     */
    public static MessageFailureMode fromConfig(String value) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        for (MessageFailureMode mode : values()) {
            if (mode.configValue.equals(normalized)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
                "Unsupported basis.message-store.failure-mode '" + value
                        + "'; expected one of: " + SUPPORTED_CONFIG_VALUES);
    }

    /**
     * Returns the external configuration spelling.
     *
     * @return configuration value
     */
    public String configValue() {
        return configValue;
    }
}
