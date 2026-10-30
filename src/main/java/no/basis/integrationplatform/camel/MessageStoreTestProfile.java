package no.basis.integrationplatform.camel;

import java.util.Arrays;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * Explicitly unsafe profiles for local and controlled integration testing.
 */
public enum MessageStoreTestProfile {
    /** Production behavior: configured persistence and managed commits remain enabled. */
    DISABLED("disabled", true, true),
    /** Replace durable adapter persistence with logging/no-op stores. */
    NO_PERSISTENCE("no-persistence", false, true),
    /** Preserve persistence but suppress the adapter-managed Kafka commit. */
    NO_COMMIT("no-commit", true, false),
    /** Disable both durable adapter persistence and managed Kafka commits. */
    NO_PERSISTENCE_NO_COMMIT("no-persistence-no-commit", false, false);

    /** Comma-separated supported test-profile values. */
    public static final String SUPPORTED_VALUES = Arrays.stream(values())
            .map(MessageStoreTestProfile::configValue)
            .collect(Collectors.joining(", "));

    private final String configValue;
    private final boolean persistenceEnabled;
    private final boolean kafkaCommitEnabled;

    MessageStoreTestProfile(String configValue, boolean persistenceEnabled, boolean kafkaCommitEnabled) {
        this.configValue = configValue;
        this.persistenceEnabled = persistenceEnabled;
        this.kafkaCommitEnabled = kafkaCommitEnabled;
    }

    /**
     * Returns the external configuration spelling.
     *
     * @return configuration value
     */
    public String configValue() {
        return configValue;
    }

    /**
     * Reports whether configured durable sinks and stores remain enabled.
     *
     * @return persistence state
     */
    public boolean persistenceEnabled() {
        return persistenceEnabled;
    }

    /**
     * Reports whether the adapter may commit a consumed Kafka offset.
     *
     * @return commit state
     */
    public boolean kafkaCommitEnabled() {
        return kafkaCommitEnabled;
    }

    /**
     * Reports whether an unsafe test profile is active.
     *
     * @return active state
     */
    public boolean active() {
        return this != DISABLED;
    }

    /**
     * Parses a configured test profile.
     *
     * @param value configured profile
     * @return parsed profile
     * @throws IllegalArgumentException when unsupported
     */
    public static MessageStoreTestProfile fromConfig(String value) {
        String normalized = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
        return Arrays.stream(values())
                .filter(profile -> profile.configValue.equals(normalized))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "Unsupported basis.message-store.test-profile value: " + value
                                + ". Supported values: " + SUPPORTED_VALUES));
    }
}
