package no.basis.integrationplatform.camel;

import org.apache.camel.component.kafka.KafkaConfiguration;

/** The Kafka consumer's effective {@code max.poll.interval.ms}, as the adapter's waits must respect it. */
public final class KafkaPollInterval {
    /** Kafka's default {@code max.poll.interval.ms}. */
    public static final long DEFAULT_MS = 300_000;

    private KafkaPollInterval() {
    }

    /**
     * Returns the consumer's poll interval. An {@code additionalProperties} entry overrides the named
     * option, as Kafka applies it last; without either, Kafka's default applies.
     *
     * @param configuration the consumer endpoint's configuration, or {@code null}
     * @return the poll interval in milliseconds
     */
    public static long of(KafkaConfiguration configuration) {
        if (configuration == null) {
            return DEFAULT_MS;
        }
        try {
            Object override = configuration.getAdditionalProperties() == null ? null
                    : configuration.getAdditionalProperties().get("max.poll.interval.ms");
            if (override != null) {
                long value = Long.parseLong(override.toString().trim());
                if (value > 0) {
                    return value;
                }
            }
            Integer configured = configuration.getMaxPollIntervalMs();
            if (configured != null && configured > 0) {
                return configured;
            }
        } catch (RuntimeException unreadable) {
            // A value Kafka would refuse: its default applies here.
        }
        return DEFAULT_MS;
    }
}
