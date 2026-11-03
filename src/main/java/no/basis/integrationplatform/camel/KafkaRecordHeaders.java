package no.basis.integrationplatform.camel;

import org.apache.camel.Message;

/**
 * Reads Kafka record headers under both Camel names. camel-kafka 4.18.0 uses
 * {@code kafka.TOPIC}, {@code kafka.PARTITION}, {@code kafka.OFFSET} and
 * {@code kafka.RECORD_META}; later 4.18.x releases use {@code CamelKafkaTopic},
 * {@code CamelKafkaPartition}, {@code CamelKafkaOffset} and {@code CamelKafkaRecordMeta}.
 * The names are literals here because {@code KafkaConstants} values are compiled
 * into the adapter, while applications choose their Camel patch version.
 */
public final class KafkaRecordHeaders {
    private KafkaRecordHeaders() {
    }

    /**
     * Returns the consumed record's topic header.
     * @param message exchange message
     * @return the topic header value, or null
     */
    public static Object topic(Message message) {
        return first(message, "CamelKafkaTopic", "kafka.TOPIC");
    }

    /**
     * Returns the consumed record's partition header.
     * @param message exchange message
     * @return the partition header value, or null
     */
    public static Object partition(Message message) {
        return first(message, "CamelKafkaPartition", "kafka.PARTITION");
    }

    /**
     * Returns the consumed record's offset header.
     * @param message exchange message
     * @return the offset header value, or null
     */
    public static Object offset(Message message) {
        return first(message, "CamelKafkaOffset", "kafka.OFFSET");
    }

    /**
     * Returns the producer's record metadata header.
     * @param message exchange message
     * @return the record metadata header value, or null
     */
    public static Object recordMetadata(Message message) {
        return first(message, "CamelKafkaRecordMeta", "kafka.RECORD_META");
    }

    private static Object first(Message message, String current, String legacy) {
        Object value = message.getHeader(current);
        return value != null ? value : message.getHeader(legacy);
    }
}
