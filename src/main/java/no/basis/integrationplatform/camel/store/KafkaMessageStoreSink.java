package no.basis.integrationplatform.camel.store;

import jakarta.annotation.PreDestroy;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/** Publishes terminal lifecycle events for a separate central persistence service. */
public class KafkaMessageStoreSink implements MessageStoreSink {
    /** Default topic for runtime-neutral lifecycle events. */
    public static final String DEFAULT_STATUS_TOPIC = "bip.message.status";

    private final Producer<String, String> producer;
    private final String statusTopic;
    private final MessageStatusEventJson serializer;

    /**
     * Creates a Kafka lifecycle-event sink.
     *
     * @param bootstrapServers Kafka bootstrap servers
     * @param statusTopic lifecycle-event topic
     */
    public KafkaMessageStoreSink(
            @ConfigProperty(name = "kafka.bootstrap.servers") String bootstrapServers,
            @ConfigProperty(name = "basis.message-store.status-topic", defaultValue = DEFAULT_STATUS_TOPIC) String statusTopic) {
        this(kafkaProducer(bootstrapServers), statusTopic, new MessageStatusEventJson());
    }

    KafkaMessageStoreSink(
            Producer<String, String> producer,
            String statusTopic,
            MessageStatusEventJson serializer) {
        this.producer = producer;
        this.statusTopic = statusTopic;
        this.serializer = serializer;
    }

    @Override
    public void record(MessageAttemptResult result) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                statusTopic,
                result.messageId(),
                serializer.serialize(result));
        addHeader(record, PlatformHeaders.MESSAGE_ID, result.messageId());
        addHeader(record, PlatformHeaders.CORRELATION_ID, result.correlationId());
        addHeader(record, PlatformHeaders.SCHEMA_VERSION, Integer.toString(result.schemaVersion()));
        addHeader(record, PlatformHeaders.CONTENT_TYPE, "application/json");

        producer.send(record).get();
    }

    @PreDestroy
    void close() {
        producer.close();
    }

    private void addHeader(ProducerRecord<String, String> record, String name, String value) {
        if (value != null) {
            record.headers().add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
        }
    }

    static Producer<String, String> kafkaProducer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        no.basis.integrationplatform.camel.KafkaClientSecurity.apply(properties);
        return new KafkaProducer<>(properties);
    }
}
