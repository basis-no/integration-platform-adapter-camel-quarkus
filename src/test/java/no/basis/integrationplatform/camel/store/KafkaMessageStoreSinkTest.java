package no.basis.integrationplatform.camel.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
class KafkaMessageStoreSinkTest {
    private static final String TOPIC = "bip.message.status";

    @Container
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void emitsTerminalStatusEventToKafka() throws Exception {
        KafkaMessageStoreSink sink = new KafkaMessageStoreSink(
                KafkaMessageStoreSink.kafkaProducer(KAFKA.getBootstrapServers()),
                TOPIC,
                new MessageStatusEventJson());
        MessageAttemptResult result = new MessageAttemptResult(
                1,
                "message-123",
                "order-456",
                "order-pipeline",
                "camel-route-id",
                1,
                Instant.parse("2026-06-14T12:00:00Z"),
                MessageStatus.COMPLETED,
                List.of(new StepResult(
                        "webhook",
                        StepStatus.DELIVERED,
                        "message-123:webhook",
                        "https://vendor.example.com/callback",
                        null,
                        null,
                        Instant.parse("2026-06-14T12:00:01Z"))),
                null);

        try {
            sink.record(result);

            try (KafkaConsumer<String, String> consumer = consumer()) {
                consumer.subscribe(List.of(TOPIC));
                var records = consumer.poll(Duration.ofSeconds(10));

                assertEquals(1, records.count());
                var record = records.iterator().next();
                assertEquals("message-123", record.key());
                assertEquals("message-123", header(record, PlatformHeaders.MESSAGE_ID));
                assertEquals("order-456", header(record, PlatformHeaders.CORRELATION_ID));
                assertEquals("1", header(record, PlatformHeaders.SCHEMA_VERSION));
                assertEquals("application/json", header(record, PlatformHeaders.CONTENT_TYPE));

                JsonNode json = objectMapper.readTree(record.value());
                assertEquals("message-123", json.get("messageId").asText());
                assertEquals("order-pipeline", json.get("integration").asText());
                assertEquals("COMPLETED", json.get("status").asText());
                assertEquals("webhook", json.get("steps").get(0).get("step").asText());
            }
        } finally {
            sink.close();
        }
    }

    private KafkaConsumer<String, String> consumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "status-event-verifier-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    private String header(org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record, String name) {
        var header = record.headers().lastHeader(name);
        assertNotNull(header, "Missing Kafka header " + name);
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
