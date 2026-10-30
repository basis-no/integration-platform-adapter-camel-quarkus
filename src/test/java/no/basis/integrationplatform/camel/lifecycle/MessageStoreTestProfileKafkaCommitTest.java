package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformHeaders;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MessageStoreTestProfileKafkaCommitTest {
    @Container
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Test
    void productionProfileCommitsOffsetToBroker() throws Exception {
        OffsetAndMetadata committed = processOneRecord(true);

        assertEquals(1L, committed.offset());
    }

    @Test
    void noCommitProfileLeavesBrokerOffsetUncommitted() throws Exception {
        OffsetAndMetadata committed = processOneRecord(false);

        assertNull(committed);
    }

    private OffsetAndMetadata processOneRecord(boolean commitEnabled) throws Exception {
        String topic = "test-profile-" + UUID.randomUUID();
        String groupId = "test-profile-group-" + UUID.randomUUID();
        createTopic(topic);
        produce(topic, "payload");

        try (KafkaConsumer<String, String> consumer = consumer(groupId)) {
            consumer.subscribe(List.of(topic));
            ConsumerRecord<String, String> record = awaitRecord(consumer);
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());

            Exchange exchange = new DefaultExchange(new DefaultCamelContext());
            exchange.getMessage().setBody(record.value());
            exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, UUID.randomUUID().toString());
            exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT,
                    (KafkaManualCommit) () -> consumer.commitSync(Map.of(
                            partition,
                            new OffsetAndMetadata(record.offset() + 1))));

            MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(
                    result -> {
                    },
                    "adapter-test",
                    PayloadCapturePolicy.defaultPolicy(),
                    MessageBoundaryKind.KAFKA,
                    commitEnabled);

            policy.onExchangeBegin(route("kafka-boundary"), exchange);
            policy.onExchangeDone(route("kafka-boundary"), exchange);

            return consumer.committed(Set.of(partition)).get(partition);
        }
    }

    private void createTopic(String topic) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of(
                "bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get();
        }
    }

    private void produce(String topic, String value) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            producer.send(new ProducerRecord<>(topic, "key", value)).get();
        }
    }

    private KafkaConsumer<String, String> consumer(String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(properties);
    }

    private ConsumerRecord<String, String> awaitRecord(KafkaConsumer<String, String> consumer) {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            var records = consumer.poll(Duration.ofMillis(250));
            if (!records.isEmpty()) {
                return records.iterator().next();
            }
        }
        throw new AssertionError("Kafka record was not consumed before timeout");
    }

    private Route route(String routeId) {
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "getRouteId", "getId" -> routeId;
            case "toString" -> "route(" + routeId + ")";
            default -> throw new UnsupportedOperationException("Unexpected Route method: " + method.getName());
        };
        return (Route) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Route.class}, handler);
    }
}
