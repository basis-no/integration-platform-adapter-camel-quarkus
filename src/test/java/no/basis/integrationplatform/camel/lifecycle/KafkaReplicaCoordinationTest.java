package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.PostgresStepOutcomeStore;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class KafkaReplicaCoordinationTest {
    @Container
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private static PostgresStepOutcomeStore stepStore;

    @BeforeAll
    static void createStepLedger() throws Exception {
        DataSource dataSource = dataSource();
        TestJdbcSupport.awaitReachable(dataSource);
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            for (String sql : schemaSql().split(";")) {
                if (!sql.isBlank()) {
                    statement.execute(sql);
                }
            }
        }
        stepStore = new PostgresStepOutcomeStore(dataSource);
    }

    @Test
    void sharedGroupDistributesPartitionsRetainsKeyOrderAndRebalances() throws Exception {
        String topic = topic("replicas");
        String group = "replica-group-" + UUID.randomUUID();
        createTopic(topic, 4);

        try (ConsumerWorker first = new ConsumerWorker(topic, group, true);
             ConsumerWorker second = new ConsumerWorker(topic, group, true)) {
            first.start();
            second.start();

            await("two consumers to share four partitions", () ->
                    !first.assignment().isEmpty()
                            && !second.assignment().isEmpty()
                            && union(first.assignment(), second.assignment()).size() == 4
                            && intersection(first.assignment(), second.assignment()).isEmpty());

            List<String> orderedValues = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                String value = "ordered-" + i;
                orderedValues.add(value);
                produce(topic, "same-business-key", value);
            }
            await("same-key records", () -> first.recordCount() + second.recordCount() >= orderedValues.size());

            List<String> observed = first.valuesForKey("same-business-key");
            if (observed.isEmpty()) {
                observed = second.valuesForKey("same-business-key");
            }
            assertEquals(orderedValues, observed,
                    "Records with one key must stay on one partition and retain partition order");

            first.close();
            await("remaining consumer to own all partitions", () -> second.assignment().size() == 4);

            int before = second.recordCount();
            for (int partition = 0; partition < 4; partition++) {
                produce(topic, partition, "after-rebalance-" + partition, "value-" + partition);
            }
            await("remaining replica to process all partitions", () -> second.recordCount() >= before + 4);
        }
    }

    @Test
    void uncommittedRecordIsRedeliveredAndDurableStepMarkerSurvivesReplicaFailure() throws Exception {
        String topic = topic("redelivery");
        String group = "redelivery-group-" + UUID.randomUUID();
        String messageId = UUID.randomUUID().toString();
        String stepId = "create-external-order";
        String idempotencyKey = messageId + ":" + stepId;
        createTopic(topic, 1);
        produce(topic, messageId, "payload");

        try (ConsumerWorker failedReplica = new ConsumerWorker(topic, group, false)) {
            failedReplica.start();
            await("first replica to receive record", () -> failedReplica.recordCount() == 1);

            stepStore.recordDelivered(StepResult.delivered(
                    stepId, idempotencyKey, "https://partner.example/orders"));
        }

        try (ConsumerWorker replacementReplica = new ConsumerWorker(topic, group, true)) {
            replacementReplica.start();
            await("replacement replica to receive uncommitted record", () -> replacementReplica.recordCount() == 1);

            assertEquals(messageId, replacementReplica.records().getFirst().key());
            assertTrue(stepStore.wasDelivered(idempotencyKey),
                    "The replacement replica must see the durable marker and skip the external side effect");
        }
    }

    private static String topic(String purpose) {
        return "adapter-" + purpose + "-" + UUID.randomUUID();
    }

    private static void createTopic(String topic, int partitions) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        try (AdminClient admin = AdminClient.create(properties)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        }
    }

    private static void produce(String topic, String key, String value) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
            producer.send(new ProducerRecord<>(topic, key, value)).get();
        }
    }

    private static void produce(String topic, int partition, String key, String value) throws Exception {
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerProperties())) {
            producer.send(new ProducerRecord<>(topic, partition, key, value)).get();
        }
    }

    private static Properties producerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static Set<TopicPartition> union(Set<TopicPartition> first, Set<TopicPartition> second) {
        Set<TopicPartition> union = new HashSet<>(first);
        union.addAll(second);
        return union;
    }

    private static Set<TopicPartition> intersection(Set<TopicPartition> first, Set<TopicPartition> second) {
        Set<TopicPartition> intersection = new HashSet<>(first);
        intersection.retainAll(second);
        return intersection;
    }

    private static void await(String description, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Timed out waiting for " + description);
    }

    private static DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }

    private static String schemaSql() throws Exception {
        try (InputStream input = KafkaReplicaCoordinationTest.class
                .getResourceAsStream("/db/message-store/bip_step_outcome.sql")) {
            if (input == null) {
                throw new IllegalStateException("Missing step outcome schema");
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8).lines()
                    .filter(line -> !line.strip().startsWith("--"))
                    .collect(java.util.stream.Collectors.joining("\n"));
        }
    }

    private static final class TestJdbcSupport {
        private static void awaitReachable(DataSource dataSource) throws Exception {
            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (true) {
                try (Connection ignored = dataSource.getConnection()) {
                    return;
                } catch (Exception e) {
                    if (System.nanoTime() >= deadline) {
                        throw e;
                    }
                    Thread.sleep(100);
                }
            }
        }
    }

    private static final class ConsumerWorker implements AutoCloseable {
        private final KafkaConsumer<String, String> consumer;
        private final boolean commitRecords;
        private final CopyOnWriteArrayList<ConsumerRecord<String, String>> records = new CopyOnWriteArrayList<>();
        private final AtomicReference<Set<TopicPartition>> assignment = new AtomicReference<>(Set.of());
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final String topic;
        private final Thread thread;
        private volatile boolean running = true;
        private volatile boolean started;

        private ConsumerWorker(String topic, String group, boolean commitRecords) {
            this.topic = topic;
            this.commitRecords = commitRecords;
            this.consumer = new KafkaConsumer<>(consumerProperties(group));
            this.thread = new Thread(this::run, "kafka-replica-" + UUID.randomUUID());
        }

        private void start() {
            started = true;
            thread.start();
        }

        private void run() {
            try {
                consumer.subscribe(List.of(topic));
                while (running) {
                    var polled = consumer.poll(Duration.ofMillis(100));
                    assignment.set(Set.copyOf(consumer.assignment()));
                    polled.forEach(records::add);
                    if (commitRecords && !polled.isEmpty()) {
                        consumer.commitSync();
                    }
                }
            } catch (WakeupException e) {
                if (running) {
                    failure.set(e);
                }
            } catch (Throwable t) {
                failure.set(t);
            } finally {
                consumer.close(Duration.ofSeconds(5));
            }
        }

        private static Properties consumerProperties(String group) {
            Properties properties = new Properties();
            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
            properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            return properties;
        }

        private Set<TopicPartition> assignment() {
            rethrowFailure();
            return assignment.get();
        }

        private int recordCount() {
            rethrowFailure();
            return records.size();
        }

        private List<ConsumerRecord<String, String>> records() {
            rethrowFailure();
            return List.copyOf(records);
        }

        private List<String> valuesForKey(String key) {
            rethrowFailure();
            return records.stream()
                    .filter(record -> key.equals(record.key()))
                    .map(ConsumerRecord::value)
                    .toList();
        }

        private void rethrowFailure() {
            Throwable thrown = failure.get();
            if (thrown != null) {
                throw new AssertionError("Kafka consumer worker failed", thrown);
            }
        }

        @Override
        public void close() throws Exception {
            if (!started || !thread.isAlive()) {
                rethrowFailure();
                return;
            }
            running = false;
            consumer.wakeup();
            thread.join(Duration.ofSeconds(10));
            assertFalse(thread.isAlive(), "Kafka consumer worker did not stop");
            rethrowFailure();
        }
    }
}
