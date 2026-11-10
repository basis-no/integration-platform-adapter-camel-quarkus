package no.basis.integrationplatform.camel.replay;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.TestJdbc;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory;
import no.basis.integrationplatform.camel.store.PostgresMessageStoreSink;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A route enrolled through {@code basis.message-store.boundary-routes} ({@code all} or explicit ids)
 * is never replayed into the input topic unless it is the Kafka input that consumes it: a route that
 * is neither a Kafka nor an HTTP boundary (a nested {@code direct:} route, a poller) is refused at
 * startup, and a Kafka route of another topic
 * records messages that the replay service refuses.
 */
@Testcontainers
class BoundaryRoutesReplayGuardTest {
    private static final String INTEGRATION = "orders";
    private static final String INPUT_TOPIC = "bip.orders.in";
    private static final String OTHER_TOPIC = "partner.events";

    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Container
    private static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        dataSource = source;
        TestJdbc.awaitReachable(dataSource);
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists bip_message_replay_audit");
            statement.execute("drop table if exists " + PostgresMessageStoreSink.MESSAGE_ATTEMPT_TABLE);
            statement.execute("drop table if exists " + PostgresMessageStoreSink.MESSAGE_LOG_TABLE);
            for (String sql : schemaSql().split(";")) {
                if (!sql.isBlank()) {
                    statement.execute(sql);
                }
            }
        }
        try (var admin = AdminClient.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            var existing = admin.listTopics().names().get(20, TimeUnit.SECONDS);
            var missing = List.of(INPUT_TOPIC, OTHER_TOPIC).stream()
                    .filter(topic -> !existing.contains(topic))
                    .map(topic -> new NewTopic(topic, 1, (short) 1))
                    .toList();
            if (!missing.isEmpty()) {
                admin.createTopics(missing).all().get(20, TimeUnit.SECONDS);
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"all", "orders-input,partner-events"})
    void aKafkaRouteOfAnotherTopicIsRecordedButNeverReplayedToTheInputTopic(String boundaryRoutes) throws Exception {
        String inputMessage = "input-" + UUID.randomUUID();
        String otherMessage = "event-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(boundaryRoutes));
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from(kafka(INPUT_TOPIC)).routeId("orders-input").process(e -> { });
                    from(kafka(OTHER_TOPIC)).routeId("partner-events").process(e -> { });
                }
            });
            context.start();
            publish(INPUT_TOPIC, inputMessage, "<Order/>");
            publish(OTHER_TOPIC, otherMessage, "<Event/>");
            awaitStatus(inputMessage, "COMPLETED");
            awaitStatus(otherMessage, "COMPLETED");
            // Both enrolled routes recorded a replayable message.
            assertEquals("true", value("select replayable::text from bip_message_log where message_id = ?", otherMessage));
            assertEquals("partner-events",
                    value("select route_id from bip_message_attempt where message_id = ?", otherMessage));

            var replay = new PostgresKafkaMessageReplayService(dataSource, brokers, "bip.%s.in", context, INTEGRATION);
            long inputPublished = published(INPUT_TOPIC);
            long otherPublished = published(OTHER_TOPIC);

            MessageReplayException refused = assertThrows(MessageReplayException.class,
                    () -> replay.replay(otherMessage));

            assertTrue(refused.getMessage().contains("input route does not consume this integration's replay topic "
                    + INPUT_TOPIC), refused.getMessage());
            assertEquals(inputPublished, published(INPUT_TOPIC), "Nothing may be published to the input topic");
            assertEquals(otherPublished, published(OTHER_TOPIC), "Nothing may be published to the other topic");
            assertEquals("0", value("select count(*)::text from bip_message_replay_audit where message_id = ?",
                    otherMessage));

            // The Kafka input itself is still replayed to its own topic.
            MessageReplayResult replayed = replay.replay(inputMessage);
            assertEquals(INPUT_TOPIC, replayed.topic());
            assertEquals(inputPublished + 1, published(INPUT_TOPIC));
        }
    }

    @Test
    void allRefusesToStartWithANestedDirectRoute() throws Exception {
        assertRefusedAtStartup("all", "orders-enrich", new RouteBuilder() {
            @Override
            public void configure() {
                from(kafka(INPUT_TOPIC)).routeId("orders-input").to("direct:enrich");
                from("direct:enrich").routeId("orders-enrich").process(e -> { });
            }
        });
    }

    @Test
    void anExplicitNestedDirectRouteIsRefusedAtStartup() throws Exception {
        assertRefusedAtStartup("orders-input,orders-enrich", "orders-enrich", new RouteBuilder() {
            @Override
            public void configure() {
                from(kafka(INPUT_TOPIC)).routeId("orders-input").to("direct:enrich");
                from("direct:enrich").routeId("orders-enrich").process(e -> { });
            }
        });
    }

    @Test
    void anExplicitPollingRouteIsRefusedAtStartup() throws Exception {
        assertRefusedAtStartup("poll-orders", "poll-orders", new RouteBuilder() {
            @Override
            public void configure() {
                from("file:target/boundary-guard-inbox").routeId("poll-orders").autoStartup(false).process(e -> { });
            }
        });
    }

    private void assertRefusedAtStartup(String boundaryRoutes, String routeId, RouteBuilder routes) throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(boundaryRoutes));
            Exception thrown = assertThrows(Exception.class, () -> {
                context.addRoutes(routes);
                context.start();
            });
            String message = causes(thrown);
            assertTrue(message.contains("Non-Kafka boundary route '" + routeId
                    + "' uses a source that has no supported message-store profile"), message);
        }
    }

    private MessageStoreRoutePolicyFactory factory(String boundaryRoutes) {
        return new MessageStoreRoutePolicyFactory(new PostgresMessageStoreSink(dataSource), INTEGRATION,
                "postgres-inline", 100_000, 30, true, "dlq", boundaryRoutes);
    }

    private static String kafka(String topic) {
        return "kafka:" + topic + "?brokers=" + KAFKA.getBootstrapServers() + "&groupId=" + topic + "-"
                + UUID.randomUUID() + "&autoOffsetReset=earliest&autoCommitEnable=false&allowManualCommit=true"
                + "&pollTimeoutMs=100";
    }

    private static String causes(Throwable thrown) {
        StringBuilder messages = new StringBuilder();
        for (Throwable cause = thrown; cause != null; cause = cause.getCause()) {
            messages.append(cause.getMessage()).append('\n');
        }
        return messages.toString();
    }

    private static void publish(String topic, String messageId, String body) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        properties.put("key.serializer", StringSerializer.class.getName());
        properties.put("value.serializer", StringSerializer.class.getName());
        try (var producer = new KafkaProducer<String, String>(properties)) {
            var record = new ProducerRecord<String, String>(topic, messageId, body);
            record.headers().add(PlatformHeaders.MESSAGE_ID, messageId.getBytes(StandardCharsets.UTF_8));
            producer.send(record).get(10, TimeUnit.SECONDS);
        }
    }

    private void awaitStatus(String messageId, String status) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        String current = null;
        while (System.nanoTime() < deadline) {
            current = value("select status from bip_message_log where message_id = ?", messageId);
            if (status.equals(current)) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Message " + messageId + " is " + current + ", not " + status);
    }

    private String value(String sql, String parameter) throws Exception {
        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement(sql)) {
            statement.setString(1, parameter);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next() ? resultSet.getString(1) : null;
            }
        }
    }

    /** The number of records ever published to the topic. */
    private static long published(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "replay-guard-verifier-" + UUID.randomUUID());
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        try (var consumer = new KafkaConsumer<String, byte[]>(properties)) {
            var partitions = consumer.partitionsFor(topic, Duration.ofSeconds(10)).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();
            return consumer.endOffsets(partitions, Duration.ofSeconds(10)).values().stream()
                    .mapToLong(Long::longValue).sum();
        }
    }

    private static String schemaSql() throws Exception {
        try (InputStream in = BoundaryRoutesReplayGuardTest.class
                .getResourceAsStream("/db/message-store/bip_message_store.sql")) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
