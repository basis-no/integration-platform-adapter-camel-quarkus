package no.basis.integrationplatform.camel.replay;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import no.basis.integrationplatform.camel.store.PostgresMessageStoreSink;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
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
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class PostgresKafkaMessageReplayServiceTest {
    private static final String TOPIC = "bip.order-pipeline.in";

    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Container
    private static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    private DefaultCamelContext context;
    private DataSource dataSource;
    private PostgresMessageStoreSink sink;
    private PostgresKafkaMessageReplayService replayService;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = dataSource();
        no.basis.integrationplatform.camel.TestJdbc.awaitReachable(dataSource);
        sink = new PostgresMessageStoreSink(dataSource);
        context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("kafka:" + TOPIC + "?brokers=" + KAFKA.getBootstrapServers())
                        .routeId("camel-route-id").autoStartup(false).process(e -> { });
                from("direct:receiver").routeId("receiver-request").process(e -> { });
                from("kafka:another-input?brokers=" + KAFKA.getBootstrapServers())
                        .routeId("other-input").autoStartup(false).process(e -> { });
                from("kafka:" + TOPIC + "?topicIsPattern=true&brokers=" + KAFKA.getBootstrapServers())
                        .routeId("pattern-input").autoStartup(false).process(e -> { });
            }
        });
        context.start();
        replayService = new PostgresKafkaMessageReplayService(
                dataSource, KAFKA.getBootstrapServers(), "bip.%s.in", context, "order-pipeline");

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
    }

    @AfterEach
    void stopContext() throws Exception {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void republishesStoredPayloadWithReplayHeadersAndNextAttempt() throws Exception {
        sink.record(result("message-123", 1, "payload-body".getBytes(StandardCharsets.UTF_8), true, null));

        MessageReplayResult result = replayService.replay(new MessageReplayRequest(
                "message-123",
                "operator@example.com",
                "Retry after vendor outage",
                List.of("sftp")));

        assertNotNull(result.replayId());
        assertEquals("message-123", result.messageId());
        assertEquals("order-pipeline", result.integration());
        assertEquals(TOPIC, result.topic());
        assertEquals(2, result.attempt());
        assertEquals("message-123", auditValue(result.replayId(), "message_id"));
        assertEquals("order-pipeline", auditValue(result.replayId(), "integration"));
        assertEquals("2", auditValue(result.replayId(), "replay_attempt"));
        assertEquals("operator@example.com", auditValue(result.replayId(), "requested_by"));
        assertEquals("Retry after vendor outage", auditValue(result.replayId(), "reason"));
        assertEquals("[\"sftp\"]", auditValue(result.replayId(), "redo_steps::text"));
        assertEquals(TOPIC, auditValue(result.replayId(), "target_topic"));
        assertEquals("PUBLISHED", auditValue(result.replayId(), "status"));
        assertNotNull(auditValue(result.replayId(), "published_at::text"));

        try (KafkaConsumer<String, byte[]> consumer = consumer()) {
            consumer.subscribe(List.of(TOPIC));
            // Other tests publish to the same topic on the shared broker.
            var records = new java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]>>();
            consumer.poll(Duration.ofSeconds(10)).forEach(candidate -> {
                var id = candidate.headers().lastHeader(PlatformHeaders.MESSAGE_ID);
                if (id != null && "message-123".equals(new String(id.value(), StandardCharsets.UTF_8))) {
                    records.add(candidate);
                }
            });

            assertEquals(1, records.size());
            var record = records.getFirst();
            assertEquals("order-456", record.key());
            assertEquals("payload-body", new String(record.value(), StandardCharsets.UTF_8));
            assertEquals("message-123", header(record, PlatformHeaders.MESSAGE_ID));
            assertEquals("order-456", header(record, PlatformHeaders.CORRELATION_ID));
            assertEquals("application/json", header(record, PlatformHeaders.CONTENT_TYPE));
            assertEquals("1", header(record, PlatformHeaders.SCHEMA_VERSION));
            assertEquals("sap", header(record, PlatformHeaders.SOURCE_SYSTEM));
            assertEquals("vendor", header(record, PlatformHeaders.TARGET_SYSTEM));
            assertEquals("true", header(record, PlatformHeaders.REPLAY));
            assertEquals("2", header(record, PlatformHeaders.ATTEMPT));
            // The redo header wire format is the same JSON array as the audit column.
            assertEquals("[\"sftp\"]", header(record, PlatformHeaders.REDO_STEPS));
        }
    }

    @Test
    void rejectsNonReplayableMessageBeforePublishing() {
        sink.record(result("message-456", 1, null, false, "payload storage mode is metadata-only"));

        MessageReplayException thrown = assertThrows(
                MessageReplayException.class,
                () -> replayService.replay("message-456"));

        assertEquals("Message message-456 is not replayable: payload storage mode is metadata-only", thrown.getMessage());
        assertEquals("0", singleValue("select count(*) from bip_message_replay_audit where message_id = 'message-456'"));
    }

    @Test
    void refusesRedoWithoutTheStepLedgerBeforeAuditOrPublication() {
        sink.record(result("message-789", 1, "payload-body".getBytes(StandardCharsets.UTF_8), true, null));
        var withoutLedger = new PostgresKafkaMessageReplayService(
                dataSource, KAFKA.getBootstrapServers(), "bip.%s.in", context, "order-pipeline", false);
        long published = published(TOPIC);

        MessageReplayException thrown = assertThrows(MessageReplayException.class, () -> withoutLedger.replay(
                new MessageReplayRequest("message-789", "operator@example.com", "Redo the upload", List.of("sftp"))));

        assertEquals("Message message-789 is not replayable with redoSteps: they need"
                + " a step outcome store that records delivered steps, such as"
                + " basis.message-store.step-outcome-store=postgres; with noop every step runs on every replay."
                + " Resend without redoSteps.", thrown.getMessage());
        assertNothingAuditedOrPublished(TOPIC, published);
    }

    @Test
    void theStepStoreInUseDecidesWhetherRedoIsAccepted() {
        sink.record(result("message-791", 1, "payload-body".getBytes(StandardCharsets.UTF_8), true, null));
        var redo = new MessageReplayRequest("message-791", "operator@example.com", "Redo the upload", List.of("sftp"));
        var recording = new no.basis.integrationplatform.camel.step.StepOutcomeStore() {
            public boolean wasDelivered(String key) { return false; }
            public void recordDelivered(StepResult step) { }
            public void recordFailed(StepResult step) { }
        };

        assertThrows(MessageReplayException.class,
                () -> configured(new no.basis.integrationplatform.camel.step.NoopStepOutcomeStore()).replay(redo));
        assertThrows(MessageReplayException.class, () -> configured(null).replay(redo));
        // An application's own store that records delivered steps, in place of the adapter's.
        assertEquals(2, configured(recording).replay(redo).attempt());
    }

    @Test
    void theStepStoreIsResolvedOnceForAllRedoRequests() {
        var redo = new MessageReplayRequest("message-792", "operator@example.com", "Redo the upload", List.of("sftp"));
        var service = configured(new no.basis.integrationplatform.camel.step.NoopStepOutcomeStore());

        assertThrows(MessageReplayException.class, () -> service.replay(redo));
        assertThrows(MessageReplayException.class, () -> service.replay(redo));

        assertEquals(1, resolutions.get(), "A dependent store must not be created again for every request");
    }

    private final java.util.concurrent.atomic.AtomicInteger resolutions = new java.util.concurrent.atomic.AtomicInteger();

    @SuppressWarnings("unchecked")
    private PostgresKafkaMessageReplayService configured(no.basis.integrationplatform.camel.step.StepOutcomeStore store) {
        var dataSources = (jakarta.enterprise.inject.Instance<DataSource>) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{jakarta.enterprise.inject.Instance.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "isResolvable" -> true;
                    case "get" -> dataSource;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        var stores = (jakarta.enterprise.inject.Instance<no.basis.integrationplatform.camel.step.StepOutcomeStore>)
                java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[]{jakarta.enterprise.inject.Instance.class},
                        (proxy, method, args) -> switch (method.getName()) {
                            case "isResolvable" -> store != null;
                            case "get" -> {
                                resolutions.incrementAndGet();
                                yield store;
                            }
                            default -> throw new UnsupportedOperationException(method.getName());
                        });
        return new PostgresKafkaMessageReplayService(dataSources, java.util.Optional.of(KAFKA.getBootstrapServers()),
                "bip.%s.in", no.basis.integrationplatform.camel.payload.PayloadObjectStore.disabled(), context,
                "order-pipeline", stores);
    }

    @Test
    void replaysWithoutRedoAndIgnoresBlankRedoEntriesWithoutTheStepLedger() {
        sink.record(result("message-790", 1, "payload-body".getBytes(StandardCharsets.UTF_8), true, null));
        var withoutLedger = new PostgresKafkaMessageReplayService(
                dataSource, KAFKA.getBootstrapServers(), "bip.%s.in", context, "order-pipeline", false);
        long published = published(TOPIC);

        withoutLedger.replay(MessageReplayRequest.forMessage("message-790"));
        withoutLedger.replay(new MessageReplayRequest("message-790", null, null, List.of(" ")));

        assertEquals(published + 2, published(TOPIC));
    }

    @Test
    void refusesAnotherIntegrationsMessageBeforeAuditOrPublication() {
        sink.record(result("other-message", 1, "<Input/>".getBytes(StandardCharsets.UTF_8), true, null,
                "other-integration"));
        long published = published("bip.other-integration.in");

        MessageReplayException thrown = assertThrows(
                MessageReplayException.class,
                () -> replayService.replay("other-message"));

        assertEquals("Message other-message is not replayable by this integration", thrown.getMessage());
        assertNothingAuditedOrPublished("bip.other-integration.in", published);
    }

    @ParameterizedTest
    @ValueSource(strings = {"receiver-request", "removed-route", "other-input", "pattern-input"})
    void refusesAMessageWhoseInputRouteDoesNotConsumeTheReplayTopic(String routeId) throws Exception {
        sink.record(result("unsafe-old", 1, "<Input/>".getBytes(StandardCharsets.UTF_8), true, null));
        try (Connection connection = dataSource.getConnection();
             var statement = connection.prepareStatement(
                     "update bip_message_attempt set route_id = ? where message_id = 'unsafe-old'")) {
            statement.setString(1, routeId);
            statement.executeUpdate();
        }
        long published = published(TOPIC);

        MessageReplayException thrown = assertThrows(
                MessageReplayException.class,
                () -> replayService.replay("unsafe-old"));

        assertTrue(thrown.getMessage().contains("input route does not consume"), thrown.getMessage());
        assertNothingAuditedOrPublished(TOPIC, published);
    }

    @Test
    void refusesAStoredPayloadThatDoesNotMatchItsLastAttempt() {
        sink.record(result("unsafe-old", 1, "<Input/>".getBytes(StandardCharsets.UTF_8), true, null));
        update("update bip_message_log set payload_bytes = decode('00', 'hex') where message_id = 'unsafe-old'");
        long published = published(TOPIC);

        assertThrows(MessageReplayException.class, () -> replayService.replay("unsafe-old"));
        assertNothingAuditedOrPublished(TOPIC, published);
    }

    @Test
    void refusesAMessageWithoutItsLastAttempt() {
        sink.record(result("unsafe-old", 1, "<Input/>".getBytes(StandardCharsets.UTF_8), true, null));
        update("delete from bip_message_attempt where message_id = 'unsafe-old'");
        long published = published(TOPIC);

        assertThrows(MessageReplayException.class, () -> replayService.replay("unsafe-old"));
        assertNothingAuditedOrPublished(TOPIC, published);
    }

    private void assertNothingAuditedOrPublished(String topic, long publishedBefore) {
        assertEquals("0", singleValue("select count(*) from bip_message_replay_audit"));
        assertEquals(publishedBefore, published(topic), "Nothing may be published to " + topic);
    }

    /** The number of records ever published to the topic; other tests share the broker. */
    private long published(String topic) {
        try (KafkaConsumer<String, byte[]> consumer = consumer()) {
            var partitions = consumer.partitionsFor(topic, Duration.ofSeconds(10)).stream()
                    .map(info -> new org.apache.kafka.common.TopicPartition(info.topic(), info.partition()))
                    .toList();
            return consumer.endOffsets(partitions, Duration.ofSeconds(10)).values().stream()
                    .mapToLong(Long::longValue).sum();
        }
    }

    private void update(String sql) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (Exception e) {
            throw new AssertionError("Failed to update test database with: " + sql, e);
        }
    }

    private MessageAttemptResult result(
            String messageId,
            int attempt,
            byte[] payload,
            boolean replayable,
            String replayDisabledReason) {
        return result(messageId, attempt, payload, replayable, replayDisabledReason, "order-pipeline");
    }

    private MessageAttemptResult result(
            String messageId,
            int attempt,
            byte[] payload,
            boolean replayable,
            String replayDisabledReason,
            String integration) {
        return new MessageAttemptResult(
                1,
                messageId,
                "order-456",
                "application/json",
                "sap",
                "vendor",
                payload == null ? "metadata_only" : "postgres_inline",
                payload == null ? null : (long) payload.length,
                payload,
                replayable ? Instant.now().plus(30, ChronoUnit.DAYS) : null,
                null,
                replayable,
                replayDisabledReason,
                integration,
                "camel-route-id",
                attempt,
                false,
                null,
                null,
                Instant.parse("2026-06-14T12:00:00Z"),
                MessageStatus.COMPLETED,
                List.of(new StepResult(
                        "sftp",
                        StepStatus.DELIVERED,
                        messageId + ":sftp",
                        "sftp:vendor/orders",
                        "vendor/orders/order-456.xml",
                        null,
                        Instant.parse("2026-06-14T12:00:01Z"))),
                null);
    }

    private KafkaConsumer<String, byte[]> consumer() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "replay-verifier-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(properties);
    }

    private String header(org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> record, String name) {
        var header = record.headers().lastHeader(name);
        assertNotNull(header, "Missing Kafka header " + name);
        return new String(header.value(), StandardCharsets.UTF_8);
    }

    private String auditValue(String replayId, String column) {
        return singleValue("select " + column + " from bip_message_replay_audit where replay_id = '" + replayId + "'");
    }

    private String singleValue(String sql) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getString(1);
        } catch (Exception e) {
            throw new AssertionError("Failed to query test database with: " + sql, e);
        }
    }

    private DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }

    private String schemaSql() {
        try (InputStream input = getClass().getResourceAsStream("/db/message-store/bip_message_store.sql")) {
            if (input == null) {
                throw new AssertionError("Missing bip_message_store schema resource");
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new AssertionError("Failed to read bip_message_store schema resource", e);
        }
    }
}
