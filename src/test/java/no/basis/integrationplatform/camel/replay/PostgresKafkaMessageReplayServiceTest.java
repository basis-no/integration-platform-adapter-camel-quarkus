package no.basis.integrationplatform.camel.replay;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import no.basis.integrationplatform.camel.store.PostgresMessageStoreSink;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.builder.RouteBuilder;
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
            public void configure() {
                from("kafka:" + TOPIC + "?brokers=" + KAFKA.getBootstrapServers())
                    .routeId("camel-route-id").autoStartup(false).process(e -> {});
                from("direct:receiver").routeId("receiver-request").process(e -> {});
                from("kafka:another-input?brokers=" + KAFKA.getBootstrapServers())
                    .routeId("other-input").autoStartup(false).process(e -> {});
                from("kafka:" + TOPIC + "?topicIsPattern=true&brokers=" + KAFKA.getBootstrapServers())
                    .routeId("pattern-input").autoStartup(false).process(e -> {});
            }
        });
        context.start();
        replayService = new PostgresKafkaMessageReplayService(dataSource, KAFKA.getBootstrapServers(), "bip.%s.in", context, "order-pipeline");

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
        if (context != null) context.close();
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"receiver-request", "removed-route", "other-input", "pattern-input"})
    void rejectsUnsafeHistoricalRouteBeforeAuditOrPublication(String routeId) throws Exception {
        sink.record(result("unsafe-old", 1, "<ReceiverRequest/>".getBytes(StandardCharsets.UTF_8), true, null));
        try (var c = dataSource.getConnection(); var s = c.prepareStatement(
                "update bip_message_attempt set route_id = ? where message_id = 'unsafe-old'")) {
            s.setString(1, routeId); s.executeUpdate();
        }
        assertThrows(MessageReplayException.class, () -> replayService.replay("unsafe-old"));
        assertEquals("0", singleValue("select count(*) from bip_message_replay_audit"));
    }

    @Test
    void retainedDeliveryAuditIsSearchableButRejectedBeforeReplayAuditOrPublication() throws Exception {
        context.addRoutePolicyFactory(new no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory(
                sink, "order-pipeline", "postgres-inline", 1000, 30, true, "delivery"));
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:delivery").routeId("delivery").setBody(constant("receiver response"));
            }
        });
        context.start();
        try (var producer = context.createProducerTemplate()) {
            for (String body : java.util.List.of("<Delivery>one</Delivery>", "<Delivery>two</Delivery>")) {
                producer.request("direct:delivery", e -> {
                    e.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "original-input");
                    e.getMessage().setBody(body);
                });
            }
        }
        assertEquals("2", singleValue("select count(*) from bip_message_log where correlation_id ilike '%original-input%'"));
        assertEquals("2", singleValue("select count(distinct message_id) from bip_message_log where correlation_id = 'original-input'"));
        assertEquals("2", singleValue("select count(*) from bip_message_log where payload_bytes is not null and replayable = false"));
        assertEquals("2", singleValue("select count(*) from bip_message_attempt where route_id = 'delivery'"));
        String id = singleValue("select message_id from bip_message_log order by message_id limit 1");
        assertThrows(MessageReplayException.class, () -> replayService.replay(id));
        assertEquals("0", singleValue("select count(*) from bip_message_replay_audit"));
        // Also retain the server-side route guard if historical metadata incorrectly claims replayability.
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.executeUpdate("update bip_message_log set replayable = true");
            statement.executeUpdate("update bip_message_attempt set replayable = true");
        }
        assertThrows(MessageReplayException.class, () -> replayService.replay(id));
        assertEquals("0", singleValue("select count(*) from bip_message_replay_audit"));
    }

    @Test
    void rejectsInconsistentStoredPayload() throws Exception {
        sink.record(result("unsafe-old", 1, "<Input/>".getBytes(StandardCharsets.UTF_8), true, null));
        try (var c = dataSource.getConnection(); var s = c.createStatement()) {
            s.executeUpdate("update bip_message_log set payload_bytes = decode('00', 'hex') where message_id = 'unsafe-old'");
        }
        assertThrows(MessageReplayException.class, () -> replayService.replay("unsafe-old"));
        assertEquals("0", singleValue("select count(*) from bip_message_replay_audit"));
    }

    @Test
    void rejectsMissingAttempt() throws Exception {
        sink.record(result("unsafe-old", 1, "<Input/>".getBytes(StandardCharsets.UTF_8), true, null));
        try (var c = dataSource.getConnection(); var s = c.createStatement()) {
            s.executeUpdate("delete from bip_message_attempt where message_id = 'unsafe-old'");
        }
        assertThrows(MessageReplayException.class, () -> replayService.replay("unsafe-old"));
        assertEquals("0", singleValue("select count(*) from bip_message_replay_audit"));
    }

    @Test
    void rejectsAnotherIntegrationsStoredMessage() throws Exception {
        sink.record(result("unsafe-old", 1, "<Input/>".getBytes(StandardCharsets.UTF_8), true, null));
        try (var c = dataSource.getConnection(); var s = c.createStatement()) {
            s.executeUpdate("update bip_message_log set integration = 'other-integration' where message_id = 'unsafe-old'");
        }
        assertThrows(MessageReplayException.class, () -> replayService.replay("unsafe-old"));
        assertEquals("0", singleValue("select count(*) from bip_message_replay_audit"));
    }

    @Test
    void republishesStoredPayloadWithReplayHeadersAndNextAttempt() throws Exception {
        sink.record(result("message-123", 1, "payload-body".getBytes(StandardCharsets.UTF_8), true, null));

        MessageReplayResult result = replayService.replay(new MessageReplayRequest(
                "message-123",
                "vegard@example.com",
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
        assertEquals("vegard@example.com", auditValue(result.replayId(), "requested_by"));
        assertEquals("Retry after vendor outage", auditValue(result.replayId(), "reason"));
        assertEquals("[\"sftp\"]", auditValue(result.replayId(), "redo_steps::text"));
        assertEquals(TOPIC, auditValue(result.replayId(), "target_topic"));
        assertEquals("PUBLISHED", auditValue(result.replayId(), "status"));
        assertNotNull(auditValue(result.replayId(), "published_at::text"));

        try (KafkaConsumer<String, byte[]> consumer = consumer()) {
            consumer.subscribe(List.of(TOPIC));
            var records = consumer.poll(Duration.ofSeconds(10));

            assertEquals(1, records.count());
            var record = records.iterator().next();
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
            // Contract D1: the redo header wire format is the same JSON array as the audit column.
            assertEquals("[\"sftp\"]", header(record, PlatformHeaders.REDO_STEPS));
        }
    }

    @Test
    void consumedResendKeepsOriginalCorrelationForReceiverAndJournal() throws Exception {
        sink.record(result("message-resend", 1, "payload-body".getBytes(StandardCharsets.UTF_8), true, null));
        var receiver = new no.basis.integrationplatform.camel.ReceiverComponent();
        // A topic of its own, so the other tests' consumers never see this resend.
        String resendTopic = "bip.order-pipeline.resend";
        try (var consumer = new DefaultCamelContext()) {
            consumer.addComponent("vendor", receiver);
            consumer.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("kafka:" + resendTopic + "?brokers=" + KAFKA.getBootstrapServers()
                            + "&groupId=resend-" + UUID.randomUUID()
                            + "&autoOffsetReset=earliest&autoCommitEnable=false&allowManualCommit=true")
                        .routeId("camel-route-id")
                        .routePolicy(new no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy(
                                sink, "order-pipeline"))
                        .filter(header(PlatformHeaders.MESSAGE_ID).isEqualTo("message-resend"))
                            .to("vendor:orders").id("sftp");
                }
            });
            consumer.start();

            new PostgresKafkaMessageReplayService(dataSource, KAFKA.getBootstrapServers(), "bip.%s.resend",
                    consumer, "order-pipeline")
                    .replay(new MessageReplayRequest("message-resend", "operator", "redo", List.of("sftp")));

            Instant deadline = Instant.now().plusSeconds(60);
            while (receiver.received().isEmpty()
                    || "0".equals(singleValue("select count(*) from bip_message_attempt where message_id = 'message-resend' and attempt = 2"))) {
                if (Instant.now().isAfter(deadline)) throw new AssertionError("Resend was not consumed and journaled");
                Thread.sleep(100);
            }
        }

        // The receiver gets the text header, not Kafka's bytes, and the journal keeps one correlation.
        assertEquals(List.of("order-456"), receiver.correlations());
        assertEquals("order-456", singleValue(
                "select correlation_id from bip_message_attempt where message_id = 'message-resend' and attempt = 2"));
        assertEquals("1", singleValue(
                "select count(distinct correlation_id) from bip_message_attempt where message_id = 'message-resend'"));
        assertEquals("2", singleValue("select last_attempt from bip_message_log where message_id = 'message-resend'"));
        assertEquals("order-456", singleValue("select correlation_id from bip_message_log where message_id = 'message-resend'"));
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

    private MessageAttemptResult result(
            String messageId,
            int attempt,
            byte[] payload,
            boolean replayable,
            String replayDisabledReason) {
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
                "order-pipeline",
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
