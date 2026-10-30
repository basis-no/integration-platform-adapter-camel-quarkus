package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class PostgresMessageStoreSinkTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private DataSource dataSource;
    private PostgresMessageStoreSink sink;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = dataSource();
        no.basis.integrationplatform.camel.TestJdbc.awaitReachable(dataSource);
        sink = new PostgresMessageStoreSink(dataSource);

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists " + PostgresMessageStoreSink.MESSAGE_ATTEMPT_TABLE);
            statement.execute("drop table if exists " + PostgresMessageStoreSink.MESSAGE_LOG_TABLE);
            for (String sql : schemaSql().split(";")) {
                if (!sql.isBlank()) {
                    statement.execute(sql);
                }
            }
        }
    }

    @Test
    void recordsCompletedMessageAttemptAndCurrentMessageLog() {
        sink.record(result(MessageStatus.COMPLETED, 1, null));

        assertEquals(1, rowCount("bip_message_log"));
        assertEquals(1, rowCount("bip_message_attempt"));
        assertEquals("COMPLETED", singleValue("select status from bip_message_log where message_id = 'message-123'"));
        assertEquals("1", singleValue("select last_attempt from bip_message_log where message_id = 'message-123'"));
        assertEquals("application/json", singleValue("select content_type from bip_message_log where message_id = 'message-123'"));
        assertEquals("sap", singleValue("select source_system from bip_message_log where message_id = 'message-123'"));
        assertEquals("vendor", singleValue("select target_system from bip_message_log where message_id = 'message-123'"));
        assertEquals("postgres_inline", singleValue("select payload_storage from bip_message_log where message_id = 'message-123'"));
        assertEquals("128", singleValue("select payload_size_bytes from bip_message_log where message_id = 'message-123'"));
        assertEquals("payload-body", singleValue("select convert_from(payload_bytes, 'UTF8') from bip_message_log where message_id = 'message-123'"));
        assertEquals("true", singleValue("select replayable::text from bip_message_log where message_id = 'message-123'"));
        assertEquals("true", singleValue("select auto_redelivered::text from bip_message_log where message_id = 'message-123'"));
        assertEquals("2", singleValue("select auto_redelivery_count from bip_message_log where message_id = 'message-123'"));
        assertEquals("3", singleValue("select auto_redelivery_max from bip_message_log where message_id = 'message-123'"));
        assertEquals("postgres_inline", singleValue("select payload_storage from bip_message_attempt where message_id = 'message-123'"));
        assertEquals("payload-body", singleValue("select convert_from(payload_bytes, 'UTF8') from bip_message_attempt where message_id = 'message-123'"));
        assertEquals("true", singleValue("select replayable::text from bip_message_attempt where message_id = 'message-123'"));
        assertEquals("true", singleValue("select auto_redelivered::text from bip_message_attempt where message_id = 'message-123'"));
        assertEquals("2", singleValue("select auto_redelivery_count from bip_message_attempt where message_id = 'message-123'"));
        assertEquals("3", singleValue("select auto_redelivery_max from bip_message_attempt where message_id = 'message-123'"));
        assertEquals("order-456", singleValue("select correlation_id from bip_message_attempt where message_id = 'message-123'"));
        assertEquals("camel-route-id", singleValue("select route_id from bip_message_attempt where message_id = 'message-123'"));
        assertEquals("sftp", singleValue("select steps_json->0->>'step' from bip_message_attempt where message_id = 'message-123'"));
        assertEquals("DELIVERED", singleValue("select steps_json->0->>'status' from bip_message_attempt where message_id = 'message-123'"));
        assertEquals("message-123:sftp", singleValue("select steps_json->0->>'idempotencyKey' from bip_message_attempt where message_id = 'message-123'"));
    }

    @Test
    void duplicateAttemptIsIdempotent() {
        sink.record(result(MessageStatus.FAILED, 1, "first failure"));
        sink.record(result(MessageStatus.COMPLETED, 1, null));

        assertEquals(1, rowCount("bip_message_log"));
        assertEquals(1, rowCount("bip_message_attempt"));
        assertEquals("COMPLETED", singleValue("select status from bip_message_log where message_id = 'message-123'"));
        assertEquals("COMPLETED", singleValue("select status from bip_message_attempt where message_id = 'message-123' and attempt = 1"));
    }

    @Test
    void laterAttemptUpdatesCurrentMessageLog() {
        sink.record(result(MessageStatus.FAILED, 1, "first failure"));
        sink.record(result(MessageStatus.COMPLETED, 2, null));

        assertEquals(1, rowCount("bip_message_log"));
        assertEquals(2, rowCount("bip_message_attempt"));
        assertEquals("COMPLETED", singleValue("select status from bip_message_log where message_id = 'message-123'"));
        assertEquals("2", singleValue("select last_attempt from bip_message_log where message_id = 'message-123'"));
        assertEquals("FAILED", singleValue("select status from bip_message_attempt where message_id = 'message-123' and attempt = 1"));
        assertEquals("COMPLETED", singleValue("select status from bip_message_attempt where message_id = 'message-123' and attempt = 2"));
    }

    // Retention 30 days: a replay resets an inline expiry, at most to first receipt + 60 days.
    private static final java.time.Duration RETENTION = java.time.Duration.ofDays(30);
    private static final Instant T0 = Instant.parse("2026-06-14T12:00:00Z");

    @Test
    void replayResetsTheInlinePayloadExpiry() {
        var capped = new PostgresMessageStoreSink(dataSource, RETENTION);
        capped.record(received(1, "postgres_inline", java.time.Duration.ZERO));
        capped.record(received(2, "postgres_inline", java.time.Duration.ofDays(20)));
        assertEquals(epoch(java.time.Duration.ofDays(50)), expiry("bip_message_log", ""));
        assertEquals(epoch(java.time.Duration.ofDays(50)), expiry("bip_message_attempt", " and attempt = 2"));
        assertEquals(epoch(java.time.Duration.ofDays(30)), expiry("bip_message_attempt", " and attempt = 1"));
    }

    @Test
    void replayInlineExpiryIsCappedAtFirstReceiptPlusTwiceTheRetention() {
        var capped = new PostgresMessageStoreSink(dataSource, RETENTION);
        capped.record(received(1, "postgres_inline", java.time.Duration.ZERO));
        capped.record(received(2, "postgres_inline", java.time.Duration.ofDays(20)));
        capped.record(received(3, "postgres_inline", java.time.Duration.ofDays(45)));
        assertEquals(epoch(java.time.Duration.ofDays(60)), expiry("bip_message_log", ""));
        assertEquals(epoch(java.time.Duration.ofDays(60)), expiry("bip_message_attempt", " and attempt = 3"));
    }

    @Test
    void objectPayloadKeepsItsFirstExpiry() {
        var capped = new PostgresMessageStoreSink(dataSource, RETENTION);
        capped.record(received(1, "s3_object", java.time.Duration.ZERO));
        capped.record(received(2, "s3_object", java.time.Duration.ofDays(20)));
        assertEquals(epoch(java.time.Duration.ofDays(30)), expiry("bip_message_log", ""));
        assertEquals(epoch(java.time.Duration.ofDays(30)), expiry("bip_message_attempt", " and attempt = 2"));
    }

    private MessageAttemptResult received(int attempt, String storage, java.time.Duration after) {
        boolean inline = storage.equals("postgres_inline");
        return new MessageAttemptResult(1, "message-123", "order-456", "application/json", "sap", "vendor", storage, 128L,
                inline ? "payload-body".getBytes(StandardCharsets.UTF_8) : null,
                inline ? null : "s3://bucket/orders/message-123", "0".repeat(64),
                T0.plus(after).plus(RETENTION), null, true, null, "order-pipeline", "camel-route-id", attempt, false, null,
                null, T0.plus(after), MessageStatus.COMPLETED, List.of(), null);
    }

    private String epoch(java.time.Duration after) {
        return Long.toString(T0.plus(after).getEpochSecond());
    }

    private String expiry(String table, String attemptFilter) {
        return singleValue("select extract(epoch from payload_expires_at)::bigint from " + table
                + " where message_id = 'message-123'" + attemptFilter);
    }

    private MessageAttemptResult result(MessageStatus status, int attempt, String error) {
        return result(status, attempt, error, Instant.parse("2026-07-14T12:00:00Z"));
    }

    private MessageAttemptResult result(MessageStatus status, int attempt, String error, Instant payloadExpiresAt) {
        return new MessageAttemptResult(
                1,
                "message-123",
                "order-456",
                "application/json",
                "sap",
                "vendor",
                "postgres_inline",
                128L,
                "payload-body".getBytes(StandardCharsets.UTF_8),
                payloadExpiresAt,
                null,
                true,
                null,
                "order-pipeline",
                "camel-route-id",
                attempt,
                true,
                2,
                3,
                Instant.parse("2026-06-14T12:00:00Z"),
                status,
                List.of(new StepResult(
                        "sftp",
                        status == MessageStatus.COMPLETED ? StepStatus.DELIVERED : StepStatus.FAILED,
                        "message-123:sftp",
                        "sftp:vendor/orders",
                        "vendor/orders/order-456.xml",
                        error,
                        Instant.parse("2026-06-14T12:00:01Z"))),
                error);
    }

    private int rowCount(String table) {
        return Integer.parseInt(singleValue("select count(*) from " + table));
    }

    private String singleValue(String sql) {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getString(1);
        } catch (Exception e) {
            throw new AssertionError("Failed to query test database", e);
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
