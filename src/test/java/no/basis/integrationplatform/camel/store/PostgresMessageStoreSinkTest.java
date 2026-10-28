package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
    private static final PostgreSQLContainer POSTGRES = boundedPostgres();

    private static PostgreSQLContainer boundedPostgres() {
        boolean bounded = Boolean.getBoolean("bip.test.bounded-services");
        String runId = System.getProperty("bip.test.run-id");
        if (bounded && (runId == null || runId.isBlank())) {
            throw new IllegalArgumentException("Unique bip.test.run-id is required");
        }
        var postgres = new PostgreSQLContainer(
                bounded ? "postgres@sha256:16bc17c64a573ef34162af9298258d1aec548232985b33ed7b1eac33ba35c229" : "postgres:16-alpine");
        if (bounded) {
            postgres.withImagePullPolicy(image -> false);
            postgres.withLabel("bip.qualification.run", runId);
            postgres.withCreateContainerCmdModifier(command -> {
                var ports = command.getExposedPorts();
                if (ports == null || ports.length == 0) throw new IllegalStateException("Missing synthetic fixture ports");
                var bindings = java.util.Arrays.stream(ports)
                        .map(port -> new com.github.dockerjava.api.model.PortBinding(
                                com.github.dockerjava.api.model.Ports.Binding.bindIp("127.0.0.1"), port))
                        .toArray(com.github.dockerjava.api.model.PortBinding[]::new);
                command.getHostConfig().withMemory(256L * 1024 * 1024).withMemorySwap(256L * 1024 * 1024)
                        .withPidsLimit(64L).withNanoCPUs(1_000_000_000L).withPortBindings(bindings);
            });
        }
        return postgres;
    }

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

    @ParameterizedTest
    @EnumSource(value = MessageStatus.class, names = {"COMPLETED", "FAILED", "DLQ"})
    void lateOlderAttemptPreservesEntireCurrentSummaryButRetainsHistory(MessageStatus latestStatus) {
        MessageStatus olderStatus = latestStatus == MessageStatus.COMPLETED
                ? MessageStatus.FAILED : MessageStatus.COMPLETED;
        sink.record(result(latestStatus, 2, "newer outcome", "newer-payload",
                Instant.parse("2026-06-14T13:00:00Z")));
        String before = singleValue("select row_to_json(m)::text from bip_message_log m where message_id = 'message-123'");

        sink.record(result(olderStatus, 1, "late older outcome", "older-payload",
                Instant.parse("2026-06-14T12:00:00Z")));

        // The guard covers status, payload, error, timestamps and all metadata together.
        assertEquals(before, singleValue("select row_to_json(m)::text from bip_message_log m where message_id = 'message-123'"));
        assertEquals(2, rowCount("bip_message_attempt"));
        assertEquals(olderStatus.name(), singleValue("select status from bip_message_attempt where message_id = 'message-123' and attempt = 1"));
        assertEquals(latestStatus.name(), singleValue("select status from bip_message_attempt where message_id = 'message-123' and attempt = 2"));

        sink.record(result(MessageStatus.COMPLETED, 3, null));
        assertEquals("3", singleValue("select last_attempt from bip_message_log where message_id = 'message-123'"));
    }

    @Test
    void reconsumedOriginalInputAfterReplayKeepsReplayedLatestAttempt() {
        // The original record fails, a replay recovers it, then the original Kafka record is
        // consumed again without an attempt header. It is attempt 1 again and must not reset
        // the summary that the dashboard reads as the recovered latest attempt.
        sink.record(result(MessageStatus.FAILED, 1, "vendor unavailable"));
        sink.record(result(MessageStatus.COMPLETED, 2, null));

        sink.record(result(MessageStatus.COMPLETED, 1, null));

        assertEquals("2", singleValue("select last_attempt from bip_message_log where message_id = 'message-123'"));
        assertEquals("COMPLETED", singleValue("select status from bip_message_log where message_id = 'message-123'"));
        assertEquals(null, singleValue("select error from bip_message_log where message_id = 'message-123'"));
        assertEquals(2, rowCount("bip_message_attempt"));
        assertEquals("COMPLETED", singleValue("select status from bip_message_attempt where message_id = 'message-123' and attempt = 2"));
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({"2, 1", "1, 2"})
    void concurrentAttemptsLeaveTheNewestAttemptAsSummary(int firstAttempt, int waitingAttempt) throws Exception {
        var readyToCommit = new java.util.concurrent.CountDownLatch(1);
        var releaseCommit = new java.util.concurrent.CountDownLatch(1);
        var gated = new PostgresMessageStoreSink(commitGated(dataSource, readyToCommit, releaseCommit));
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var first = executor.submit(() -> gated.record(result(
                    MessageStatus.DLQ, firstAttempt, "attempt " + firstAttempt, "payload-" + firstAttempt,
                    Instant.parse("2026-06-14T12:00:00Z"))));
            assertEquals(true, readyToCommit.await(30, java.util.concurrent.TimeUnit.SECONDS));

            var waiting = executor.submit(() -> sink.record(result(
                    MessageStatus.COMPLETED, waitingAttempt, "attempt " + waitingAttempt, "payload-" + waitingAttempt,
                    Instant.parse("2026-06-14T12:00:00Z"))));
            // The second summary upsert must wait on the uncommitted first one rather than read around it.
            awaitLockWait();
            releaseCommit.countDown();
            first.get(30, java.util.concurrent.TimeUnit.SECONDS);
            waiting.get(30, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            releaseCommit.countDown();
            executor.shutdownNow();
        }

        String newest = Integer.toString(Math.max(firstAttempt, waitingAttempt));
        assertEquals(newest, singleValue("select last_attempt from bip_message_log where message_id = 'message-123'"));
        assertEquals("attempt " + newest, singleValue("select error from bip_message_log where message_id = 'message-123'"));
        assertEquals("payload-" + newest,
                singleValue("select convert_from(payload_bytes, 'UTF8') from bip_message_log where message_id = 'message-123'"));
        assertEquals(2, rowCount("bip_message_attempt"));
    }

    private void awaitLockWait() throws InterruptedException {
        Instant deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            if (!"0".equals(singleValue("select count(*) from pg_stat_activity where wait_event_type = 'Lock'"))) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("Second attempt did not wait for the first attempt's summary lock");
    }

    /** Pauses the first commit so a second attempt can be recorded while it is in flight. */
    private static DataSource commitGated(DataSource delegate, java.util.concurrent.CountDownLatch readyToCommit,
            java.util.concurrent.CountDownLatch releaseCommit) {
        return (DataSource) java.lang.reflect.Proxy.newProxyInstance(
                DataSource.class.getClassLoader(), new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                    Object value = invoke(method, delegate, args);
                    if (!(value instanceof Connection connection)) {
                        return value;
                    }
                    return java.lang.reflect.Proxy.newProxyInstance(
                            Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                            (connectionProxy, connectionMethod, connectionArgs) -> {
                                if (connectionMethod.getName().equals("commit")) {
                                    readyToCommit.countDown();
                                    if (!releaseCommit.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
                                        throw new IllegalStateException("Commit gate was not released");
                                    }
                                }
                                return invoke(connectionMethod, connection, connectionArgs);
                            });
                });
    }

    private static Object invoke(java.lang.reflect.Method method, Object target, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (java.lang.reflect.InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private MessageAttemptResult result(MessageStatus status, int attempt, String error) {
        return result(status, attempt, error, "payload-body", Instant.parse("2026-06-14T12:00:00Z"));
    }

    private MessageAttemptResult result(MessageStatus status, int attempt, String error, String payload, Instant occurredAt) {
        return new MessageAttemptResult(
                1,
                "message-123",
                "order-456",
                "application/json",
                "sap",
                "vendor",
                "postgres_inline",
                128L,
                payload.getBytes(StandardCharsets.UTF_8),
                Instant.parse("2026-07-14T12:00:00Z"),
                null,
                true,
                null,
                "order-pipeline",
                "camel-route-id",
                attempt,
                true,
                2,
                3,
                occurredAt,
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
