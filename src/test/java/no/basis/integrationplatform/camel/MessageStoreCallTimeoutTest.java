package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.PostgresStepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepOutcomeStoreException;
import no.basis.integrationplatform.camel.store.MessageStoreSinkException;
import no.basis.integrationplatform.camel.store.PostgresMessageStoreSink;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

/**
 * A Message Store call that waits on the database ends with a store failure within the
 * adapter's timeouts, instead of holding the Kafka consumer thread.
 */
@Testcontainers
class MessageStoreCallTimeoutTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    /** Well over the default lock timeout (5 s), well under the poll interval. */
    private static final Duration BOUND = Duration.ofSeconds(20);

    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = dataSource();
        TestJdbc.awaitReachable(dataSource);
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists bip_message_attempt, bip_message_log, bip_step_outcome");
            for (String resource : List.of("bip_message_store.sql", "bip_step_outcome.sql")) {
                for (String sql : resource(resource).split(";")) {
                    if (!sql.isBlank()) {
                        statement.execute(sql);
                    }
                }
            }
        }
    }

    @Test
    void outcomeWriteBlockedByAnotherTransactionFailsWithinTheLockTimeout() throws Exception {
        PostgresMessageStoreSink sink = new PostgresMessageStoreSink(dataSource);
        try (Connection holder = holdLock("insert into bip_message_log (message_id, integration, schema_version, "
                + "status, last_attempt, occurred_at) values ('message-123', 'orders', 1, 'RECEIVED', 1, now())")) {
            MessageStoreSinkException failure = assertTimeoutPreemptively(BOUND,
                    () -> assertThrows(MessageStoreSinkException.class, () -> sink.record(result())));
            assertEquals("55P03", sqlState(failure));
        }
    }

    @Test
    void stepOutcomeWriteBlockedByAnotherTransactionFailsWithinTheLockTimeout() throws Exception {
        PostgresStepOutcomeStore store = new PostgresStepOutcomeStore(dataSource);
        try (Connection holder = holdLock("insert into bip_step_outcome (idempotency_key, message_id, step_id, status, "
                + "occurred_at) values ('message-123:write', 'message-123', 'write', 'FAILED', now())")) {
            StepOutcomeStoreException failure = assertTimeoutPreemptively(BOUND, () -> assertThrows(
                    StepOutcomeStoreException.class,
                    () -> store.recordDelivered(StepResult.delivered("write", "message-123:write", "sql:write"))));
            assertEquals("55P03", sqlState(failure));
        }
    }

    @Test
    void stepOutcomeCheckBlockedByATableLockFailsWithinTheLockTimeout() throws Exception {
        PostgresStepOutcomeStore store = new PostgresStepOutcomeStore(dataSource);
        try (Connection holder = holdLock("lock table bip_step_outcome in access exclusive mode")) {
            StepOutcomeStoreException failure = assertTimeoutPreemptively(BOUND, () -> assertThrows(
                    StepOutcomeStoreException.class, () -> store.wasDelivered("message-123:write")));
            assertEquals("55P03", sqlState(failure));
        }
    }

    /** Runs the statement in an open transaction on its own connection, which the caller closes. */
    private Connection holdLock(String sql) throws SQLException {
        Connection holder = dataSource.getConnection();
        holder.setAutoCommit(false);
        try (Statement statement = holder.createStatement()) {
            statement.execute(sql);
        }
        return holder;
    }

    private static String sqlState(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof SQLException sql) {
                return sql.getSQLState();
            }
        }
        return null;
    }

    private static MessageAttemptResult result() {
        return new MessageAttemptResult(
                1, "message-123", "order-456", "application/json", "source", "target", "metadata_only",
                null, null, null, null, null, null, false, "replay disabled by policy", "orders", "orders-route", 1,
                false, null, null, Instant.parse("2026-06-14T12:00:00Z"), MessageStatus.COMPLETED, List.of(), null);
    }

    private DataSource dataSource() {
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        return source;
    }

    private String resource(String name) {
        try (InputStream input = getClass().getResourceAsStream("/db/message-store/" + name)) {
            if (input == null) {
                throw new AssertionError("Missing schema resource " + name);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new AssertionError("Failed to read schema resource " + name, e);
        }
    }
}
