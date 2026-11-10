package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.PostgresStepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepOutcomeStoreException;
import no.basis.integrationplatform.camel.store.MessageStoreSinkException;
import no.basis.integrationplatform.camel.store.PostgresMessageStoreSink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A commit that a synchronous standby did not confirm is a store failure, and writing the
 * outcome again is safe.
 */
@Testcontainers
class SynchronousCommitTest {
    /** Its own server: the test turns synchronous replication on for every commit. */
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private static final String USER_CANCEL = "canceling wait for synchronous replication due to user request";

    private DataSource direct;

    @BeforeEach
    void setUp() throws Exception {
        direct = dataSource();
        requireStandby(false);
        try (Connection connection = direct.getConnection(); Statement statement = connection.createStatement()) {
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

    @AfterEach
    void standbyNotRequired() throws Exception {
        requireStandby(false);
    }

    @Test
    void aCommitReportedWithASynchronousReplicationWarningIsAStoreFailure() {
        DataSource warned = commitWarning(direct, new SQLWarning(USER_CANCEL, "01000"));

        MessageStoreSinkException sink = assertThrows(MessageStoreSinkException.class,
                () -> new PostgresMessageStoreSink(warned).record(result()));
        assertTrue(message(sink).contains("a synchronous standby did not confirm it"), message(sink));

        StepOutcomeStoreException step = assertThrows(StepOutcomeStoreException.class,
                () -> new PostgresStepOutcomeStore(warned).recordDelivered(delivered()));
        assertTrue(message(step).contains("a synchronous standby did not confirm it"), message(step));
    }

    @Test
    void anyWarningLevelMessageOnCommitIsAFailureButANoticeIsNot() throws Exception {
        // The text follows the server's lc_messages, so a translated warning still fails.
        SQLWarning translated = new SQLWarning("Warten auf synchrone Replikation wird abgebrochen", "01000");
        assertThrows(MessageStoreSinkException.class,
                () -> new PostgresMessageStoreSink(commitWarning(direct, translated)).record(result()));

        SQLWarning terminated = new SQLWarning("canceling the wait for synchronous replication and terminating "
                + "connection due to administrator command", "57P01");
        assertThrows(StepOutcomeStoreException.class,
                () -> new PostgresStepOutcomeStore(commitWarning(direct, terminated)).recordDelivered(delivered()));

        SQLWarning notice = new SQLWarning("a notice", "00000");
        assertDoesNotThrow(() -> new PostgresStepOutcomeStore(commitWarning(direct, notice))
                .recordDelivered(StepResult.delivered("other", "message-123:other", "sql:other")));
    }

    @Test
    void aWarningLeftOnAPooledConnectionBeforeTheCallIsNotMisread() throws Exception {
        try (Connection physical = direct.getConnection()) {
            DataSource stale = staleWarning(physical, new SQLWarning(USER_CANCEL, "01000"));

            assertDoesNotThrow(() -> new PostgresMessageStoreSink(stale).record(result()));
        }
    }

    @Test
    void aCancelledWaitForTheStandbyFailsTheCallAndTheOutcomeIsWrittenAgainOnce() throws Exception {
        requireStandby(true);
        try (Connection physical = direct.getConnection()) {
            DataSource pool = singleConnection(physical);
            int pid = pid(physical);
            PostgresMessageStoreSink sink = new PostgresMessageStoreSink(pool);
            PostgresStepOutcomeStore store = new PostgresStepOutcomeStore(pool);

            MessageStoreSinkException sinkFailure = assertInstanceOf(MessageStoreSinkException.class,
                    cancelWhileWaiting(pid, () -> sink.record(result())));
            assertTrue(message(sinkFailure).contains("a synchronous standby did not confirm it"), message(sinkFailure));
            StepOutcomeStoreException stepFailure = assertInstanceOf(StepOutcomeStoreException.class,
                    cancelWhileWaiting(pid, () -> store.recordDelivered(delivered())));
            assertTrue(message(stepFailure).contains("a synchronous standby did not confirm it"), message(stepFailure));

            // The writes did commit locally; the Kafka record is read again and both are written again.
            assertEquals("1", query("select count(*) from bip_message_attempt"));
            requireStandby(false);
            sink.record(result());
            store.recordDelivered(delivered());

            assertEquals("1", query("select count(*) from bip_message_log"));
            assertEquals("1", query("select count(*) from bip_message_attempt"));
            assertEquals("COMPLETED/1", query("select status || '/' || last_attempt from bip_message_log"));
            assertEquals("1", query("select count(*) from bip_step_outcome where status = 'DELIVERED'"));
        }
    }

    @Test
    void aLoginThatHidesWarningsStillGetsTheUnconfirmedCommitReported() throws Exception {
        try (Connection admin = direct.getConnection(); Statement statement = admin.createStatement()) {
            statement.execute("drop role if exists quiet_writer");
            statement.execute("create role quiet_writer login password 'quiet'");
            statement.execute("grant select, insert, update on bip_message_log, bip_message_attempt, bip_step_outcome "
                    + "to quiet_writer");
            statement.execute("alter role quiet_writer set client_min_messages = error");
        }
        PGSimpleDataSource quiet = new PGSimpleDataSource();
        quiet.setURL(POSTGRES.getJdbcUrl());
        quiet.setUser("quiet_writer");
        quiet.setPassword("quiet");
        requireStandby(true);
        try (Connection physical = quiet.getConnection()) {
            assertEquals("error", queryOn(physical, "show client_min_messages"));
            DataSource pool = singleConnection(physical);
            int pid = pid(physical);

            StepOutcomeStoreException failure = assertInstanceOf(StepOutcomeStoreException.class,
                    cancelWhileWaiting(pid, () -> new PostgresStepOutcomeStore(pool).recordDelivered(delivered())));
            assertTrue(message(failure).contains("a synchronous standby did not confirm it"), message(failure));
            assertEquals("error", queryOn(physical, "show client_min_messages"), "the setting held for the call only");
        }
    }

    @Test
    void theNetworkTimeoutNotTheStatementTimeoutEndsACommitWaitingForTheStandby() throws Exception {
        requireStandby(true);
        MessageStoreTimeouts timeouts = MessageStoreTimeouts.of(Duration.ofSeconds(1), Duration.ofSeconds(1));
        try (Connection physical = direct.getConnection()) {
            long started = System.nanoTime();
            SQLException failure = assertThrows(SQLException.class, () -> timeouts.inTransaction(
                    singleConnection(physical), connection -> {
                        try (var statement = timeouts.prepare(connection,
                                "insert into bip_step_outcome (idempotency_key, message_id, step_id, status, "
                                        + "occurred_at) values ('message-9:write', 'message-9', 'write', "
                                        + "'DELIVERED', now())")) {
                            return statement.executeUpdate();
                        }
                    }));
            Duration waited = Duration.ofNanos(System.nanoTime() - started);

            assertTrue(causedBy(failure, SocketTimeoutException.class), String.valueOf(failure));
            // Past the 1 s statement timeout, at the 6 s network timeout.
            assertTrue(waited.compareTo(Duration.ofSeconds(5)) > 0, waited.toString());
            assertTrue(waited.compareTo(Duration.ofSeconds(15)) < 0, waited.toString());
        }
    }

    /** Runs the call, cancels its backend once it waits for the standby, and returns the call's failure. */
    private Throwable cancelWhileWaiting(int pid, Runnable call) throws Exception {
        CompletableFuture<Void> running = CompletableFuture.runAsync(call);
        if (!waitsForStandby(pid, running, Duration.ofSeconds(10))) {
            throw new AssertionError("The commit did not wait for the synchronous standby");
        }
        cancel(pid);
        ExecutionException failure = assertThrows(ExecutionException.class, () -> running.get(20, TimeUnit.SECONDS));
        return failure.getCause();
    }

    /**
     * Sets or resets the required standby and proves that commits follow it. {@code show
     * synchronous_standby_names} reflects the reload, not whether committing backends
     * already wait, so a throwaway commit on its own connection is the probe: it must wait
     * for the standby (and is then cancelled), or must complete.
     */
    private void requireStandby(boolean required) throws Exception {
        try (Connection connection = direct.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(required
                    ? "alter system set synchronous_standby_names = 'standby_that_never_connects'"
                    : "alter system reset synchronous_standby_names");
            statement.execute("select pg_reload_conf()");
        }
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            try (Connection probe = direct.getConnection()) {
                int pid = pid(probe);
                // A commit waits for the standby only when its transaction wrote WAL; the catalog
                // rows of a temporary table do, and need no table of their own.
                CompletableFuture<Void> commit = CompletableFuture.runAsync(() -> {
                    try (Statement statement = probe.createStatement()) {
                        statement.execute("create temporary table sync_probe (id int)");
                    } catch (SQLException e) {
                        throw new IllegalStateException(e);
                    }
                });
                boolean waited = waitsForStandby(pid, commit, Duration.ofSeconds(2));
                if (waited) {
                    cancel(pid);
                }
                commit.get(10, TimeUnit.SECONDS);
                if (waited == required) {
                    return;
                }
            }
        }
        throw new AssertionError("Commits did not " + (required ? "start" : "stop") + " waiting for the standby");
    }

    private boolean waitsForStandby(int pid, CompletableFuture<?> commit, Duration within) throws Exception {
        Instant deadline = Instant.now().plus(within);
        while (Instant.now().isBefore(deadline) && !commit.isDone()) {
            if ("SyncRep".equals(query("select wait_event from pg_stat_activity where pid = " + pid))) {
                return true;
            }
            Thread.sleep(20);
        }
        return false;
    }

    private void cancel(int pid) throws SQLException {
        try (Connection admin = direct.getConnection(); Statement statement = admin.createStatement()) {
            statement.execute("select pg_cancel_backend(" + pid + ")");
        }
    }

    private static String queryOn(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getString(1);
        }
    }

    private static int pid(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery("select pg_backend_pid()")) {
            resultSet.next();
            return resultSet.getInt(1);
        }
    }

    private String query(String sql) throws SQLException {
        // A read-only session does not wait for the standby.
        try (Connection connection = direct.getConnection(); Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            return resultSet.next() ? resultSet.getString(1) : null;
        }
    }

    private static String message(Throwable failure) {
        StringBuilder text = new StringBuilder();
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            text.append(cause.getMessage()).append(" | ");
        }
        return text.toString();
    }

    private static boolean causedBy(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (type.isInstance(cause)) {
                return true;
            }
        }
        return false;
    }

    /** Connections whose commit succeeds and then reports the warning, as PostgreSQL does. */
    private static DataSource commitWarning(DataSource real, SQLWarning warning) {
        return dataSource(() -> {
            Connection physical = open(real);
            boolean[] committed = {false};
            return connection(physical, (method, args) -> switch (method) {
                case "clearWarnings" -> {
                    committed[0] = false;
                    physical.clearWarnings();
                    yield null;
                }
                case "commit" -> {
                    physical.commit();
                    committed[0] = true;
                    yield null;
                }
                case "getWarnings" -> committed[0] ? warning : physical.getWarnings();
                default -> NOT_HANDLED;
            });
        });
    }

    /** One connection that carries a warning from an earlier use until it is cleared. */
    private static DataSource staleWarning(Connection physical, SQLWarning warning) {
        boolean[] cleared = {false};
        Connection borrowed = connection(physical, (method, args) -> switch (method) {
            case "clearWarnings" -> {
                cleared[0] = true;
                physical.clearWarnings();
                yield null;
            }
            case "getWarnings" -> cleared[0] ? physical.getWarnings() : warning;
            case "close" -> null;
            default -> NOT_HANDLED;
        });
        return dataSource(() -> borrowed);
    }

    private static DataSource singleConnection(Connection physical) {
        Connection borrowed = connection(physical, (method, args) -> "close".equals(method) ? null : NOT_HANDLED);
        return dataSource(() -> borrowed);
    }

    private static final Object NOT_HANDLED = new Object();

    @FunctionalInterface
    private interface Handler {
        Object handle(String method, Object[] args) throws Exception;
    }

    private static Connection connection(Connection physical, Handler override) {
        return (Connection) Proxy.newProxyInstance(SynchronousCommitTest.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                    Object handled = override.handle(method.getName(), args);
                    if (handled != NOT_HANDLED) {
                        return handled;
                    }
                    try {
                        return method.invoke(physical, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private static Connection open(DataSource real) {
        try {
            return real.getConnection();
        } catch (SQLException e) {
            throw new AssertionError(e);
        }
    }

    private static DataSource dataSource(Supplier<Connection> connections) {
        return (DataSource) Proxy.newProxyInstance(SynchronousCommitTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                    if ("getConnection".equals(method.getName())) {
                        return connections.get();
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static MessageAttemptResult result() {
        return new MessageAttemptResult(
                1, "message-123", "order-456", "application/json", "source", "target", "metadata_only",
                null, null, null, null, null, null, false, "replay disabled by policy", "orders", "orders-route", 1,
                false, null, null, Instant.parse("2026-06-14T12:00:00Z"), MessageStatus.COMPLETED, List.of(), null);
    }

    private static StepResult delivered() {
        return StepResult.delivered("write", "message-123:write", "sql:write");
    }

    private static DataSource dataSource() {
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        TestJdbc.awaitReachable(source);
        return source;
    }

    private String resource(String name) {
        try (InputStream input = getClass().getResourceAsStream("/db/message-store/" + name)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new AssertionError("Failed to read schema resource " + name, e);
        }
    }
}
