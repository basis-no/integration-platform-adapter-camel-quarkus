package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.PostgresStepOutcomeStore;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class MessageStoreTimeoutsTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private static final MessageStoreTimeouts SHORT = MessageStoreTimeouts.of(Duration.ofSeconds(1), Duration.ofSeconds(1));

    @Test
    void defaultsBoundACallWellUnderTheKafkaPollInterval() {
        MessageStoreTimeouts defaults = MessageStoreTimeouts.DEFAULT;
        assertEquals(Duration.ofSeconds(10), defaults.statementTimeout());
        assertEquals(Duration.ofSeconds(5), defaults.lockTimeout());
        assertEquals(Duration.ofSeconds(15), defaults.networkTimeout());
        assertTrue(defaults.networkTimeout().compareTo(Duration.ofSeconds(60)) < 0);
    }

    @Test
    void refusesTimeoutsThatWouldNotBoundACall() {
        assertThrows(IllegalArgumentException.class, () -> MessageStoreTimeouts.of(Duration.ZERO, Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class, () -> MessageStoreTimeouts.of(Duration.ofSeconds(1), Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> MessageStoreTimeouts.of(null, Duration.ofSeconds(1)));
        // Under 1 ms would be sent as "0ms", which turns the timeout off.
        assertThrows(IllegalArgumentException.class,
                () -> MessageStoreTimeouts.of(Duration.ofNanos(999_999), Duration.ofNanos(999_999)));
        assertThrows(IllegalArgumentException.class,
                () -> MessageStoreTimeouts.of(Duration.ofSeconds(1), Duration.ofNanos(999_999)));
        MessageStoreTimeouts.of(Duration.ofMillis(1), Duration.ofMillis(1));
        // The network timeout (statement timeout plus 5 s) must fit an int of milliseconds.
        IllegalArgumentException tooLong = assertThrows(IllegalArgumentException.class,
                () -> MessageStoreTimeouts.of(Duration.ofMillis(Integer.MAX_VALUE), Duration.ofSeconds(1)));
        assertTrue(tooLong.getMessage().contains("network timeout"), tooLong.getMessage());
        MessageStoreTimeouts.of(Duration.ofMillis(Integer.MAX_VALUE).minusSeconds(5), Duration.ofSeconds(1));
        IllegalArgumentException longerLock = assertThrows(IllegalArgumentException.class,
                () -> MessageStoreTimeouts.of(Duration.ofSeconds(1), Duration.ofSeconds(2)));
        assertTrue(longerLock.getMessage().contains("basis.message-store.lock-timeout"), longerLock.getMessage());
    }

    @Test
    void aSlowStatementEndsWithTheStatementTimeout() {
        SQLException failure = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> assertThrows(SQLException.class,
                () -> SHORT.inTransaction(dataSource(), connection -> query(connection, "select pg_sleep(30)"))));
        assertEquals("57014", failure.getSQLState());
    }

    @Test
    void theTimeoutsHoldForTheCallOnlyAndLeaveThePooledConnectionAsItWas() throws Exception {
        try (Connection physical = dataSource().getConnection()) {
            DataSource pool = singleConnection(physical);
            String sessionDefault = query(physical, "show statement_timeout");
            int networkDefault = physical.getNetworkTimeout();

            String during = SHORT.inTransaction(pool, connection -> {
                assertEquals(6000, connection.getNetworkTimeout());
                assertFalse(connection.getAutoCommit());
                return query(connection, "show statement_timeout") + "/" + query(connection, "show lock_timeout");
            });

            assertEquals("1s/1s", during);
            assertEquals(sessionDefault, query(physical, "show statement_timeout"));
            assertEquals(sessionDefault, query(physical, "show lock_timeout"));
            assertEquals(networkDefault, physical.getNetworkTimeout());
            assertTrue(physical.getAutoCommit());

            assertThrows(SQLException.class, () -> SHORT.inTransaction(pool,
                    connection -> query(connection, "select pg_sleep(30)")));
            assertEquals(sessionDefault, query(physical, "show statement_timeout"));
            assertEquals(networkDefault, physical.getNetworkTimeout());
            assertTrue(physical.getAutoCommit());
        }
    }

    @Test
    void aCallOnAConnectionAlreadyInATransactionJoinsItWithoutChangingIt() throws Exception {
        try (Connection physical = dataSource().getConnection()) {
            physical.setAutoCommit(false);
            execute(physical, "create temporary table joined (id int) on commit preserve rows");
            physical.commit();
            DataSource pool = singleConnection(physical);
            String sessionDefault = query(physical, "show statement_timeout");
            int networkDefault = physical.getNetworkTimeout();
            execute(physical, "insert into joined values (1)");

            String during = SHORT.inTransactionOrJoin(pool, connection -> {
                assertEquals(6000, connection.getNetworkTimeout());
                try (var statement = SHORT.prepare(connection, "insert into joined values (2)")) {
                    statement.executeUpdate();
                }
                return query(connection, "show statement_timeout");
            });

            assertEquals(sessionDefault, during, "no transaction-local setting in the application's transaction");
            assertFalse(physical.getAutoCommit());
            assertEquals(networkDefault, physical.getNetworkTimeout());
            assertEquals("2", query(physical, "select count(*) from joined"));
            physical.rollback();
            assertEquals("0", query(physical, "select count(*) from joined"), "the call neither committed nor began");
        }
    }

    @Test
    void aStatementInAJoinedTransactionEndsWithItsQueryTimeout() throws Exception {
        try (Connection physical = dataSource().getConnection()) {
            physical.setAutoCommit(false);
            DataSource pool = singleConnection(physical);
            SQLException failure = assertTimeoutPreemptively(Duration.ofSeconds(10), () -> assertThrows(SQLException.class,
                    () -> SHORT.inTransactionOrJoin(pool, connection -> {
                        try (var statement = SHORT.prepare(connection, "select pg_sleep(30)")) {
                            return statement.execute();
                        }
                    })));
            assertEquals("57014", failure.getSQLState());
            assertFalse(physical.getAutoCommit(), "the application's transaction is left to the application");
            physical.rollback();
        }
    }

    @Test
    void theCommittingCallCommitsAConnectionAlreadyInATransactionAsPlainJdbcDoes() throws Exception {
        DataSource direct = dataSource();
        try (Connection setup = direct.getConnection()) {
            execute(setup, "drop table if exists committed_call");
            execute(setup, "create table committed_call (id int)");
        }
        try (Connection physical = direct.getConnection()) {
            physical.setAutoCommit(false);
            SHORT.inTransaction(singleConnection(physical), connection -> {
                try (var statement = SHORT.prepare(connection, "insert into committed_call values (1)")) {
                    return statement.executeUpdate();
                }
            });
            assertFalse(physical.getAutoCommit());
        }
        try (Connection other = direct.getConnection()) {
            assertEquals("1", query(other, "select count(*) from committed_call"));
        }
    }

    @Test
    void theStepOutcomeStoreWorksInsideAnApplicationTransaction() throws Exception {
        DataSource direct = dataSource();
        try (Connection setup = direct.getConnection()) {
            execute(setup, "drop table if exists bip_step_outcome");
            try (InputStream input = getClass().getResourceAsStream("/db/message-store/bip_step_outcome.sql")) {
                for (String sql : new String(input.readAllBytes(), StandardCharsets.UTF_8).split(";")) {
                    if (!sql.isBlank()) {
                        execute(setup, sql);
                    }
                }
            }
        }
        try (Connection physical = direct.getConnection()) {
            physical.setAutoCommit(false);
            PostgresStepOutcomeStore store = new PostgresStepOutcomeStore(singleConnection(physical));

            store.recordDelivered(StepResult.delivered("write", "message-123:write", "sql:write"));

            assertTrue(store.wasDelivered("message-123:write"));
            assertFalse(physical.getAutoCommit());
            physical.rollback();
            assertFalse(store.wasDelivered("message-123:write"), "the outcome belonged to the rolled-back transaction");
        }
    }

    @Test
    void aDatabaseThatStopsAnsweringEndsWithTheNetworkTimeout() throws Exception {
        try (Connection physical = dataSource().getConnection()) {
            DataSource pool = singleConnection(physical);
            String containerId = POSTGRES.getContainerId();
            POSTGRES.getDockerClient().pauseContainerCmd(containerId).exec();
            try {
                SQLException failure = assertTimeoutPreemptively(Duration.ofSeconds(20), () -> assertThrows(
                        SQLException.class, () -> SHORT.inTransaction(pool, connection -> query(connection, "select 1"))));
                assertTrue(causedBy(failure, SocketTimeoutException.class), String.valueOf(failure));
            } finally {
                POSTGRES.getDockerClient().unpauseContainerCmd(containerId).exec();
            }
        }
    }

    private static boolean causedBy(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (type.isInstance(cause)) {
                return true;
            }
        }
        return false;
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String query(Connection connection, String sql) throws SQLException {
        try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getString(1);
        }
    }

    /** A pool of one: every call gets the same physical connection, and closing it keeps it open. */
    private static DataSource singleConnection(Connection physical) {
        Connection borrowed = (Connection) Proxy.newProxyInstance(MessageStoreTimeoutsTest.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    try {
                        return method.invoke(physical, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        return (DataSource) Proxy.newProxyInstance(MessageStoreTimeoutsTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class}, (proxy, method, args) -> {
                    if ("getConnection".equals(method.getName())) {
                        return borrowed;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static DataSource dataSource() {
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        TestJdbc.awaitReachable(source);
        return source;
    }
}
