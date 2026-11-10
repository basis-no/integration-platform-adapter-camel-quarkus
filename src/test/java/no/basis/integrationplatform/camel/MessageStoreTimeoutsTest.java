package no.basis.integrationplatform.camel;

import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.net.SocketTimeoutException;
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
