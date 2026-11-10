package no.basis.integrationplatform.camel;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.util.concurrent.Executor;

/**
 * Bounds each Message Store call, so that a database that hangs or a lock that is held
 * ends the call with a store failure instead of holding the Kafka consumer thread past
 * its poll interval.
 *
 * <p>A call runs in its own transaction with transaction-local {@code statement_timeout}
 * and {@code lock_timeout}, so a pooled connection keeps no session setting. The JDBC
 * network timeout, which ends a read from a database that stopped answering, is set to
 * the statement timeout plus {@value #NETWORK_MARGIN_SECONDS} seconds for the call and
 * restored afterwards.</p>
 */
public final class MessageStoreTimeouts {
    /** Default statement timeout for one Message Store call. */
    public static final Duration DEFAULT_STATEMENT_TIMEOUT = Duration.ofSeconds(10);
    /** Default lock timeout for one Message Store call. */
    public static final Duration DEFAULT_LOCK_TIMEOUT = Duration.ofSeconds(5);
    /** The default timeouts. */
    public static final MessageStoreTimeouts DEFAULT = new MessageStoreTimeouts(
            DEFAULT_STATEMENT_TIMEOUT, DEFAULT_LOCK_TIMEOUT);

    private static final long NETWORK_MARGIN_SECONDS = 5;
    private static final String APPLY_SQL =
            "select set_config('statement_timeout', ?, true), set_config('lock_timeout', ?, true)";
    private static final Executor DIRECT = Runnable::run;

    private final Duration statementTimeout;
    private final Duration lockTimeout;

    private MessageStoreTimeouts(Duration statementTimeout, Duration lockTimeout) {
        this.statementTimeout = statementTimeout;
        this.lockTimeout = lockTimeout;
    }

    /**
     * Returns validated timeouts.
     *
     * @param statementTimeout longest a statement may run; positive
     * @param lockTimeout longest a statement may wait for a lock; positive and no longer
     *        than the statement timeout
     * @return the timeouts
     * @throws IllegalArgumentException when a value is missing, not positive, or the lock
     *         timeout is longer than the statement timeout
     */
    public static MessageStoreTimeouts of(Duration statementTimeout, Duration lockTimeout) {
        requirePositive("basis.message-store.statement-timeout", statementTimeout);
        requirePositive("basis.message-store.lock-timeout", lockTimeout);
        if (lockTimeout.compareTo(statementTimeout) > 0) {
            throw new IllegalArgumentException("basis.message-store.lock-timeout (" + lockTimeout
                    + ") must not be longer than basis.message-store.statement-timeout (" + statementTimeout + ")");
        }
        return new MessageStoreTimeouts(statementTimeout, lockTimeout);
    }

    /**
     * Returns the statement timeout.
     *
     * @return the statement timeout
     */
    public Duration statementTimeout() {
        return statementTimeout;
    }

    /**
     * Returns the lock timeout.
     *
     * @return the lock timeout
     */
    public Duration lockTimeout() {
        return lockTimeout;
    }

    /**
     * Returns the JDBC network timeout applied during a call.
     *
     * @return the statement timeout plus a margin
     */
    public Duration networkTimeout() {
        return statementTimeout.plusSeconds(NETWORK_MARGIN_SECONDS);
    }

    /**
     * Runs work in one transaction on a connection from the data source, with these
     * timeouts, and commits it. On failure the transaction is rolled back and the failure
     * thrown; a timeout is an {@link SQLException} like any other database failure.
     *
     * @param dataSource the Message Store data source
     * @param work the statements to run
     * @param <T> the result type
     * @return the work's result
     * @throws Exception the work's or the database's failure
     */
    public <T> T inTransaction(DataSource dataSource, Work<T> work) throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            int previousNetworkTimeout = networkTimeout(connection);
            boolean previousAutoCommit = connection.getAutoCommit();
            Exception failure = null;
            try {
                setNetworkTimeout(connection, Math.toIntExact(networkTimeout().toMillis()));
                connection.setAutoCommit(false);
                apply(connection);
                T result = work.run(connection);
                connection.commit();
                return result;
            } catch (Exception e) {
                failure = e;
                try {
                    connection.rollback();
                } catch (SQLException rollback) {
                    e.addSuppressed(rollback);
                }
                throw e;
            } finally {
                try {
                    connection.setAutoCommit(previousAutoCommit);
                    setNetworkTimeout(connection, previousNetworkTimeout);
                } catch (SQLException restore) {
                    // Only a broken connection cannot be restored; the pool discards it. After a
                    // commit the call has succeeded, so this must not turn it into a failure.
                    if (failure != null) {
                        failure.addSuppressed(restore);
                    }
                }
            }
        }
    }

    private void apply(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(APPLY_SQL)) {
            statement.setString(1, statementTimeout.toMillis() + "ms");
            statement.setString(2, lockTimeout.toMillis() + "ms");
            statement.execute();
        }
    }

    private static int networkTimeout(Connection connection) throws SQLException {
        try {
            return connection.getNetworkTimeout();
        } catch (SQLFeatureNotSupportedException e) {
            return -1;
        }
    }

    private static void setNetworkTimeout(Connection connection, int millis) throws SQLException {
        if (millis < 0) {
            return;
        }
        try {
            connection.setNetworkTimeout(DIRECT, millis);
        } catch (SQLFeatureNotSupportedException e) {
            // The statement and lock timeouts still apply.
        }
    }

    private static void requirePositive(String property, Duration value) {
        if (value == null || value.isZero() || value.isNegative()) {
            throw new IllegalArgumentException(property + " must be a positive duration, was " + value);
        }
    }

    /**
     * Statements run in one Message Store transaction.
     *
     * @param <T> the result type
     */
    @FunctionalInterface
    public interface Work<T> {
        /**
         * Runs the statements.
         *
         * @param connection the transaction's connection
         * @return the result
         * @throws Exception a database or mapping failure
         */
        T run(Connection connection) throws Exception;
    }
}
