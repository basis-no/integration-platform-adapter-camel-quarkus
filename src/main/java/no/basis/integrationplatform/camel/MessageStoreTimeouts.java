package no.basis.integrationplatform.camel;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.Executor;

/**
 * Bounds each statement of a Message Store call, so that a database that hangs or a lock
 * that is held ends the call with a store failure instead of holding the Kafka consumer
 * thread past its poll interval.
 *
 * <p>On a connection in autocommit mode, a call runs in its own transaction with
 * transaction-local {@code statement_timeout} and {@code lock_timeout}, so a pooled
 * connection keeps no session setting. A connection that is already in a transaction on
 * entry (autocommit off, as on a connection enlisted in an application's JTA transaction)
 * belongs to the application: the call sets nothing that would outlive it in that
 * transaction, and each statement is bounded by its JDBC query timeout instead. In both
 * cases the JDBC network timeout, which ends a read from a database that stopped answering,
 * is set to the statement timeout plus {@value #NETWORK_MARGIN_SECONDS} seconds for the
 * call and restored afterwards.</p>
 *
 * <p>A commit counts only when PostgreSQL reports it without a warning. Under synchronous
 * replication, {@code statement_timeout} does not end a commit's wait for the standby: the
 * network timeout does, by breaking the connection, and a cancelled wait returns with a
 * warning only. Both end the call with a failure.</p>
 */
public final class MessageStoreTimeouts {
    /** Default statement timeout for each statement of a Message Store call. */
    public static final Duration DEFAULT_STATEMENT_TIMEOUT = Duration.ofSeconds(10);
    /** Default lock timeout for each statement of a Message Store call. */
    public static final Duration DEFAULT_LOCK_TIMEOUT = Duration.ofSeconds(5);
    /** The default timeouts. */
    public static final MessageStoreTimeouts DEFAULT = new MessageStoreTimeouts(
            DEFAULT_STATEMENT_TIMEOUT, DEFAULT_LOCK_TIMEOUT);

    private static final long NETWORK_MARGIN_SECONDS = 5;
    private static final Duration MINIMUM = Duration.ofMillis(1);
    /** The SQLSTATE of a commit that PostgreSQL reported with a warning: its outcome is not confirmed. */
    static final String UNCONFIRMED_COMMIT = "08007";
    /**
     * The timeouts, and {@code client_min_messages} at {@code warning}: a role, database or
     * connection setting of {@code error} would suppress the warning of an unconfirmed
     * synchronous commit, which then would count as confirmed.
     */
    private static final String APPLY_SQL = "select set_config('statement_timeout', ?, true), "
            + "set_config('lock_timeout', ?, true), set_config('client_min_messages', 'warning', true)";
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
     * @param statementTimeout longest a statement may run; at least 1 ms
     * @param lockTimeout longest a statement may wait for a lock; at least 1 ms and no
     *        longer than the statement timeout
     * @return the timeouts
     * @throws IllegalArgumentException when a value is missing or under 1 ms, the lock
     *         timeout is longer than the statement timeout, or the network timeout derived
     *         from the statement timeout does not fit the JDBC API
     */
    public static MessageStoreTimeouts of(Duration statementTimeout, Duration lockTimeout) {
        requireAtLeastOneMillisecond("basis.message-store.statement-timeout", statementTimeout);
        requireAtLeastOneMillisecond("basis.message-store.lock-timeout", lockTimeout);
        if (lockTimeout.compareTo(statementTimeout) > 0) {
            throw new IllegalArgumentException("basis.message-store.lock-timeout (" + lockTimeout
                    + ") must not be longer than basis.message-store.statement-timeout (" + statementTimeout + ")");
        }
        if (statementTimeout.compareTo(Duration.ofMillis(Integer.MAX_VALUE).minusSeconds(NETWORK_MARGIN_SECONDS)) > 0) {
            throw new IllegalArgumentException("basis.message-store.statement-timeout (" + statementTimeout
                    + ") is too long: the call's network timeout must fit " + Integer.MAX_VALUE + " ms");
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
     * Runs work on a connection from the data source and commits it, as one transaction.
     * On a connection in autocommit mode the transaction is the call's own, with these
     * timeouts; on a connection already in a transaction, the work runs in it and the
     * connection is committed or rolled back as with plain JDBC. On failure the transaction
     * is rolled back and the failure thrown; a timeout is an {@link SQLException} like any
     * other database failure.
     *
     * @param dataSource the Message Store data source
     * @param work the statements to run; prepare them with {@link #prepare}
     * @param <T> the result type
     * @return the work's result
     * @throws Exception the work's or the database's failure
     */
    public <T> T inTransaction(DataSource dataSource, Work<T> work) throws Exception {
        return run(dataSource, work, true);
    }

    /**
     * Runs work in the call's own transaction with these timeouts, or, on a connection that
     * is already in a transaction, as part of that transaction without committing or rolling
     * it back.
     *
     * @param dataSource the Message Store data source
     * @param work the statements to run; prepare them with {@link #prepare}
     * @param <T> the result type
     * @return the work's result
     * @throws Exception the work's or the database's failure
     */
    public <T> T inTransactionOrJoin(DataSource dataSource, Work<T> work) throws Exception {
        return run(dataSource, work, false);
    }

    /**
     * Prepares a statement bounded by its JDBC query timeout (the statement timeout,
     * rounded up to whole seconds), which applies also where the transaction-local
     * timeouts are not set.
     *
     * @param connection the call's connection
     * @param sql the statement
     * @return the prepared statement
     * @throws SQLException when the statement cannot be prepared
     */
    public PreparedStatement prepare(Connection connection, String sql) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        try {
            statement.setQueryTimeout(queryTimeoutSeconds());
        } catch (SQLException | RuntimeException e) {
            statement.close();
            throw e;
        }
        return statement;
    }

    private <T> T run(DataSource dataSource, Work<T> work, boolean commitJoined) throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            int previousNetworkTimeout = networkTimeout(connection);
            boolean ownTransaction = connection.getAutoCommit();
            boolean completes = ownTransaction || commitJoined;
            Throwable failure = null;
            try {
                setNetworkTimeout(connection, Math.toIntExact(networkTimeout().toMillis()));
                if (ownTransaction) {
                    connection.setAutoCommit(false);
                    apply(connection);
                }
                T result = work.run(connection);
                if (completes) {
                    // Only what the commit itself reports counts, not an earlier warning.
                    connection.clearWarnings();
                    connection.commit();
                    requireConfirmedCommit(connection.getWarnings());
                }
                return result;
            } catch (Throwable e) {
                failure = e;
                if (completes) {
                    try {
                        connection.rollback();
                    } catch (Throwable rollback) {
                        e.addSuppressed(rollback);
                    }
                }
                throw e;
            } finally {
                if (ownTransaction) {
                    restore(failure, () -> connection.setAutoCommit(true));
                }
                restore(failure, () -> setNetworkTimeout(connection, previousNetworkTimeout));
            }
        }
    }

    /**
     * Fails a commit that PostgreSQL reported with a warning. The case this is for: under
     * synchronous replication, a commit whose wait for the standby is cancelled (a client
     * cancel, {@code pg_cancel_backend}) returns normally with only a WARNING ("canceling
     * wait for synchronous replication due to user request", detail "The transaction has
     * already committed locally, but might not have been replicated to the standby."), so
     * the outcome may be lost in a failover. That warning has no SQLSTATE of its own (it is
     * the generic {@code 01000}) and its text follows the server's {@code lc_messages}, so
     * neither identifies it reliably. Any warning-level message (SQLSTATE outside class
     * {@code 00}, which notices use) reported by the commit is therefore a failure; the
     * outcome writes are idempotent, so a failure here costs only a read again. The text is
     * matched only to say in the error what happened.
     */
    static void requireConfirmedCommit(SQLWarning warnings) throws SQLException {
        for (SQLWarning warning = warnings; warning != null; warning = warning.getNextWarning()) {
            String state = warning.getSQLState();
            if (state != null && state.startsWith("00")) {
                continue;
            }
            String reason = isSynchronousReplicationWarning(warning)
                    ? "committed locally, but a synchronous standby did not confirm it"
                    : "committed, but the commit reported a warning";
            // 08007, transaction resolution unknown: the warning's own SQLSTATE says nothing about
            // the record's data, so it must not decide how the failure is classified.
            SQLException failure = new SQLException("The Message Store transaction was " + reason
                    + "; treated as a store failure so that the outcome is written again: " + warning.getMessage()
                    + " (warning SQLSTATE " + state + ")", UNCONFIRMED_COMMIT, warning);
            throw failure;
        }
    }

    static boolean isSynchronousReplicationWarning(SQLWarning warning) {
        String text = (warning.getMessage() + " " + warning).toLowerCase(Locale.ROOT);
        return text.contains("synchronous replication") || text.contains("might not have been replicated");
    }

    /** Only a broken connection cannot be restored, and the pool discards it; after a commit the call succeeded. */
    private static void restore(Throwable failure, Restore restore) {
        try {
            restore.run();
        } catch (SQLException e) {
            if (failure != null) {
                failure.addSuppressed(e);
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

    private int queryTimeoutSeconds() {
        long millis = statementTimeout.toMillis();
        return (int) Math.min(Integer.MAX_VALUE, (millis + 999) / 1000);
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
            // The statement timeouts still apply.
        }
    }

    private static void requireAtLeastOneMillisecond(String property, Duration value) {
        if (value == null || value.compareTo(MINIMUM) < 0) {
            throw new IllegalArgumentException(property + " must be at least 1ms, was " + value);
        }
    }

    @FunctionalInterface
    private interface Restore {
        void run() throws SQLException;
    }

    /**
     * Statements run in one Message Store call.
     *
     * @param <T> the result type
     */
    @FunctionalInterface
    public interface Work<T> {
        /**
         * Runs the statements.
         *
         * @param connection the call's connection
         * @return the result
         * @throws Exception a database or mapping failure
         */
        T run(Connection connection) throws Exception;
    }
}
