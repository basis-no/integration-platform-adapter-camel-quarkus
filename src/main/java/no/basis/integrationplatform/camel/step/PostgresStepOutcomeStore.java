package no.basis.integrationplatform.camel.step;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.basis.integrationplatform.camel.LogText;
import no.basis.integrationplatform.camel.MessageStoreTimeouts;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class PostgresStepOutcomeStore implements StepOutcomeStore {
    public static final String TABLE_NAME = "bip_step_outcome";

    private static final String WAS_DELIVERED_SQL = """
            select 1
            from bip_step_outcome
            where idempotency_key = ?
              and status = 'DELIVERED'
            """;

    private static final String RECORD_DELIVERED_SQL = """
            insert into bip_step_outcome (
                idempotency_key,
                message_id,
                step_id,
                status,
                endpoint_uri,
                target_ref,
                error,
                occurred_at,
                updated_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
            on conflict (idempotency_key) do update
            set status = excluded.status,
                endpoint_uri = excluded.endpoint_uri,
                target_ref = excluded.target_ref,
                error = excluded.error,
                occurred_at = excluded.occurred_at,
                updated_at = current_timestamp
            """;

    private static final String RECORD_FAILED_SQL = """
            insert into bip_step_outcome (
                idempotency_key,
                message_id,
                step_id,
                status,
                endpoint_uri,
                target_ref,
                error,
                occurred_at,
                updated_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
            on conflict (idempotency_key) do update
            set status = excluded.status,
                endpoint_uri = excluded.endpoint_uri,
                target_ref = excluded.target_ref,
                error = excluded.error,
                occurred_at = excluded.occurred_at,
                updated_at = current_timestamp
            where bip_step_outcome.status <> 'DELIVERED'
            """;

    // Only the adapter's own replay carries a redo list: the replay service writes its audit
    // row before it publishes and marks it PUBLISHED after, so both states count. A FAILED row
    // does not, nor does an attempt that is completed or parked: a redo runs once. A FAILED
    // attempt is read again, so it may still redo. The lists are compared here, not by the
    // database, which never parses the record's own text.
    private static final String AUTHORISED_REPLAY_SQL = """
            select a.redo_steps::text
            from bip_message_replay_audit a
            where a.message_id = ?
              and a.integration = ?
              and a.target_topic = ?
              and a.replay_attempt = ?
              and a.status in ('REQUESTED', 'PUBLISHED')
              and not exists (
                      select 1
                      from bip_message_attempt m
                      where m.message_id = a.message_id
                        and m.attempt = a.replay_attempt
                        and m.status <> 'FAILED')
            """;
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String SAVEPOINT = "bip_replay_audit_read";

    private final DataSource dataSource;
    private final MessageStoreTimeouts timeouts;

    public PostgresStepOutcomeStore(DataSource dataSource) {
        this(dataSource, MessageStoreTimeouts.DEFAULT);
    }

    PostgresStepOutcomeStore(DataSource dataSource, MessageStoreTimeouts timeouts) {
        this.dataSource = dataSource;
        this.timeouts = timeouts;
    }

    @Override
    public boolean wasDelivered(String idempotencyKey) {
        try {
            return timeouts.inTransactionOrJoin(dataSource, connection -> {
                try (PreparedStatement statement = timeouts.prepare(connection, WAS_DELIVERED_SQL)) {
                    statement.setString(1, idempotencyKey);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        return resultSet.next();
                    }
                }
            });
        } catch (Exception e) {
            throw new StepOutcomeStoreException("Failed to check step outcome for idempotency key " + LogText.escape(idempotencyKey), e);
        }
    }

    @Override
    public boolean isAuthorisedReplay(RedoClaim claim) {
        try {
            return timeouts.inTransactionOrJoin(dataSource, connection -> {
                // A failed statement aborts the whole transaction, also an application's own one
                // this call joined. A refused read drops the redo list and the attempt goes on, so
                // the read runs under a savepoint that undoes its failure. It is set in SQL: a pool
                // refuses JDBC's savepoint rollback while the connection is enlisted in a JTA
                // transaction, and an XA connection refuses JDBC's savepoints altogether.
                boolean savepoint = setSavepoint(connection);
                try (PreparedStatement statement = timeouts.prepare(connection, AUTHORISED_REPLAY_SQL)) {
                    statement.setString(1, claim.messageId());
                    statement.setString(2, claim.integration());
                    statement.setString(3, claim.topic());
                    statement.setInt(4, claim.attempt());
                    JsonNode claimed = JSON.readTree(claim.redoStepsJson());
                    boolean authorised = false;
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (!authorised && resultSet.next()) {
                            authorised = claimed.equals(JSON.readTree(resultSet.getString(1)));
                        }
                    }
                    if (savepoint) {
                        execute(connection, "release savepoint " + SAVEPOINT);
                    }
                    return authorised;
                } catch (SQLException e) {
                    if (savepoint) {
                        try {
                            execute(connection, "rollback to savepoint " + SAVEPOINT);
                        } catch (SQLException rollback) {
                            // The transaction stays aborted: report that, so the attempt does not go on.
                            rollback.addSuppressed(e);
                            throw rollback;
                        }
                    }
                    throw e;
                } catch (Exception e) {
                    // Nothing failed in the database: the savepoint is only released.
                    if (savepoint) {
                        try {
                            execute(connection, "release savepoint " + SAVEPOINT);
                        } catch (SQLException release) {
                            e.addSuppressed(release);
                        }
                    }
                    throw e;
                }
            });
        } catch (Exception e) {
            throw new StepOutcomeStoreException("Failed to check the replay audit for message "
                    + LogText.escape(claim.messageId()), e);
        }
    }

    @Override
    public void recordDelivered(StepResult step) {
        record(step, StepStatus.DELIVERED, RECORD_DELIVERED_SQL);
    }

    @Override
    public void recordFailed(StepResult step) {
        record(step, StepStatus.FAILED, RECORD_FAILED_SQL);
    }

    // Whether the savepoint was set; a connection that refuses one runs the read without it, as before.
    private boolean setSavepoint(Connection connection) {
        try {
            execute(connection, "savepoint " + SAVEPOINT);
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    private void execute(Connection connection, String sql) throws SQLException {
        try (PreparedStatement statement = timeouts.prepare(connection, sql)) {
            statement.execute();
        }
    }

    private void record(StepResult step, StepStatus status, String sql) {
        try {
            timeouts.inTransactionOrJoin(dataSource, connection -> {
                try (PreparedStatement statement = timeouts.prepare(connection, sql)) {
                    statement.setString(1, step.idempotencyKey());
                    statement.setString(2, messageId(step.idempotencyKey()));
                    statement.setString(3, step.step());
                    statement.setString(4, status.name());
                    statement.setString(5, step.endpointUri());
                    statement.setString(6, step.targetRef());
                    statement.setString(7, step.error());
                    statement.setTimestamp(8, Timestamp.from(step.occurredAt()));
                    return statement.executeUpdate();
                }
            });
        } catch (Exception e) {
            throw new StepOutcomeStoreException("Failed to record " + status + " step outcome for "
                    + LogText.escape(step.idempotencyKey()), e);
        }
    }

    private String messageId(String idempotencyKey) {
        if (idempotencyKey == null) {
            return null;
        }
        int separator = idempotencyKey.indexOf(':');
        if (separator < 0) {
            return null;
        }
        return idempotencyKey.substring(0, separator);
    }
}
