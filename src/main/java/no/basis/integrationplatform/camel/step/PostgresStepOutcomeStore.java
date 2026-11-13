package no.basis.integrationplatform.camel.step;

import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.Map;
import java.util.Optional;

/** Durable Postgres side-effect ledger with optional output checkpoints. */
public class PostgresStepOutcomeStore implements StepOutcomeStore {
    /** Canonical durable step-outcome table. */
    public static final String TABLE_NAME = "bip_step_outcome";
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String SAVEPOINT = "bip_replay_audit_read";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {
    };

    private static final String WAS_DELIVERED_SQL = """
            select 1
            from bip_step_outcome
            where idempotency_key = ?
              and status = 'DELIVERED'
            """;

    private static final String READ_CHECKPOINT_SQL = """
            select checkpoint_storage,
                   checkpoint_body_bytes,
                   checkpoint_body_ref,
                   checkpoint_body_size_bytes,
                   checkpoint_body_sha256,
                   checkpoint_body_encoding,
                   checkpoint_content_type,
                   checkpoint_expires_at,
                   checkpoint_headers_json::text,
                   checkpoint_properties_json::text
            from bip_step_outcome
            where idempotency_key = ?
              and status = 'DELIVERED'
              and checkpoint_storage is not null
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
                checkpoint_storage,
                checkpoint_body_bytes,
                checkpoint_body_ref,
                checkpoint_body_size_bytes,
                checkpoint_body_sha256,
                checkpoint_body_encoding,
                checkpoint_content_type,
                checkpoint_expires_at,
                checkpoint_headers_json,
                checkpoint_properties_json,
                updated_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, current_timestamp)
            on conflict (idempotency_key) do update
            set status = excluded.status,
                endpoint_uri = excluded.endpoint_uri,
                target_ref = excluded.target_ref,
                error = excluded.error,
                occurred_at = excluded.occurred_at,
                checkpoint_storage = excluded.checkpoint_storage,
                checkpoint_body_bytes = excluded.checkpoint_body_bytes,
                checkpoint_body_ref = excluded.checkpoint_body_ref,
                checkpoint_body_size_bytes = excluded.checkpoint_body_size_bytes,
                checkpoint_body_sha256 = excluded.checkpoint_body_sha256,
                checkpoint_body_encoding = excluded.checkpoint_body_encoding,
                checkpoint_content_type = excluded.checkpoint_content_type,
                checkpoint_expires_at = excluded.checkpoint_expires_at,
                checkpoint_headers_json = excluded.checkpoint_headers_json,
                checkpoint_properties_json = excluded.checkpoint_properties_json,
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

    private final DataSource dataSource;
    private final MessageStoreTimeouts timeouts;

    /**
     * Creates a Postgres step-outcome store.
     *
     * @param dataSource message-store datasource
     */
    public PostgresStepOutcomeStore(DataSource dataSource) {
        this(dataSource, MessageStoreTimeouts.DEFAULT);
    }

    PostgresStepOutcomeStore(DataSource dataSource, MessageStoreTimeouts timeouts) {
        this.dataSource = dataSource;
        this.timeouts = timeouts;
    }

    @Override
    public boolean supportsOutputCheckpoints() {
        return true;
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
    public Optional<StepOutputCheckpoint> readCheckpoint(String idempotencyKey) {
        try {
            return timeouts.inTransactionOrJoin(dataSource, connection -> {
                try (PreparedStatement statement = timeouts.prepare(connection, READ_CHECKPOINT_SQL)) {
                    statement.setString(1, idempotencyKey);
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (!resultSet.next()) {
                            return Optional.<StepOutputCheckpoint>empty();
                        }
                        Timestamp expiresAt = resultSet.getTimestamp("checkpoint_expires_at");
                        return Optional.of(new StepOutputCheckpoint(
                                resultSet.getString("checkpoint_storage"),
                                resultSet.getBytes("checkpoint_body_bytes"),
                                resultSet.getString("checkpoint_body_ref"),
                                resultSet.getLong("checkpoint_body_size_bytes"),
                                resultSet.getString("checkpoint_body_sha256"),
                                resultSet.getString("checkpoint_body_encoding"),
                                resultSet.getString("checkpoint_content_type"),
                                expiresAt == null ? null : expiresAt.toInstant(),
                                readMap(resultSet.getString("checkpoint_headers_json")),
                                readMap(resultSet.getString("checkpoint_properties_json"))));
                    }
                }
            });
        } catch (Exception e) {
            throw new StepOutcomeStoreException(
                    "Failed to read output checkpoint for idempotency key " + LogText.escape(idempotencyKey), e);
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
        record(step, StepStatus.DELIVERED, RECORD_DELIVERED_SQL, null);
    }

    @Override
    public void recordDelivered(StepResult step, StepOutputCheckpoint checkpoint) {
        record(step, StepStatus.DELIVERED, RECORD_DELIVERED_SQL, checkpoint);
    }

    @Override
    public void recordFailed(StepResult step) {
        record(step, StepStatus.FAILED, RECORD_FAILED_SQL, null);
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

    private void record(StepResult step, StepStatus status, String sql, StepOutputCheckpoint checkpoint) {
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
                    if (status == StepStatus.DELIVERED) {
                        setCheckpoint(statement, checkpoint);
                    }
                    return statement.executeUpdate();
                }
            });
        } catch (Exception e) {
            throw new StepOutcomeStoreException("Failed to record " + status + " step outcome for "
                    + LogText.escape(step.idempotencyKey()), e);
        }
    }

    private void setCheckpoint(PreparedStatement statement, StepOutputCheckpoint checkpoint) throws Exception {
        statement.setString(9, checkpoint == null ? null : checkpoint.storage());
        statement.setBytes(10, checkpoint == null ? null : checkpoint.bodyBytes());
        statement.setString(11, checkpoint == null ? null : checkpoint.bodyRef());
        if (checkpoint == null) {
            statement.setObject(12, null);
        } else {
            statement.setLong(12, checkpoint.bodySizeBytes());
        }
        statement.setString(13, checkpoint == null ? null : checkpoint.bodySha256());
        statement.setString(14, checkpoint == null ? null : checkpoint.bodyEncoding());
        statement.setString(15, checkpoint == null ? null : checkpoint.contentType());
        statement.setTimestamp(16, checkpoint == null || checkpoint.expiresAt() == null
                ? null : Timestamp.from(checkpoint.expiresAt()));
        statement.setString(17, JSON.writeValueAsString(checkpoint == null ? Map.of() : checkpoint.headers()));
        statement.setString(18, JSON.writeValueAsString(checkpoint == null ? Map.of() : checkpoint.properties()));
    }

    private Map<String, Object> readMap(String json) throws Exception {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        return JSON.readValue(json, MAP_TYPE);
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
