package no.basis.integrationplatform.camel.step;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private final DataSource dataSource;

    /**
     * Creates a Postgres step-outcome store.
     *
     * @param dataSource platform message-store datasource
     */
    public PostgresStepOutcomeStore(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @Override
    public boolean supportsOutputCheckpoints() {
        return true;
    }

    @Override
    public boolean wasDelivered(String idempotencyKey) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(WAS_DELIVERED_SQL)) {
            statement.setString(1, idempotencyKey);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        } catch (SQLException e) {
            throw new StepOutcomeStoreException("Failed to check step outcome for idempotency key " + idempotencyKey, e);
        }
    }

    @Override
    public Optional<StepOutputCheckpoint> readCheckpoint(String idempotencyKey) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(READ_CHECKPOINT_SQL)) {
            statement.setString(1, idempotencyKey);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    return Optional.empty();
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
        } catch (Exception e) {
            throw new StepOutcomeStoreException(
                    "Failed to read output checkpoint for idempotency key " + idempotencyKey, e);
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

    private void record(StepResult step, StepStatus status, String sql, StepOutputCheckpoint checkpoint) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
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
            statement.executeUpdate();
        } catch (Exception e) {
            throw new StepOutcomeStoreException("Failed to record " + status + " step outcome for "
                    + step.idempotencyKey(), e);
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
