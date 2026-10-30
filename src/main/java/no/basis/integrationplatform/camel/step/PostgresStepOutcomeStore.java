package no.basis.integrationplatform.camel.step;

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

    private final DataSource dataSource;

    public PostgresStepOutcomeStore(DataSource dataSource) {
        this.dataSource = dataSource;
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
    public void recordDelivered(StepResult step) {
        record(step, StepStatus.DELIVERED, RECORD_DELIVERED_SQL);
    }

    @Override
    public void recordFailed(StepResult step) {
        record(step, StepStatus.FAILED, RECORD_FAILED_SQL);
    }

    private void record(StepResult step, StepStatus status, String sql) {
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
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new StepOutcomeStoreException("Failed to record " + status + " step outcome for "
                    + step.idempotencyKey(), e);
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
