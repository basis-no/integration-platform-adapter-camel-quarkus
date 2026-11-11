package no.basis.integrationplatform.camel.step;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import no.basis.integrationplatform.camel.LogText;
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
            throw new StepOutcomeStoreException("Failed to check step outcome for idempotency key " + LogText.escape(idempotencyKey), e);
        }
    }

    @Override
    public boolean isAuthorisedReplay(RedoClaim claim) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(AUTHORISED_REPLAY_SQL)) {
            statement.setString(1, claim.messageId());
            statement.setString(2, claim.integration());
            statement.setString(3, claim.topic());
            statement.setInt(4, claim.attempt());
            JsonNode claimed = JSON.readTree(claim.redoStepsJson());
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    if (claimed.equals(JSON.readTree(resultSet.getString(1)))) {
                        return true;
                    }
                }
                return false;
            }
        } catch (SQLException | java.io.IOException e) {
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
