package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;

public class PostgresMessageStoreSink implements MessageStoreSink {
    private static final Logger LOG = LoggerFactory.getLogger(PostgresMessageStoreSink.class);
    public static final String MESSAGE_LOG_TABLE = "bip_message_log";
    public static final String MESSAGE_ATTEMPT_TABLE = "bip_message_attempt";

    private static final String UPSERT_MESSAGE_LOG_SQL = """
            insert into bip_message_log (
                message_id,
                correlation_id,
                content_type,
                source_system,
                target_system,
                payload_storage,
                payload_size_bytes,
                payload_bytes,
                payload_expires_at,
                payload_deleted_at,
                replayable,
                replay_disabled_reason,
                integration,
                schema_version,
                status,
                last_attempt,
                auto_redelivered,
                auto_redelivery_count,
                auto_redelivery_max,
                occurred_at,
                error,
                updated_at
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
            on conflict (message_id) do update
            set correlation_id = excluded.correlation_id,
                content_type = excluded.content_type,
                source_system = excluded.source_system,
                target_system = excluded.target_system,
                payload_storage = excluded.payload_storage,
                payload_size_bytes = excluded.payload_size_bytes,
                payload_bytes = excluded.payload_bytes,
                payload_expires_at = excluded.payload_expires_at,
                payload_deleted_at = excluded.payload_deleted_at,
                replayable = excluded.replayable,
                replay_disabled_reason = excluded.replay_disabled_reason,
                integration = excluded.integration,
                schema_version = excluded.schema_version,
                status = excluded.status,
                last_attempt = excluded.last_attempt,
                auto_redelivered = excluded.auto_redelivered,
                auto_redelivery_count = excluded.auto_redelivery_count,
                auto_redelivery_max = excluded.auto_redelivery_max,
                occurred_at = excluded.occurred_at,
                error = excluded.error,
                updated_at = current_timestamp
            where excluded.last_attempt >= bip_message_log.last_attempt
            """;

    // The CTE reads another integration that already journaled this message id, without
    // an extra round trip. Its snapshot is taken before the insert.
    private static final String UPSERT_MESSAGE_ATTEMPT_SQL = """
            with other as (
                select integration from bip_message_attempt
                where message_id = ? and integration is distinct from ?
                limit 1
            )
            insert into bip_message_attempt (
                message_id,
                attempt,
                schema_version,
                correlation_id,
                content_type,
                source_system,
                target_system,
                payload_storage,
                payload_size_bytes,
                payload_bytes,
                payload_expires_at,
                payload_deleted_at,
                replayable,
                replay_disabled_reason,
                integration,
                route_id,
                status,
                auto_redelivered,
                auto_redelivery_count,
                auto_redelivery_max,
                occurred_at,
                steps_json,
                error
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
            on conflict (message_id, attempt) do update
            set schema_version = excluded.schema_version,
                correlation_id = excluded.correlation_id,
                content_type = excluded.content_type,
                source_system = excluded.source_system,
                target_system = excluded.target_system,
                payload_storage = excluded.payload_storage,
                payload_size_bytes = excluded.payload_size_bytes,
                payload_bytes = excluded.payload_bytes,
                payload_expires_at = excluded.payload_expires_at,
                payload_deleted_at = excluded.payload_deleted_at,
                replayable = excluded.replayable,
                replay_disabled_reason = excluded.replay_disabled_reason,
                integration = excluded.integration,
                route_id = excluded.route_id,
                status = excluded.status,
                auto_redelivered = excluded.auto_redelivered,
                auto_redelivery_count = excluded.auto_redelivery_count,
                auto_redelivery_max = excluded.auto_redelivery_max,
                occurred_at = excluded.occurred_at,
                steps_json = excluded.steps_json,
                error = excluded.error
            returning (select integration from other)
            """;

    private final DataSource dataSource;
    private final MessageStatusEventJson serializer;

    public PostgresMessageStoreSink(DataSource dataSource) {
        this(dataSource, new MessageStatusEventJson());
    }

    PostgresMessageStoreSink(DataSource dataSource, MessageStatusEventJson serializer) {
        this.dataSource = dataSource;
        this.serializer = serializer;
    }

    @Override
    public void record(MessageAttemptResult result) {
        try (Connection connection = dataSource.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                upsertMessageAttempt(connection, result);
                upsertMessageLog(connection, result);
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (Exception e) {
            throw new MessageStoreSinkException("Failed to record message attempt " + result.messageId()
                    + "/" + result.attempt(), e);
        }
    }

    private void upsertMessageAttempt(Connection connection, MessageAttemptResult result) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(UPSERT_MESSAGE_ATTEMPT_SQL)) {
            statement.setString(1, result.messageId());
            statement.setString(2, result.integration());
            statement.setString(3, result.messageId());
            statement.setInt(4, result.attempt());
            statement.setInt(5, result.schemaVersion());
            statement.setString(6, result.correlationId());
            statement.setString(7, result.contentType());
            statement.setString(8, result.sourceSystem());
            statement.setString(9, result.targetSystem());
            statement.setString(10, result.payloadStorage());
            setLongOrNull(statement, 11, result.payloadSizeBytes());
            setBytesOrNull(statement, 12, result.payloadBytes());
            setTimestampOrNull(statement, 13, result.payloadExpiresAt());
            setTimestampOrNull(statement, 14, result.payloadDeletedAt());
            statement.setBoolean(15, result.replayable());
            statement.setString(16, result.replayDisabledReason());
            statement.setString(17, result.integration());
            statement.setString(18, result.routeId());
            statement.setString(19, result.status().name());
            statement.setBoolean(20, result.autoRedelivered());
            setIntegerOrNull(statement, 21, result.autoRedeliveryCount());
            setIntegerOrNull(statement, 22, result.autoRedeliveryMax());
            statement.setTimestamp(23, Timestamp.from(result.occurredAt()));
            statement.setString(24, serializer.serializeSteps(result));
            statement.setString(25, result.error());
            try (ResultSet upserted = statement.executeQuery()) {
                String other = upserted.next() ? upserted.getString(1) : null;
                if (other != null) {
                    // bip_message_log and bip_message_attempt are keyed by message id alone, so
                    // two integrations on one Message Store replace each other's rows.
                    LOG.warn("Message id {} is also journaled by integration {}; integration {} now replaces its "
                                    + "summary and attempt {} rows. Mint a new x-bip-message-id per integration and "
                                    + "keep continuity in x-bip-correlation-id",
                            result.messageId(), other, result.integration(), result.attempt());
                }
            }
        }
    }

    private void upsertMessageLog(Connection connection, MessageAttemptResult result) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(UPSERT_MESSAGE_LOG_SQL)) {
            statement.setString(1, result.messageId());
            statement.setString(2, result.correlationId());
            statement.setString(3, result.contentType());
            statement.setString(4, result.sourceSystem());
            statement.setString(5, result.targetSystem());
            statement.setString(6, result.payloadStorage());
            setLongOrNull(statement, 7, result.payloadSizeBytes());
            setBytesOrNull(statement, 8, result.payloadBytes());
            setTimestampOrNull(statement, 9, result.payloadExpiresAt());
            setTimestampOrNull(statement, 10, result.payloadDeletedAt());
            statement.setBoolean(11, result.replayable());
            statement.setString(12, result.replayDisabledReason());
            statement.setString(13, result.integration());
            statement.setInt(14, result.schemaVersion());
            statement.setString(15, result.status().name());
            statement.setInt(16, result.attempt());
            statement.setBoolean(17, result.autoRedelivered());
            setIntegerOrNull(statement, 18, result.autoRedeliveryCount());
            setIntegerOrNull(statement, 19, result.autoRedeliveryMax());
            statement.setTimestamp(20, Timestamp.from(result.occurredAt()));
            statement.setString(21, result.error());
            statement.executeUpdate();
        }
    }

    private void setLongOrNull(PreparedStatement statement, int index, Long value) throws SQLException {
        if (value == null) {
            statement.setObject(index, null);
        } else {
            statement.setLong(index, value);
        }
    }

    private void setIntegerOrNull(PreparedStatement statement, int index, Integer value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.INTEGER);
        } else {
            statement.setInt(index, value);
        }
    }

    private void setBytesOrNull(PreparedStatement statement, int index, byte[] value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.BINARY);
        } else {
            statement.setBytes(index, value);
        }
    }

    private void setTimestampOrNull(PreparedStatement statement, int index, java.time.Instant value) throws SQLException {
        if (value == null) {
            statement.setObject(index, null);
        } else {
            statement.setTimestamp(index, Timestamp.from(value));
        }
    }
}
