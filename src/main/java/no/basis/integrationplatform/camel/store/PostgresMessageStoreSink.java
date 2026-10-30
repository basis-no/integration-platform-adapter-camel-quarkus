package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.model.MessageAttemptResult;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;

/** Transactionally upserts terminal attempts and current message state in Postgres. */
public class PostgresMessageStoreSink implements MessageStoreSink {
    /** Canonical current-message table. */
    public static final String MESSAGE_LOG_TABLE = "bip_message_log";
    /** Canonical per-attempt history table. */
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
                payload_ref,
                payload_sha256,
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
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
            on conflict (message_id) do update
            set correlation_id = excluded.correlation_id,
                content_type = excluded.content_type,
                source_system = excluded.source_system,
                target_system = excluded.target_system,
                payload_storage = excluded.payload_storage,
                payload_size_bytes = excluded.payload_size_bytes,
                payload_bytes = excluded.payload_bytes,
                payload_ref = excluded.payload_ref,
                payload_sha256 = excluded.payload_sha256,
                payload_expires_at = case
                    when excluded.payload_storage = 's3_object'
                        then coalesce(bip_message_log.payload_expires_at, excluded.payload_expires_at)
                    else excluded.payload_expires_at
                end,
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
            returning payload_expires_at
            """;

    private static final String UPSERT_MESSAGE_ATTEMPT_SQL = """
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
                payload_ref,
                payload_sha256,
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
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
            on conflict (message_id, attempt) do update
            set schema_version = excluded.schema_version,
                correlation_id = excluded.correlation_id,
                content_type = excluded.content_type,
                source_system = excluded.source_system,
                target_system = excluded.target_system,
                payload_storage = excluded.payload_storage,
                payload_size_bytes = excluded.payload_size_bytes,
                payload_bytes = excluded.payload_bytes,
                payload_ref = excluded.payload_ref,
                payload_sha256 = excluded.payload_sha256,
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
            """;

    // The earliest outcome recorded for this message by this integration: its first receipt.
    private static final String FIRST_RECEIPT_SQL = """
            select min(occurred_at) from bip_message_attempt where message_id = ? and integration = ?
            """;

    private final DataSource dataSource;
    private final Duration retention;
    private final MessageStatusEventJson serializer;

    /**
     * Creates a direct Postgres outcome sink.
     *
     * @param dataSource platform message-store datasource
     */
    public PostgresMessageStoreSink(DataSource dataSource) {
        this(dataSource, new MessageStatusEventJson());
    }

    /**
     * Creates the sink.
     * @param dataSource platform message-store datasource
     * @param retention configured payload retention; a replay's new inline expiry is capped at
     *        first receipt plus twice this duration
     */
    public PostgresMessageStoreSink(DataSource dataSource, Duration retention) {
        this(dataSource, new MessageStatusEventJson(), retention);
    }

    PostgresMessageStoreSink(DataSource dataSource, MessageStatusEventJson serializer) {
        this(dataSource, serializer, null);
    }

    PostgresMessageStoreSink(DataSource dataSource, MessageStatusEventJson serializer, Duration retention) {
        this.dataSource = dataSource;
        this.serializer = serializer;
        this.retention = retention;
    }

    /**
     * An inline payload is captured again on every attempt with an expiry of capture time
     * plus retention, so a replay resets retention. The reset is capped at first receipt plus
     * twice the retention. First receipt is the earliest outcome this integration recorded for
     * the message. Without a configured retention, the attempt's own expiry minus its outcome
     * time stands in for it. An object payload keeps its first expiry, with its object.
     */
    private Instant cappedInlineExpiry(Connection connection, MessageAttemptResult result) throws SQLException {
        Instant requested = result.payloadExpiresAt();
        if (requested == null || !"postgres_inline".equals(result.payloadStorage())) {
            return requested;
        }
        Instant firstReceipt = result.occurredAt();
        try (PreparedStatement statement = connection.prepareStatement(FIRST_RECEIPT_SQL)) {
            statement.setString(1, result.messageId());
            statement.setString(2, result.integration());
            try (ResultSet earliest = statement.executeQuery()) {
                Timestamp recorded = earliest.next() ? earliest.getTimestamp(1) : null;
                if (recorded != null && recorded.toInstant().isBefore(firstReceipt)) {
                    firstReceipt = recorded.toInstant();
                }
            }
        }
        Duration window = retention != null ? retention : Duration.between(result.occurredAt(), requested);
        Instant cap = firstReceipt.plus((window.isNegative() ? Duration.ZERO : window).multipliedBy(2));
        return requested.isAfter(cap) ? cap : requested;
    }

    @Override
    public void record(MessageAttemptResult result) {
        try (Connection connection = dataSource.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                Instant payloadExpiresAt = upsertMessageLog(connection, result, cappedInlineExpiry(connection, result));
                upsertMessageAttempt(connection, result, payloadExpiresAt);
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

    private void upsertMessageAttempt(
            Connection connection,
            MessageAttemptResult result,
            Instant payloadExpiresAt) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(UPSERT_MESSAGE_ATTEMPT_SQL)) {
            statement.setString(1, result.messageId());
            statement.setInt(2, result.attempt());
            statement.setInt(3, result.schemaVersion());
            statement.setString(4, result.correlationId());
            statement.setString(5, result.contentType());
            statement.setString(6, result.sourceSystem());
            statement.setString(7, result.targetSystem());
            statement.setString(8, result.payloadStorage());
            setLongOrNull(statement, 9, result.payloadSizeBytes());
            setBytesOrNull(statement, 10, result.payloadBytes());
            statement.setString(11, result.payloadRef());
            statement.setString(12, result.payloadSha256());
            setTimestampOrNull(statement, 13, payloadExpiresAt);
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
            statement.executeUpdate();
        }
    }

    private Instant upsertMessageLog(Connection connection, MessageAttemptResult result, Instant requestedExpiry)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(UPSERT_MESSAGE_LOG_SQL)) {
            statement.setString(1, result.messageId());
            statement.setString(2, result.correlationId());
            statement.setString(3, result.contentType());
            statement.setString(4, result.sourceSystem());
            statement.setString(5, result.targetSystem());
            statement.setString(6, result.payloadStorage());
            setLongOrNull(statement, 7, result.payloadSizeBytes());
            setBytesOrNull(statement, 8, result.payloadBytes());
            statement.setString(9, result.payloadRef());
            statement.setString(10, result.payloadSha256());
            setTimestampOrNull(statement, 11, requestedExpiry);
            setTimestampOrNull(statement, 12, result.payloadDeletedAt());
            statement.setBoolean(13, result.replayable());
            statement.setString(14, result.replayDisabledReason());
            statement.setString(15, result.integration());
            statement.setInt(16, result.schemaVersion());
            statement.setString(17, result.status().name());
            statement.setInt(18, result.attempt());
            statement.setBoolean(19, result.autoRedelivered());
            setIntegerOrNull(statement, 20, result.autoRedeliveryCount());
            setIntegerOrNull(statement, 21, result.autoRedeliveryMax());
            statement.setTimestamp(22, Timestamp.from(result.occurredAt()));
            statement.setString(23, result.error());
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                Timestamp payloadExpiresAt = resultSet.getTimestamp(1);
                return payloadExpiresAt == null ? null : payloadExpiresAt.toInstant();
            }
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
