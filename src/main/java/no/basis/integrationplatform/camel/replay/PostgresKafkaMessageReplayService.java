package no.basis.integrationplatform.camel.replay;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.PlatformHeaders;
import org.apache.camel.CamelContext;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@ApplicationScoped
public class PostgresKafkaMessageReplayService implements MessageReplayService {
    private static final String LOAD_REPLAYABLE_MESSAGE_SQL = """
            select message_id,
                   correlation_id,
                   content_type,
                   source_system,
                   target_system,
                   integration,
                   schema_version,
                   last_attempt,
                   payload_bytes,
                   replayable,
                   replay_disabled_reason,
                   payload_expires_at,
                   payload_deleted_at,
                   (select a.route_id from bip_message_attempt a
                    where a.message_id = l.message_id and a.attempt = l.last_attempt
                      and a.integration = l.integration
                      and a.payload_bytes is not distinct from l.payload_bytes) as route_id
            from bip_message_log l
            where message_id = ?
            """;

    private static final String INSERT_REPLAY_AUDIT_SQL = """
            insert into bip_message_replay_audit (
                replay_id,
                message_id,
                integration,
                replay_attempt,
                requested_by,
                reason,
                redo_steps,
                target_topic,
                status,
                requested_at
            ) values (?::uuid, ?, ?, ?, ?, ?, ?::jsonb, ?, 'REQUESTED', ?)
            """;

    private static final String MARK_REPLAY_PUBLISHED_SQL = """
            update bip_message_replay_audit
            set status = 'PUBLISHED',
                published_at = ?,
                error = null
            where replay_id = ?::uuid
            """;

    private static final String MARK_REPLAY_FAILED_SQL = """
            update bip_message_replay_audit
            set status = 'FAILED',
                error = ?
            where replay_id = ?::uuid
            """;

    private final Supplier<DataSource> dataSource;
    private final String bootstrapServers;
    private final String inputTopicPattern;
    private final CamelContext camelContext;
    private final String integrationName;

    @Inject
    public PostgresKafkaMessageReplayService(
            Instance<DataSource> dataSource,
            @ConfigProperty(name = "kafka.bootstrap.servers", defaultValue = "") String bootstrapServers,
            @ConfigProperty(name = "basis.message-store.replay.input-topic-pattern",
                    defaultValue = "bip.%s.in") String inputTopicPattern,
            CamelContext camelContext,
            @ConfigProperty(name = "app.name", defaultValue = "unknown-integration") String integrationName) {
        this.dataSource = () -> dataSource.isResolvable() ? dataSource.get() : null;
        this.bootstrapServers = bootstrapServers;
        this.inputTopicPattern = inputTopicPattern;
        this.camelContext = camelContext;
        this.integrationName = integrationName;
    }

    PostgresKafkaMessageReplayService(
            DataSource dataSource,
            String bootstrapServers,
            String inputTopicPattern, CamelContext camelContext, String integrationName) {
        this(() -> dataSource, bootstrapServers, inputTopicPattern, camelContext, integrationName);
    }

    private PostgresKafkaMessageReplayService(
            Supplier<DataSource> dataSource,
            String bootstrapServers,
            String inputTopicPattern, CamelContext camelContext, String integrationName) {
        this.dataSource = dataSource;
        this.bootstrapServers = bootstrapServers;
        this.inputTopicPattern = inputTopicPattern;
        this.camelContext = camelContext;
        this.integrationName = integrationName;
    }

    @Override
    public MessageReplayResult replay(String messageId) {
        return replay(MessageReplayRequest.forMessage(messageId));
    }

    @Override
    public MessageReplayResult replay(MessageReplayRequest request) {
        if (request == null) {
            throw new MessageReplayException("Replay request is required");
        }
        String messageId = request.messageId();
        ReplayableMessage message = loadReplayableMessage(messageId);
        int nextAttempt = message.lastAttempt() + 1;
        String topic = inputTopic(message.integration());
        validateReplayBoundary(message, topic);
        String replayId = UUID.randomUUID().toString();

        insertReplayAudit(replayId, request, message, topic, nextAttempt);
        try (Producer<String, byte[]> producer = kafkaProducer(requiredKafkaBootstrapServers())) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                    topic,
                    message.correlationId(),
                    message.payloadBytes());
            addHeader(record, PlatformHeaders.MESSAGE_ID, message.messageId());
            addHeader(record, PlatformHeaders.CORRELATION_ID, message.correlationId());
            addHeader(record, PlatformHeaders.SCHEMA_VERSION, Integer.toString(message.schemaVersion()));
            addHeader(record, PlatformHeaders.CONTENT_TYPE, message.contentType());
            addHeader(record, PlatformHeaders.SOURCE_SYSTEM, message.sourceSystem());
            addHeader(record, PlatformHeaders.TARGET_SYSTEM, message.targetSystem());
            addHeader(record, PlatformHeaders.REPLAY, "true");
            addHeader(record, PlatformHeaders.ATTEMPT, Integer.toString(nextAttempt));
            addHeader(record, PlatformHeaders.REDO_STEPS, redoStepsHeader(request.redoSteps()));
            producer.send(record).get();
            markReplayPublished(replayId);
        } catch (Exception e) {
            markReplayFailed(replayId, e);
            throw new MessageReplayException("Failed to replay message " + messageId + " to " + topic, e);
        }

        return new MessageReplayResult(replayId, message.messageId(), message.integration(), topic, nextAttempt);
    }

    private ReplayableMessage loadReplayableMessage(String messageId) {
        try (Connection connection = requiredDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(LOAD_REPLAYABLE_MESSAGE_SQL)) {
            statement.setString(1, messageId);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new MessageReplayException("Message " + messageId + " was not found in bip_message_log");
                }
                ReplayableMessage message = new ReplayableMessage(
                        resultSet.getString("message_id"),
                        resultSet.getString("correlation_id"),
                        resultSet.getString("content_type"),
                        resultSet.getString("source_system"),
                        resultSet.getString("target_system"),
                        resultSet.getString("integration"),
                        resultSet.getInt("schema_version"),
                        resultSet.getInt("last_attempt"),
                        resultSet.getBytes("payload_bytes"),
                        resultSet.getBoolean("replayable"),
                        resultSet.getString("replay_disabled_reason"),
                        instant(resultSet, "payload_expires_at"),
                        instant(resultSet, "payload_deleted_at"),
                        resultSet.getString("route_id"));
                validateReplayable(message);
                return message;
            }
        } catch (MessageReplayException e) {
            throw e;
        } catch (Exception e) {
            throw new MessageReplayException("Failed to load message " + messageId + " for replay", e);
        }
    }

    private void validateReplayable(ReplayableMessage message) {
        if (!message.replayable()) {
            String reason = message.replayDisabledReason() == null
                    ? "message is marked non-replayable"
                    : message.replayDisabledReason();
            throw new MessageReplayException("Message " + message.messageId() + " is not replayable: " + reason);
        }
        if (message.payloadDeletedAt() != null) {
            throw new MessageReplayException("Message " + message.messageId() + " is not replayable: payload was deleted");
        }
        if (message.payloadExpiresAt() != null && message.payloadExpiresAt().isBefore(Instant.now())) {
            throw new MessageReplayException("Message " + message.messageId() + " is not replayable: payload has expired");
        }
        if (message.payloadBytes() == null) {
            throw new MessageReplayException("Message " + message.messageId() + " is not replayable: payload bytes are missing");
        }
        if (message.integration() == null || message.integration().isBlank()) {
            throw new MessageReplayException("Message " + message.messageId() + " is not replayable: integration is missing");
        }
    }

    private void validateReplayBoundary(ReplayableMessage message, String topic) {
        if (!integrationName.equals(message.integration())) {
            throw new MessageReplayException("Message is not replayable by this integration");
        }
        var route = message.routeId() == null ? null : camelContext.getRoute(message.routeId());
        if (route == null || !(route.getEndpoint() instanceof KafkaEndpoint endpoint)
                || endpoint.getConfiguration().isTopicIsPattern()
                || !topic.equals(endpoint.getConfiguration().getTopic())) {
            throw new MessageReplayException("Message is not replayable: stored input route does not match "
                    + "this integration's Kafka replay input. Internal, missing or changed routes require review.");
        }
    }

    private DataSource requiredDataSource() {
        DataSource resolved = dataSource.get();
        if (resolved != null) {
            return resolved;
        }
        throw new MessageReplayException("Manual replay requires a JDBC DataSource");
    }

    private String requiredKafkaBootstrapServers() {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new MessageReplayException("Manual replay requires kafka.bootstrap.servers");
        }
        return bootstrapServers;
    }

    private String inputTopic(String integration) {
        return inputTopicPattern.formatted(integration);
    }

    private void insertReplayAudit(
            String replayId,
            MessageReplayRequest request,
            ReplayableMessage message,
            String topic,
            int attempt) {
        try (Connection connection = requiredDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(INSERT_REPLAY_AUDIT_SQL)) {
            statement.setString(1, replayId);
            statement.setString(2, message.messageId());
            statement.setString(3, message.integration());
            statement.setInt(4, attempt);
            statement.setString(5, request.requestedBy());
            statement.setString(6, request.reason());
            statement.setString(7, redoStepsJson(request.redoSteps()));
            statement.setString(8, topic);
            statement.setTimestamp(9, Timestamp.from(Instant.now()));
            statement.executeUpdate();
        } catch (Exception e) {
            throw new MessageReplayException("Failed to audit replay request for message " + message.messageId(), e);
        }
    }

    private void markReplayPublished(String replayId) {
        try (Connection connection = requiredDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(MARK_REPLAY_PUBLISHED_SQL)) {
            statement.setTimestamp(1, Timestamp.from(Instant.now()));
            statement.setString(2, replayId);
            statement.executeUpdate();
        } catch (Exception e) {
            throw new MessageReplayException("Failed to mark replay " + replayId + " as published", e);
        }
    }

    private void markReplayFailed(String replayId, Exception failure) {
        try (Connection connection = requiredDataSource().getConnection();
             PreparedStatement statement = connection.prepareStatement(MARK_REPLAY_FAILED_SQL)) {
            statement.setString(1, failure.getMessage());
            statement.setString(2, replayId);
            statement.executeUpdate();
        } catch (Exception auditFailure) {
            failure.addSuppressed(auditFailure);
        }
    }

    private Instant instant(ResultSet resultSet, String column) throws Exception {
        var timestamp = resultSet.getTimestamp(column);
        return timestamp == null ? null : timestamp.toInstant();
    }

    /**
     * Contract §3 (DECISION D1): the {@code x-bip-redo-steps} wire format is the
     * same JSON array written to the {@code redo_steps} audit column. The header
     * is omitted when no redo was requested.
     */
    private String redoStepsHeader(List<String> redoSteps) {
        return redoSteps == null || redoSteps.isEmpty() ? null : redoStepsJson(redoSteps);
    }

    private String redoStepsJson(List<String> redoSteps) {
        if (redoSteps == null || redoSteps.isEmpty()) {
            return "[]";
        }
        return redoSteps.stream()
                .map(this::jsonString)
                .collect(Collectors.joining(",", "[", "]"));
    }

    private String jsonString(String value) {
        if (value == null) {
            return "null";
        }
        return "\"" + value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\b", "\\b")
                .replace("\f", "\\f")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t") + "\"";
    }

    private void addHeader(ProducerRecord<String, byte[]> record, String name, String value) {
        if (value != null) {
            record.headers().add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
        }
    }

    static Producer<String, byte[]> kafkaProducer(String bootstrapServers) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        no.basis.integrationplatform.camel.KafkaClientSecurity.apply(properties);
        return new KafkaProducer<>(properties);
    }

    private record ReplayableMessage(
            String messageId,
            String correlationId,
            String contentType,
            String sourceSystem,
            String targetSystem,
            String integration,
            int schemaVersion,
            int lastAttempt,
            byte[] payloadBytes,
            boolean replayable,
            String replayDisabledReason,
            Instant payloadExpiresAt,
            Instant payloadDeletedAt,
            String routeId) {
    }
}
