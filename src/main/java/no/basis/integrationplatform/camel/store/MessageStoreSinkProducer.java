package no.basis.integrationplatform.camel.store;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import no.basis.integrationplatform.camel.MessageStoreTestProfileConfig;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;
import java.util.Optional;

/** CDI producer selecting the configured terminal outcome sink. */
@ApplicationScoped
public class MessageStoreSinkProducer {
    /** Configuration value selecting the non-durable logging sink. */
    public static final String LOGGING_SINK = "log";
    /** Configuration value selecting direct Postgres persistence. */
    public static final String POSTGRES_SINK = "postgres";
    /** Configuration value selecting Kafka lifecycle-event publication. */
    public static final String KAFKA_SINK = "kafka";
    /** Comma-separated supported sink configuration values. */
    public static final String SUPPORTED_SINKS = LOGGING_SINK + ", " + POSTGRES_SINK + ", " + KAFKA_SINK;

    /** Creates the stateless CDI producer. */
    public MessageStoreSinkProducer() {
    }

    @Produces
    @DefaultBean
    @ApplicationScoped
    MessageStoreSink messageStoreSink(
            @ConfigProperty(name = "basis.message-store.sink", defaultValue = LOGGING_SINK) String sink,
            @ConfigProperty(name = "kafka.bootstrap.servers") Optional<String> bootstrapServers,
            @ConfigProperty(name = "basis.message-store.status-topic",
                    defaultValue = KafkaMessageStoreSink.DEFAULT_STATUS_TOPIC) String statusTopic,
            @ConfigProperty(name = "basis.message-store.payload.retention-days", defaultValue = "30") long retentionDays,
            Instance<DataSource> dataSource,
            MessageStoreTestProfileConfig testProfile) {
        String effectiveSink = testProfile.effectiveSink(sink);
        return switch (effectiveSink) {
            case POSTGRES_SINK -> new PostgresMessageStoreSink(requiredDataSource(dataSource),
                    java.time.Duration.ofDays(retentionDays));
            case KAFKA_SINK -> new KafkaMessageStoreSink(
                    KafkaMessageStoreSink.kafkaProducer(requiredKafkaBootstrapServers(bootstrapServers.orElse(""))),
                    statusTopic,
                    new MessageStatusEventJson());
            case LOGGING_SINK -> new LoggingMessageStoreSink();
            default -> throw new IllegalArgumentException(
                    "Unsupported basis.message-store.sink value: " + effectiveSink
                            + ". Supported values: " + SUPPORTED_SINKS);
        };
    }

    void close(@Disposes MessageStoreSink sink) {
        if (sink instanceof KafkaMessageStoreSink kafkaSink) {
            kafkaSink.close();
        }
    }

    private DataSource requiredDataSource(Instance<DataSource> dataSource) {
        if (dataSource.isResolvable()) {
            return dataSource.get();
        }
        throw new IllegalStateException("basis.message-store.sink=postgres requires a JDBC DataSource");
    }

    private String requiredKafkaBootstrapServers(String bootstrapServers) {
        if (bootstrapServers == null || bootstrapServers.isBlank()) {
            throw new IllegalStateException("basis.message-store.sink=kafka requires kafka.bootstrap.servers");
        }
        return bootstrapServers;
    }
}
