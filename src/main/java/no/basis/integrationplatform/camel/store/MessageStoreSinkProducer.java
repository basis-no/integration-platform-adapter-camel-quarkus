package no.basis.integrationplatform.camel.store;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Disposes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.sql.DataSource;

@ApplicationScoped
public class MessageStoreSinkProducer {
    public static final String LOGGING_SINK = "log";
    public static final String POSTGRES_SINK = "postgres";
    public static final String KAFKA_SINK = "kafka";
    public static final String SUPPORTED_SINKS = LOGGING_SINK + ", " + POSTGRES_SINK + ", " + KAFKA_SINK;

    @Produces
    @DefaultBean
    @ApplicationScoped
    MessageStoreSink messageStoreSink(
            @ConfigProperty(name = "basis.message-store.sink", defaultValue = LOGGING_SINK) String sink,
            @ConfigProperty(name = "kafka.bootstrap.servers", defaultValue = "") String bootstrapServers,
            @ConfigProperty(name = "basis.message-store.status-topic",
                    defaultValue = KafkaMessageStoreSink.DEFAULT_STATUS_TOPIC) String statusTopic,
            Instance<DataSource> dataSource) {
        return switch (sink) {
            case POSTGRES_SINK -> new PostgresMessageStoreSink(requiredDataSource(dataSource));
            case KAFKA_SINK -> new KafkaMessageStoreSink(
                    KafkaMessageStoreSink.kafkaProducer(requiredKafkaBootstrapServers(bootstrapServers)),
                    statusTopic,
                    new MessageStatusEventJson());
            case LOGGING_SINK -> new LoggingMessageStoreSink();
            default -> throw new IllegalArgumentException(
                    "Unsupported basis.message-store.sink value: " + sink
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
