package no.basis.integrationplatform.camel.store;

import jakarta.enterprise.inject.Instance;
import no.basis.integrationplatform.camel.MessageStoreTestProfileConfig;
import no.basis.integrationplatform.camel.TestJdbc;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A configured lock timeout reaches the sink the producer creates. */
@Testcontainers
class MessageStoreSinkTimeoutConfigTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Test
    void aConfiguredLockTimeoutTakesEffect() throws Exception {
        DataSource dataSource = dataSource();
        try (Connection setup = dataSource.getConnection(); Statement statement = setup.createStatement()) {
            statement.execute("drop table if exists bip_message_attempt, bip_message_log");
            for (String sql : schemaSql().split(";")) {
                if (!sql.isBlank()) {
                    statement.execute(sql);
                }
            }
        }
        MessageStoreSink sink = new MessageStoreSinkProducer().messageStoreSink(
                "postgres", Optional.empty(), KafkaMessageStoreSink.DEFAULT_STATUS_TOPIC, 30,
                Duration.ofSeconds(2), Duration.ofMillis(500), instance(dataSource),
                new MessageStoreTestProfileConfig("disabled", false));

        try (Connection holder = dataSource.getConnection()) {
            holder.setAutoCommit(false);
            try (Statement statement = holder.createStatement()) {
                statement.execute("insert into bip_message_log (message_id, integration, schema_version, status, "
                        + "last_attempt, occurred_at) values ('message-123', 'orders', 1, 'RECEIVED', 1, now())");
            }
            long started = System.nanoTime();
            MessageStoreSinkException failure = assertThrows(MessageStoreSinkException.class, () -> sink.record(result()));
            Duration waited = Duration.ofNanos(System.nanoTime() - started);

            assertEquals("55P03", ((SQLException) failure.getCause()).getSQLState());
            // The default lock timeout is 5 s; the configured 500 ms applies.
            assertTrue(waited.compareTo(Duration.ofSeconds(3)) < 0, waited.toString());
        }
    }

    private static MessageAttemptResult result() {
        return new MessageAttemptResult(
                1, "message-123", "order-456", "application/json", "source", "target", "metadata_only",
                null, null, null, null, null, null, false, "replay disabled by policy", "orders", "orders-route", 1,
                false, null, null, Instant.parse("2026-06-14T12:00:00Z"), MessageStatus.COMPLETED, List.of(), null);
    }

    @SuppressWarnings("unchecked")
    private static Instance<DataSource> instance(DataSource dataSource) {
        return (Instance<DataSource>) Proxy.newProxyInstance(MessageStoreSinkTimeoutConfigTest.class.getClassLoader(),
                new Class<?>[]{Instance.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "isResolvable" -> true;
                    case "isAmbiguous", "isUnsatisfied" -> false;
                    case "get" -> dataSource;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static DataSource dataSource() {
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(POSTGRES.getUsername());
        source.setPassword(POSTGRES.getPassword());
        TestJdbc.awaitReachable(source);
        return source;
    }

    private static String schemaSql() throws Exception {
        try (InputStream input = MessageStoreSinkTimeoutConfigTest.class
                .getResourceAsStream("/db/message-store/bip_message_store.sql")) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
