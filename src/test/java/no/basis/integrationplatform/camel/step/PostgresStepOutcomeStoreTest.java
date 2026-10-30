package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.model.StepResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class PostgresStepOutcomeStoreTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private PostgresStepOutcomeStore store;

    @BeforeEach
    void setUp() throws Exception {
        DataSource dataSource = dataSource();
        no.basis.integrationplatform.camel.TestJdbc.awaitReachable(dataSource);
        store = new PostgresStepOutcomeStore(dataSource);

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists " + PostgresStepOutcomeStore.TABLE_NAME);
            for (String sql : schemaSql().split(";")) {
                if (!sql.isBlank()) {
                    statement.execute(sql);
                }
            }
        }
    }

    @Test
    void absentKeyIsNotDelivered() {
        assertFalse(store.wasDelivered("message-123:write-product"));
    }

    @Test
    void deliveredKeyIsDelivered() {
        store.recordDelivered(StepResult.delivered(
                "write-product",
                "message-123:write-product",
                "sql:insert-product"));

        assertTrue(store.wasDelivered("message-123:write-product"));
    }

    @Test
    void failedKeyIsNotDelivered() {
        store.recordFailed(StepResult.failed(
                "write-product",
                "message-123:write-product",
                "sql:insert-product",
                new RuntimeException("database unavailable")));

        assertFalse(store.wasDelivered("message-123:write-product"));
        assertEquals("FAILED", status("message-123:write-product"));
    }

    @Test
    void duplicateDeliveredRecordIsIdempotent() {
        StepResult first = StepResult.delivered(
                "write-product",
                "message-123:write-product",
                "sql:first-endpoint");
        StepResult second = StepResult.delivered(
                "write-product",
                "message-123:write-product",
                "sql:second-endpoint");

        store.recordDelivered(first);
        store.recordDelivered(second);

        assertTrue(store.wasDelivered("message-123:write-product"));
        assertEquals(1, rowCount());
        assertEquals("DELIVERED", status("message-123:write-product"));
        assertEquals("sql:second-endpoint", endpointUri("message-123:write-product"));
    }

    @Test
    void failedRecordDoesNotRegressDeliveredStep() {
        store.recordDelivered(StepResult.delivered(
                "write-product",
                "message-123:write-product",
                "sql:insert-product"));
        store.recordFailed(StepResult.failed(
                "write-product",
                "message-123:write-product",
                "sql:insert-product",
                new RuntimeException("late failure")));

        assertTrue(store.wasDelivered("message-123:write-product"));
        assertEquals("DELIVERED", status("message-123:write-product"));
    }

    @Test
    void storesDeliveredMarkerAndOutputCheckpointInSameRow() {
        StepResult delivered = StepResult.delivered(
                "create-order",
                "message-123:create-order",
                "https://partner.example.test/orders");

        store.recordDelivered(delivered, checkpoint("partner-order-42"));

        StepOutputCheckpoint restored = store.readCheckpoint("message-123:create-order").orElseThrow();
        assertTrue(store.wasDelivered("message-123:create-order"));
        assertEquals(StepOutputCheckpoint.POSTGRES_INLINE, restored.storage());
        assertArrayEquals("partner-order-42".getBytes(StandardCharsets.UTF_8), restored.bodyBytes());
        assertEquals("request-123", restored.headers().get("Partner-Request-Id"));
        assertEquals(202, restored.properties().get("partnerStatus"));
    }

    @Test
    void failedOutcomeCannotEraseDeliveredCheckpoint() {
        StepResult delivered = StepResult.delivered(
                "create-order",
                "message-123:create-order",
                "https://partner.example.test/orders");
        store.recordDelivered(delivered, checkpoint("partner-order-42"));

        store.recordFailed(StepResult.failed(
                "create-order",
                "message-123:create-order",
                "https://partner.example.test/orders",
                new IllegalStateException("late route failure")));

        StepOutputCheckpoint restored = store.readCheckpoint("message-123:create-order").orElseThrow();
        assertEquals("partner-order-42", new String(restored.bodyBytes(), StandardCharsets.UTF_8));
        assertEquals("DELIVERED", status("message-123:create-order"));
    }

    @Test
    void explicitRedoReplacesCheckpointForSameStableKey() {
        StepResult delivered = StepResult.delivered(
                "create-order",
                "message-123:create-order",
                "https://partner.example.test/orders");
        store.recordDelivered(delivered, checkpoint("partner-order-42"));

        store.recordDelivered(delivered, checkpoint("partner-order-43"));

        StepOutputCheckpoint restored = store.readCheckpoint("message-123:create-order").orElseThrow();
        assertEquals("partner-order-43", new String(restored.bodyBytes(), StandardCharsets.UTF_8));
        assertEquals(1, rowCount());
    }

    private StepOutputCheckpoint checkpoint(String body) {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        return new StepOutputCheckpoint(
                StepOutputCheckpoint.POSTGRES_INLINE,
                bytes,
                null,
                bytes.length,
                sha256(bytes),
                StepOutputCheckpoint.UTF8_BODY,
                "text/plain",
                Instant.now().plusSeconds(3600),
                Map.of("Partner-Request-Id", "request-123"),
                Map.of("partnerStatus", 202));
    }

    private String sha256(byte[] bytes) {
        try {
            return java.util.HexFormat.of().formatHex(
                    java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private String status(String idempotencyKey) {
        return singleValue("select status from bip_step_outcome where idempotency_key = '" + idempotencyKey + "'");
    }

    private String endpointUri(String idempotencyKey) {
        return singleValue("select endpoint_uri from bip_step_outcome where idempotency_key = '" + idempotencyKey + "'");
    }

    private int rowCount() {
        return Integer.parseInt(singleValue("select count(*) from bip_step_outcome"));
    }

    private String singleValue(String sql) {
        try (Connection connection = dataSource().getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getString(1);
        } catch (Exception e) {
            throw new AssertionError("Failed to query test database", e);
        }
    }

    private DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }

    private String schemaSql() {
        try (InputStream input = getClass().getResourceAsStream("/db/message-store/bip_step_outcome.sql")) {
            if (input == null) {
                throw new AssertionError("Missing bip_step_outcome schema resource");
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new AssertionError("Failed to read bip_step_outcome schema resource", e);
        }
    }
}
