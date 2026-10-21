package example;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(ApplicationDataPostgresResource.class)
class ApplicationDataContinuityTest {
    @Inject DataSource messageStore;
    @Inject @io.quarkus.agroal.DataSource("application") DataSource application;

    @Test void existingApplicationDatabaseWorksWithoutKeyValueProvisioningOrMigration() throws Exception {
        String messageDatabase;
        try (var connection = messageStore.getConnection(); var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("select current_database(), to_regclass('bip_key_value') is null, to_regclass('existing_application_cursor') is null")) {
                assertTrue(rows.next()); messageDatabase = rows.getString(1);
                assertTrue(rows.getBoolean(2), "Optional KV is not provisioned in this legacy Message Store");
                assertTrue(rows.getBoolean(3), "Application tables must not leak into the Message Store");
            }
            try (var rows = statement.executeQuery("select contract_version from bip_contract where contract_major=1")) {
                assertTrue(rows.next()); assertEquals("1.0.0-rc2", rows.getString(1));
            }
        }
        try (var connection = application.getConnection(); var statement = connection.createStatement()) {
            try (var rows = statement.executeQuery("select current_database(), to_regclass('bip_contract') is null")) {
                assertTrue(rows.next()); assertNotEquals(messageDatabase, rows.getString(1)); assertTrue(rows.getBoolean(2));
            }
            try (var rows = statement.executeQuery("select next_date::text from existing_application_cursor where key='synthetic'")) {
                assertTrue(rows.next()); assertEquals("2026-01-01", rows.getString(1));
            }
            assertEquals(1, statement.executeUpdate("update existing_application_cursor set next_date=date '2026-01-02' where key='synthetic'"));
            try (var rows = statement.executeQuery("select next_date::text from existing_application_cursor where key='synthetic'")) {
                assertTrue(rows.next()); assertEquals("2026-01-02", rows.getString(1));
            }
        }
        assertFalse(Thread.getAllStackTraces().keySet().stream().anyMatch(t -> t.isAlive() && t.getName().equals("bip-key-value-sweep")),
                "An unused optional KV API must not start cleanup or require its schema");
    }
}
