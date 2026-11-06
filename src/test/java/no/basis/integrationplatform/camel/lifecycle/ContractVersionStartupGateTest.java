package no.basis.integrationplatform.camel.lifecycle;

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
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
class ContractVersionStartupGateTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private DataSource dataSource;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = dataSource();
        no.basis.integrationplatform.camel.TestJdbc.awaitReachable(dataSource);
        execute("drop table if exists " + ContractVersionStartupGate.CONTRACT_TABLE);
        execute("drop table if exists bip_message_replay_audit, bip_message_attempt, bip_message_log, bip_step_outcome");
        execute(resource(MessageStoreSchema.MESSAGE_STORE_RESOURCE));
        execute(resource(MessageStoreSchema.STEP_OUTCOME_RESOURCE));
    }

    @Test
    void refusesAStoreProvisionedForAnEarlierAdapterAndNamesTheMissingColumns() throws Exception {
        createContractTable();
        execute("insert into bip_contract (contract_major, contract_version) values (1, '1.0.0')");
        // The columns an earlier adapter's schema does not have.
        for (String table : List.of("bip_message_log", "bip_message_attempt")) {
            execute("alter table " + table + " drop column payload_ref, drop column payload_sha256");
        }
        execute("alter table bip_step_outcome drop column checkpoint_storage, drop column checkpoint_properties_json");
        ContractVersionStartupGate gate = new ContractVersionStartupGate("postgres", "postgres", true, () -> dataSource, 1, 0L);

        IllegalStateException thrown = assertThrows(IllegalStateException.class, gate::verify);

        String message = thrown.getMessage();
        for (String missing : List.of("bip_message_log.payload_ref", "bip_message_log.payload_sha256",
                "bip_message_attempt.payload_ref", "bip_message_attempt.payload_sha256",
                "bip_step_outcome.checkpoint_storage", "bip_step_outcome.checkpoint_properties_json")) {
            assertTrue(message.contains(missing), "Must name " + missing + ": " + message);
        }
        assertTrue(message.contains("Refusing to start"), message);
        // The statements it names bring the store to the adapter's schema.
        String statements = message.substring(message.indexOf("before starting: ") + "before starting: ".length());
        for (String statement : statements.substring(0, statements.length() - 1).split("; ")) {
            assertTrue(statement.startsWith("alter table "), statement);
            execute(statement);
        }
        assertDoesNotThrow(gate::verify);
    }

    @Test
    void saysHowToAddARequiredColumnToATableWithRows() throws Exception {
        execute("alter table bip_message_attempt drop column route_id");

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                new ContractVersionStartupGate("postgres", "noop", true, () -> dataSource, 1, 0L)::verify);

        assertTrue(thrown.getMessage().contains("alter table bip_message_attempt add column if not exists route_id text,"
                + " then give existing rows a value and run alter table bip_message_attempt alter column route_id"
                + " set not null"), thrown.getMessage());
    }

    @Test
    void refusesThePostgresStepStoreWithoutADataSource() {
        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                new ContractVersionStartupGate("log", "postgres", true, () -> null, 1, 0L)::verify);

        assertTrue(thrown.getMessage().contains("requires a JDBC DataSource"), thrown.getMessage());
    }

    @Test
    void refusesAStoreWithoutTheStepOutcomeTableOnlyWhenStepsAreStoredThere() throws Exception {
        execute("drop table bip_step_outcome");

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                new ContractVersionStartupGate("postgres", "postgres", true, () -> dataSource, 1, 0L)::verify);

        assertTrue(thrown.getMessage().contains("table bip_step_outcome"), thrown.getMessage());
        assertTrue(thrown.getMessage().contains("create bip_step_outcome with db/message-store/bip_step_outcome.sql"),
                thrown.getMessage());
        assertDoesNotThrow(new ContractVersionStartupGate("postgres", "noop", true, () -> dataSource, 1, 0L)::verify);
    }

    @Test
    void checksTheStepOutcomeTableAlsoWithoutThePostgresSink() throws Exception {
        execute("drop table bip_message_log, bip_message_attempt, bip_message_replay_audit");
        execute("alter table bip_step_outcome drop column checkpoint_body_ref");

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                new ContractVersionStartupGate("log", "postgres", true, () -> dataSource, 1, 0L)::verify);

        assertTrue(thrown.getMessage().contains("bip_step_outcome.checkpoint_body_ref"), thrown.getMessage());
        assertFalse(thrown.getMessage().contains("bip_message_log"), "The log sink writes no message tables");
    }

    @Test
    void requiresTheReplayAuditTableOnlyWhenReplayIsEnabled() throws Exception {
        execute("drop table bip_message_replay_audit");

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                new ContractVersionStartupGate("postgres", "noop", true, () -> dataSource, 1, 0L)::verify);

        assertTrue(thrown.getMessage().contains("table bip_message_replay_audit"), thrown.getMessage());
        assertDoesNotThrow(new ContractVersionStartupGate("postgres", "noop", false, () -> dataSource, 1, 0L)::verify);
    }

    @Test
    void theShippedSchemaHasEveryColumnTheWritersName() throws Exception {
        var schema = MessageStoreSchema.required(true, true, true).tables();
        Map<String, String> inserts = Map.of(
                "bip_message_log", sql(no.basis.integrationplatform.camel.store.PostgresMessageStoreSink.class, "UPSERT_MESSAGE_LOG_SQL"),
                "bip_message_attempt", sql(no.basis.integrationplatform.camel.store.PostgresMessageStoreSink.class, "UPSERT_MESSAGE_ATTEMPT_SQL"),
                "bip_message_replay_audit", sql(no.basis.integrationplatform.camel.replay.PostgresKafkaMessageReplayService.class, "INSERT_REPLAY_AUDIT_SQL"),
                "bip_step_outcome", sql(no.basis.integrationplatform.camel.step.PostgresStepOutcomeStore.class, "RECORD_DELIVERED_SQL")
                        + sql(no.basis.integrationplatform.camel.step.PostgresStepOutcomeStore.class, "RECORD_FAILED_SQL"));
        inserts.forEach((table, sql) -> {
            var matcher = java.util.regex.Pattern.compile("insert into " + table + " \\(([^)]*)\\)").matcher(sql);
            assertTrue(matcher.find(), "No insert into " + table);
            do {
                for (String column : matcher.group(1).split(",")) {
                    assertTrue(schema.get(table).containsKey(column.trim()), table + "." + column.trim());
                }
            } while (matcher.find());
        });
        assertFalse(schema.get("bip_message_attempt").containsKey("primary"), "A table constraint is not a column");
        assertEquals("checkpoint_headers_json jsonb not null default '{}'::jsonb",
                schema.get("bip_step_outcome").get("checkpoint_headers_json"));
    }

    private static String sql(Class<?> owner, String field) {
        try {
            var declared = owner.getDeclaredField(field);
            declared.setAccessible(true);
            return (String) declared.get(null);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("Missing SQL constant " + owner.getSimpleName() + "." + field, e);
        }
    }

    private String resource(String name) throws Exception {
        try (InputStream input = getClass().getResourceAsStream(name)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void ignoresNonPostgresSinkWithoutTouchingTheDataSource() {
        ContractVersionStartupGate gate = new ContractVersionStartupGate("log", () -> {
            throw new AssertionError("The gate must not resolve a DataSource when the sink is not postgres");
        });

        assertDoesNotThrow(gate::verify);
    }

    @Test
    void continuesWhenNoDataSourceIsResolvable() {
        ContractVersionStartupGate gate = new ContractVersionStartupGate("postgres", () -> null);

        assertDoesNotThrow(gate::verify);
    }

    @Test
    void warnsAndContinuesWhenContractTableIsAbsent() {
        // Absent bip_contract ⇒ assume store schema version (contract major) 1, warn loudly.
        ContractVersionStartupGate gate = new ContractVersionStartupGate("postgres", () -> dataSource);

        assertDoesNotThrow(gate::verify);
    }

    @Test
    void continuesWhenContractTableIsEmpty() throws Exception {
        createContractTable();
        ContractVersionStartupGate gate = new ContractVersionStartupGate("postgres", () -> dataSource);

        assertDoesNotThrow(gate::verify);
    }

    @Test
    void passesWhenProvisionedContractMajorMatches() throws Exception {
        createContractTable();
        execute("insert into bip_contract (contract_major, contract_version) values (1, '1.0.0-rc2')");
        ContractVersionStartupGate gate = new ContractVersionStartupGate("postgres", () -> dataSource);

        assertDoesNotThrow(gate::verify);
    }

    @Test
    void failsStartupWhenProvisionedContractMajorDiffers() throws Exception {
        // bip_contract.contract_major=2 provisioned ⇒ adapter refuses to start.
        createContractTable();
        execute("insert into bip_contract (contract_major, contract_version) values (1, '1.0.0-rc2')");
        execute("insert into bip_contract (contract_major, contract_version) values (2, '2.0.0')");
        ContractVersionStartupGate gate = new ContractVersionStartupGate("postgres", () -> dataSource);

        IllegalStateException thrown = assertThrows(IllegalStateException.class, gate::verify);

        assertTrue(thrown.getMessage().contains("major 2"),
                "Error must name the found contract major: " + thrown.getMessage());
        assertTrue(thrown.getMessage().contains("major 1"),
                "Error must name the expected contract major: " + thrown.getMessage());
    }

    @Test
    void retriesTransientConnectionFailuresBeforeVerifying() throws Exception {
        // Startup race: the pool may not be warm yet (Agroal acquisition timeout).
        // The gate must retry transient failures so the check actually runs.
        createContractTable();
        execute("insert into bip_contract (contract_major, contract_version) values (1, '1.0.0-rc2')");
        DataSource flaky = failingThenDelegating(2, dataSource);
        ContractVersionStartupGate gate = new ContractVersionStartupGate("postgres", () -> flaky, 5, 5L);

        assertDoesNotThrow(gate::verify);
    }

    @Test
    void retriesStillFailFastOnContractMajorMismatch() throws Exception {
        createContractTable();
        execute("insert into bip_contract (contract_major, contract_version) values (2, '2.0.0')");
        DataSource flaky = failingThenDelegating(1, dataSource);
        ContractVersionStartupGate gate = new ContractVersionStartupGate("postgres", () -> flaky, 5, 5L);

        assertThrows(IllegalStateException.class, gate::verify);
    }

    @Test
    void failsFastWhenAllRetryAttemptsAreExhausted() {
        // Transient grace, then startup failure - fail fast
        // over masked errors (safe under rolling updates).
        DataSource alwaysDown = failingThenDelegating(Integer.MAX_VALUE, dataSource);
        ContractVersionStartupGate gate = new ContractVersionStartupGate("postgres", () -> alwaysDown, 3, 5L);

        IllegalStateException thrown = assertThrows(IllegalStateException.class, gate::verify);

        assertTrue(thrown.getMessage().contains(ContractVersionStartupGate.CONTRACT_TABLE),
                "Error must name the contract table: " + thrown.getMessage());
    }

    /** DataSource that throws a transient SQLException for the first {@code failures} getConnection calls. */
    private DataSource failingThenDelegating(int failures, DataSource delegate) {
        java.util.concurrent.atomic.AtomicInteger remaining = new java.util.concurrent.atomic.AtomicInteger(failures);
        return (DataSource) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                    if ("getConnection".equals(method.getName()) && remaining.getAndDecrement() > 0) {
                        throw new java.sql.SQLException("Acquisition timeout while waiting for new connection");
                    }
                    try {
                        return method.invoke(delegate, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private void createContractTable() throws Exception {
        try (InputStream input = getClass().getResourceAsStream("/db/message-store/bip_contract.sql")) {
            if (input == null) {
                throw new AssertionError("Missing bip_contract schema resource");
            }
            execute(new String(input.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private void execute(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private DataSource dataSource() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }
}
