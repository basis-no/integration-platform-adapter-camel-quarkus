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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
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
        // Contract §10 v1.0 rollout rule: absent bip_contract ⇒ assume major 1, warn loudly.
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
        // Certification S19: bip_contract.contract_major=2 provisioned ⇒ adapter refuses to start.
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
        // Certification S19b: transient grace, then startup failure - fail fast
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
