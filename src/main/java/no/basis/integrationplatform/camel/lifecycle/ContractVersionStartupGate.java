package no.basis.integrationplatform.camel.lifecycle;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.MessageStoreDataSource;
import no.basis.integrationplatform.camel.MessageStoreTestProfileConfig;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Supplier;

/**
 * Message-store schema startup gate.
 *
 * <p>When the adapter runs with the Postgres sink it must refuse to start
 * against a schema provisioned for a different store schema version (contract
 * major). The database operator records the provisioned version in
 * {@code bip_contract}. An absent table or empty row is treated as major 1,
 * with a loud warning.</p>
 *
 * <p>With the Postgres sink or the Postgres step store, it also refuses to start when the
 * store lacks a table or column this adapter writes, such as a store provisioned for an
 * earlier adapter version: every outcome would otherwise fail to record, and each Kafka
 * record would be processed again and again. One catalog query checks them against the
 * schema the adapter ships in {@code db/message-store/}.</p>
 */
@ApplicationScoped
public class ContractVersionStartupGate {
    /** Contract major implemented by this adapter. */
    public static final int EXPECTED_CONTRACT_MAJOR = 1;

    static final String CONTRACT_TABLE = "bip_contract";
    static final String SELECT_CONTRACT_MAJOR_SQL = "select max(contract_major) from " + CONTRACT_TABLE;

    private static final Logger LOG = LoggerFactory.getLogger(ContractVersionStartupGate.class);
    private static final String POSTGRES_UNDEFINED_TABLE_SQL_STATE = "42P01";

    /**
     * Transient-failure retry budget. At startup the connection pool may not be
     * warm yet (observed as Agroal acquisition timeouts), which would otherwise
     * degrade the gate to warn-and-continue on every boot and never actually
     * verify anything. Bounded so a genuinely unreachable database delays boot
     * by at most attempts * delay before the documented degrade.
     */
    static final int DEFAULT_MAX_ATTEMPTS = 10;
    static final long DEFAULT_RETRY_DELAY_MS = 3_000L;

    private final String sinkMode;
    private final boolean postgresSteps;
    private final MessageStoreSchema schema;
    private final Supplier<DataSource> dataSource;
    private final int maxAttempts;
    private final long retryDelayMs;

    /**
     * Creates the startup gate from the effective sink and step store and the optional datasource.
     *
     * @param sinkMode configured message-store sink
     * @param stepOutcomeStore configured step outcome store
     * @param replayEnabled whether messages are captured for replay, which writes replay audit rows
     * @param dataSource Quarkus datasource handle
     * @param testProfile active test-profile policy
     */
    @Inject
    public ContractVersionStartupGate(
            @ConfigProperty(name = "basis.message-store.sink", defaultValue = "log") String sinkMode,
            @ConfigProperty(name = "basis.message-store.step-outcome-store", defaultValue = "noop") String stepOutcomeStore,
            @ConfigProperty(name = "basis.message-store.replay.enabled", defaultValue = "true") boolean replayEnabled,
            Instance<DataSource> dataSource,
            MessageStoreTestProfileConfig testProfile) {
        this(testProfile.effectiveSink(sinkMode), testProfile.effectiveStepOutcomeStore(stepOutcomeStore), replayEnabled, () -> {
            if (dataSource.isAmbiguous()) {
                // Otherwise the app reports ready and every journal write fails.
                throw MessageStoreDataSource.ambiguous("basis.message-store.sink=postgres");
            }
            return dataSource.isResolvable() ? dataSource.get() : null;
        }, DEFAULT_MAX_ATTEMPTS, DEFAULT_RETRY_DELAY_MS);
    }

    ContractVersionStartupGate(String sinkMode, Supplier<DataSource> dataSource) {
        this(sinkMode, dataSource, DEFAULT_MAX_ATTEMPTS, DEFAULT_RETRY_DELAY_MS);
    }

    ContractVersionStartupGate(String sinkMode, Supplier<DataSource> dataSource, int maxAttempts, long retryDelayMs) {
        this(sinkMode, "noop", true, dataSource, maxAttempts, retryDelayMs);
    }

    ContractVersionStartupGate(String sinkMode, String stepOutcomeStore, boolean replayEnabled,
            Supplier<DataSource> dataSource, int maxAttempts, long retryDelayMs) {
        this.sinkMode = sinkMode;
        boolean postgresSink = "postgres".equals(sinkMode);
        this.postgresSteps = "postgres".equals(stepOutcomeStore);
        this.schema = MessageStoreSchema.required(postgresSink, postgresSink && replayEnabled, postgresSteps);
        this.dataSource = dataSource;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.retryDelayMs = retryDelayMs;
    }

    void onStartup(@Observes StartupEvent event) {
        verify();
    }

    /**
     * @throws IllegalStateException when the provisioned contract major differs
     *         from {@link #EXPECTED_CONTRACT_MAJOR}, or when the store lacks a table or
     *         column this adapter writes; failing fast beats silently writing a
     *         mismatched schema shape.
     */
    void verify() {
        boolean postgresSink = "postgres".equals(sinkMode);
        if (!postgresSink && schema.isEmpty()) {
            return;
        }
        DataSource resolved = dataSource.get();
        if (resolved == null) {
            // The sink and the step store are created on their first use, so they would fail only
            // after a message was processed, and the record would be read and processed again.
            throw new IllegalStateException((postgresSink ? "basis.message-store.sink=postgres"
                    : "basis.message-store.step-outcome-store=postgres") + " requires a JDBC DataSource");
        }

        SQLException lastFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try (Connection connection = resolved.getConnection()) {
                if (postgresSink) {
                    checkContract(connection);
                }
                schema.verify(connection);
                return;
            } catch (SQLException e) {
                lastFailure = e;
                if (attempt < maxAttempts) {
                    LOG.info("Contract check attempt {}/{} failed ({}); retrying in {} ms",
                            attempt, maxAttempts, e.getMessage(), retryDelayMs);
                    try {
                        Thread.sleep(retryDelayMs);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        // Fail fast rather than mask errors. Startup failure is safe under
        // rolling updates (previous pods keep serving) and an adapter that
        // cannot reach its journal must not consume anyway.
        throw new IllegalStateException("Message-store contract gate could not read " + CONTRACT_TABLE
                + " or the store's tables after " + maxAttempts + " attempts ("
                + (lastFailure == null ? "interrupted" : lastFailure.getMessage())
                + "). Refusing to start: fix grants/connectivity (the runtime role needs select on "
                + CONTRACT_TABLE + ").", lastFailure);
    }

    private void checkContract(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(SELECT_CONTRACT_MAJOR_SQL);
             ResultSet resultSet = statement.executeQuery()) {
            if (!resultSet.next()) {
                warnAssumingMajorOne("table " + CONTRACT_TABLE + " is empty");
                return;
            }
            int contractMajor = resultSet.getInt(1);
            if (resultSet.wasNull()) {
                warnAssumingMajorOne("table " + CONTRACT_TABLE + " is empty");
                return;
            }
            if (contractMajor != EXPECTED_CONTRACT_MAJOR) {
                throw new IllegalStateException("Message-store contract major mismatch: " + CONTRACT_TABLE
                        + " reports major " + contractMajor + " but this adapter implements major "
                        + EXPECTED_CONTRACT_MAJOR + ". Refusing to start against a schema provisioned for a"
                        + " different contract major.");
            }
            LOG.info("Message-store contract check passed: {} reports major {}", CONTRACT_TABLE, contractMajor);
        } catch (SQLException e) {
            if (!POSTGRES_UNDEFINED_TABLE_SQL_STATE.equals(e.getSQLState())) {
                throw e;
            }
            if (!connection.getAutoCommit()) {
                connection.rollback();
            }
            warnAssumingMajorOne("table " + CONTRACT_TABLE + " does not exist");
        }
    }

    private void warnAssumingMajorOne(String cause) {
        LOG.warn("MESSAGE-STORE CONTRACT VERSION UNVERIFIED: {}. Assuming contract major {}, the"
                        + " store schema version this adapter implements. Provision the contract metadata"
                        + " (see db/message-store/bip_contract.sql in the adapter JAR) so the"
                        + " adapter can fail fast on schema/contract mismatches.",
                cause, EXPECTED_CONTRACT_MAJOR);
    }
}
