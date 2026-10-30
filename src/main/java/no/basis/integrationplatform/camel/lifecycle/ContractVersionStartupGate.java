package no.basis.integrationplatform.camel.lifecycle;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
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
 * Message-store contract §10 startup gate.
 *
 * <p>When the adapter runs with the Postgres sink it must refuse to start
 * against a schema provisioned for a different contract major. The provisioned
 * version is recorded in {@code bip_contract} by the platform. An absent table
 * is treated as major 1 during the v1.0 rollout, with a loud warning.</p>
 */
@ApplicationScoped
public class ContractVersionStartupGate {
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
    private final Supplier<DataSource> dataSource;
    private final int maxAttempts;
    private final long retryDelayMs;

    @Inject
    public ContractVersionStartupGate(
            @ConfigProperty(name = "basis.message-store.sink", defaultValue = "log") String sinkMode,
            Instance<DataSource> dataSource) {
        this(sinkMode, () -> dataSource.isResolvable() ? dataSource.get() : null);
    }

    ContractVersionStartupGate(String sinkMode, Supplier<DataSource> dataSource) {
        this(sinkMode, dataSource, DEFAULT_MAX_ATTEMPTS, DEFAULT_RETRY_DELAY_MS);
    }

    ContractVersionStartupGate(String sinkMode, Supplier<DataSource> dataSource, int maxAttempts, long retryDelayMs) {
        this.sinkMode = sinkMode;
        this.dataSource = dataSource;
        this.maxAttempts = Math.max(1, maxAttempts);
        this.retryDelayMs = retryDelayMs;
    }

    void onStartup(@Observes StartupEvent event) {
        verify();
    }

    /**
     * @throws IllegalStateException when the provisioned contract major differs
     *         from {@link #EXPECTED_CONTRACT_MAJOR}; failing fast beats silently
     *         writing a mismatched schema shape.
     */
    void verify() {
        if (!"postgres".equals(sinkMode)) {
            return;
        }
        DataSource resolved = dataSource.get();
        if (resolved == null) {
            return;
        }

        SQLException lastFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                queryAndCheck(resolved);
                return;
            } catch (SQLException e) {
                if (POSTGRES_UNDEFINED_TABLE_SQL_STATE.equals(e.getSQLState())) {
                    warnAssumingMajorOne("table " + CONTRACT_TABLE + " does not exist");
                    return;
                }
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
        // Platform principle: fail fast over masked errors. Startup failure is
        // safe under rolling updates (previous pods keep serving) and an adapter
        // that cannot reach its journal must not consume anyway (contract §10).
        throw new IllegalStateException("Message-store contract gate could not read " + CONTRACT_TABLE
                + " after " + maxAttempts + " attempts ("
                + (lastFailure == null ? "interrupted" : lastFailure.getMessage())
                + "). Refusing to start: fix grants/connectivity (sql/postgres/003 grants select on "
                + CONTRACT_TABLE + " to the runtime role).", lastFailure);
    }

    private void queryAndCheck(DataSource resolved) throws SQLException {
        try (Connection connection = resolved.getConnection();
             PreparedStatement statement = connection.prepareStatement(SELECT_CONTRACT_MAJOR_SQL);
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
        }
    }

    private void warnAssumingMajorOne(String cause) {
        LOG.warn("MESSAGE-STORE CONTRACT VERSION UNVERIFIED: {}. Assuming contract major {} per the v1.0"
                        + " rollout rule (contract §10). Provision the contract metadata"
                        + " (see db/message-store/bip_contract.sql in the adapter JAR) so the"
                        + " adapter can fail fast on schema/contract mismatches.",
                cause, EXPECTED_CONTRACT_MAJOR);
    }
}
