package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.TestJdbc;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A redo list that the store refuses to check (the login may not read the replay audit) is
 * dropped, and the attempt goes on, also when the step outcome store joined an application's
 * transaction: the refused read must not leave that transaction aborted.
 */
@Testcontainers
class JoinedTransactionRedoTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    @Test
    void aRefusedReplayAuditReadLeavesTheJoinedTransactionUsable() throws Exception {
        PGSimpleDataSource owner = source(POSTGRES.getUsername(), POSTGRES.getPassword());
        TestJdbc.awaitReachable(owner);
        try (Connection connection = owner.getConnection(); Statement statement = connection.createStatement()) {
            for (String resource : new String[]{"bip_message_store.sql", "bip_step_outcome.sql"}) {
                try (InputStream input = getClass().getResourceAsStream("/db/message-store/" + resource)) {
                    for (String sql : new String(input.readAllBytes(), StandardCharsets.UTF_8).split(";")) {
                        if (!sql.isBlank()) {
                            statement.execute(sql);
                        }
                    }
                }
            }
            // The login may use the step ledger but not read the replay audit.
            statement.execute("create role restricted_writer login password 'test-only'");
            statement.execute("grant select, insert, update on bip_step_outcome to restricted_writer");
        }
        PostgresStepOutcomeStore ownerStore = new PostgresStepOutcomeStore(owner);
        ownerStore.recordDelivered(StepResult.delivered("notify-vendor", "message-123:notify-vendor", "https://v/a"));
        ownerStore.recordDelivered(StepResult.delivered("notify-other", "message-123:notify-other", "https://v/b"));

        // One connection in a transaction the application owns, as a JTA-enlisted connection is:
        // like Agroal's or an XA connection while enlisted, it refuses JDBC's transaction and
        // savepoint methods.
        Connection real = source("restricted_writer", "test-only").getConnection();
        try {
            real.setAutoCommit(false);
            java.util.Set<String> refused = java.util.Set.of("commit", "rollback", "setAutoCommit", "setSavepoint",
                    "releaseSavepoint");
            Connection held = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                        if (method.getName().equals("close")) {
                            return null;
                        }
                        if (refused.contains(method.getName())) {
                            throw new java.sql.SQLException("Attempting to " + method.getName()
                                    + " while enlisted in a transaction");
                        }
                        try {
                            return method.invoke(real, args);
                        } catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
            DataSource joined = (DataSource) Proxy.newProxyInstance(getClass().getClassLoader(),
                    new Class<?>[]{DataSource.class},
                    (proxy, method, args) -> method.getName().equals("getConnection") ? held : null);
            DefaultStepTracker tracker = new DefaultStepTracker(new PostgresStepOutcomeStore(joined));

            Exchange exchange = new DefaultExchange(new DefaultCamelContext());
            exchange.setProperty(Exchange.INTERCEPTED_ENDPOINT, "https://vendor.example.com/callback");
            exchange.setProperty(Exchange.INTERCEPTED_NODE_ID, "notify-vendor");
            exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
            exchange.getMessage().setHeader(Exchange.HTTP_METHOD, "POST");
            exchange.getMessage().setHeader(PlatformHeaders.REPLAY, "true");
            exchange.getMessage().setHeader(PlatformHeaders.REDO_STEPS, "[\"notify-vendor\"]");
            DefaultStepTracker.acceptRedoSteps(exchange, "message-123", "order-pipeline", "bip.order-pipeline.in", 2,
                    true);

            tracker.beforeSend(exchange);
            assertTrue(exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class),
                    "The list is dropped with a warning and the delivered step is skipped");

            // The next step's check and the application's own work still run in that transaction.
            exchange.setProperty(Exchange.INTERCEPTED_NODE_ID, "notify-other");
            tracker.beforeSend(exchange);
            assertTrue(exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class));
            try (Statement statement = real.createStatement(); ResultSet result = statement.executeQuery(
                    "select count(*) from bip_step_outcome")) {
                result.next();
                assertEquals(2, result.getInt(1));
            }
        } finally {
            real.rollback();
            real.close();
        }
    }

    private static PGSimpleDataSource source(String user, String password) {
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(POSTGRES.getJdbcUrl());
        source.setUser(user);
        source.setPassword(password);
        return source;
    }
}
