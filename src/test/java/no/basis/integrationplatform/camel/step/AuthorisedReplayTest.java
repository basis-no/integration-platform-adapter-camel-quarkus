package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.TestJdbc;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A redo list is the adapter's own replay only with a matching audit row, and only once. */
@Testcontainers
class AuthorisedReplayTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");
    private static final String REDO = "[\"post-order\"]";

    private DataSource dataSource;
    private PostgresStepOutcomeStore store;

    @BeforeEach
    void setUp() throws Exception {
        PGSimpleDataSource pg = new PGSimpleDataSource();
        pg.setURL(POSTGRES.getJdbcUrl());
        pg.setUser(POSTGRES.getUsername());
        pg.setPassword(POSTGRES.getPassword());
        dataSource = pg;
        TestJdbc.awaitReachable(dataSource);
        store = new PostgresStepOutcomeStore(dataSource);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("drop table if exists bip_message_replay_audit, bip_message_attempt, bip_message_log, "
                    + "bip_step_outcome cascade");
            for (String resource : new String[]{"bip_message_store.sql", "bip_step_outcome.sql"}) {
                for (String sql : schema(resource).split(";")) {
                    if (!sql.isBlank()) {
                        statement.execute(sql);
                    }
                }
            }
        }
    }

    @Test
    void aRequestedOrPublishedReplayOfThisIntegrationTopicAndAttemptAuthorises() throws Exception {
        audit("message-1", "order-pipeline", "bip.order-pipeline.in", 2, REDO, "REQUESTED");
        assertTrue(store.isAuthorisedReplay(claim("message-1", 2, REDO)), "Consumed before the audit row is PUBLISHED");
        execute("update bip_message_replay_audit set status = 'PUBLISHED'");
        assertTrue(store.isAuthorisedReplay(claim("message-1", 2, REDO)));
        assertTrue(store.isAuthorisedReplay(claim("message-1", 2, "[ \"post-order\" ]")), "Compared as JSON");
    }

    @Test
    void anythingElseDoesNotAuthorise() throws Exception {
        audit("message-1", "order-pipeline", "bip.order-pipeline.in", 2, REDO, "PUBLISHED");
        audit("message-2", "order-pipeline", "bip.order-pipeline.in", 2, REDO, "FAILED");

        assertFalse(store.isAuthorisedReplay(claim("message-9", 2, REDO)), "No replay of this message");
        assertFalse(store.isAuthorisedReplay(claim("message-1", 3, REDO)), "Another attempt");
        assertFalse(store.isAuthorisedReplay(claim("message-1", 2, "[\"post-order\",\"post-invoice\"]")),
                "Another redo list");
        assertFalse(store.isAuthorisedReplay(new RedoClaim("message-1", "billing", "bip.order-pipeline.in", 2, REDO)),
                "Another integration's replay");
        assertFalse(store.isAuthorisedReplay(new RedoClaim("message-1", "order-pipeline", "bip.other.in", 2, REDO)),
                "Published to another topic");
        assertFalse(store.isAuthorisedReplay(claim("message-2", 2, REDO)), "A FAILED replay");
    }

    @Test
    void aReplayIsHonouredOnlyUntilItsAttemptHasAnOutcome() throws Exception {
        audit("message-1", "order-pipeline", "bip.order-pipeline.in", 2, REDO, "PUBLISHED");
        execute("insert into bip_message_attempt (message_id, attempt, schema_version, integration, route_id, status,"
                + " occurred_at) values ('message-1', 2, 1, 'order-pipeline', 'input', 'COMPLETED', now())");

        assertFalse(store.isAuthorisedReplay(claim("message-1", 2, REDO)),
                "A copy of the replay record, or the same record read again after its outcome");
    }

    @Test
    void aLoginThatMayNotReadTheAuditFailsWithItsSqlState() throws Exception {
        execute("drop role if exists replay_reader");
        execute("create role replay_reader login password 'reader-test-only'");
        execute("grant select on bip_step_outcome, bip_message_attempt to replay_reader");
        PGSimpleDataSource reader = new PGSimpleDataSource();
        reader.setURL(POSTGRES.getJdbcUrl());
        reader.setUser("replay_reader");
        reader.setPassword("reader-test-only");

        var failure = assertThrows(StepOutcomeStoreException.class,
                () -> new PostgresStepOutcomeStore(reader).isAuthorisedReplay(claim("message-1", 2, REDO)));
        assertEquals("42501", assertInstanceOf(SQLException.class, failure.getCause()).getSQLState());
    }

    private static RedoClaim claim(String messageId, int attempt, String redo) {
        return new RedoClaim(messageId, "order-pipeline", "bip.order-pipeline.in", attempt, redo);
    }

    private void audit(String messageId, String integration, String topic, int attempt, String redo, String status)
            throws Exception {
        execute("insert into bip_message_replay_audit (replay_id, message_id, integration, replay_attempt, redo_steps,"
                + " target_topic, status, requested_at) values ('" + UUID.randomUUID() + "', '" + messageId + "', '"
                + integration + "', " + attempt + ", '" + redo + "'::jsonb, '" + topic + "', '" + status + "', now())");
    }

    private void execute(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private String schema(String resource) throws Exception {
        try (InputStream input = getClass().getResourceAsStream("/db/message-store/" + resource)) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
