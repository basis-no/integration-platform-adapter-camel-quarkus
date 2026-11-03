package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.replay.MessageReplayException;
import no.basis.integrationplatform.camel.replay.PostgresKafkaMessageReplayService;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two integrations on one Message Store. The upstream forwards its x-bip-message-id, as
 * Camel's Kafka producer does by default, and the downstream Kafka input adopts it. The
 * journal is keyed by message id alone, so the downstream replaces the upstream's rows.
 */
@Testcontainers
class SharedMessageIdJournalTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");
    private PGSimpleDataSource dataSource;

    @BeforeEach
    void schema() throws Exception {
        dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        no.basis.integrationplatform.camel.TestJdbc.awaitReachable(dataSource);
        try (var c = dataSource.getConnection(); var s = c.createStatement();
             var in = getClass().getResourceAsStream("/db/message-store/bip_message_store.sql")) {
            s.execute("drop table if exists bip_message_replay_audit, bip_message_attempt, bip_message_log cascade");
            for (String sql : new String(in.readAllBytes(), StandardCharsets.UTF_8).split(";")) {
                if (!sql.isBlank()) s.execute(sql);
            }
        }
    }

    @Test
    void downstreamReplacesTheUpstreamJournalRowsAndIsReported() throws Exception {
        var sink = new PostgresMessageStoreSink(dataSource);
        try (var context = new DefaultCamelContext(); var logs = LogCapture.of(PostgresMessageStoreSink.class)) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:parking").errorHandler(noErrorHandler()).log("parked");
                    from("direct:upstream").routeId("upstream-input")
                            .routePolicy(new MessageStoreRoutePolicy(sink, "upstream"))
                            .log("published to the downstream topic");
                    from("direct:downstream").routeId("downstream-input")
                            .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .routePolicy(new MessageStoreRoutePolicy(sink, "downstream"))
                            .process(e -> { throw new IllegalStateException("downstream receiver rejected"); });
                }
            });
            context.start();
            var producer = context.createProducerTemplate();
            var upstream = producer.send("direct:upstream", e -> input(e, null));
            String forwardedId = upstream.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
            assertEquals("upstream", value("select integration from bip_message_log where message_id = '" + forwardedId + "'"));
            assertEquals("COMPLETED", value("select status from bip_message_log where message_id = '" + forwardedId + "'"));

            // The forwarded record keeps the id; the downstream input adopts it.
            producer.send("direct:downstream", e -> input(e, forwardedId));

            assertEquals("downstream", value("select integration from bip_message_log where message_id = '" + forwardedId + "'"));
            assertEquals("DLQ", value("select status from bip_message_log where message_id = '" + forwardedId + "'"));
            assertEquals("downstream", value("select integration from bip_message_attempt where message_id = '" + forwardedId + "' and attempt = 1"));
            assertEquals("0", value("select count(*) from bip_message_log where integration = 'upstream'"),
                    "The upstream's per-integration view no longer contains its message");
            var warnings = logs.at(Level.WARNING);
            assertEquals(1, warnings.size());
            assertTrue(LogCapture.text(warnings.getFirst()).startsWith("Message id " + forwardedId
                    + " is also journaled by integration upstream; integration downstream records attempt 1"));
        }
        // Replay belongs to the integration recorded on the row: the upstream can no longer replay it.
        var constructor = PostgresKafkaMessageReplayService.class.getDeclaredConstructor(javax.sql.DataSource.class,
                String.class, String.class, org.apache.camel.CamelContext.class, String.class);
        constructor.setAccessible(true);
        var upstreamReplay = constructor.newInstance(dataSource, "localhost:1", "bip.%s.in", new DefaultCamelContext(), "upstream");
        String id = value("select message_id from bip_message_log");
        assertThrows(MessageReplayException.class, () -> upstreamReplay.replay(id));
    }

    @Test
    void sharedIdsAreReportedOncePerPairAndThenPeriodicallyWithACount() {
        var now = new java.util.concurrent.atomic.AtomicReference<>(java.time.Instant.parse("2026-10-28T12:00:00Z"));
        var clock = new java.time.Clock() {
            public java.time.ZoneId getZone() { return java.time.ZoneOffset.UTC; }
            public java.time.Clock withZone(java.time.ZoneId zone) { return this; }
            public java.time.Instant instant() { return now.get(); }
        };
        var sink = new PostgresMessageStoreSink(dataSource, new MessageStatusEventJson(), clock);
        try (var logs = LogCapture.of(PostgresMessageStoreSink.class)) {
            for (int i = 1; i <= 4; i++) {
                if (i == 4) now.set(now.get().plus(PostgresMessageStoreSink.SHARED_ID_WARNING_INTERVAL));
                sink.record(row("message-" + i, "upstream"));
                sink.record(row("message-" + i, "downstream"));
            }
            sink.record(row("message-1", "third"));

            var warnings = logs.at(Level.WARNING).stream().map(LogCapture::text).toList();
            assertEquals(3, warnings.size(), warnings.toString());
            assertTrue(warnings.get(0).contains("1 such message(s) for this pair"), warnings.get(0));
            assertTrue(warnings.get(1).startsWith("Message id message-4 is also journaled by integration upstream"), warnings.get(1));
            assertTrue(warnings.get(1).contains("3 such message(s) for this pair"), warnings.get(1));
            assertTrue(warnings.get(2).contains("integration third records attempt 1"), warnings.get(2));
        }
    }

    private static no.basis.integrationplatform.camel.model.MessageAttemptResult row(String messageId, String integration) {
        return new no.basis.integrationplatform.camel.model.MessageAttemptResult(1, messageId, "order-1", null, null, null,
                "metadata_only", null, null, null, null, false, "test", integration, "route", 1, false, null, null,
                java.time.Instant.parse("2026-10-28T12:00:00Z"), no.basis.integrationplatform.camel.model.MessageStatus.COMPLETED,
                java.util.List.of(), null);
    }

    @Test
    void sameIntegrationReplayIsNotReportedAsShared() throws Exception {
        var sink = new PostgresMessageStoreSink(dataSource);
        try (var context = new DefaultCamelContext(); var logs = LogCapture.of(PostgresMessageStoreSink.class)) {
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:input").routePolicy(new MessageStoreRoutePolicy(sink, "upstream")).log("ok");
                }
            });
            context.start();
            var producer = context.createProducerTemplate();
            producer.send("direct:input", e -> input(e, "message-1"));
            producer.send("direct:input", e -> { input(e, "message-1"); e.getMessage().setHeader(PlatformHeaders.ATTEMPT, "2"); });
            producer.send("direct:input", e -> input(e, "message-1"));
            assertTrue(logs.at(Level.WARNING).isEmpty());
            assertEquals("2", value("select last_attempt from bip_message_log where message_id = 'message-1'"));
        }
    }

    private static void input(org.apache.camel.Exchange e, String messageId) {
        e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.input");
        e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
        if (messageId != null) e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, messageId);
        e.getMessage().setBody("<Order/>");
    }

    private String value(String sql) {
        try (var c = dataSource.getConnection(); var s = c.createStatement(); var r = s.executeQuery(sql)) {
            r.next();
            return r.getString(1);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
