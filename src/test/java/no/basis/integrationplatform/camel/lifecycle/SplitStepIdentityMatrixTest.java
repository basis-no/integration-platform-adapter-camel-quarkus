package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.NoopStepOutcomeStore;
import no.basis.integrationplatform.camel.step.PostgresStepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.builder.AggregationStrategies;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.ProcessorEndpoint;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * How many of three parts reach an external receiver, per route shape and step store.
 * Pins the conditions documented in docs/split-step-identity.md: with a durable step
 * store, parts that share a message id and a send node are skipped after the first.
 * Each such skip is logged at ERROR, and the input still completes.
 */
@Testcontainers
class SplitStepIdentityMatrixTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");
    private PGSimpleDataSource dataSource;

    @BeforeEach
    void schema() throws Exception {
        dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        try (var c = dataSource.getConnection(); var s = c.createStatement();
             var in = getClass().getResourceAsStream("/db/message-store/bip_step_outcome.sql")) {
            s.execute("drop table if exists bip_step_outcome");
            s.execute(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @ParameterizedTest(name = "{0} store={1}")
    @CsvSource({
            // case, store, expected parts delivered of 3, scope named in the ERROR for each skipped part
            "kafka-split-to,             postgres, 1, split part",
            "kafka-split-to,             noop,     3, ",
            "kafka-split-direct-subroute,postgres, 1, split part",
            "kafka-split-toD-per-part,   postgres, 1, split part",
            "kafka-split-grouped-agg,    postgres, 1, split part",
            "kafka-split-stream,         postgres, 1, split part",
            "kafka-split-per-part-id,    postgres, 3, ",
            "kafka-split-new-exchange,   postgres, 3, ",
            "kafka-split-only-internal,  postgres, 3, ",
            "poll-send-before-split,     postgres, 1, split part",
            "poll-no-send-before-split,  postgres, 3, ",
            "poll-send-before-split,     noop,     3, ",
            "kafka-loop-same-exchange,   postgres, 1, loop iteration",
            "kafka-loop-copy,            postgres, 1, loop iteration",
    })
    void partsDelivered(String shape, String storeKind, int expected, String scope) throws Exception {
        StepOutcomeStore store = storeKind.equals("postgres") ? new PostgresStepOutcomeStore(dataSource) : new NoopStepOutcomeStore();
        List<String> received = Collections.synchronizedList(new ArrayList<>());
        Exchange result;
        try (var context = new DefaultCamelContext();
             var log = LogCapture.of(DefaultStepTracker.class)) {
            context.addComponent("vendor", new DefaultComponent() {
                @Override protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
                    return new ProcessorEndpoint(uri, this, e -> received.add(remaining + ":" + e.getMessage().getBody(String.class)));
                }
            });
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    var policy = new MessageStoreRoutePolicy(r -> { }, "impact");
                    switch (shape) {
                        case "kafka-split-to" -> from("direct:in").routePolicy(policy)
                                .split(body().tokenize(",")).shareUnitOfWork().to("vendor:out").id("send-part").end();
                        case "kafka-split-direct-subroute" -> {
                            from("direct:in").routePolicy(policy)
                                    .split(body().tokenize(",")).to("direct:deliver").end();
                            from("direct:deliver").to("vendor:out").id("send-part");
                        }
                        case "kafka-split-toD-per-part" -> from("direct:in").routePolicy(policy)
                                .split(body().tokenize(",")).toD("vendor:out-${body}").id("send-part").end();
                        case "kafka-split-grouped-agg" -> from("direct:in").routePolicy(policy)
                                .split(body().tokenize(","), AggregationStrategies.groupedExchange())
                                .to("vendor:out").id("send-part").end();
                        case "kafka-loop-same-exchange" -> from("direct:in").routePolicy(policy)
                                .loop(3).to("vendor:out").id("send-part").end();
                        case "kafka-loop-copy" -> from("direct:in").routePolicy(policy)
                                .loop(3).copy().to("vendor:out").id("send-part").end();
                        case "kafka-split-stream" -> from("direct:in").routePolicy(policy)
                                .split(body().tokenize(",")).streaming().to("vendor:out").id("send-part").end();
                        case "kafka-split-per-part-id" -> from("direct:in").routePolicy(policy)
                                .split(body().tokenize(","))
                                .setHeader(PlatformHeaders.MESSAGE_ID,
                                        simple("${header." + PlatformHeaders.MESSAGE_ID + "}-${exchangeProperty.CamelSplitIndex}"))
                                .to("vendor:out").id("send-part").end();
                        case "kafka-split-new-exchange" -> {
                            from("direct:in").routePolicy(policy)
                                    .split(body().tokenize(","))
                                    .process(e -> {
                                        try (var producer = getContext().createProducerTemplate()) {
                                            producer.sendBody("direct:request", e.getMessage().getBody(String.class));
                                        }
                                    }).end();
                            from("direct:request").to("vendor:out").id("send-part");
                        }
                        case "kafka-split-only-internal" -> {
                            from("direct:in").routePolicy(policy)
                                    .split(body().tokenize(",")).to("direct:internal").end();
                            from("direct:internal").process(e -> received.add("internal:" + e.getMessage().getBody(String.class)));
                        }
                        case "poll-send-before-split" -> from("direct:in")
                                .to("vendor:fetch").id("fetch-document")
                                .split(body().tokenize(",")).to("vendor:out").id("send-part").end();
                        case "poll-no-send-before-split" -> from("direct:in")
                                .split(body().tokenize(",")).to("vendor:out").id("send-part").end();
                        default -> throw new IllegalArgumentException(shape);
                    }
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                result = producer.send("direct:in", e -> {
                    if (shape.startsWith("kafka")) {
                        e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.impact.in");
                        e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                        e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                    }
                    e.getMessage().setBody("a,b,c");
                });
            }
            // Detection only: the input completes as before, and each part that was not sent is logged.
            assertNull(result.getException());
            var errors = log.at(Level.SEVERE);
            assertEquals(3 - expected, errors.size(), errors.stream().map(LogCapture::text).toList().toString());
            errors.forEach(error -> assertTrue(LogCapture.text(error).startsWith("Step send-part was skipped in a " + scope
                    + " of input route "), LogCapture.text(error)));
        }
        long parts = received.stream().filter(r -> !r.startsWith("fetch:")).count();
        assertEquals(expected, parts, received.toString());
    }

    @Test
    void theErrorNamesTheRouteStepAndScopeButNoMessageData() throws Exception {
        StepOutcomeStore store = new PostgresStepOutcomeStore(dataSource);
        try (var context = new DefaultCamelContext();
             var log = LogCapture.of(DefaultStepTracker.class)) {
            context.addComponent("vendor", new DefaultComponent() {
                @Override protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
                    return new ProcessorEndpoint(uri, this, e -> { });
                }
            });
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:in").routeId("orders-in").routePolicy(new MessageStoreRoutePolicy(r -> { }, "impact"))
                            .split(body().tokenize(",")).toD("vendor:out-${body}").id("send-part").end();
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.send("direct:in", e -> {
                    e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.impact.in");
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                    e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                    e.getMessage().setHeader("customerNumber", "header-secret-42");
                    e.getMessage().setBody("body-secret-a,body-secret-b");
                });
            }
            var errors = log.at(Level.SEVERE);
            assertEquals(1, errors.size());
            assertEquals("Step send-part was skipped in a split part of input route orders-in, so this part was not "
                    + "delivered: another part of the same attempt already sent or skipped this step key. In 0.3.x split "
                    + "parts, loop iterations and multicast and recipient list branches keep the input's x-bip-message-id, "
                    + "so they share one step key. Give each part its own x-bip-message-id, send each part through a new "
                    + "exchange with its own id, or publish each part with its own id. See docs/split-step-identity.md",
                    LogCapture.text(errors.getFirst()));
            assertTrue(log.records().stream().map(LogCapture::text).noneMatch(text -> text.contains("SKIPPED")),
                    "the skip is not also logged as an earlier attempt's");
            assertTrue(log.records().stream().map(LogCapture::text).noneMatch(text -> text.contains("secret")
                    || text.contains("message-1")), "no message content or header values");
        }
    }

    /** A poller: tracked fetch, row split, document split, fan-out to two topics. */
    @ParameterizedTest(name = "poller perRecordIds={0}")
    @CsvSource({"false, 2", "true, 8"})
    void pollerShape(boolean perRecordIds, int expected) throws Exception {
        StepOutcomeStore store = new PostgresStepOutcomeStore(dataSource);
        List<String> received = Collections.synchronizedList(new ArrayList<>());
        Exchange result;
        try (var context = new DefaultCamelContext();
             var log = LogCapture.of(DefaultStepTracker.class)) {
            context.addComponent("vendor", new DefaultComponent() {
                @Override protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
                    return new ProcessorEndpoint(uri, this, e -> received.add(remaining + ":" + e.getMessage().getBody(String.class)));
                }
            });
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    var document = from("direct:poll")
                            .to("vendor:jdbc")
                            .split(body().tokenize(";")).stopOnException()
                                .split(body().tokenize(",")).stopOnException();
                    if (perRecordIds) {
                        // Each published record gets its own message id before its send.
                        document.multicast()
                                .pipeline().process(e -> e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, java.util.UUID.randomUUID().toString()))
                                    .toD("vendor:gw").id("jdbc-publish-gw").end()
                                .pipeline().process(e -> e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, java.util.UUID.randomUUID().toString()))
                                    .toD("vendor:car").id("jdbc-publish-car").end()
                            .end();
                    } else {
                        // No per-record identity: every record shares the id generated at the fetch.
                        document.multicast().toD("vendor:gw").toD("vendor:car").end();
                    }
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                // Two fetched rows with two documents each.
                result = producer.send("direct:poll", e -> e.getMessage().setBody("p1,p2;p3,p4"));
            }
            assertNull(result.getException());
            assertEquals(8 - expected, log.at(Level.SEVERE).size(), "one ERROR per document send that was skipped");
        }
        long published = received.stream().filter(r -> !r.startsWith("jdbc:")).count();
        assertEquals(expected, published, received.toString());
    }
}
