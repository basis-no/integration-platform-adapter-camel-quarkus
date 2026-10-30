package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.NoopStepOutcomeStore;
import no.basis.integrationplatform.camel.step.PostgresStepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.Endpoint;
import org.apache.camel.builder.AggregationStrategies;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.ProcessorEndpoint;
import org.junit.jupiter.api.BeforeEach;
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

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * How many of three parts reach an external receiver, per route shape and step store.
 * Pins the conditions documented in docs/split-step-identity.md: with a durable step
 * store, parts that share a message id and a send node are skipped after the first.
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
            // case, store, expected parts delivered of 3
            "kafka-split-to,             postgres, 1",
            "kafka-split-to,             noop,     3",
            "kafka-split-direct-subroute,postgres, 1",
            "kafka-split-toD-per-part,   postgres, 1",
            "kafka-split-grouped-agg,    postgres, 1",
            "kafka-split-stream,         postgres, 1",
            "kafka-split-per-part-id,    postgres, 3",
            "kafka-split-new-exchange,   postgres, 3",
            "kafka-split-only-internal,  postgres, 3",
            "poll-send-before-split,     postgres, 1",
            "poll-no-send-before-split,  postgres, 3",
            "poll-send-before-split,     noop,     3",
            "kafka-loop-same-exchange,   postgres, 1",
            "kafka-loop-copy,            postgres, 1",
    })
    void partsDelivered(String shape, String storeKind, int expected) throws Exception {
        StepOutcomeStore store = storeKind.equals("postgres") ? new PostgresStepOutcomeStore(dataSource) : new NoopStepOutcomeStore();
        List<String> received = Collections.synchronizedList(new ArrayList<>());
        try (var context = new DefaultCamelContext()) {
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
                producer.send("direct:in", e -> {
                    if (shape.startsWith("kafka")) {
                        e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.impact.in");
                        e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                        e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                    }
                    e.getMessage().setBody("a,b,c");
                });
            }
        }
        long parts = received.stream().filter(r -> !r.startsWith("fetch:")).count();
        assertEquals(expected, parts, received.toString());
    }

    /** A poller: tracked fetch, row split, document split, fan-out to two topics. */
    @ParameterizedTest(name = "poller perRecordIds={0}")
    @CsvSource({"false, 2", "true, 8"})
    void pollerShape(boolean perRecordIds, int expected) throws Exception {
        StepOutcomeStore store = new PostgresStepOutcomeStore(dataSource);
        List<String> received = Collections.synchronizedList(new ArrayList<>());
        try (var context = new DefaultCamelContext()) {
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
                producer.sendBody("direct:poll", "p1,p2;p3,p4");
            }
        }
        long published = received.stream().filter(r -> !r.startsWith("jdbc:")).count();
        assertEquals(expected, published, received.toString());
    }
}
