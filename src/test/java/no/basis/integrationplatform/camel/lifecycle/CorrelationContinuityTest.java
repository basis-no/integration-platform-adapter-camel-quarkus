package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.builder.AggregationStrategies;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The original Kafka correlation reaches every receiver call and the journal unchanged
 * through split, in-process retry, DLQ parking and an operator resend with redo.
 * Each input carries the correlation as Kafka delivers it: UTF-8 header bytes.
 */
class CorrelationContinuityTest {
    private static final String CORRELATION = "order-ø-42";

    private final Set<String> delivered = Collections.synchronizedSet(new HashSet<>());
    private final List<MessageAttemptResult> rows = Collections.synchronizedList(new ArrayList<>());
    private final List<Object> parkedCorrelations = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger commits = new AtomicInteger();
    private DefaultCamelContext context;
    /** The default {@code noop} step store never reports a delivered step; {@code postgres} does. */
    private boolean durableSteps = true;

    @AfterEach
    void stop() throws Exception {
        if (context != null) context.close();
    }

    @ParameterizedTest
    @ValueSource(strings = {"original", "latest", "grouped"})
    void splitSendsOriginalCorrelationWithEveryPart(String aggregation) throws Exception {
        durableSteps = false;
        var receiver = new ReceiverComponent();
        start(receiver, new RouteBuilder() {
            public void configure() {
                var split = from("direct:input").routeId("input").routePolicy(policy());
                AggregationStrategy strategy = switch (aggregation) {
                    case "latest" -> AggregationStrategies.useLatest();
                    case "grouped" -> AggregationStrategies.groupedExchange();
                    default -> null;
                };
                (strategy == null ? split.split(body().tokenize(",")) : split.split(body().tokenize(","), strategy))
                        .shareUnitOfWork()
                        .to("vendor:lines").id("send-line")
                        .end();
            }
        });

        send(input("a,b,c", null, null));

        assertEquals(List.of(CORRELATION, CORRELATION, CORRELATION), receiver.correlations());
        assertEquals(1, rows.size());
        // A grouped aggregation replaces the message without its headers; the row still
        // belongs to the consumed input rather than a generated id with no correlation.
        assertEquals("message-1", rows.getFirst().messageId());
        assertEquals(CORRELATION, rows.getFirst().correlationId());
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
        assertEquals(1, commits.get());
    }

    @Test
    void routeThatDropsPlatformHeadersStillJournalsInputIdentity() throws Exception {
        durableSteps = false;
        var receiver = new ReceiverComponent();
        start(receiver, new RouteBuilder() {
            public void configure() {
                from("direct:input").routeId("input").routePolicy(policy())
                        .to("vendor:orders").id("send-order")
                        .removeHeaders("*");
            }
        });

        send(input("<Order/>", null, null));

        assertEquals(List.of(CORRELATION), receiver.correlations());
        assertEquals("message-1", rows.getFirst().messageId());
        assertEquals(CORRELATION, rows.getFirst().correlationId());
    }

    @Test
    void replayThroughARouteThatDropsHeadersKeepsItsAttemptAndEnvelope() throws Exception {
        durableSteps = false;
        start(new ReceiverComponent(), new RouteBuilder() {
            public void configure() {
                from("direct:input").routeId("input").routePolicy(policy()).removeHeaders("*");
            }
        });

        send(input("<Order/>", null, null));
        send(input("<Order/>", "2", null));

        assertEquals(List.of(1, 2), rows.stream().map(MessageAttemptResult::attempt).toList());
        assertEquals("application/xml", rows.getLast().contentType());
        assertEquals("pos", rows.getLast().sourceSystem());
        assertEquals("sap", rows.getLast().targetSystem());
        assertEquals(3, rows.getLast().schemaVersion());
    }

    @Test
    void correlationSetByTheRouteStillWins() throws Exception {
        durableSteps = false;
        start(new ReceiverComponent(), new RouteBuilder() {
            public void configure() {
                from("direct:input").routeId("input").routePolicy(policy())
                        .setHeader(PlatformHeaders.CORRELATION_ID, constant("derived-from-body"));
            }
        });

        send(input("<Order/>", null, null));

        assertEquals("derived-from-body", rows.getFirst().correlationId());
    }

    @Test
    void inProcessRetryResendsAndJournalsOriginalCorrelation() throws Exception {
        var calls = new AtomicInteger();
        var receiver = new ReceiverComponent(exchange -> {
            if (calls.incrementAndGet() == 1) throw new IllegalStateException("synthetic vendor timeout");
        });
        // The platform default configuration retries after one second.
        start(receiver, new RouteBuilder() {
            public void configure() {
                from("direct:input").routeId("input").routePolicy(policy())
                        .to("vendor:orders").id("send-order");
            }
        });

        send(input("<Order/>", null, null));

        assertEquals(List.of(CORRELATION, CORRELATION), receiver.correlations());
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
        assertTrue(rows.getFirst().autoRedelivered());
        assertEquals(CORRELATION, rows.getFirst().correlationId());
    }

    @Test
    void exhaustedFailureParksAndJournalsOriginalCorrelation() throws Exception {
        var receiver = new ReceiverComponent(exchange -> {
            throw new IllegalStateException("synthetic vendor rejection");
        });
        start(receiver, new RouteBuilder() {
            public void configure() {
                from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .routePolicy(policy())
                        .split(body().tokenize(",")).shareUnitOfWork().stopOnException()
                            .to("vendor:lines").id("send-line")
                        .end();
            }
        });

        send(input("a,b", null, null));

        assertEquals(List.of(CORRELATION), receiver.correlations());
        assertEquals(List.of(CORRELATION), parkedCorrelations);
        assertEquals(MessageStatus.DLQ, rows.getFirst().status());
        assertEquals(CORRELATION, rows.getFirst().correlationId());
        assertEquals(1, commits.get());
    }

    @Test
    void resendWithAndWithoutRedoKeepsOriginalCorrelation() throws Exception {
        var receiver = new ReceiverComponent();
        start(receiver, new RouteBuilder() {
            public void configure() {
                from("direct:input").routeId("input").routePolicy(policy())
                        .to("vendor:orders").id("send-order");
            }
        });

        send(input("<Order/>", null, null));
        // Replay headers as PostgresKafkaMessageReplayService publishes them, with and without redo.
        send(input("<Order/>", "2", null));
        send(input("<Order/>", "3", "[\"send-order\"]"));

        assertEquals(List.of(CORRELATION, CORRELATION), receiver.correlations(), "Only the redo repeats the send");
        assertEquals(List.of(1, 2, 3), rows.stream().map(MessageAttemptResult::attempt).toList());
        assertEquals(List.of(StepStatus.DELIVERED, StepStatus.SKIPPED, StepStatus.DELIVERED),
                rows.stream().map(row -> row.steps().getFirst().status()).toList());
        assertTrue(rows.stream().allMatch(row -> CORRELATION.equals(row.correlationId())));
        assertTrue(rows.stream().allMatch(row -> "message-1".equals(row.messageId())));
    }

    private MessageStoreRoutePolicy policy() {
        return new MessageStoreRoutePolicy(rows::add, "order-pipeline");
    }

    private void start(ReceiverComponent receiver, RouteBuilder routes) throws Exception {
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return durableSteps && delivered.contains(key); }
            public void recordDelivered(StepResult step) { delivered.add(step.idempotencyKey()); }
            public void recordFailed(StepResult step) { }
        };
        context = new DefaultCamelContext();
        var properties = new Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        context.addComponent("vendor", receiver);
        context.getCamelContextExtension().addInterceptStrategy(
                new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
        context.addRoutes(new PlatformRouteConfiguration());
        context.addRoutes(new PlatformDeadLetterRoute());
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                        .process(e -> parkedCorrelations.add(e.getMessage().getHeader(PlatformHeaders.CORRELATION_ID)));
            }
        });
        context.addRoutes(routes);
        context.start();
    }

    private java.util.Map<String, Object> input(String body, String attempt, String redoSteps) {
        var headers = new java.util.HashMap<String, Object>();
        headers.put(KafkaConstants.TOPIC, "bip.order-pipeline.in");
        headers.put(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
        headers.put(PlatformHeaders.MESSAGE_ID, "message-1".getBytes(StandardCharsets.UTF_8));
        headers.put(PlatformHeaders.CORRELATION_ID, CORRELATION.getBytes(StandardCharsets.UTF_8));
        headers.put(PlatformHeaders.CONTENT_TYPE, "application/xml".getBytes(StandardCharsets.UTF_8));
        headers.put(PlatformHeaders.SOURCE_SYSTEM, "pos".getBytes(StandardCharsets.UTF_8));
        headers.put(PlatformHeaders.TARGET_SYSTEM, "sap".getBytes(StandardCharsets.UTF_8));
        headers.put(PlatformHeaders.SCHEMA_VERSION, "3".getBytes(StandardCharsets.UTF_8));
        if (attempt != null) {
            headers.put(PlatformHeaders.REPLAY, "true".getBytes(StandardCharsets.UTF_8));
            headers.put(PlatformHeaders.ATTEMPT, attempt.getBytes(StandardCharsets.UTF_8));
        }
        if (redoSteps != null) {
            headers.put(PlatformHeaders.REDO_STEPS, redoSteps.getBytes(StandardCharsets.UTF_8));
        }
        headers.put("body", body);
        return headers;
    }

    private void send(java.util.Map<String, Object> input) {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                input.forEach((name, value) -> {
                    if (!name.equals("body")) e.getMessage().setHeader(name, value);
                });
                e.getMessage().setBody(input.get("body"));
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
