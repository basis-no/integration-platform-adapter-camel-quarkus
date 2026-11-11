package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** A mark belongs to one step key: split parts with their own ids never take each other's. */
class MarkKeyTest {
    private final Set<String> delivered = ConcurrentHashMap.newKeySet();
    private final AtomicInteger commits = new AtomicInteger();
    private volatile boolean firstLineFails = true;
    private volatile String deliveredWriteFailsFor = "message-1-1:post-line";
    private final ReceiverComponent receiver = new ReceiverComponent(exchange -> {
        if (firstLineFails && "a".equals(exchange.getMessage().getBody(String.class))) {
            throw new IllegalStateException("synthetic receiver failure");
        }
    });
    private DefaultCamelContext context;

    @AfterEach
    void stop() throws Exception {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void aFailedPartIsSentAgainAndOnlyTheMarkedPartIsWrittenFromItsMark() throws Exception {
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return delivered.contains(key); }
            public void recordDelivered(StepResult step) {
                if (step.idempotencyKey().equals(deliveredWriteFailsFor)) {
                    throw new StepOutcomeStoreException("read-only", new SQLException("read-only", "25006"));
                }
                delivered.add(step.idempotencyKey());
            }
            public void recordFailed(StepResult step) { }
        };
        DefaultStepTracker tracker = new DefaultStepTracker(store, new DefaultStepClassifier(Optional.empty(),
                Optional.empty(), Optional.of("post-line"), Optional.empty()));
        tracker.setRetrySleeper(millis -> true);
        context = new DefaultCamelContext();
        context.addComponent("receiver", receiver);
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(tracker));
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:input").routeId("input").errorHandler(noErrorHandler())
                        .routePolicy(new MessageStoreRoutePolicy(result -> { }, "order-pipeline"))
                        .split(body().tokenize(","))
                            .setHeader(PlatformHeaders.MESSAGE_ID,
                                    simple("${header.x-bip-message-id}-${exchangeProperty.CamelSplitIndex}"))
                            .doTry().to("receiver:lines").id("post-line").doCatch(Exception.class).end()
                        .end();
            }
        });
        context.start();

        read();
        assertEquals(List.of("a", "b"), bodies(), "Part a failed and was handled; part b's outcome was not written");
        assertEquals(0, commits.get());

        firstLineFails = false;
        deliveredWriteFailsFor = null;
        read();

        assertEquals(List.of("a", "b", "a"), bodies(), "Part a is sent again; part b is written from its mark");
        assertEquals(Set.of("message-1-0:post-line", "message-1-1:post-line"), delivered);
        assertEquals(1, commits.get());
    }

    @Test
    void aSecondPartWithTheSameStepKeyIsNotListedAsDeliveredFromTheMark() throws Exception {
        firstLineFails = false;
        deliveredWriteFailsFor = "message-1:post-line";
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return delivered.contains(key); }
            public void recordDelivered(StepResult step) {
                if (step.idempotencyKey().equals(deliveredWriteFailsFor)) {
                    throw new StepOutcomeStoreException("read-only", new SQLException("read-only", "25006"));
                }
                delivered.add(step.idempotencyKey());
            }
            public void recordFailed(StepResult step) { }
        };
        DefaultStepTracker tracker = new DefaultStepTracker(store, new DefaultStepClassifier(Optional.empty(),
                Optional.empty(), Optional.of("post-line"), Optional.empty()));
        tracker.setRetrySleeper(millis -> true);
        context = new DefaultCamelContext();
        context.addComponent("receiver", receiver);
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(tracker));
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                // Both parts keep the input's message id, so they share one step key.
                from("direct:input").routeId("input").errorHandler(noErrorHandler())
                        .routePolicy(new MessageStoreRoutePolicy(result -> { }, "order-pipeline"))
                        .split(body().tokenize(","))
                            .to("receiver:lines").id("post-line")
                        .end();
            }
        });
        context.start();

        read();
        assertEquals(List.of("a"), bodies(), "Part a was sent; part b was not, after the failed write");
        deliveredWriteFailsFor = null;
        try (var logs = no.basis.integrationplatform.camel.LogCapture.of(DefaultStepTracker.class)) {
            read();

            assertEquals(List.of("a"), bodies(), "Nothing is sent again");
            List<String> warnings = logs.at(java.util.logging.Level.WARNING).stream()
                    .map(no.basis.integrationplatform.camel.LogCapture::text).toList();
            List<String> errors = logs.at(java.util.logging.Level.SEVERE).stream()
                    .map(no.basis.integrationplatform.camel.LogCapture::text).toList();
            assertEquals(1, warnings.stream().filter(line -> line.contains("was sent in an earlier read")).count(),
                    "Only the part that took the mark's send says it was sent: " + warnings);
            assertEquals(1, errors.stream().filter(line -> line.contains("so this part was not delivered")).count(),
                    "The other part is reported as not delivered: " + errors);
        }
    }

    private List<String> bodies() {
        return receiver.received().stream().map(e -> e.getMessage().getBody(String.class)).toList();
    }

    private void read() {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.PARTITION, 0);
                e.getMessage().setHeader(KafkaConstants.OFFSET, 7L);
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                e.getMessage().setBody("a,b");
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
