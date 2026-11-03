package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import no.basis.integrationplatform.camel.store.LoggingMessageStoreSink;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A replay that skips an already delivered step must not look delivered in logs or to the route. */
class SkippedStepVisibilityTest {
    private final Set<String> delivered = new HashSet<>();
    private final List<MessageAttemptResult> rows = new ArrayList<>();
    private final List<String> routeView = new ArrayList<>();
    private final ReceiverComponent receiver = new ReceiverComponent();
    private DefaultCamelContext context;

    @BeforeEach
    void start() throws Exception {
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return delivered.contains(key); }
            public void recordDelivered(StepResult step) { delivered.add(step.idempotencyKey()); }
            public void recordFailed(StepResult step) { }
        };
        var logging = new LoggingMessageStoreSink();
        context = new DefaultCamelContext();
        context.addComponent("vendor", receiver);
        context.getCamelContextExtension().addInterceptStrategy(
                new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:input").routeId("input")
                    .routePolicy(new MessageStoreRoutePolicy(row -> { rows.add(row); logging.record(row); }, "order-pipeline"))
                    .to("vendor:orders").id("send-order")
                    // The supported way for a route to tell a skipped send from a performed one.
                    .process(e -> routeView.add(Boolean.TRUE.equals(e.getProperty(
                            PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class)) ? "skipped" : "sent"));
            }
        });
        context.start();
    }

    @AfterEach
    void stop() throws Exception {
        context.close();
    }

    @Test
    void replayOfDeliveredStepLogsSkippedAndClaimsNoDelivery() {
        send(null);
        assertEquals(1, receiver.received().size());

        try (var tracker = LogCapture.of(DefaultStepTracker.class);
             var sink = LogCapture.of(LoggingMessageStoreSink.class)) {
            send(null);

            assertEquals(1, receiver.received().size(), "The replay must not repeat the send");
            assertEquals(StepStatus.SKIPPED, rows.getLast().steps().getFirst().status());
            List<LogRecord> skips = tracker.at(Level.INFO);
            assertEquals(1, skips.size());
            assertEquals("Step send-order SKIPPED for message message-1: an earlier attempt recorded it as delivered, "
                    + "so the send to vendor://orders was not repeated in this attempt; list the step in "
                    + PlatformHeaders.REDO_STEPS + " to repeat it", LogCapture.text(skips.getFirst()));
            String summary = LogCapture.text(sink.at(Level.INFO).getFirst());
            assertTrue(summary.endsWith("status=COMPLETED steps=1 delivered=0 skipped=1 failed=0"), summary);
        }
        assertEquals(List.of("sent", "skipped"), routeView);
    }

    @Test
    void redoRepeatsTheSendWithoutASkipLog() {
        send(null);

        try (var tracker = LogCapture.of(DefaultStepTracker.class);
             var sink = LogCapture.of(LoggingMessageStoreSink.class)) {
            send("[\"send-order\"]");

            assertEquals(2, receiver.received().size());
            assertTrue(tracker.records().stream().noneMatch(record -> LogCapture.text(record).contains("SKIPPED")));
            String summary = LogCapture.text(sink.at(Level.INFO).getFirst());
            assertTrue(summary.endsWith("delivered=1 skipped=0 failed=0"), summary);
        }
        assertEquals(List.of("sent", "sent"), routeView);
    }

    @Test
    void firstDeliveryLogsNoSkip() {
        try (var tracker = LogCapture.of(DefaultStepTracker.class)) {
            send(null);

            assertFalse(tracker.records().stream().anyMatch(record -> LogCapture.text(record).contains("SKIPPED")));
        }
        assertEquals(StepStatus.DELIVERED, rows.getFirst().steps().getFirst().status());
    }

    private void send(String redoSteps) {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                if (redoSteps != null) {
                    e.getMessage().setHeader(PlatformHeaders.REDO_STEPS, redoSteps);
                }
                e.getMessage().setBody("<Order/>");
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
