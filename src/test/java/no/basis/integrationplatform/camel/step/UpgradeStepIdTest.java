package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Upgrading from generated step ids: a send whose explicit id is its current generated id
 * keeps its step key, so messages processed again after the upgrade are not sent again.
 */
class UpgradeStepIdTest {
    private final Set<String> ledger = Collections.synchronizedSet(new HashSet<>());
    private final StepOutcomeStore store = new StepOutcomeStore() {
        public boolean wasDelivered(String key) { return ledger.contains(key); }
        public void recordDelivered(StepResult step) { ledger.add(step.idempotencyKey()); }
        public void recordFailed(StepResult step) { }
    };
    private final List<MessageAttemptResult> rows = new ArrayList<>();

    @Test
    void explicitIdEqualToTheGeneratedIdKeepsTheStepKeyAcrossTheUpgrade() throws Exception {
        // Before: generated ids are allowed, as on 0.3.5. The send gets the generated id to1.
        var before = new ReceiverComponent();
        run(before, true, route -> route.to("receiver:sap"), null, null);
        assertEquals(Set.of("message-1:to1"), ledger);
        assertEquals("message-1:to1", before.received().getFirst().getMessage().getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY));

        // After: enforcement is on, and the send keeps to1 as its explicit id. An internal send
        // in front of it is generated as to1 as well; internal sends are not tracked.
        var after = new ReceiverComponent();
        run(after, false, route -> route.to("direct:audit").to("receiver:sap").id("to1"), "2", null);
        assertTrue(after.received().isEmpty(), "A message processed again after the upgrade is not sent again");
        assertEquals("SKIPPED", rows.getLast().steps().getFirst().status().name());
        assertEquals("message-1:to1", rows.getLast().steps().getFirst().idempotencyKey());

        // A deliberate redo still repeats it under the same receiver idempotency key.
        var redo = new ReceiverComponent();
        run(redo, false, route -> route.to("direct:audit").to("receiver:sap").id("to1"), "3", "[\"to1\"]");
        assertEquals(1, redo.received().size());
        assertEquals("message-1:to1", redo.received().getFirst().getMessage().getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY));
    }

    @Test
    void renamingTheStepIdSendsAMessageProcessedAgainOnceMore() throws Exception {
        run(new ReceiverComponent(), true, route -> route.to("receiver:sap"), null, null);
        var after = new ReceiverComponent();
        run(after, false, route -> route.to("receiver:sap").id("send-to-sap"), "2", null);
        assertEquals(1, after.received().size(), "The renamed step has a new key, so the replay sends again");
    }

    @Test
    void explicitIdsThatLookGeneratedAreAccepted() throws Exception {
        var receiver = new ReceiverComponent();
        run(receiver, false, route -> route.toD("receiver:${header.target}").id("toD2").to("receiver:sap").id("to7"), null, null);
        assertEquals(2, receiver.received().size());
        assertEquals(Set.of("message-1:toD2", "message-1:to7"), ledger);
    }

    @Test
    void generatedNumbersCountAcrossAllRoutesIncludingTheAdaptersDeadLetterRoute() throws Exception {
        var ids = new java.util.LinkedHashMap<String, String>();
        NodeIds.reset();
        try (var context = new DefaultCamelContext()) {
            context.addComponent("receiver", new ReceiverComponent());
            context.addRoutes(new no.basis.integrationplatform.camel.PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:first").routeId("first").to("receiver:a").toD("receiver:${header.target}");
                    from("direct:second").routeId("second").to("receiver:b");
                }
            });
            context.start();
            for (var route : context.getRouteDefinitions()) {
                for (var output : route.getOutputs()) {
                    ids.put(route.getRouteId() + "/" + output.getId(), output.getLabel());
                }
            }
        }
        assertTrue(ids.containsKey("first/to1"), ids.toString());
        assertTrue(ids.containsKey("first/toD2"), "The dead letter route took toD1: " + ids);
        assertTrue(ids.containsKey("second/to2"), ids.toString());
    }

    /** Starts one application run with fresh node-id counters, as a new JVM would, and processes message-1. */
    private void run(ReceiverComponent receiver, boolean allowGenerated, Consumer<RouteDefinition> route,
            String attempt, String redoSteps) throws Exception {
        NodeIds.reset();
        try (var context = new DefaultCamelContext()) {
            context.addComponent("receiver", receiver);
            context.getCamelContextExtension().addInterceptStrategy(
                    new StepTrackingInterceptStrategy(new DefaultStepTracker(store), null, allowGenerated));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:audit").routeId("audit").setBody(body());
                    route.accept(from("direct:input").routeId("input")
                            .routePolicy(new MessageStoreRoutePolicy(rows::add, "order-pipeline")));
                }
            });
            context.start();
            context.createProducerTemplate().send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                e.getMessage().setHeader("target", "sap");
                if (attempt != null) {
                    e.getMessage().setHeader(PlatformHeaders.REPLAY, "true");
                    e.getMessage().setHeader(PlatformHeaders.ATTEMPT, attempt);
                }
                if (redoSteps != null) {
                    e.getMessage().setHeader(PlatformHeaders.REDO_STEPS, redoSteps);
                }
                e.getMessage().setBody("<IDoc/>");
            });
        }
    }

    /** Camel numbers generated node ids with JVM-wide counters. */
    private static final class NodeIds extends org.apache.camel.impl.engine.DefaultNodeIdFactory {
        static void reset() {
            resetAllCounters();
        }
    }
}
