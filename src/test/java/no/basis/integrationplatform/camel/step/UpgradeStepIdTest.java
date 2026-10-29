package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Upgrading from generated step ids: a send whose explicit id is its current generated id
 * keeps its step key and its classification, so a message processed again is not sent again.
 */
class UpgradeStepIdTest {
    private final Set<String> ledger = Collections.synchronizedSet(new HashSet<>());
    private final StepOutcomeStore store = new StepOutcomeStore() {
        public boolean wasDelivered(String key) { return ledger.contains(key); }
        public void recordDelivered(StepResult step) { ledger.add(step.idempotencyKey()); }
        public void recordFailed(StepResult step) { }
    };

    @Test
    void explicitIdEqualToTheGeneratedIdKeepsTheStepKeyAcrossTheUpgrade() throws Exception {
        // Before: generated ids are allowed and the send is classified by its generated id.
        var before = new ReceiverComponent();
        run(before, true, route -> route.to("receiver:sap"), null);
        assertEquals(Set.of("message-1:to1"), ledger);
        assertEquals("message-1:to1", before.received().getFirst().getMessage().getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY));

        // After: enforcement is on and the send keeps to1 as its explicit id.
        var after = new ReceiverComponent();
        run(after, false, route -> route.to("direct:audit").to("receiver:sap").id("to1"), null);
        assertTrue(after.received().isEmpty(), "A message processed again after the upgrade is not sent again");

        // A deliberate redo still repeats it under the same receiver idempotency key.
        var redo = new ReceiverComponent();
        run(redo, false, route -> route.to("direct:audit").to("receiver:sap").id("to1"), "[\"to1\"]");
        assertEquals(1, redo.received().size());
        assertEquals("message-1:to1", redo.received().getFirst().getMessage().getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY));
    }

    @Test
    void renamingTheStepIdSendsAMessageProcessedAgainOnceMore() throws Exception {
        run(new ReceiverComponent(), true, route -> route.to("receiver:sap"), null);
        var after = new ReceiverComponent();
        run(after, false, route -> route.to("receiver:sap").id("send-to-sap"), null);
        assertEquals(1, after.received().size(), "The renamed step has a new key, so it is sent again");
    }

    /** Starts one application run with fresh node-id counters, as a new JVM would, and processes message-1. */
    private void run(ReceiverComponent receiver, boolean allowGenerated, Consumer<RouteDefinition> route,
            String redoSteps) throws Exception {
        NodeIds.reset();
        var classifier = new DefaultStepClassifier("", "", "to1,send-to-sap");
        try (var context = new DefaultCamelContext()) {
            context.addComponent("receiver", receiver);
            context.getCamelContextExtension().addInterceptStrategy(
                    new StepTrackingInterceptStrategy(new DefaultStepTracker(store, classifier), allowGenerated));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:audit").routeId("audit").setBody(body());
                    route.accept(from("direct:input").routeId("input"));
                }
            });
            context.start();
            context.createProducerTemplate().send("direct:input", e -> {
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
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
