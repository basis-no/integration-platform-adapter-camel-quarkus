package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.RedoClaim;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A route that is not a managed Kafka input, such as an HTTP intake, gets the adapter's redo-list
 * policy. A caller's x-bip-redo-steps must not make it send one of its delivered steps again.
 */
class CallerRedoListTest {
    private final Set<String> ledger = new HashSet<>(Set.of("message-1:post-order"));
    private final StepOutcomeStore store = new StepOutcomeStore() {
        public boolean wasDelivered(String key) { return ledger.contains(key); }
        public void recordDelivered(StepResult step) { ledger.add(step.idempotencyKey()); }
        public void recordFailed(StepResult step) { }
        // Even a store that would confirm any replay is not asked for a caller's list.
        public boolean isAuthorisedReplay(RedoClaim claim) { return true; }
    };

    @Test
    void aCallersRedoListOnARouteThatIsNotAKafkaInputIsDropped() throws Exception {
        var receiver = new ReceiverComponent();
        try (var context = new DefaultCamelContext()) {
            context.addComponent("receiver", receiver);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(row -> { }, "orders", "postgres-inline", 1000, 30, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:intake").routeId("orders-intake").to("receiver:orders").id("post-order");
                }
            });
            context.start();
            assertTrue(context.getRoute("orders-intake").getRoutePolicyList().stream()
                    .anyMatch(RedoStepsRoutePolicy.class::isInstance));

            context.createProducerTemplate().send("direct:intake", e -> {
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                e.getMessage().setHeader(PlatformHeaders.REPLAY, "true");
                e.getMessage().setHeader(PlatformHeaders.ATTEMPT, "2");
                e.getMessage().setHeader(PlatformHeaders.REDO_STEPS, "[\"post-order\"]");
                e.getMessage().setBody("<Order/>");
            });
        }
        assertEquals(0, receiver.received().size(), "The delivered step is skipped, as without the header");
    }
}
