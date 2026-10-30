package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Two integrations share one durable step store and one message id: an upstream poller
 * hands its message id downstream, as Camel's Kafka producer does by default.
 */
class CrossIntegrationStepKeyTest {
    private final Set<String> ledger = Collections.synchronizedSet(new HashSet<>());
    private final StepOutcomeStore sharedStore = new StepOutcomeStore() {
        public boolean wasDelivered(String key) { return ledger.contains(key); }
        public void recordDelivered(StepResult step) { ledger.add(step.idempotencyKey()); }
        public void recordFailed(StepResult step) { }
    };

    /** Pins 0.3.x behaviour: generated ids collide, explicit distinct ids do not. */
    @ParameterizedTest
    @CsvSource({"false, 0", "true, 1"})
    void downstreamSendIsSkippedOnlyWhenGeneratedStepIdsCollide(boolean explicitIds, int downstreamSends) throws Exception {
        var fetch = new ReceiverComponent();
        var receiver = new ReceiverComponent();
        List<MessageAttemptResult> rows = new ArrayList<>();
        try (var poller = new DefaultCamelContext(); var gateway = new DefaultCamelContext()) {
            poller.addComponent("db", fetch);
            gateway.addComponent("receiver", receiver);
            poller.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(sharedStore)));
            gateway.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(sharedStore)));
            poller.addRoutes(new RouteBuilder() {
                public void configure() {
                    var to = from("direct:poll").to("db:select");
                    if (explicitIds) to.id("poller-fetch");
                }
            });
            gateway.addRoutes(new RouteBuilder() {
                public void configure() {
                    var to = from("direct:input").routePolicy(new MessageStoreRoutePolicy(rows::add, "gateway"))
                            .to("receiver:deliver");
                    if (explicitIds) to.id("gateway-deliver");
                }
            });
            // Each integration is its own JVM, where Camel's generated node ids start at to1.
            NodeIds.reset();
            poller.start();
            NodeIds.reset();
            gateway.start();
            // The poller's first tracked send generates the message id that travels downstream.
            var polled = poller.createProducerTemplate().send("direct:poll", e -> e.getMessage().setBody("row"));
            String messageId = polled.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
            gateway.createProducerTemplate().send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.gateway.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, messageId);
                e.getMessage().setBody("<POSLog/>");
            });
        }
        assertEquals(downstreamSends, receiver.received().size());
        assertEquals(explicitIds ? "DELIVERED" : "SKIPPED", rows.getFirst().steps().getFirst().status().name());
    }

    /** Camel numbers generated node ids with JVM-wide counters. */
    private static final class NodeIds extends org.apache.camel.impl.engine.DefaultNodeIdFactory {
        static void reset() {
            resetAllCounters();
        }
    }

    @Test
    void warnsOnceAtStartupForEachTrackedSendWithoutAnExplicitId() throws Exception {
        try (var logs = LogCapture.of(StepTrackingInterceptStrategy.class); var context = new DefaultCamelContext()) {
            context.addComponent("receiver", new ReceiverComponent());
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(sharedStore)));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:in").routeId("deliveries")
                            .to("receiver:generated?password=s3cr3t")
                            .to("receiver:named").id("named-send")
                            .to("direct:internal");
                    from("direct:internal").log("internal");
                }
            });
            context.start();

            var warnings = logs.at(Level.WARNING);
            assertEquals(1, warnings.size());
            String text = LogCapture.text(warnings.getFirst());
            assertTrue(text.startsWith("Tracked send to receiver://generated?password=xxxxxx in route deliveries has no explicit id"), text);
        }
    }

    @Test
    void generatedStepIdFailsRouteCreationByDefault() throws Exception {
        try (var context = enforcing(false)) {
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:in").routeId("deliveries").to("receiver:generated?password=s3cr3t");
                }
            });
            var failure = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, context::start);
            Throwable cause = failure;
            while (cause.getCause() != null && !(cause instanceof IllegalStateException)) cause = cause.getCause();
            String message = cause.getMessage();
            assertTrue(message.startsWith("Tracked send to receiver://generated?password=xxxxxx in route deliveries has no explicit id"), message);
            assertTrue(message.contains(StepTrackingInterceptStrategy.EMERGENCY_ALLOW_GENERATED_STEP_IDS + "=true"), message);
        }
    }

    @Test
    void enforcementIgnoresExplicitIdsInternalEndpointsWireTapsAndTheDeadLetterRoute() throws Exception {
        try (var context = enforcing(false)) {
            var properties = new java.util.Properties();
            properties.setProperty("basis.message-store.dlq-uri", "receiver:dead-letters");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addRoutes(new no.basis.integrationplatform.camel.PlatformRouteConfiguration());
            context.addRoutes(new no.basis.integrationplatform.camel.PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:in").routeId("deliveries")
                            .to("receiver:named").id("named-send")
                            .toD("receiver:${header.target}").id("dynamic-send")
                            .to("direct:internal")
                            .wireTap("receiver:audit-copy");
                    from("direct:internal").log("internal");
                }
            });
            context.start();
            assertTrue(context.isStarted());
        }
    }

    @Test
    void emergencyFlagStartsWithAWarning() throws Exception {
        try (var logs = LogCapture.of(StepTrackingInterceptStrategy.class); var context = enforcing(true)) {
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:in").routeId("deliveries").to("receiver:generated");
                }
            });
            context.start();
            var warnings = logs.at(Level.WARNING);
            assertEquals(1, warnings.size());
            assertTrue(LogCapture.text(warnings.getFirst()).contains(
                    StepTrackingInterceptStrategy.EMERGENCY_ALLOW_GENERATED_STEP_IDS + "=true is set"));
        }
    }

    private DefaultCamelContext enforcing(boolean allowGenerated) {
        var context = new DefaultCamelContext();
        context.addComponent("receiver", new ReceiverComponent());
        context.getCamelContextExtension().addInterceptStrategy(
                new StepTrackingInterceptStrategy(new DefaultStepTracker(sharedStore), null, allowGenerated));
        return context;
    }
}
