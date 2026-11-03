package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.Processor;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins known 0.3 split limitations so that any change is deliberate. Split parts share
 * the input's message id; per-part step identity and per-part parking belong to 0.4.
 * Neither changes the input's message id or correlation.
 */
class SplitBoundaryLimitationsTest {
    private final Set<String> delivered = new HashSet<>();
    private final List<MessageAttemptResult> rows = new ArrayList<>();
    private final List<String> parked = new ArrayList<>();
    private final AtomicInteger commits = new AtomicInteger();
    private DefaultCamelContext context;

    @AfterEach
    void stop() throws Exception {
        if (context != null) context.close();
    }

    @Test
    void durableStepStoreSkipsLaterPartsThatShareOneStepIdentity() throws Exception {
        // With basis.message-store.step-outcome-store=postgres every part has the key
        // message-1:send-line, so only the first part is sent, even in the first attempt.
        var receiver = start(exchange -> { }, false);

        send("a,b,c");

        assertEquals(List.of("a"), receiver.received().stream().map(e -> e.getMessage().getBody(String.class)).toList());
        assertEquals(Set.of("message-1:send-line"), delivered);
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
        // Steps recorded on split parts are not added to the input's attempt row here.
        assertTrue(rows.getFirst().steps().isEmpty());
    }

    @Test
    void failedPartsWithoutStopOnExceptionAreParkedSeparatelyWhileTheInputCompletes() throws Exception {
        var receiver = start(exchange -> {
            throw new IllegalStateException("synthetic rejection of " + exchange.getMessage().getBody(String.class));
        }, false);

        send("a,b");

        assertEquals(2, receiver.received().size());
        // Each part is parked with the original input payload; the input row does not say DLQ.
        assertEquals(List.of("a,b", "a,b"), parked);
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
        assertEquals(1, commits.get());
    }

    @Test
    void stopOnExceptionParksTheInputOnce() throws Exception {
        start(exchange -> {
            throw new IllegalStateException("synthetic rejection of " + exchange.getMessage().getBody(String.class));
        }, true);

        send("a,b");

        assertEquals(List.of("a,b"), parked);
        assertEquals(MessageStatus.DLQ, rows.getFirst().status());
        assertEquals("synthetic rejection of a", rows.getFirst().error());
        assertEquals(1, commits.get());
    }

    private ReceiverComponent start(Processor behaviour, boolean stopOnException) throws Exception {
        var receiver = new ReceiverComponent(behaviour);
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return delivered.contains(key); }
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
                        .process(e -> parked.add(e.getMessage().getBody(String.class)));
                var split = from("direct:input").routeId("input")
                        .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .routePolicy(new MessageStoreRoutePolicy(rows::add, "order-pipeline"))
                        .split(body().tokenize(",")).shareUnitOfWork();
                (stopOnException ? split.stopOnException() : split)
                        .to("vendor:lines").id("send-line")
                        .end();
            }
        });
        context.start();
        return receiver;
    }

    private void send(String body) {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                e.getMessage().setBody(body);
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
