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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * The README's forms for removing headers before a tracked send: {@code removeHeaders("*")}
 * also removes {@code x-bip-message-id}, so every delivery sends again under a new key; keeping
 * the {@code x-bip-} prefix keeps the consumed record's key, and the Kafka key header for parking.
 */
class RemoveHeadersBeforeTrackedSendTest {
    private final Set<String> delivered = new HashSet<>();
    private final List<MessageAttemptResult> rows = new ArrayList<>();
    private final ReceiverComponent receiver = new ReceiverComponent();
    private DefaultCamelContext context;

    @AfterEach
    void stop() throws Exception {
        if (context != null) context.close();
    }

    @Test
    void removingEveryHeaderSendsAgainUnderANewKeyOnEachDelivery() throws Exception {
        start(route -> route.removeHeaders("*"));

        send();
        send();

        assertEquals(2, receiver.received().size(), "The second delivery is not skipped");
        List<Object> keys = receiver.received().stream()
                .map(e -> e.getMessage().getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY)).toList();
        assertNotEquals(keys.get(0), keys.get(1));
        assertNotEquals("message-1", rows.getLast().messageId(), "Journaled under the generated id");
    }

    @Test
    void keepingTheAdaptersHeadersKeepsTheStepKey() throws Exception {
        start(route -> route.removeHeaders("*", "x-bip-.*"));

        send();
        send();

        assertEquals(1, receiver.received().size(), "The second delivery skips the delivered step");
        var sent = receiver.received().getFirst().getMessage();
        assertEquals("message-1:send-order", sent.getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY));
        assertEquals(null, sent.getHeader("Authorization"), "The record's other headers are removed");
        assertEquals("message-1", rows.getLast().messageId());
    }

    @Test
    void theKafkaKeyHeaderCanBeKeptForParking() throws Exception {
        start(route -> route.removeHeaders("*", "x-bip-.*", "kafka.KEY", "CamelKafkaKey"));

        send();

        var sent = receiver.received().getFirst().getMessage();
        assertEquals("order-key", sent.getHeader(KafkaConstants.KEY));
        assertEquals("message-1:send-order", sent.getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY));
        assertEquals(null, sent.getHeader("Authorization"));
    }

    private void start(Function<RouteDefinition, RouteDefinition> removeHeaders) throws Exception {
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return delivered.contains(key); }
            public void recordDelivered(StepResult step) { delivered.add(step.idempotencyKey()); }
            public void recordFailed(StepResult step) { }
        };
        context = new DefaultCamelContext();
        context.addComponent("vendor", receiver);
        context.getCamelContextExtension().addInterceptStrategy(
                new StepTrackingInterceptStrategy(new DefaultStepTracker(store)));
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                removeHeaders.apply(from("direct:input").routeId("input")
                                .routePolicy(new MessageStoreRoutePolicy(rows::add, "order-pipeline")))
                        .to("vendor:orders").id("send-order");
            }
        });
        context.start();
    }

    private void send() {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.KEY, "order-key");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                e.getMessage().setHeader("Authorization", "Bearer test-only");
                e.getMessage().setBody("order");
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
