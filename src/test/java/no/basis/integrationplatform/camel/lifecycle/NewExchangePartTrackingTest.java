package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What a split part sent through a new exchange ({@code ProducerTemplate}) gets from the
 * adapter: a step key, an {@code Idempotency-Key}, a journal entry and a skip when the input
 * is replayed. Only a send in a route is tracked; the input's journal row never lists it.
 */
class NewExchangePartTrackingTest {
    private static final String INPUT_ID = "message-1";

    @ParameterizedTest(name = "target={0} ownId={1}")
    @CsvSource({"endpoint, false", "endpoint, true", "route, false", "route, true"})
    void partSentThroughANewExchange(String target, boolean ownId) throws Exception {
        var delivered = Collections.synchronizedSet(new LinkedHashSet<String>());
        var rows = Collections.synchronizedList(new ArrayList<MessageAttemptResult>());
        var receiver = new ReceiverComponent();
        try (var context = new DefaultCamelContext()) {
            context.addComponent("receiver", receiver);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(
                    new DefaultStepTracker(new DurableStore(delivered))));
            String uri = target.equals("route") ? "direct:send-line" : "receiver:lines";
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:input").routeId("orders-in")
                            .routePolicy(new MessageStoreRoutePolicy(rows::add, "order-pipeline"))
                            .split(body().tokenize(","))
                            .process(part -> {
                                String line = part.getMessage().getBody(String.class);
                                try (var producer = getContext().createProducerTemplate()) {
                                    producer.send(uri, request -> {
                                        request.getMessage().setBody(line);
                                        if (ownId) {
                                            request.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, INPUT_ID + "-" + line);
                                        }
                                    });
                                }
                            })
                            .end();
                    from("direct:send-line").routeId("line-sender").to("receiver:lines").id("send-line");
                }
            });
            context.start();
            consume(context, 1);
            consume(context, 2); // the same record again, as a replay
        }

        List<Object> keys = receiver.received().stream()
                .map(e -> e.getMessage().getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY)).toList();
        // The input is journaled once per attempt; no row lists the part's send.
        assertEquals(2, rows.size());
        assertTrue(rows.stream().allMatch(row -> row.messageId().equals(INPUT_ID)), rows.toString());
        assertTrue(rows.stream().allMatch(row -> row.steps().isEmpty()), rows.toString());

        if (target.equals("endpoint")) {
            // Untracked: no step key, no Idempotency-Key, and the replay sends every part again.
            assertEquals(Set.of(), delivered);
            assertEquals(4, keys.size());
            assertTrue(keys.stream().allMatch(Objects::isNull), keys.toString());
        } else if (!ownId) {
            // Tracked under a new random id for each send, so the replay sends every part again.
            assertEquals(4, delivered.size(), delivered.toString());
            assertEquals(4, keys.size());
            keys.forEach(key -> assertNotNull(key));
            assertEquals(4, new LinkedHashSet<>(keys).size(), keys.toString());
            assertNotEquals(keys.get(0), keys.get(2));
        } else {
            // Tracked under the part's own id: the replay skips the delivered parts.
            assertEquals(Set.of("message-1-a:send-line", "message-1-b:send-line"), delivered);
            assertEquals(List.of("message-1-a:send-line", "message-1-b:send-line"), keys);
        }
    }

    @org.junit.jupiter.api.Test
    void aPartSentToADeliveryAuditRouteIsJournaledAsItsOwnRequest() throws Exception {
        var delivered = Collections.synchronizedSet(new LinkedHashSet<String>());
        var rows = Collections.synchronizedList(new ArrayList<MessageAttemptResult>());
        var receiver = new ReceiverComponent();
        try (var context = new DefaultCamelContext()) {
            context.addComponent("receiver", receiver);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(
                    new DefaultStepTracker(new DurableStore(delivered))));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:input").routeId("orders-in")
                            .routePolicy(new MessageStoreRoutePolicy(rows::add, "order-pipeline"))
                            .split(body().tokenize(","))
                            .process(part -> {
                                String line = part.getMessage().getBody(String.class);
                                try (var producer = getContext().createProducerTemplate()) {
                                    // Delivery audit refuses a request that carries its own x-bip-message-id.
                                    var reply = producer.request("direct:send-line", r -> r.getMessage().setBody(line));
                                    assertNull(reply.getException());
                                }
                            })
                            .end();
                    from("direct:send-line").routeId("line-sender")
                            .routePolicy(new MessageStoreAuditRoutePolicy(rows::add, "order-pipeline",
                                    PayloadCapturePolicy.defaultPolicy()))
                            .to("receiver:lines").id("send-line");
                }
            });
            context.start();
            consume(context, 1);
            consume(context, 2);
        }

        var inputRows = rows.stream().filter(row -> row.routeId().equals("orders-in")).toList();
        var requestRows = rows.stream().filter(row -> row.routeId().equals("line-sender")).toList();
        assertEquals(2, inputRows.size());
        assertTrue(inputRows.stream().allMatch(row -> row.steps().isEmpty()), inputRows.toString());
        // Each part is its own non-replayable row with its step, under a new id, so the replay sends it again.
        assertEquals(4, requestRows.size());
        assertTrue(requestRows.stream().allMatch(row -> row.steps().size() == 1 && !row.replayable()), requestRows.toString());
        assertEquals(4, receiver.received().size());
        assertEquals(4, delivered.size(), delivered.toString());
    }

    private static void consume(DefaultCamelContext context, int attempt) {
        try (var producer = context.createProducerTemplate()) {
            var result = producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, INPUT_ID);
                e.getMessage().setHeader(PlatformHeaders.ATTEMPT, attempt);
                e.getMessage().setBody("a,b");
            });
            assertNull(result.getException());
        } catch (java.io.IOException e) {
            throw new java.io.UncheckedIOException(e);
        }
    }

    /** Remembers delivered keys, as the Postgres step store does. */
    private record DurableStore(Set<String> delivered) implements StepOutcomeStore {
        public boolean wasDelivered(String idempotencyKey) {
            return delivered.contains(idempotencyKey);
        }

        public void recordDelivered(StepResult step) {
            delivered.add(step.idempotencyKey());
        }

        public void recordFailed(StepResult step) {
        }
    }
}
