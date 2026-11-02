package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.lifecycle.PayloadCapturePolicy;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepOutcomeStoreException;
import no.basis.integrationplatform.camel.step.StepTracker;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultAsyncProducer;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.DefaultEndpoint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

/** No receiver/Kafka access: exercises the real Camel error handler and route-policy callbacks. */
class FailureOwnershipTest {
    @Test
    void adapterOwnedFailureParksOriginalInputThenCommits() throws Exception {
        var outcome = run(false, false, false);
        assertEquals(1, outcome.parked.size());
        assertEquals("<Input/>", outcome.parked.get(0));
        assertEquals(MessageStatus.DLQ, outcome.rows.get(0).status());
        assertEquals(1, outcome.commits.get());
    }

    @Test
    void exhaustedFailureIsLoggedOnceWithItsStackTraceBeforeParking() throws Exception {
        try (var logs = LogCapture.of(PlatformRouteConfiguration.class)) {
            var outcome = run(false, false, false);

            var errors = logs.at(java.util.logging.Level.SEVERE);
            assertEquals(1, errors.size());
            assertInstanceOf(IllegalArgumentException.class, errors.getFirst().getThrown());
            assertEquals("synthetic receiver rejection", errors.getFirst().getThrown().getMessage());
            assertEquals(MessageStatus.DLQ, outcome.rows.getFirst().status());
        }
    }

    @Test
    void applicationCatchAllBypassesPlatformParking() throws Exception {
        var outcome = run(true, false, false);
        assertEquals(6, outcome.calls.get(), "One initial call and the application's five redeliveries");
        assertTrue(outcome.parked.isEmpty(), "Application onException bypasses the adapter dead-letter handler");
        assertEquals(MessageStatus.FAILED, outcome.rows.get(0).status());
        assertEquals(0, outcome.commits.get());
    }

    @Test
    void journalFailureDoesNotCommit() throws Exception {
        assertEquals(0, run(false, false, true).commits.get());
    }

    @Test
    void dlqPublicationFailureMustNotCommitOrClaimParked() throws Exception {
        var outcome = run(false, true, false);
        assertEquals(0, outcome.commits.get());
        assertTrue(outcome.rows.stream().noneMatch(r -> r.status() == MessageStatus.DLQ));
    }

    @Test
    void freshInternalRequestFailurePropagatesToInputAndParksOnlyOriginal() throws Exception {
        Result result = new Result();
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new StepTracker() {
                public void beforeSend(Exchange e) { fail("Infrastructure parking must not be a business step"); }
                public void recordDelivered(Exchange e) {}
                public void recordSkipped(Exchange e) {}
                public void recordFailed(Exchange e) {}
                public void recordFailedAndRethrow(Exchange e) {}
            }));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .routePolicy(new MessageStoreRoutePolicy(result.rows::add, "order-pipeline"))
                        .process(e -> {
                            try (var producer = context.createProducerTemplate()) {
                                var reply = producer.request("direct:receiver", child ->
                                        child.getMessage().setBody("<Transformed/>"));
                                assertNotNull(reply.getException(), "Internal error must reach caller");
                                assertInstanceOf(IllegalArgumentException.class, reply.getException());
                                assertEquals("synthetic receiver rejection", reply.getException().getMessage());
                                e.setException(reply.getException());
                            }
                        });
                    from("direct:receiver").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .process(e -> { throw new IllegalArgumentException("synthetic receiver rejection"); });
                    from("direct:parking").errorHandler(noErrorHandler())
                        .process(e -> result.parked.add(e.getMessage().getBody(String.class)));
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.request("direct:input", e -> {
                    e.getMessage().setBody("<Input/>");
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT,
                            (KafkaManualCommit) result.commits::incrementAndGet);
                });
            }
        }
        assertEquals(List.of("<Input/>"), result.parked);
        assertEquals(1, result.rows.size());
        assertEquals(MessageStatus.DLQ, result.rows.getFirst().status());
        assertEquals("synthetic receiver rejection", result.rows.getFirst().error());
        assertEquals(1, result.commits.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void sendCompletesFailedWithoutCommitWhenTheMessageStoreCannotRecordItsOutcome(boolean asyncReceiver) throws Exception {
        // A Message Store outage that starts while the receiver handles the send: the check
        // before the send succeeded, then the step outcome and the journal cannot be written.
        var result = deliver(asyncReceiver, "send", true, false, null);

        assertTrue(result.exchange.isFailed(), "The outcome was not recorded, so the exchange must fail");
        assertEquals(0, result.commits.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void stepOutcomeWriteFailureFailsTheSendWithoutCommit(boolean asyncReceiver) throws Exception {
        // The journal works and the route has no adapter parking: the failed step write alone
        // must fail the exchange, so the journal records FAILED and the offset stays.
        var result = deliver(asyncReceiver, "send", false, true, null);

        assertTrue(result.exchange.isFailed(), "The step outcome was not recorded, so the exchange must fail");
        assertEquals(1, result.rows.size());
        assertEquals(MessageStatus.FAILED, result.rows.getFirst().status());
        assertEquals("synthetic step outcome outage", result.rows.getFirst().error());
        assertEquals(0, result.commits.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void stepOutcomeWriteFailureKeepsTheReceiverFailure(boolean asyncReceiver) throws Exception {
        var rejection = new IllegalArgumentException("synthetic receiver rejection");
        var result = deliver(asyncReceiver, "send", false, true, rejection);

        assertTrue(result.exchange.isFailed());
        assertEquals(1, rejection.getSuppressed().length);
        assertInstanceOf(StepOutcomeStoreException.class, rejection.getSuppressed()[0]);
        assertEquals(MessageStatus.FAILED, result.rows.getFirst().status());
        assertEquals("synthetic receiver rejection", result.rows.getFirst().error());
        assertEquals(0, result.commits.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void stepOutcomeWriteFailureIsNeitherParkedNorCommittedWhileTheJournalWorks(boolean asyncReceiver) throws Exception {
        // Only the step outcome store fails: the receiver got the input, but the step is not
        // known as delivered. Parking would commit; the input must be consumed again instead.
        var result = deliver(asyncReceiver, "send", true, true, null);

        assertTrue(result.exchange.isFailed());
        assertEquals(List.of("<Input/>"), result.received);
        assertTrue(result.parked.isEmpty(), "A parked copy would repeat the input that is consumed again");
        assertEquals(MessageStatus.FAILED, result.rows.getFirst().status());
        assertEquals(0, result.commits.get());
    }

    @ParameterizedTest
    @CsvSource({"false, split", "true, split", "false, split-handled", "true, split-handled"})
    void stepOutcomeWriteFailureInASplitPartKeepsTheInputFromCommitting(boolean asyncReceiver, String shape) throws Exception {
        // A part runs on a copy of the input exchange; the failure must still reach the input,
        // also when the part catches it and the input otherwise completes.
        var result = deliver(asyncReceiver, shape, true, true, null);

        assertTrue(result.exchange.isFailed());
        assertEquals(List.of("a", "b"), result.received);
        assertTrue(result.parked.isEmpty());
        assertEquals(MessageStatus.FAILED, result.rows.getLast().status());
        assertEquals(0, result.commits.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void stepOutcomeWriteFailureIsNotCommittedWhenTheRouteHandlesIt(boolean asyncReceiver) throws Exception {
        var result = deliver(asyncReceiver, "handled", true, true, null);

        assertTrue(result.exchange.isFailed());
        assertTrue(result.parked.isEmpty());
        assertEquals(MessageStatus.FAILED, result.rows.getFirst().status());
        assertEquals("synthetic step outcome outage", result.rows.getFirst().error());
        assertEquals(0, result.commits.get());
    }

    @Test
    void parkingAndReplayStorageKeepTheInputBytesWhenTheRouteChangesThemInPlace() throws Exception {
        var outcome = park("ORIGINAL".getBytes(StandardCharsets.UTF_8), PayloadCapturePolicy.defaultPolicy(), e -> {
            e.getMessage().getBody(byte[].class)[0] = 'X';
            throw new IllegalArgumentException("synthetic receiver rejection");
        });

        assertEquals(List.of("ORIGINAL"), outcome.parked);
        assertEquals(MessageStatus.DLQ, outcome.rows.getFirst().status());
        assertEquals("ORIGINAL", new String(outcome.rows.getFirst().payloadBytes(), StandardCharsets.UTF_8));
        assertEquals(1, outcome.commits.get());
    }

    @Test
    void replayStorageKeepsTheInputBytesWhenASuccessfulRouteChangesThemInPlace() throws Exception {
        var outcome = park("ORIGINAL".getBytes(StandardCharsets.UTF_8), PayloadCapturePolicy.defaultPolicy(),
                e -> e.getMessage().getBody(byte[].class)[0] = 'X');

        assertTrue(outcome.parked.isEmpty());
        assertEquals(MessageStatus.COMPLETED, outcome.rows.getFirst().status());
        assertEquals("ORIGINAL", new String(outcome.rows.getFirst().payloadBytes(), StandardCharsets.UTF_8));
        assertEquals(1, outcome.commits.get());
    }

    @ParameterizedTest
    @CsvSource({
            "METADATA_ONLY, 1000, true, bytes", "METADATA_ONLY, 1000, true, text",
            "POSTGRES_INLINE, 1000, false, bytes", "POSTGRES_INLINE, 1000, false, text",
            "POSTGRES_INLINE, 4, true, bytes", "POSTGRES_INLINE, 4, true, text"})
    void parkingSendsTheInputWhenReplayStorageDoesNotKeepIt(
            no.basis.integrationplatform.camel.lifecycle.PayloadMode mode, long maxBytes, boolean replayEnabled,
            String input) throws Exception {
        var policy = new PayloadCapturePolicy(mode, maxBytes, Duration.ofDays(30), replayEnabled);
        Object body = input.equals("bytes") ? "ORIGINAL".getBytes(StandardCharsets.UTF_8) : "ORIGINAL";
        var outcome = park(body, policy, e -> {
            if (e.getMessage().getBody() instanceof byte[] bytes) {
                bytes[0] = 'X';
            }
            e.getMessage().setBody("<Transformed/>");
            throw new IllegalArgumentException("synthetic receiver rejection");
        });

        assertEquals(List.of("ORIGINAL"), outcome.parked);
        assertEquals(MessageStatus.DLQ, outcome.rows.getFirst().status());
        assertNull(outcome.rows.getFirst().payloadBytes(), "Replay storage does not keep this payload");
        assertFalse(outcome.rows.getFirst().replayable());
        assertEquals(1, outcome.commits.get());
    }

    @Test
    void noKafkaApplicationCanStartWithoutBrokerConfiguration() throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.start();
            assertTrue(context.isStarted());
        }
    }

    /**
     * Sends a managed Kafka input to a tracked receiver while the step outcome store fails its
     * writes. The {@code send} and {@code handled} shapes send {@code <Input/>}, the second in a
     * {@code doTry} that catches every failure; {@code split} sends the parts of {@code a,b} with
     * their own ids, and only the second part's write fails, and {@code split-handled} catches
     * each part's failure in the part. Without {@code adapterParking} the route has no error handler.
     */
    private Delivery deliver(boolean asyncReceiver, String shape, boolean adapterParking, boolean journalWorks,
            RuntimeException rejection) throws Exception {
        var result = new Delivery();
        ExecutorService receiverThread = Executors.newSingleThreadExecutor();
        try (var context = new DefaultCamelContext()) {
            context.getShutdownStrategy().setTimeout(1);
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            Processor receiver = e -> {
                result.received.add(e.getMessage().getBody(String.class));
                if (rejection != null) throw rejection;
            };
            context.addComponent("receiver", asyncReceiver ? new AsyncReceiver(receiverThread, receiver)
                    : new ReceiverComponent(receiver));
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(
                    new DefaultStepTracker(new StepOutcomeStore() {
                        public boolean wasDelivered(String idempotencyKey) { return false; }
                        public void recordDelivered(StepResult step) { write(step); }
                        public void recordFailed(StepResult step) { write(step); }

                        private void write(StepResult step) {
                            if (!shape.startsWith("split") || step.idempotencyKey().startsWith("message-1-1:")) throw outage();
                        }
                    })));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    var input = from("direct:input").routeId("input");
                    if (adapterParking) {
                        input.routeConfigurationId(PlatformRouteConfiguration.NO_RETRY);
                    } else {
                        input.errorHandler(noErrorHandler());
                    }
                    input.routePolicy(new MessageStoreRoutePolicy(row -> {
                        if (!journalWorks) throw new IllegalStateException("synthetic journal outage");
                        result.rows.add(row);
                    }, "order-pipeline"));
                    switch (shape) {
                        case "send" -> input.to("receiver:orders").id("deliver-order");
                        case "handled" -> input.doTry().to("receiver:orders").id("deliver-order")
                                .doCatch(Exception.class).log("handled by the route").end();
                        case "split" -> input.split(body().tokenize(","))
                                .setHeader(PlatformHeaders.MESSAGE_ID, simple("message-1-${exchangeProperty.CamelSplitIndex}"))
                                .to("receiver:orders").id("deliver-order").end();
                        case "split-handled" -> input.split(body().tokenize(","))
                                .setHeader(PlatformHeaders.MESSAGE_ID, simple("message-1-${exchangeProperty.CamelSplitIndex}"))
                                .doTry().to("receiver:orders").id("deliver-order")
                                .doCatch(Exception.class).log("part handled by the route").end()
                                .end();
                        default -> throw new IllegalArgumentException(shape);
                    }
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                            .process(e -> result.parked.add(e.getMessage().getBody(String.class)));
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                result.exchange = producer.asyncSend("direct:input", e -> {
                    e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                    e.getMessage().setBody(shape.startsWith("split") ? "a,b" : "<Input/>");
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT,
                            (KafkaManualCommit) result.commits::incrementAndGet);
                }).get(10, TimeUnit.SECONDS);
            }
        } finally {
            receiverThread.shutdownNow();
        }
        return result;
    }

    private static StepOutcomeStoreException outage() {
        return new StepOutcomeStoreException("synthetic step outcome outage", null);
    }

    /** Runs {@code input} from a managed Kafka input through {@code route} with the adapter's parking. */
    private Result park(Object input, PayloadCapturePolicy policy, Processor route) throws Exception {
        Result result = new Result();
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .routePolicy(new MessageStoreRoutePolicy(result.rows::add, "order-pipeline", policy))
                        .process(route);
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                        .process(e -> result.parked.add(e.getMessage().getBody(String.class)));
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.request("direct:input", e -> {
                    e.getMessage().setBody(input);
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT,
                            (KafkaManualCommit) result.commits::incrementAndGet);
                });
            }
        }
        return result;
    }

    private Result run(boolean appHandler, boolean dlqFailure, boolean journalFailure) throws Exception {
        Result result = new Result();
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new StepTracker() {
                public void beforeSend(Exchange e) { fail("Infrastructure parking must not be a business step"); }
                public void recordDelivered(Exchange e) {}
                public void recordSkipped(Exchange e) {}
                public void recordFailed(Exchange e) {}
                public void recordFailedAndRethrow(Exchange e) {}
            }));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    if (appHandler) {
                        onException(Exception.class).maximumRedeliveries(5).redeliveryDelay(0)
                            .handled(false).process(e -> {});
                    }
                    // Explicit policy simulates the Kafka lifecycle while using a local source.
                    from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .routePolicy(new MessageStoreRoutePolicy(row -> {
                            if (journalFailure) throw new IllegalStateException("synthetic journal outage");
                            result.rows.add(row);
                        }, "order-pipeline"))
                        .process(e -> {
                            result.calls.incrementAndGet();
                            e.getMessage().setBody("<Transformed/>");
                            throw new IllegalArgumentException("synthetic receiver rejection");
                        });
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                        .process(e -> {
                            if (dlqFailure) throw new IllegalStateException("synthetic DLQ outage");
                            result.parked.add(e.getMessage().getBody(String.class));
                        });
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.request("direct:input", e -> {
                    e.getMessage().setBody("<Input/>");
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT,
                            (KafkaManualCommit) result.commits::incrementAndGet);
                });
            }
        }
        return result;
    }

    private static class Result {
        final List<MessageAttemptResult> rows = new ArrayList<>();
        final List<String> parked = new ArrayList<>();
        final AtomicInteger commits = new AtomicInteger();
        final AtomicInteger calls = new AtomicInteger();
    }

    private static final class Delivery extends Result {
        final List<String> received = new java.util.concurrent.CopyOnWriteArrayList<>();
        Exchange exchange;
    }

    /** A receiver that completes each send on its own thread, as Kafka's producer does. */
    private static final class AsyncReceiver extends DefaultComponent {
        private final ExecutorService thread;
        private final Processor behaviour;

        AsyncReceiver(ExecutorService thread, Processor behaviour) {
            this.thread = thread;
            this.behaviour = behaviour;
        }

        @Override
        protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
            return new AsyncReceiverEndpoint(uri, this, thread, behaviour);
        }
    }

    private static final class AsyncReceiverEndpoint extends DefaultEndpoint {
        private final ExecutorService thread;
        private final Processor behaviour;

        AsyncReceiverEndpoint(String uri, Component component, ExecutorService thread, Processor behaviour) {
            super(uri, component);
            this.thread = thread;
            this.behaviour = behaviour;
        }

        @Override
        public Producer createProducer() {
            return new DefaultAsyncProducer(this) {
                @Override
                public boolean process(Exchange exchange, AsyncCallback callback) {
                    thread.execute(() -> {
                        try {
                            behaviour.process(exchange);
                        } catch (Exception failure) {
                            exchange.setException(failure);
                        }
                        callback.done(false);
                    });
                    return false;
                }
            };
        }

        @Override
        public Consumer createConsumer(Processor processor) {
            throw new UnsupportedOperationException("The receiver only accepts sends");
        }
    }
}
