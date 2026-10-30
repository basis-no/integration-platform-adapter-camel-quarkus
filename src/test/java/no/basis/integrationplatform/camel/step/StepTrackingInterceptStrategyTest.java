package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import no.basis.integrationplatform.camel.lifecycle.PayloadCapturePolicy;
import no.basis.integrationplatform.camel.lifecycle.PayloadMode;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
import org.apache.camel.AsyncProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.AsyncProcessorConverterHelper;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Optional;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StepTrackingInterceptStrategyTest {
    @Test
    void skipsDeliveredSideEffectBeforeInvokingEndpoint() throws Exception {
        RecordingStore store = new RecordingStore();
        store.alreadyDelivered.add("message-123:publish-order");
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = processor(
                store,
                new DefaultStepClassifier("", "", "publish-order"),
                "https://api.example.test/orders",
                "publish-order",
                endpointCalls,
                exchange -> {
                });
        Exchange exchange = exchange("POST");

        processor.process(exchange);

        assertEquals(0, endpointCalls.get());
        assertEquals(StepStatus.SKIPPED, steps(exchange).getFirst().status());
    }

    @Test
    void repeatableAuthenticationCallAlwaysInvokesEndpoint() throws Exception {
        RecordingStore store = new RecordingStore();
        store.alreadyDelivered.add("message-123:fetch-token");
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = processor(
                store,
                new DefaultStepClassifier("", "fetch-token", ""),
                "https://api.example.test/token",
                "fetch-token",
                endpointCalls,
                exchange -> exchange.getMessage().setBody("new-token"));
        Exchange exchange = exchange("POST");

        processor.process(exchange);

        assertEquals(1, endpointCalls.get());
        assertEquals("new-token", exchange.getMessage().getBody());
        assertTrue(store.checkedKeys.isEmpty());
        assertTrue(store.delivered.isEmpty());
    }

    @Test
    void recordsSuccessfulSideEffectAfterEndpointReturns() throws Exception {
        RecordingStore store = new RecordingStore();
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = processor(
                store,
                new DefaultStepClassifier("", "", "publish-order"),
                "https://api.example.test/orders",
                "publish-order",
                endpointCalls,
                exchange -> {
                });
        Exchange exchange = exchange("POST");

        processor.process(exchange);

        assertEquals(1, endpointCalls.get());
        assertEquals(1, store.delivered.size());
        assertEquals(StepStatus.DELIVERED, steps(exchange).getFirst().status());
    }

    @Test
    void ambiguousEndpointExecutesWithoutLedgerProtection() throws Exception {
        RecordingStore store = new RecordingStore();
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = processor(
                store,
                DefaultStepClassifier.defaults(),
                "https://api.example.test/operation",
                "unclassified-operation",
                endpointCalls,
                exchange -> {
                });
        Exchange exchange = exchange("POST");

        processor.process(exchange);

        assertEquals(1, endpointCalls.get());
        assertTrue(store.checkedKeys.isEmpty());
        assertTrue(store.delivered.isEmpty());
    }

    @Test
    void recordsSideEffectFailureAndRethrowsIt() {
        RecordingStore store = new RecordingStore();
        AtomicInteger endpointCalls = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("write failed");
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = processor(
                store,
                new DefaultStepClassifier("", "", "publish-order"),
                "https://api.example.test/orders",
                "publish-order",
                endpointCalls,
                exchange -> {
                    throw failure;
                });
        Exchange exchange = exchange("POST");

        IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> processor.process(exchange));

        assertEquals(failure, thrown);
        assertEquals(1, endpointCalls.get());
        assertEquals(1, store.failed.size());
        assertEquals(StepStatus.FAILED, steps(exchange).getFirst().status());
    }

    @Test
    void callbackSkipsDeliveredSideEffectSynchronously() {
        RecordingStore store = new RecordingStore();
        store.alreadyDelivered.add("message-123:publish-order");
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = processor(
                store,
                new DefaultStepClassifier("", "", "publish-order"),
                "https://api.example.test/orders",
                "publish-order",
                endpointCalls,
                exchange -> {
                });
        Exchange exchange = exchange("POST");
        AtomicBoolean callbackDoneSync = new AtomicBoolean();

        boolean doneSync = processor.process(exchange, callbackDoneSync::set);

        assertTrue(doneSync);
        assertTrue(callbackDoneSync.get());
        assertEquals(0, endpointCalls.get());
        assertEquals(StepStatus.SKIPPED, steps(exchange).getFirst().status());
    }

    @Test
    void callbackRecordsSuccessfulSideEffect() {
        RecordingStore store = new RecordingStore();
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = processor(
                store,
                new DefaultStepClassifier("", "", "publish-order"),
                "https://api.example.test/orders",
                "publish-order",
                endpointCalls,
                exchange -> {
                });
        Exchange exchange = exchange("POST");
        AtomicBoolean callbackDoneSync = new AtomicBoolean();

        boolean doneSync = processor.process(exchange, callbackDoneSync::set);

        assertTrue(doneSync);
        assertTrue(callbackDoneSync.get());
        assertEquals(1, endpointCalls.get());
        assertEquals(1, store.delivered.size());
        assertEquals(StepStatus.DELIVERED, steps(exchange).getFirst().status());
    }

    @Test
    void callbackSurfacesMissingOutputCheckpointOnExchangeWithoutCallingEndpoint() {
        RecordingStore store = new RecordingStore();
        store.alreadyDelivered.add("message-123:create-order");
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = outputProcessor(
                store,
                checkpointService(),
                endpointCalls,
                exchange -> exchange.getMessage().setBody("must-not-run"));
        Exchange exchange = exchange("POST");
        AtomicBoolean callbackDoneSync = new AtomicBoolean();

        boolean doneSync = processor.process(exchange, callbackDoneSync::set);

        assertTrue(doneSync);
        assertTrue(callbackDoneSync.get());
        assertEquals(0, endpointCalls.get());
        assertTrue(exchange.getException() instanceof StepOutputCheckpointException);
    }

    @Test
    void futureSkipsDeliveredSideEffectWithoutInvokingEndpoint() {
        RecordingStore store = new RecordingStore();
        store.alreadyDelivered.add("message-123:publish-order");
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = processor(
                store,
                new DefaultStepClassifier("", "", "publish-order"),
                "https://api.example.test/orders",
                "publish-order",
                endpointCalls,
                exchange -> {
                });
        Exchange exchange = exchange("POST");

        Exchange result = processor.processAsync(exchange).join();

        assertEquals(exchange, result);
        assertEquals(0, endpointCalls.get());
        assertEquals(StepStatus.SKIPPED, steps(exchange).getFirst().status());
    }

    @Test
    void futureRecordsSuccessfulSideEffect() {
        RecordingStore store = new RecordingStore();
        Exchange exchange = exchange("POST");
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = processor(
                store,
                new DefaultStepClassifier("", "", "publish-order"),
                "https://api.example.test/orders",
                "publish-order",
                successfulFutureDelegate());

        Exchange result = processor.processAsync(exchange).join();

        assertEquals(exchange, result);
        assertEquals(1, store.delivered.size());
        assertEquals(StepStatus.DELIVERED, steps(exchange).getFirst().status());
    }

    @Test
    void futureFailsWithoutCallingEndpointWhenOutputCheckpointIsMissing() {
        RecordingStore store = new RecordingStore();
        store.alreadyDelivered.add("message-123:create-order");
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = outputProcessor(
                store,
                checkpointService(),
                endpointCalls,
                exchange -> exchange.getMessage().setBody("must-not-run"));
        Exchange exchange = exchange("POST");

        CompletionException failure = assertThrows(
                CompletionException.class,
                () -> processor.processAsync(exchange).join());

        assertTrue(failure.getCause() instanceof StepOutputCheckpointException);
        assertEquals(0, endpointCalls.get());
        assertTrue(exchange.getException() instanceof StepOutputCheckpointException);
    }

    @Test
    void futureRecordsOriginalSideEffectFailure() {
        RecordingStore store = new RecordingStore();
        IllegalStateException failure = new IllegalStateException("async write failed");
        Exchange exchange = exchange("POST");
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = processor(
                store,
                new DefaultStepClassifier("", "", "publish-order"),
                "https://api.example.test/orders",
                "publish-order",
                failedFutureDelegate(failure));

        CompletionException thrown = assertThrows(
                CompletionException.class,
                () -> processor.processAsync(exchange).join());

        assertEquals(failure, thrown.getCause());
        assertEquals(failure, exchange.getProperty(Exchange.EXCEPTION_CAUGHT));
        assertEquals(1, store.failed.size());
        assertEquals(StepStatus.FAILED, steps(exchange).getFirst().status());
    }

    @Test
    void replayRestoresOutputWithoutInvokingDeliveredEndpointAgain() throws Exception {
        RecordingStore store = new RecordingStore();
        StepOutputCheckpointService checkpointService = checkpointService();
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = outputProcessor(
                store,
                checkpointService,
                endpointCalls,
                exchange -> {
                    int call = endpointCalls.get();
                    exchange.getMessage().setBody("partner-order-" + call);
                    exchange.getMessage().setHeader("Partner-Request-Id", "request-" + call);
                });

        Exchange firstAttempt = exchange("POST");
        processor.process(firstAttempt);
        Exchange replay = exchange("POST");
        replay.getMessage().setBody("original replay input");
        processor.process(replay);

        assertEquals(1, endpointCalls.get());
        assertEquals("partner-order-1", replay.getMessage().getBody());
        assertEquals("request-1", replay.getMessage().getHeader("Partner-Request-Id"));
        assertEquals(StepStatus.SKIPPED, steps(replay).getFirst().status());
    }

    @Test
    void replayStopsBeforeEndpointWhenDeliveredOutputHasNoCheckpoint() {
        RecordingStore store = new RecordingStore();
        store.alreadyDelivered.add("message-123:create-order");
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = outputProcessor(
                store,
                checkpointService(),
                endpointCalls,
                exchange -> exchange.getMessage().setBody("must-not-run"));

        StepOutputCheckpointException failure = assertThrows(
                StepOutputCheckpointException.class,
                () -> processor.process(exchange("POST")));

        assertEquals(0, endpointCalls.get());
        assertTrue(failure.getMessage().contains("no output checkpoint"));
        assertTrue(failure.getMessage().contains("explicit redo"));
    }

    @Test
    void explicitRedoReexecutesOutputStepWithSameStableIdentity() throws Exception {
        RecordingStore store = new RecordingStore();
        StepOutputCheckpointService checkpointService = checkpointService();
        AtomicInteger endpointCalls = new AtomicInteger();
        StepTrackingInterceptStrategy.StepTrackingProcessor processor = outputProcessor(
                store,
                checkpointService,
                endpointCalls,
                exchange -> exchange.getMessage().setBody("partner-order-" + endpointCalls.get()));

        processor.process(exchange("POST"));
        Exchange replay = exchange("POST");
        replay.getMessage().setHeader(PlatformHeaders.REDO_STEPS, "[\"create-order\"]");
        processor.process(replay);

        assertEquals(2, endpointCalls.get());
        assertEquals("partner-order-2", replay.getMessage().getBody());
        assertEquals(2, store.delivered.size());
        assertEquals("message-123:create-order", store.delivered.get(0).idempotencyKey());
        assertEquals("message-123:create-order", store.delivered.get(1).idempotencyKey());
    }

    private StepTrackingInterceptStrategy.StepTrackingProcessor outputProcessor(
            RecordingStore store,
            StepOutputCheckpointService checkpointService,
            AtomicInteger endpointCalls,
            org.apache.camel.Processor endpoint) {
        AsyncProcessor delegate = AsyncProcessorConverterHelper.convert(exchange -> {
            endpointCalls.incrementAndGet();
            endpoint.process(exchange);
        });
        return new StepTrackingInterceptStrategy.StepTrackingProcessor(
                delegate,
                new StepTrackingInterceptStrategy.EndpointSend(
                        "https://api.example.test/orders", "create-order"),
                new DefaultStepTracker(
                        store,
                        new DefaultStepClassifier("", "", "", "create-order"),
                        checkpointService));
    }

    private StepOutputCheckpointService checkpointService() {
        return new StepOutputCheckpointService(
                PayloadObjectStore.disabled(),
                new PayloadCapturePolicy(
                        PayloadMode.POSTGRES_INLINE,
                        1024,
                        10_000,
                        Duration.ofDays(30),
                        true),
                "checkpoint-test",
                "Partner-Request-Id",
                "");
    }

    private StepTrackingInterceptStrategy.StepTrackingProcessor processor(
            RecordingStore store,
            StepClassifier classifier,
            String endpointUri,
            String nodeId,
            AtomicInteger endpointCalls,
            org.apache.camel.Processor endpoint) {
        AsyncProcessor delegate = AsyncProcessorConverterHelper.convert(exchange -> {
            endpointCalls.incrementAndGet();
            endpoint.process(exchange);
        });
        return new StepTrackingInterceptStrategy.StepTrackingProcessor(
                delegate,
                new StepTrackingInterceptStrategy.EndpointSend(endpointUri, nodeId),
                new DefaultStepTracker(store, classifier));
    }

    private StepTrackingInterceptStrategy.StepTrackingProcessor processor(
            RecordingStore store,
            StepClassifier classifier,
            String endpointUri,
            String nodeId,
            AsyncProcessor delegate) {
        return new StepTrackingInterceptStrategy.StepTrackingProcessor(
                delegate,
                new StepTrackingInterceptStrategy.EndpointSend(endpointUri, nodeId),
                new DefaultStepTracker(store, classifier));
    }

    private AsyncProcessor successfulFutureDelegate() {
        return new TestAsyncProcessor(CompletableFuture::completedFuture);
    }

    private AsyncProcessor failedFutureDelegate(RuntimeException failure) {
        return new TestAsyncProcessor(exchange -> CompletableFuture.failedFuture(failure));
    }

    private Exchange exchange(String httpMethod) {
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(Exchange.HTTP_METHOD, httpMethod);
        return exchange;
    }

    @SuppressWarnings("unchecked")
    private List<StepResult> steps(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.STEPS, List.class);
    }

    private static class RecordingStore implements StepOutcomeStore {
        private final Set<String> alreadyDelivered = new HashSet<>();
        private final List<String> checkedKeys = new ArrayList<>();
        private final List<StepResult> delivered = new ArrayList<>();
        private final List<StepResult> failed = new ArrayList<>();
        private final Map<String, StepOutputCheckpoint> checkpoints = new HashMap<>();

        @Override
        public boolean supportsOutputCheckpoints() {
            return true;
        }

        @Override
        public boolean wasDelivered(String idempotencyKey) {
            checkedKeys.add(idempotencyKey);
            return alreadyDelivered.contains(idempotencyKey);
        }

        @Override
        public void recordDelivered(StepResult step) {
            delivered.add(step);
            alreadyDelivered.add(step.idempotencyKey());
        }

        @Override
        public void recordDelivered(StepResult step, StepOutputCheckpoint checkpoint) {
            recordDelivered(step);
            checkpoints.put(step.idempotencyKey(), checkpoint);
        }

        @Override
        public Optional<StepOutputCheckpoint> readCheckpoint(String idempotencyKey) {
            return Optional.ofNullable(checkpoints.get(idempotencyKey));
        }

        @Override
        public void recordFailed(StepResult step) {
            failed.add(step);
        }
    }

    private static class TestAsyncProcessor implements AsyncProcessor {
        private final java.util.function.Function<Exchange, CompletableFuture<Exchange>> future;

        private TestAsyncProcessor(java.util.function.Function<Exchange, CompletableFuture<Exchange>> future) {
            this.future = future;
        }

        @Override
        public void process(Exchange exchange) {
            processAsync(exchange).join();
        }

        @Override
        public boolean process(Exchange exchange, org.apache.camel.AsyncCallback callback) {
            processAsync(exchange).whenComplete((result, failure) -> callback.done(false));
            return false;
        }

        @Override
        public CompletableFuture<Exchange> processAsync(Exchange exchange) {
            return future.apply(exchange);
        }
    }

    @org.junit.jupiter.api.Test
    void sendWithoutAnExplicitIdFailsRouteCreationByDefault() throws Exception {
        try (var context = enforcing(false)) {
            context.addRoutes(new org.apache.camel.builder.RouteBuilder() {
                public void configure() {
                    from("direct:in").routeId("deliveries").to("receiver:generated?password=s3cr3t");
                }
            });
            Exception failure = org.junit.jupiter.api.Assertions.assertThrows(Exception.class, context::start);
            Throwable cause = failure;
            while (cause.getCause() != null && !(cause instanceof IllegalStateException)) cause = cause.getCause();
            org.junit.jupiter.api.Assertions.assertTrue(cause.getMessage().startsWith(
                    "Endpoint send to receiver://generated?password=xxxxxx in route deliveries has no explicit id"), cause.getMessage());
            org.junit.jupiter.api.Assertions.assertTrue(cause.getMessage().contains(
                    StepTrackingInterceptStrategy.EMERGENCY_ALLOW_GENERATED_STEP_IDS + "=true"));
        }
    }

    @org.junit.jupiter.api.Test
    void explicitIdsInternalEndpointsAndWireTapsAreNotChecked() throws Exception {
        try (var context = enforcing(false)) {
            context.addRoutes(new org.apache.camel.builder.RouteBuilder() {
                public void configure() {
                    from("direct:in").routeId("deliveries")
                            .to("receiver:named").id("named-send")
                            .toD("receiver:${header.target}").id("dynamic-send")
                            .to("direct:internal")
                            .wireTap("receiver:audit-copy");
                    from("direct:internal").setBody(constant("internal"));
                }
            });
            context.start();
            org.junit.jupiter.api.Assertions.assertTrue(context.isStarted());
        }
    }

    @org.junit.jupiter.api.Test
    void emergencyFlagStartsWithAWarning() throws Exception {
        try (var logs = no.basis.integrationplatform.camel.LogCapture.of(StepTrackingInterceptStrategy.class);
             var context = enforcing(true)) {
            context.addRoutes(new org.apache.camel.builder.RouteBuilder() {
                public void configure() {
                    from("direct:in").routeId("deliveries").to("receiver:generated");
                }
            });
            context.start();
            var warnings = logs.at(java.util.logging.Level.WARNING);
            org.junit.jupiter.api.Assertions.assertEquals(1, warnings.size());
            org.junit.jupiter.api.Assertions.assertTrue(no.basis.integrationplatform.camel.LogCapture.text(warnings.getFirst())
                    .contains(StepTrackingInterceptStrategy.EMERGENCY_ALLOW_GENERATED_STEP_IDS + "=true is set"));
        }
    }

    private org.apache.camel.impl.DefaultCamelContext enforcing(boolean allowGenerated) {
        var context = new org.apache.camel.impl.DefaultCamelContext();
        context.addComponent("receiver", new no.basis.integrationplatform.camel.ReceiverComponent());
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(
                new DefaultStepTracker(new NoopStepOutcomeStore()), allowGenerated));
        return context;
    }
}
