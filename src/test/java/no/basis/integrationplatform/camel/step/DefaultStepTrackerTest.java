package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultStepTrackerTest {
    private final RecordingStepOutcomeStore stepOutcomeStore = new RecordingStepOutcomeStore();
    private final DefaultStepTracker tracker = new DefaultStepTracker(
            stepOutcomeStore,
            new DefaultStepClassifier("", "", "notify-vendor"));

    @Test
    void injectsIdempotencyKeyFromMessageIdAndInterceptedNodeId() {
        // Given a real external endpoint send with a stable message id and Camel node id.
        Exchange exchange = exchange("https://vendor.example.com/callback", "notify-vendor");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");

        // When the adapter observes the send and the endpoint call succeeds.
        tracker.beforeSend(exchange);
        tracker.recordDelivered(exchange);

        // Then the same stable key is available for the target system and step journal.
        assertEquals("message-123:notify-vendor", exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertEquals("message-123:notify-vendor", exchange.getMessage().getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY));
        assertEquals(false, exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class));
        assertEquals(false, exchange.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));

        List<StepResult> steps = steps(exchange);
        assertEquals(1, steps.size());
        assertEquals("notify-vendor", steps.getFirst().step());
        assertEquals(StepStatus.DELIVERED, steps.getFirst().status());
        assertEquals("message-123:notify-vendor", steps.getFirst().idempotencyKey());
        assertEquals("https://vendor.example.com/callback", steps.getFirst().endpointUri());
        assertEquals(steps.getFirst(), stepOutcomeStore.delivered.getFirst());
    }

    @Test
    void producesSameIdempotencyKeyForSameMessageAndStepAcrossRedelivery() {
        // Given Camel processes the same platform message and external step again after a retry/redelivery.
        Exchange firstAttempt = exchange("https://vendor.example.com/callback", "notify-vendor");
        firstAttempt.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        Exchange secondAttempt = exchange("https://vendor.example.com/callback", "notify-vendor");
        secondAttempt.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");

        // When the adapter prepares both endpoint sends.
        tracker.beforeSend(firstAttempt);
        tracker.beforeSend(secondAttempt);

        // Then downstream systems see the same idempotency key both times.
        assertEquals("message-123:notify-vendor", firstAttempt.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertEquals("message-123:notify-vendor", secondAttempt.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertEquals(firstAttempt.getMessage().getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY),
                secondAttempt.getMessage().getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY));
    }

    @Test
    void producesDifferentIdempotencyKeysForDifferentStepsInSameMessage() {
        // Given one platform message performs two different external side-effect steps.
        Exchange sftpStep = exchange("sftp:vendor/orders", "deliver-file");
        sftpStep.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        Exchange webhookStep = exchange("https://vendor.example.com/callback", "notify-vendor");
        webhookStep.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");

        // When the adapter prepares each endpoint send.
        tracker.beforeSend(sftpStep);
        tracker.beforeSend(webhookStep);

        // Then each step gets its own deterministic key.
        assertEquals("message-123:deliver-file", sftpStep.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertEquals("message-123:notify-vendor", webhookStep.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
    }

    @Test
    void generatesMessageIdWhenMissing() {
        // Given a message that has not yet been wrapped in the platform envelope.
        Exchange exchange = exchange("sftp:vendor/orders", "deliver-file");

        // When the adapter sees the first external send.
        tracker.beforeSend(exchange);
        tracker.recordDelivered(exchange);

        // Then it creates the message id needed for deterministic step keys.
        String messageId = exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
        assertNotNull(messageId);
        assertEquals(messageId + ":deliver-file", exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
    }

    @Test
    void ignoresInternalEndpointSchemes() {
        // Given an internal Camel endpoint that should not become a business step.
        Exchange exchange = exchange("log:debug", "log-message");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");

        // When interception runs for that endpoint.
        tracker.beforeSend(exchange);
        tracker.recordDelivered(exchange);

        // Then the original send may continue, but no platform step is recorded.
        assertEquals(StepRole.INTERNAL.name(),
                exchange.getProperty(PlatformExchangeProperties.CURRENT_STEP_ROLE, String.class));
        assertFalse(exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class));
        assertNull(exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertNull(exchange.getProperty(PlatformExchangeProperties.STEPS));
        assertTrue(stepOutcomeStore.checkedKeys.isEmpty());
    }

    @Test
    void ignoresXsltTransformationEndpoints() {
        // XSLT changes the in-flight payload; it is not an external side effect and must run on every replay.
        for (String endpointUri : List.of("xslt:classpath:mappings/order.xsl", "xslt-saxon:classpath:mappings/order.xsl")) {
            Exchange exchange = exchange(endpointUri, "transform-order");
            exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");

            tracker.beforeSend(exchange);
            tracker.recordDelivered(exchange);

            assertEquals(StepRole.INTERNAL.name(),
                    exchange.getProperty(PlatformExchangeProperties.CURRENT_STEP_ROLE, String.class));
            assertFalse(exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class));
            assertNull(exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
            assertNull(exchange.getProperty(PlatformExchangeProperties.STEPS));
        }

        assertTrue(stepOutcomeStore.checkedKeys.isEmpty());
    }

    @Test
    void marksPreviouslyDeliveredExternalStepForSkip() {
        // Given the durable outcome store already knows this side effect was delivered.
        stepOutcomeStore.alreadyDelivered.add("message-123:notify-vendor");
        Exchange exchange = exchange("https://vendor.example.com/callback", "notify-vendor");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");

        // When the adapter prepares the endpoint send.
        tracker.beforeSend(exchange);

        // Then the intercept strategy has enough state to skip the actual endpoint call.
        assertEquals("message-123:notify-vendor", exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertTrue(exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class));
        assertTrue(exchange.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));

        tracker.recordSkipped(exchange);

        List<StepResult> steps = steps(exchange);
        assertEquals(1, steps.size());
        assertEquals("notify-vendor", steps.getFirst().step());
        assertEquals(StepStatus.SKIPPED, steps.getFirst().status());
        assertEquals("message-123:notify-vendor", steps.getFirst().idempotencyKey());
    }

    @Test
    void redoStepsHeaderForcesReexecutionOfDeliveredStep() {
        // Given the ledger says delivered, but the replay carries an explicit redo request (contract §9, D1).
        stepOutcomeStore.alreadyDelivered.add("message-123:notify-vendor");
        Exchange exchange = exchange("https://vendor.example.com/callback", "notify-vendor");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(PlatformHeaders.REDO_STEPS, "[\"notify-vendor\"]");

        // When the adapter prepares the endpoint send.
        tracker.beforeSend(exchange);

        // Then the step is executed and recorded again instead of being skipped.
        assertFalse(Boolean.TRUE.equals(
                exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class)));
        assertFalse(exchange.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));

        tracker.recordDelivered(exchange);
        assertEquals(1, stepOutcomeStore.delivered.size());
        assertEquals(StepStatus.DELIVERED, steps(exchange).getFirst().status());
    }

    @Test
    void redoStepsHeaderMatchesFullIdempotencyKeyToo() {
        stepOutcomeStore.alreadyDelivered.add("message-123:notify-vendor");
        Exchange exchange = exchange("https://vendor.example.com/callback", "notify-vendor");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(PlatformHeaders.REDO_STEPS, "[\"message-123:notify-vendor\"]");

        tracker.beforeSend(exchange);

        assertFalse(exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class));
    }

    @Test
    void deliveredStepNotListedInRedoStepsIsStillSkipped() {
        stepOutcomeStore.alreadyDelivered.add("message-123:notify-vendor");
        Exchange exchange = exchange("https://vendor.example.com/callback", "notify-vendor");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(PlatformHeaders.REDO_STEPS, "[\"some-other-step\"]");

        tracker.beforeSend(exchange);

        assertTrue(exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class));
        assertTrue(exchange.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));
    }

    @Test
    void redoStepsHeaderAcceptsUtf8Bytes() {
        // Kafka consumers may surface the header as raw bytes.
        stepOutcomeStore.alreadyDelivered.add("message-123:notify-vendor");
        Exchange exchange = exchange("https://vendor.example.com/callback", "notify-vendor");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(PlatformHeaders.REDO_STEPS,
                "[\"notify-vendor\"]".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        tracker.beforeSend(exchange);

        assertFalse(exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class));
    }

    @Test
    void malformedRedoStepsHeaderIsTreatedAsEmptyAndDoesNotFailProcessing() {
        // Given a hand-crafted, non-JSON redo header (contract §3, D1: warn + treat as empty).
        stepOutcomeStore.alreadyDelivered.add("message-123:notify-vendor");
        Exchange exchange = exchange("https://vendor.example.com/callback", "notify-vendor");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(PlatformHeaders.REDO_STEPS, "notify-vendor,other-step");

        // When the adapter prepares the endpoint send, it must not throw.
        tracker.beforeSend(exchange);

        // Then the malformed value behaves exactly like "no redo": delivered step stays skipped.
        assertTrue(exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class));
        assertTrue(exchange.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));
    }

    @Test
    void derivesFallbackStepIdWhenNodeIdIsMissing() {
        // Given an external endpoint without a Camel node id.
        Exchange exchange = exchange("kafka:products", null);
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");

        // When the adapter has to identify the step anyway.
        tracker.beforeSend(exchange);
        tracker.recordDelivered(exchange);

        // Then it derives a stable, non-sensitive fallback instead of exposing full SQL.
        String stepId = exchange.getProperty(PlatformExchangeProperties.CURRENT_STEP_ID, String.class);
        assertNotNull(stepId);
        assertTrue(stepId.startsWith("unknown-route:kafka:"));
        assertFalse(stepId.contains("products"));
        assertEquals("message-123:" + stepId, exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
    }

    @Test
    void recordsFailureAndRethrowsOriginalException() {
        // Given an external send where Camel captured the endpoint failure.
        Exchange exchange = exchange("https://vendor.example.com/callback", "notify-vendor");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        RuntimeException failure = new RuntimeException("vendor unavailable");

        tracker.beforeSend(exchange);
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, failure);

        // When the adapter records the failed step.
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> tracker.recordFailedAndRethrow(exchange));

        // Then Camel still sees the original exception and normal retry/DLQ can handle it.
        assertEquals(failure, thrown);

        List<StepResult> steps = steps(exchange);
        assertEquals(1, steps.size());
        assertEquals("notify-vendor", steps.getFirst().step());
        assertEquals(StepStatus.FAILED, steps.getFirst().status());
        assertEquals("vendor unavailable", steps.getFirst().error());
        assertEquals(steps.getFirst(), stepOutcomeStore.failed.getFirst());
    }

    @Test
    void suppressedExchangeDoesNotReadOrWriteTheStepOutcomeStore() {
        Exchange exchange = exchange("https://vendor.example.com/callback", "notify-vendor");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.setProperty(PlatformExchangeProperties.RECORD_MESSAGE, "false");

        tracker.beforeSend(exchange);
        tracker.recordDelivered(exchange);

        assertFalse(Boolean.TRUE.equals(
                exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class)));
        assertNull(exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertNull(exchange.getProperty(PlatformExchangeProperties.STEPS));
        assertTrue(stepOutcomeStore.checkedKeys.isEmpty());
        assertTrue(stepOutcomeStore.delivered.isEmpty());
    }

    @Test
    void configuredAuthenticationCallExecutesWithoutUsingStepLedger() {
        stepOutcomeStore.alreadyDelivered.add("message-123:fetch-token");
        DefaultStepTracker repeatableTracker = new DefaultStepTracker(
                stepOutcomeStore,
                new DefaultStepClassifier("", "fetch-token", ""));
        Exchange exchange = exchange("https://api.example.test/token", "fetch-token");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(Exchange.HTTP_METHOD, "POST");

        repeatableTracker.beforeSend(exchange);
        repeatableTracker.recordDelivered(exchange);

        assertEquals(StepRole.REPEATABLE.name(),
                exchange.getProperty(PlatformExchangeProperties.CURRENT_STEP_ROLE, String.class));
        assertFalse(exchange.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));
        assertNull(exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertTrue(stepOutcomeStore.checkedKeys.isEmpty());
        assertNull(exchange.getProperty(PlatformExchangeProperties.STEPS));
    }

    @Test
    void safeHttpReadExecutesOnEveryReplayWithoutUsingStepLedger() {
        DefaultStepTracker repeatableTracker = new DefaultStepTracker(
                stepOutcomeStore,
                DefaultStepClassifier.defaults());
        Exchange exchange = exchange("https://api.example.test/orders", "fetch-orders");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(Exchange.HTTP_METHOD, "GET");

        repeatableTracker.beforeSend(exchange);
        repeatableTracker.recordDelivered(exchange);

        assertEquals(StepRole.REPEATABLE.name(),
                exchange.getProperty(PlatformExchangeProperties.CURRENT_STEP_ROLE, String.class));
        assertFalse(exchange.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));
        assertTrue(stepOutcomeStore.checkedKeys.isEmpty());
        assertNull(exchange.getProperty(PlatformExchangeProperties.STEPS));
    }

    @Test
    void ambiguousEndpointExecutesWithoutUnsafeDeduplication() {
        DefaultStepTracker defaultTracker = new DefaultStepTracker(
                stepOutcomeStore,
                DefaultStepClassifier.defaults());
        Exchange exchange = exchange("https://api.example.test/operation", "unknown-operation");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(Exchange.HTTP_METHOD, "POST");

        defaultTracker.beforeSend(exchange);
        defaultTracker.recordDelivered(exchange);

        assertEquals(StepRole.AMBIGUOUS.name(),
                exchange.getProperty(PlatformExchangeProperties.CURRENT_STEP_ROLE, String.class));
        assertFalse(exchange.getProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, Boolean.class));
        assertNull(exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertTrue(stepOutcomeStore.checkedKeys.isEmpty());
        assertNull(exchange.getProperty(PlatformExchangeProperties.STEPS));
    }

    @Test
    void customClassifierCanProtectAComponentUnknownToTheAdapter() {
        StepClassifier customClassifier = context -> StepRole.SIDE_EFFECT;
        DefaultStepTracker customTracker = new DefaultStepTracker(stepOutcomeStore, customClassifier);
        Exchange exchange = exchange("custom-client:write", "write-custom-record");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");

        customTracker.beforeSend(exchange);
        customTracker.recordDelivered(exchange);

        assertEquals("message-123:write-custom-record",
                exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertEquals(1, stepOutcomeStore.checkedKeys.size());
        assertEquals(1, stepOutcomeStore.delivered.size());
    }

    @Test
    void outputProducingStepRejectsNoopStoreBeforeExternalSend() {
        DefaultStepTracker outputTracker = new DefaultStepTracker(
                new NoopStepOutcomeStore(),
                new DefaultStepClassifier("", "", "", "create-order"),
                null);
        Exchange exchange = exchange("https://api.example.test/orders", "create-order");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");

        StepOutputCheckpointException failure = assertThrows(
                StepOutputCheckpointException.class,
                () -> outputTracker.beforeSend(exchange));

        assertTrue(failure.getMessage().contains("step-outcome-store=postgres"));
    }

    @Test
    void repeatableCallDoesNotInheritAdapterIdempotencyHeadersFromPreviousWrite() {
        Exchange exchange = exchange("kafka:orders", "publish-order");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");

        tracker.beforeSend(exchange);
        tracker.recordDelivered(exchange);
        assertEquals("message-123:publish-order",
                exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));

        exchange.setProperty(Exchange.INTERCEPTED_ENDPOINT, "https://api.example.test/orders");
        exchange.setProperty(Exchange.INTERCEPTED_NODE_ID, "fetch-orders");
        exchange.getMessage().setHeader(Exchange.HTTP_METHOD, "GET");
        tracker.beforeSend(exchange);

        assertEquals(StepRole.REPEATABLE.name(),
                exchange.getProperty(PlatformExchangeProperties.CURRENT_STEP_ROLE, String.class));
        assertNull(exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertNull(exchange.getMessage().getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY));
    }

    private Exchange exchange(String endpointUri, String nodeId) {
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.setProperty(Exchange.INTERCEPTED_ENDPOINT, endpointUri);
        if (nodeId != null) {
            exchange.setProperty(Exchange.INTERCEPTED_NODE_ID, nodeId);
        }
        return exchange;
    }

    @SuppressWarnings("unchecked")
    private List<StepResult> steps(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.STEPS, List.class);
    }

    private static class RecordingStepOutcomeStore implements StepOutcomeStore {
        private final Set<String> alreadyDelivered = new HashSet<>();
        private final List<String> checkedKeys = new ArrayList<>();
        private final List<StepResult> delivered = new ArrayList<>();
        private final List<StepResult> failed = new ArrayList<>();

        @Override
        public boolean wasDelivered(String idempotencyKey) {
            checkedKeys.add(idempotencyKey);
            return alreadyDelivered.contains(idempotencyKey);
        }

        @Override
        public void recordDelivered(StepResult step) {
            delivered.add(step);
        }

        @Override
        public void recordFailed(StepResult step) {
            failed.add(step);
        }
    }
}
