package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageStoreRoutePolicyTest {

    @Test
    void normalizesKafkaByteArrayCorrelationForDownstreamHeaders() {
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        String correlation = "00000000-0000-0000-0000-000000000099";
        byte[] encoded = correlation.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.getMessage().setHeader(KafkaConstants.TOPIC, "synthetic-input");
        exchange.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, encoded);
        policy.onExchangeBegin(route("synthetic-input"), exchange);
        assertEquals(correlation, exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID),
                "Downstream components must see text, not the Kafka byte-array object");
        policy.onExchangeDone(route("synthetic-input"), exchange);
        assertEquals(correlation, sink.result.correlationId());
    }

    @Test
    void keepsOtherCorrelationTypesAndUnownedHeadersUntouched() {
        Object[] values = {"already-text", java.nio.ByteBuffer.wrap(new byte[]{65}), List.of(65), new byte[]{65}};
        for (Object value : values) {
            MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(result -> {}, "adapter-test");
            Exchange exchange = exchange();
            byte[] unowned = {0, 1, 2};
            exchange.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, value);
            exchange.getMessage().setHeader("X-Binary-Context", unowned);
            policy.onExchangeBegin(route("synthetic-input"), exchange);
            org.junit.jupiter.api.Assertions.assertSame(value, exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID));
            org.junit.jupiter.api.Assertions.assertSame(unowned, exchange.getMessage().getHeader("X-Binary-Context"));
        }
    }
    @Test
    void recordsCompletedResultBeforeKafkaCommit() {
        // Given a completed Kafka exchange with collected step results and a manual commit handle.
        RecordingSink sink = new RecordingSink();
        RecordingManualCommit commit = new RecordingManualCommit(sink);
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "order-123");
        exchange.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, "application/json");
        exchange.getMessage().setHeader(PlatformHeaders.SOURCE_SYSTEM, "sap");
        exchange.getMessage().setHeader(PlatformHeaders.TARGET_SYSTEM, "vendor");
        exchange.getMessage().setHeader(PlatformHeaders.ATTEMPT, 2);
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);
        exchange.getMessage().setBody("{\"hello\":\"basis\"}");
        exchange.setProperty(PlatformExchangeProperties.STEPS, List.of(
                StepResult.delivered("notify-vendor", "message-123:notify-vendor", "https://vendor.example.com/callback")));

        // When Camel reports that route processing is done.
        policy.onExchangeBegin(route("order-pipeline"), exchange);
        policy.onExchangeDone(route("order-pipeline"), exchange);

        // Then the operational outcome is recorded before the Kafka offset is committed.
        assertTrue(sink.recordedBeforeCommit);
        assertTrue(commit.committed);
        assertNotNull(sink.result);
        assertEquals(MessageStatus.COMPLETED, sink.result.status());
        assertEquals("message-123", sink.result.messageId());
        assertEquals("order-123", sink.result.correlationId());
        assertEquals("application/json", sink.result.contentType());
        assertEquals("sap", sink.result.sourceSystem());
        assertEquals("vendor", sink.result.targetSystem());
        assertEquals("postgres_inline", sink.result.payloadStorage());
        assertEquals(17L, sink.result.payloadSizeBytes());
        assertEquals("{\"hello\":\"basis\"}", new String(sink.result.payloadBytes()));
        assertNotNull(sink.result.payloadExpiresAt());
        assertTrue(sink.result.replayable());
        assertEquals(null, sink.result.replayDisabledReason());
        assertEquals("adapter-test", sink.result.integration());
        assertEquals("order-pipeline", sink.result.routeId());
        assertEquals(2, sink.result.attempt());
        assertFalse(sink.result.autoRedelivered());
        assertNull(sink.result.autoRedeliveryCount());
        assertNull(sink.result.autoRedeliveryMax());
        assertEquals(1, sink.result.steps().size());
    }

    @Test
    void recordsCamelAutoRedeliveryDiagnosticsWithoutChangingPlatformAttempt() {
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(PlatformHeaders.ATTEMPT, 1);
        exchange.getMessage().setHeader(Exchange.REDELIVERED, true);
        exchange.getMessage().setHeader(Exchange.REDELIVERY_COUNTER, 2);
        exchange.getMessage().setHeader(Exchange.REDELIVERY_MAX_COUNTER, 3);

        policy.onExchangeDone(route("order-pipeline"), exchange);

        assertEquals(1, sink.result.attempt());
        assertTrue(sink.result.autoRedelivered());
        assertEquals(2, sink.result.autoRedeliveryCount());
        assertEquals(3, sink.result.autoRedeliveryMax());
    }

    @Test
    void recordsRedeliveryDiagnosticsFromCapturedPropertiesWhenHeadersWereStripped() {
        // Contract §5: on the DLQ path Camel's error handler removes the
        // redelivery headers before dead-letter delivery; the diagnostics
        // captured into exchange properties during redelivery must win.
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(PlatformHeaders.ATTEMPT, 1);
        exchange.setProperty(PlatformExchangeProperties.DLQ_PARKED, true);
        exchange.setProperty(PlatformExchangeProperties.AUTO_REDELIVERED, true);
        exchange.setProperty(PlatformExchangeProperties.AUTO_REDELIVERY_COUNT, 3);
        exchange.setProperty(PlatformExchangeProperties.AUTO_REDELIVERY_MAX, 3);
        // No Exchange.REDELIVERED / REDELIVERY_COUNTER / REDELIVERY_MAX_COUNTER
        // headers: they were stripped by the error handler.

        policy.onExchangeDone(route("order-pipeline"), exchange);

        assertEquals(MessageStatus.DLQ, sink.result.status());
        assertEquals(1, sink.result.attempt());
        assertTrue(sink.result.autoRedelivered());
        assertEquals(3, sink.result.autoRedeliveryCount());
        assertEquals(3, sink.result.autoRedeliveryMax());
    }

    @Test
    void recordsMetadataOnlyWhenPayloadExceedsConfiguredCap() {
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(
                sink,
                "adapter-test",
                new PayloadCapturePolicy(PayloadMode.POSTGRES_INLINE, 4, java.time.Duration.ofDays(30), true));
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setBody("too large");

        policy.onExchangeBegin(route("order-pipeline"), exchange);
        policy.onExchangeDone(route("order-pipeline"), exchange);

        assertEquals("metadata_only", sink.result.payloadStorage());
        assertEquals(9L, sink.result.payloadSizeBytes());
        assertEquals(null, sink.result.payloadBytes());
        assertFalse(sink.result.replayable());
        assertEquals("payload exceeds max inline size", sink.result.replayDisabledReason());
    }

    @Test
    void commitsKafkaOffsetFromCapturedExchangeProperty() {
        RecordingSink sink = new RecordingSink();
        RecordingManualCommit commit = new RecordingManualCommit(sink);
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);

        policy.onExchangeBegin(route("order-pipeline"), exchange);
        exchange.getMessage().removeHeader(KafkaConstants.MANUAL_COMMIT);

        policy.onExchangeDone(route("order-pipeline"), exchange);

        assertTrue(commit.committed);
    }

    @Test
    void doesNotCommitKafkaOffsetWhenSinkFails() {
        // Given a sink failure, for example Postgres or status-event publishing is unavailable.
        FailingSink sink = new FailingSink();
        RecordingManualCommit commit = new RecordingManualCommit(sink);
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);

        // When the route policy tries to record the message outcome.
        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> policy.onExchangeDone(route("order-pipeline"), exchange));

        // Then the Kafka offset is left uncommitted so the message can be retried.
        assertEquals("Failed to record message outcome; Kafka offset was not committed", thrown.getMessage());
        assertFalse(commit.committed);
    }

    @Test
    void recordsFailedResultWhenExchangeHasException() {
        // Given Camel finished the exchange with an exception.
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.setException(new IllegalArgumentException("bad payload"));

        // An unparked failure is recorded, but remains failed for the consumer.
        assertThrows(IllegalStateException.class,
                () -> policy.onExchangeDone(route("order-pipeline"), exchange));

        // Then the message-store sink receives a FAILED outcome with the error text.
        assertEquals(MessageStatus.FAILED, sink.result.status());
        assertEquals("bad payload", sink.result.error());
    }

    @Test
    void recordsCompletedResultWhenRetryEventuallySucceeds() {
        // Given Camel redelivery succeeded but kept the earlier failure in EXCEPTION_CAUGHT.
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("temporary failure"));

        // When the exchange completes without a terminal exception.
        policy.onExchangeDone(route("order-pipeline"), exchange);

        // Then the stale retry exception does not turn the final outcome into FAILED.
        assertEquals(1, sink.recordCalls());
        assertEquals(MessageStatus.COMPLETED, sink.result.status());
        assertEquals(null, sink.result.error());
    }

    @Test
    void recordsDlqResultWhenExchangeWasParkedInDeadLetterQueue() {
        // Given the shared dead letter channel marked the exchange as parked.
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalArgumentException("poison message"));
        exchange.setProperty(PlatformExchangeProperties.DLQ_PARKED, true);

        // When the policy records the terminal attempt result.
        policy.onExchangeDone(route("order-pipeline"), exchange);

        // Then dashboard/status consumers see the parked state, not just a generic failure.
        assertEquals(MessageStatus.DLQ, sink.result.status());
        assertEquals("poison message", sink.result.error());
    }

    @Test
    void createsMessageIdWhenMissingAtExchangeBegin() {
        // Given a route starts with no platform message id on the exchange.
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();

        // When the policy sees the beginning of message processing.
        policy.onExchangeBegin(route("order-pipeline"), exchange);

        // Then downstream step tracking has a stable message id to build from.
        assertNotNull(exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class));
    }

    private Route route(String routeId) {
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "getRouteId", "getId" -> routeId;
            case "toString" -> "route(" + routeId + ")";
            default -> throw new UnsupportedOperationException("Unexpected Route method in test: " + method.getName());
        };
        return (Route) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Route.class}, handler);
    }

    private Exchange exchange() {
        return new DefaultExchange(new DefaultCamelContext());
    }

    private static class RecordingSink implements MessageStoreSink {
        private MessageAttemptResult result;
        private boolean recordedBeforeCommit;
        private int recordCalls;

        @Override
        public void record(MessageAttemptResult result) {
            recordCalls++;
            this.result = result;
            this.recordedBeforeCommit = true;
        }

        private int recordCalls() {
            return recordCalls;
        }
    }

    private static class FailingSink extends RecordingSink {
        @Override
        public void record(MessageAttemptResult result) {
            throw new RuntimeException("database unavailable");
        }
    }

    private static class RecordingManualCommit implements KafkaManualCommit {
        private final RecordingSink sink;
        private boolean committed;

        private RecordingManualCommit(RecordingSink sink) {
            this.sink = sink;
        }

        @Override
        public void commit() {
            if (!sink.recordedBeforeCommit) {
                throw new AssertionError("Kafka offset was committed before MessageStoreSink.record");
            }
            committed = true;
        }
    }
}
