package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
import no.basis.integrationplatform.camel.payload.PayloadReference;
import no.basis.integrationplatform.camel.payload.PayloadStorageException;
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
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageStoreRoutePolicyTest {
    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings = {"kafka.TOPIC", "CamelKafkaTopic"})
    void decodesKafkaCorrelationBytesAsUtf8UnderEitherCamelTopicHeaderName(String topicHeader) {
        // camel-kafka 4.18.0 sets kafka.TOPIC; later 4.18.x releases set CamelKafkaTopic.
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.setProperty(Exchange.CHARSET_NAME, "ISO-8859-1");
        exchange.getMessage().setHeader(topicHeader, "synthetic-input");
        exchange.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "order-ø-€".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        exchange.getMessage().setBody("{}");
        policy.onExchangeBegin(route("order-pipeline"), exchange);
        assertEquals("order-ø-€", exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID));
        policy.onExchangeDone(route("order-pipeline"), exchange);
        assertEquals("order-ø-€", sink.result.correlationId());
    }

    @Test
    void leavesCorrelationBytesOfNonKafkaExchangesUntouched() {
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(new RecordingSink(), "adapter-test");
        Exchange exchange = exchange();
        byte[] correlation = "order-1".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, correlation);
        exchange.getMessage().setBody("{}");
        policy.onExchangeBegin(route("order-pipeline"), exchange);
        org.junit.jupiter.api.Assertions.assertSame(correlation, exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID));
    }

    /** A route that drops the platform headers still records the input's identity, attempt and envelope. */
    @Test
    void routeThatDropsHeadersStillRecordsTheInputIdentityAndAttempt() throws Exception {
        RecordingSink sink = new RecordingSink();
        var rows = new java.util.ArrayList<MessageAttemptResult>();
        try (var context = new DefaultCamelContext()) {
            context.addRoutes(new org.apache.camel.builder.RouteBuilder() {
                public void configure() {
                    from("direct:input").routeId("input")
                            .routePolicy(new MessageStoreRoutePolicy(result -> { sink.record(result); rows.add(result); }, "adapter-test"))
                            .removeHeaders("*");
                }
            });
            context.start();
            for (Integer attempt : new Integer[]{null, 2}) {
                context.createProducerTemplate().send("direct:input", e -> {
                    e.getMessage().setHeader("CamelKafkaTopic", "synthetic-input");
                    e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
                    e.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "order-123");
                    e.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, "application/xml");
                    e.getMessage().setHeader(PlatformHeaders.SOURCE_SYSTEM, "pos");
                    e.getMessage().setHeader(PlatformHeaders.TARGET_SYSTEM, "sap");
                    if (attempt != null) e.getMessage().setHeader(PlatformHeaders.ATTEMPT, attempt);
                    e.getMessage().setBody("{}");
                });
            }
        }
        assertEquals(List.of("message-123", "message-123"), rows.stream().map(MessageAttemptResult::messageId).toList());
        assertEquals(List.of(1, 2), rows.stream().map(MessageAttemptResult::attempt).toList());
        assertEquals("order-123", rows.getLast().correlationId());
        assertEquals("application/xml", rows.getLast().contentType());
        assertEquals("pos", rows.getLast().sourceSystem());
        assertEquals("sap", rows.getLast().targetSystem());
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
    void recordsOutcomeWithoutCommittingWhenTestProfileDisablesCommit() {
        RecordingSink sink = new RecordingSink();
        CountingManualCommit commit = new CountingManualCommit();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(
                sink,
                "adapter-test",
                PayloadCapturePolicy.defaultPolicy(),
                MessageBoundaryKind.KAFKA,
                false);
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);

        policy.onExchangeBegin(route("order-pipeline"), exchange);
        policy.onExchangeDone(route("order-pipeline"), exchange);

        assertEquals(1, sink.recordCalls());
        assertFalse(commit.committed);
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

        // When the policy builds the terminal attempt result.
        policy.onExchangeDone(route("order-pipeline"), exchange);

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
    void recordsHandledHttpExceptionAsFailed() {
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = httpAuditPolicy(sink);
        Exchange exchange = exchange();
        exchange.getMessage().setBody("request");
        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 500);
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("downstream unavailable"));

        policy.onExchangeBegin(route("receive-order"), exchange);
        policy.onExchangeDone(route("receive-order"), exchange);

        assertEquals(MessageStatus.FAILED, sink.result.status());
        assertEquals("downstream unavailable", sink.result.error());
        assertEquals("metadata_only", sink.result.payloadStorage());
        assertFalse(sink.result.replayable());
        assertEquals("replay disabled by policy", sink.result.replayDisabledReason());
    }

    @Test
    void recordsHandledHttpErrorResponseAsFailedWithoutCaughtException() {
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = httpAuditPolicy(sink);
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 422);

        policy.onExchangeBegin(route("receive-order"), exchange);
        policy.onExchangeDone(route("receive-order"), exchange);

        assertEquals(MessageStatus.FAILED, sink.result.status());
        assertEquals("HTTP response status 422", sink.result.error());
    }

    @Test
    void recordsSuccessfulHttpBoundaryWithoutKafkaCommit() {
        RecordingSink sink = new RecordingSink();
        CountingManualCommit commit = new CountingManualCommit();
        MessageStoreRoutePolicy policy = httpAuditPolicy(sink);
        Exchange exchange = exchange();
        exchange.getMessage().setBody("request");
        exchange.getMessage().setHeader(Exchange.HTTP_RESPONSE_CODE, 200);
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);

        policy.onExchangeBegin(route("receive-order"), exchange);
        policy.onExchangeDone(route("receive-order"), exchange);

        assertEquals(MessageStatus.COMPLETED, sink.result.status());
        assertEquals(1, sink.recordCalls());
        assertFalse(sink.result.replayable());
        assertFalse(commit.committed);
        assertNull(exchange.getProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT));
    }

    @Test
    void propagatesHttpSinkFailureWithoutCompletingSourceResponse() {
        MessageStoreRoutePolicy policy = httpAuditPolicy(new FailingSink());
        Exchange exchange = exchange();
        exchange.getMessage().setBody("request");

        policy.onExchangeBegin(route("receive-order"), exchange);

        IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> policy.onExchangeDone(route("receive-order"), exchange));

        assertEquals("Failed to record message outcome; source response was not completed", thrown.getMessage());
        assertNotNull(thrown.getCause());
        assertEquals(thrown, exchange.getException());
        assertEquals(500, exchange.getMessage().getHeader(Exchange.HTTP_RESPONSE_CODE));
        assertEquals("Message outcome could not be recorded", exchange.getMessage().getBody(String.class));
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

    @Test
    void suppressedExchangeIsAcknowledgedWithoutRecordingAnOutcome() {
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        CountingManualCommit commit = new CountingManualCommit();
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);

        policy.onExchangeBegin(route("poll-settlements"), exchange);
        // YAML DSL constants may arrive as text; Camel converts this to Boolean.
        exchange.setProperty(PlatformExchangeProperties.RECORD_MESSAGE, "false");
        policy.onExchangeDone(route("poll-settlements"), exchange);

        assertEquals(0, sink.recordCalls());
        assertTrue(commit.committed);
    }

    @Test
    void emptyPayloadIsRecordedUnlessExchangeIsExplicitlySuppressed() {
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setBody(null);

        policy.onExchangeBegin(route("orders"), exchange);
        policy.onExchangeDone(route("orders"), exchange);

        assertEquals(1, sink.recordCalls());
        assertEquals(0L, sink.result.payloadSizeBytes());
    }

    @Test
    void objectStoreFailurePreventsKafkaCommitAndSuccessfulOutcome() {
        RecordingSink sink = new RecordingSink();
        CountingManualCommit commit = new CountingManualCommit();
        PayloadObjectStore unavailable = new PayloadObjectStore() {
            @Override
            public PayloadReference store(String integration, String messageId, InputStream payload,
                                          Long declaredSize, String contentType, Instant expiresAt, long maxBytes) {
                throw new PayloadStorageException("object store unavailable");
            }

            @Override
            public InputStream open(PayloadReference reference) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void verify(PayloadReference reference) {
                throw new UnsupportedOperationException();
            }
        };
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(
                sink,
                "adapter-test",
                new PayloadCapturePolicy(PayloadMode.TIERED, 1, 1024, Duration.ofDays(30), true),
                MessageBoundaryKind.KAFKA,
                true,
                unavailable);
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);
        exchange.getMessage().setBody(new byte[]{1, 2});

        PayloadStorageException failure = assertThrows(PayloadStorageException.class,
                () -> policy.onExchangeBegin(route("orders"), exchange));

        assertEquals("object store unavailable", failure.getMessage());
        assertFalse(commit.committed);
        assertEquals(0, sink.recordCalls());
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

    private MessageStoreRoutePolicy httpAuditPolicy(MessageStoreSink sink) {
        return new MessageStoreRoutePolicy(
                sink,
                "adapter-test",
                new PayloadCapturePolicy(
                        PayloadMode.POSTGRES_INLINE,
                        PayloadCapturePolicy.DEFAULT_MAX_BYTES,
                        java.time.Duration.ofDays(30),
                        false),
                MessageBoundaryKind.HTTP);
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

    private static class CountingManualCommit implements KafkaManualCommit {
        private boolean committed;

        @Override
        public void commit() {
            committed = true;
        }
    }
}
