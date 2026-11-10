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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageStoreRoutePolicyTest {

    @Test
    void keepsEveryKafkaEnvelopeHeaderAsTextForDownstreamHeaders() {
        // A receiver keyed on x-bip-message-id got "[55, 101, ...]" when the id stayed a byte array.
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        java.nio.charset.Charset utf8 = java.nio.charset.StandardCharsets.UTF_8;
        byte[] unowned = {0, 1, 2};
        exchange.setProperty(Exchange.CHARSET_NAME, "ISO-8859-1");
        exchange.getMessage().setHeader(KafkaConstants.TOPIC, "synthetic-input");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "7e5a-ø".getBytes(utf8));
        exchange.getMessage().setHeader(PlatformHeaders.ATTEMPT, "2".getBytes(utf8));
        exchange.getMessage().setHeader(PlatformHeaders.SOURCE_SYSTEM, "lager-ø".getBytes(utf8));
        exchange.getMessage().setHeader(PlatformHeaders.REPLAY, "true".getBytes(utf8));
        exchange.getMessage().setHeader("X-Binary-Context", unowned);

        policy.onExchangeBegin(route("synthetic-input"), exchange);

        assertEquals("7e5a-ø", exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
        assertEquals("2", exchange.getMessage().getHeader(PlatformHeaders.ATTEMPT));
        assertEquals("lager-ø", exchange.getMessage().getHeader(PlatformHeaders.SOURCE_SYSTEM));
        assertEquals("true", exchange.getMessage().getHeader(PlatformHeaders.REPLAY));
        org.junit.jupiter.api.Assertions.assertSame(unowned, exchange.getMessage().getHeader("X-Binary-Context"));
        policy.onExchangeDone(route("synthetic-input"), exchange);
        assertEquals("7e5a-ø", sink.result.messageId());
        assertEquals(2, sink.result.attempt());
        assertEquals("lager-ø", sink.result.sourceSystem());
    }

    @Test
    void leavesEnvelopeBytesOfNonKafkaExchangesUntouched() {
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(result -> {}, "adapter-test");
        Exchange exchange = exchange();
        byte[] id = "direct-id".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, id);
        policy.onExchangeBegin(route("synthetic-input"), exchange);
        org.junit.jupiter.api.Assertions.assertSame(id, exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
    }

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

    /** A route that drops the x-bip-* headers still records the input's identity, attempt and envelope. */
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
        // On the DLQ path Camel's error handler removes the
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
    void failedKafkaOutcomeIsRecordedButNotCommitted() {
        // Given an exchange that is still failed: it was neither completed nor parked.
        RecordingSink sink = new RecordingSink();
        RecordingManualCommit commit = new RecordingManualCommit(sink);
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);
        IllegalStateException publishFailure = new IllegalStateException("DLQ unavailable");
        exchange.setException(publishFailure);

        policy.onExchangeDone(route("order-pipeline"), exchange);

        // Then the outcome is FAILED, the offset stays uncommitted and the exchange stays failed,
        // so the Kafka consumer reads the record again.
        assertEquals(MessageStatus.FAILED, sink.result.status());
        assertFalse(commit.committed);
        assertSame(publishFailure, exchange.getException());
    }

    @Test
    void ineligibleExchangeThatFailedIsNotCommitted() {
        RecordingSink sink = new RecordingSink();
        sink.recordedBeforeCommit = true;
        RecordingManualCommit commit = new RecordingManualCommit(sink);
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.setProperty(PlatformExchangeProperties.RECORD_MESSAGE, false);
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);
        exchange.setException(new IllegalStateException("DLQ unavailable"));

        policy.onExchangeDone(route("order-pipeline"), exchange);

        assertEquals(0, sink.recordCalls());
        assertFalse(commit.committed);
    }

    @Test
    void commitFailureAfterRecordingSaysTheOutcomeWasRecorded() {
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT,
                (KafkaManualCommit) () -> { throw new IllegalStateException("rebalance in progress"); });

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> policy.onExchangeDone(route("order-pipeline"), exchange));

        assertEquals("Message outcome was recorded but Kafka offset commit failed", thrown.getMessage());
        assertEquals(1, sink.recordCalls());
        assertSame(thrown, exchange.getException());
    }

    @Test
    void keepsOneCopyOfAByteArrayInputForReplayAndParking() throws Exception {
        Exchange exchange = kafkaInput("ORIGINAL".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        new MessageStoreRoutePolicy(new RecordingSink(), "adapter-test").onExchangeBegin(route("order-pipeline"), exchange);

        byte[] captured = exchange.getProperty(PlatformExchangeProperties.PAYLOAD_BYTES, byte[].class);
        assertNotSame(exchange.getMessage().getBody(), captured);
        assertArrayEquals(exchange.getMessage().getBody(byte[].class), captured);
        assertEquals(sha256(captured), exchange.getProperty(PlatformExchangeProperties.PAYLOAD_SHA256));
        assertNull(exchange.getProperty(PlatformExchangeProperties.PARKING_PAYLOAD), "Parking uses the replay copy");
    }

    @Test
    void aSmallStreamGivesTheRouteItsOwnArray() {
        Exchange exchange = kafkaInput(new java.io.ByteArrayInputStream("ORIGINAL".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        var policy = new MessageStoreRoutePolicy(new RecordingSink(), "adapter-test",
                new PayloadCapturePolicy(PayloadMode.TIERED, 1000, Duration.ofDays(30), true));

        policy.onExchangeBegin(route("order-pipeline"), exchange);

        byte[] captured = exchange.getProperty(PlatformExchangeProperties.PAYLOAD_BYTES, byte[].class);
        assertNotSame(exchange.getMessage().getBody(), captured);
        assertArrayEquals(exchange.getMessage().getBody(byte[].class), captured);
    }

    @Test
    void keepsAParkingCopyOnlyWhenReplayStorageDoesNotKeepTheInputAndTheInputCanBeParked() {
        var policy = new MessageStoreRoutePolicy(new RecordingSink(), "adapter-test",
                new PayloadCapturePolicy(PayloadMode.METADATA_ONLY, 1000, Duration.ofDays(30), true));
        Exchange bytes = kafkaInput("ORIGINAL".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        Exchange text = kafkaInput("ORIGINAL");
        Exchange empty = kafkaInput(null);
        Exchange internal = exchange();
        internal.getMessage().setBody("ORIGINAL".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        for (Exchange exchange : List.of(bytes, text, empty, internal)) {
            policy.onExchangeBegin(route("order-pipeline"), exchange);
            assertNull(exchange.getProperty(PlatformExchangeProperties.PAYLOAD_BYTES));
        }

        byte[] parked = bytes.getProperty(PlatformExchangeProperties.PARKING_PAYLOAD, byte[].class);
        assertNotSame(bytes.getMessage().getBody(), parked);
        assertArrayEquals(bytes.getMessage().getBody(byte[].class), parked);
        assertSame(text.getMessage().getBody(), text.getProperty(PlatformExchangeProperties.PARKING_PAYLOAD),
                "A String cannot change, so it is not copied");
        assertArrayEquals(new byte[0], empty.getProperty(PlatformExchangeProperties.PARKING_PAYLOAD, byte[].class));
        assertNull(internal.getProperty(PlatformExchangeProperties.PARKING_PAYLOAD),
                "An input without a Kafka commit is never parked");
    }

    @Test
    void anInputWhoseStepOutcomeWasNotRecordedIsRecordedFailedAndNotCommittedAlsoWhenHandled() {
        RecordingSink sink = new RecordingSink();
        RecordingManualCommit commit = new RecordingManualCommit(sink);
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);
        policy.onExchangeBegin(route("order-pipeline"), exchange);
        var outage = new IllegalStateException("step outcome store unavailable");
        exchange.getProperty(PlatformExchangeProperties.STEP_OUTCOME_FAILURE,
                java.util.concurrent.atomic.AtomicReference.class).set(outage);

        // The route caught the failure, so the exchange completed without an exception.
        policy.onExchangeDone(route("order-pipeline"), exchange);

        assertEquals(MessageStatus.FAILED, sink.result.status());
        assertEquals("step outcome store unavailable", sink.result.error());
        assertFalse(commit.committed);
        assertSame(outage, exchange.getException().getCause(), "The Kafka consumer reads only a failed exchange again");
    }

    @Test
    void anInputWhoseStepOutcomeWasNotRecordedKeepsItsOwnException() {
        RecordingSink sink = new RecordingSink();
        RecordingManualCommit commit = new RecordingManualCommit(sink);
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(sink, "adapter-test");
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);
        policy.onExchangeBegin(route("order-pipeline"), exchange);
        exchange.getProperty(PlatformExchangeProperties.STEP_OUTCOME_FAILURE,
                java.util.concurrent.atomic.AtomicReference.class).set(new IllegalStateException("outage"));
        var receiverFailure = new IllegalArgumentException("receiver rejected");
        exchange.setException(receiverFailure);

        policy.onExchangeDone(route("order-pipeline"), exchange);

        assertSame(receiverFailure, exchange.getException());
        assertEquals(0, receiverFailure.getSuppressed().length);
        assertEquals(MessageStatus.FAILED, sink.result.status());
        assertFalse(commit.committed);
    }

    private Exchange kafkaInput(Object body) {
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
        exchange.getMessage().setBody(body);
        return exchange;
    }

    private static String sha256(byte[] bytes) throws Exception {
        return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
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
        assertEquals("a synchronous HTTP boundary is not replayable", sink.result.replayDisabledReason());
    }

    @Test
    void anHttpRequestWithAClaimCheckIsRecordedMetadataOnlyAndNotReplayable() {
        RecordingSink sink = new RecordingSink();
        byte[] object = "<a/>\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        // The object's real SHA-256: the adapter checks the whole object before the route reads it.
        var reference = new PayloadReference(1, PayloadReference.STORAGE, "s3://payloads/orders/order-1",
                object.length, sha256Hex(object), "application/xml", Instant.now().plus(Duration.ofDays(1)).toString());
        PayloadObjectStore store = new PayloadObjectStore() {
            @Override
            public PayloadReference store(String integration, String messageId, InputStream payload,
                                          Long declaredSize, String contentType, Instant expiresAt, long maxBytes) {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputStream open(PayloadReference ignored) {
                return new java.io.ByteArrayInputStream(object);
            }

            @Override
            public void verify(PayloadReference ignored) {
            }
        };
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(
                sink, "adapter-test",
                new PayloadCapturePolicy(PayloadMode.TIERED, 1, 1024, Duration.ofDays(30), true),
                MessageBoundaryKind.HTTP, false, store);
        Exchange exchange = exchange();
        exchange.getMessage().setBody(reference.toJsonBytes());
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_STORAGE, reference.storage());
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_REF, reference.ref());
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_SIZE, Long.toString(reference.sizeBytes()));
        exchange.getMessage().setHeader(PlatformHeaders.PAYLOAD_SHA256, reference.sha256());

        policy.onExchangeBegin(route("receive-order"), exchange);
        assertInstanceOf(InputStream.class, exchange.getMessage().getBody(), "The route still reads the object");
        policy.onExchangeDone(route("receive-order"), exchange);

        assertEquals(MessageStatus.COMPLETED, sink.result.status());
        assertEquals("metadata_only", sink.result.payloadStorage());
        assertNull(sink.result.payloadRef());
        assertFalse(sink.result.replayable());
        assertEquals("a synchronous HTTP boundary is not replayable", sink.result.replayDisabledReason());
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

        // Then read-model/status consumers see the parked state, not just a generic failure.
        assertEquals(MessageStatus.DLQ, sink.result.status());
        assertEquals("poison message", sink.result.error());
    }

    @Test
    void createsMessageIdWhenMissingAtExchangeBegin() {
        // Given a route starts with no message id on the exchange.
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

        // Camel ignores an exception from onExchangeBegin, so the policy fails and stops the
        // exchange instead: no processor runs, nothing is parked, and nothing is committed.
        policy.onExchangeBegin(route("orders"), exchange);

        PayloadStorageException failure = exchange.getException(PayloadStorageException.class);
        assertEquals("object store unavailable", failure.getMessage());
        assertTrue(exchange.isRouteStop());
        assertNull(exchange.getMessage().getBody());
        assertFalse(commit.committed);
        assertEquals(0, sink.recordCalls());

        policy.onExchangeDone(route("orders"), exchange);

        assertEquals(1, sink.recordCalls());
        assertEquals(MessageStatus.FAILED, sink.result.status());
        assertFalse(commit.committed);
    }

    @Test
    void aMalformedClaimCheckDoesNotLogItsBody() {
        RecordingSink sink = new RecordingSink();
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(
                sink,
                "adapter-test",
                new PayloadCapturePolicy(PayloadMode.TIERED, 1, 1024, Duration.ofDays(30), true),
                MessageBoundaryKind.KAFKA,
                true,
                no.basis.integrationplatform.camel.payload.PayloadObjectStore.disabled());
        Exchange exchange = exchange();
        // An ordinary payload sent with the headers of a claim check.
        String body = "{\"customer\": \"private-order-details-4711\",\n \"card\": \"0000-1111\"";
        exchange.getMessage().setHeader(PlatformHeaders.TRANSPORT_CONTENT_TYPE, PayloadReference.CONTENT_TYPE);
        exchange.getMessage().setBody(body);

        try (var log = no.basis.integrationplatform.camel.LogCapture.of(MessageStoreRoutePolicy.class)) {
            policy.onExchangeBegin(route("orders"), exchange);

            var errors = log.at(java.util.logging.Level.SEVERE);
            assertEquals(1, errors.size());
            String logged = no.basis.integrationplatform.camel.LogCapture.text(errors.getFirst());
            assertFalse(logged.contains("private-order-details"), logged);
            assertFalse(logged.contains("0000-1111"), logged);
            assertFalse(logged.contains("\n"), "one line: " + logged);
            assertTrue(logged.contains("Invalid payload-reference document"), logged);
        }
        PayloadStorageException failure = exchange.getException(PayloadStorageException.class);
        assertFalse(failure.getMessage().contains("private-order-details"), failure.getMessage());
        assertNull(failure.getCause(), "the parser's exception, which quotes the body, is not kept");
        assertTrue(exchange.isRouteStop());
    }

    @Test
    void onlyCopyAndObjectStoreErrorsShowTheirMessage() {
        var disk = new PayloadStorageException("copy failed",
                new java.io.IOException("wrapped", new java.nio.file.FileSystemException("/spool: No space left\non device")));
        var application = new PayloadStorageException("capture failed",
                new IllegalStateException("payload was: private-order-details-4711"));

        assertEquals(" (cause: java.nio.file.FileSystemException: /spool: No space left on device)",
                MessageStoreRoutePolicy.rootCause(disk));
        assertEquals(" (cause: java.lang.IllegalStateException)", MessageStoreRoutePolicy.rootCause(application));
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
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
