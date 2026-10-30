package no.basis.integrationplatform.camel.lifecycle;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class InputAuditTest {
    @Test
    void inputSnapshotIsIndependentOfPublishedKafkaIdentityAndReply() throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        try (var context = context(rows, "postgres-inline", 1000, false)) {
            context.start();
            try (var producer = context.createProducerTemplate()) {
                Exchange reply = producer.request("direct:input", e -> {
                    e.getMessage().setBody("original input");
                    e.getMessage().setHeaders(Map.of(PlatformHeaders.MESSAGE_ID, "upstream-id",
                            PlatformHeaders.CORRELATION_ID, "business-group", Exchange.CONTENT_TYPE, "application/xml"));
                });
                assertNull(reply.getException());
                assertEquals("published-kafka-id", reply.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
                assertEquals("published-group", reply.getMessage().getHeader(PlatformHeaders.CORRELATION_ID));
                assertEquals("accepted", reply.getMessage().getBody());
            }
        }
        var row = rows.getFirst();
        assertNotEquals("upstream-id", row.messageId());
        assertNotEquals("published-kafka-id", row.messageId());
        assertEquals("business-group", row.correlationId());
        assertEquals("application/xml", row.contentType());
        assertArrayEquals("original input".getBytes(StandardCharsets.UTF_8), row.payloadBytes());
        assertFalse(row.replayable());
        assertNotNull(row.payloadExpiresAt());
        assertEquals(MessageStatus.COMPLETED, row.status());
    }

    @Test
    void missingIdentityDefaultsCorrelationAndRepeatedCallerIdsStaySeparate() throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(rows, "input", "postgres-inline", 1000, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() { from("direct:input").routeId("input").setBody(constant("response")); }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                Exchange fresh = producer.request("direct:input", e -> e.getMessage().setBody("first"));
                assertNull(fresh.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
                assertNull(fresh.getMessage().getHeader(PlatformHeaders.CORRELATION_ID));
                assertEquals(rows.getFirst().messageId(), rows.getFirst().correlationId());
                for (int i = 0; i < 2; i++) {
                    producer.request("direct:input", e -> {
                        e.getMessage().setBody("next");
                        e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "same-upstream-id");
                    });
                }
            }
        }
        assertEquals(3, rows.size());
        assertNotEquals(rows.get(1).messageId(), rows.get(2).messageId());
        assertEquals("same-upstream-id", rows.get(1).correlationId());
    }

    @Test
    void batchInspectionDoesNotSeedIdentityOrCaptureStateIntoSplitRecords() throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        List<String> emitted = new ArrayList<>();
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(rows, "input", "postgres-inline", 1000, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:input").routeId("input").split(body()).process(e -> {
                        assertNull(e.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
                        assertNull(e.getMessage().getHeader(PlatformHeaders.CORRELATION_ID));
                        assertNull(e.getProperty(PlatformExchangeProperties.PAYLOAD_STORAGE));
                        assertNull(e.getProperty(PlatformExchangeProperties.REPLAYABLE));
                        emitted.add(e.getMessage().getBody(String.class));
                    }).end();
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                assertNull(producer.request("direct:input", e -> e.getMessage().setBody(List.of("first", "second"))).getException());
            }
        }
        assertEquals(List.of("first", "second"), emitted);
        assertEquals(1, rows.size());
        assertFalse(rows.getFirst().replayable());
        assertEquals(rows.getFirst().messageId(), rows.getFirst().correlationId());
    }

    @Test
    void capturedBytesSurviveProducerMutation() throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(rows, "input", "postgres-inline", 1000, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:input").routeId("input").process(e -> e.getMessage().getBody(byte[].class)[0] = 'x');
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.requestBody("direct:input", new byte[] {'a', 'b'});
            }
        }
        assertArrayEquals(new byte[] {'a', 'b'}, rows.getFirst().payloadBytes());
    }

    @Test
    void sourceFailureIsRecordedAndNeverClaimedAsManagedDlqOrCommitted() throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        AtomicInteger commits = new AtomicInteger();
        var rejection = new IllegalArgumentException("source-specific failure");
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(rows, "input", "postgres-inline", 1000, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    errorHandler(noErrorHandler());
                    from("direct:input").routeId("input").process(e -> {
                        e.setProperty(PlatformExchangeProperties.DLQ_PARKED, true);
                        e.setProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                        throw rejection;
                    });
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                Exchange reply = producer.request("direct:input", e -> e.getMessage().setBody("input"));
                assertSame(rejection, reply.getException());
            }
        }
        assertEquals(MessageStatus.FAILED, rows.getFirst().status());
        assertEquals("source-specific failure", rows.getFirst().error());
        assertFalse(rows.getFirst().replayable());
        assertEquals(0, commits.get());
    }

    @Test
    void payloadModesLimitsAndUnsafeStreamsRemainNonReplayable() throws Exception {
        for (Object body : List.of("oversized", new ByteArrayInputStream(new byte[]{1, 2}))) {
            for (String mode : List.of("postgres-inline", "metadata-only")) {
                List<MessageAttemptResult> rows = new ArrayList<>();
                try (var context = context(rows, mode, 2, true)) {
                    context.start();
                    try (var producer = context.createProducerTemplate()) { producer.requestBody("direct:input", body); }
                }
                assertEquals("metadata_only", rows.getFirst().payloadStorage());
                assertNull(rows.getFirst().payloadBytes());
                assertFalse(rows.getFirst().replayable());
                if (body instanceof ByteArrayInputStream stream) assertEquals(2, stream.available());
            }
        }
    }

    @Test
    void inheritedReplayOrCaptureOrCommitStateCannotEnrollNestedInputs() throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(rows, "input", "postgres-inline", 1000, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    errorHandler(noErrorHandler());
                    from("direct:input").routeId("input").process(e -> calls.incrementAndGet());
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                for (String name : List.of(PlatformHeaders.ATTEMPT, PlatformHeaders.REPLAY, KafkaConstants.MANUAL_COMMIT,
                        PlatformExchangeProperties.KAFKA_MANUAL_COMMIT, PlatformExchangeProperties.PAYLOAD_STORAGE)) {
                    Exchange reply = producer.request("direct:input", e -> {
                        e.getMessage().setBody("input");
                        if (name.startsWith("basis.")) e.setProperty(name, "inherited");
                        else e.getMessage().setHeader(name, "inherited");
                    });
                    assertNotNull(reply.getException(), name);
                }
            }
        }
        assertTrue(rows.isEmpty());
        assertEquals(0, calls.get());
    }

    @Test
    void journalFailureFailsInputAndPreservesExistingSourceFailure() throws Exception {
        for (boolean sourceFails : List.of(false, true)) {
            var rejection = new IllegalArgumentException("source rejected input");
            try (var context = new DefaultCamelContext()) {
                context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(
                        r -> { throw new IllegalStateException("journal unavailable"); },
                        "file-pipeline", "postgres-inline", 1000, 30, true, "", "input"));
                context.addRoutes(new RouteBuilder() {
                    public void configure() {
                        errorHandler(noErrorHandler());
                        from("direct:input").routeId("input").process(e -> { if (sourceFails) throw rejection; });
                    }
                });
                context.start();
                try (var producer = context.createProducerTemplate()) {
                    Exchange reply = producer.request("direct:input", e -> e.getMessage().setBody("input"));
                    assertNotNull(reply.getException());
                    if (sourceFails) {
                        assertSame(rejection, reply.getException());
                        assertEquals(1, rejection.getSuppressed().length);
                    }
                }
            }
        }
    }

    @Test
    void defaultOffAndInvalidSelectionCannotReplaceKafkaRecovery() throws Exception {
        for (String selection : List.of("*", "input,", "input,?")) {
            assertThrows(IllegalArgumentException.class, () -> factory(new ArrayList<>(), selection, "postgres-inline", 1000, true));
        }
        assertThrows(IllegalArgumentException.class, () -> new MessageStoreRoutePolicyFactory(
                r -> {}, "pipeline", "postgres-inline", 1000, 30, true, "input", "input"));
        try (var context = new DefaultCamelContext()) {
            var factory = factory(new ArrayList<>(), "input", "postgres-inline", 1000, true);
            assertThrows(IllegalArgumentException.class, () -> factory.createRoutePolicy(context, "input", new RouteDefinition().from("kafka:source")));
            assertThrows(IllegalArgumentException.class, () -> factory.validateAuditRoutes(null));
            assertInstanceOf(MessageStoreInputAuditRoutePolicy.class, factory.createRoutePolicy(context, "input", new RouteDefinition().from("platform-http:/input")));
            assertDoesNotThrow(() -> factory.validateAuditRoutes(null));
            var off = factory(new ArrayList<>(), "", "postgres-inline", 1000, true);
            assertNull(off.createRoutePolicy(context, "input", new RouteDefinition().from("platform-http:/input")));
            assertInstanceOf(MessageStoreRoutePolicy.class, off.createRoutePolicy(context, "source", new RouteDefinition().from("kafka:source")));
        }
    }

    private DefaultCamelContext context(List<MessageAttemptResult> rows, String mode, long cap, boolean replay) throws Exception {
        var context = new DefaultCamelContext();
        context.addRoutePolicyFactory(factory(rows, "input", mode, cap, replay));
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:input").routeId("input").process(e -> {
                    e.getMessage().setHeaders(Map.of(PlatformHeaders.MESSAGE_ID, "published-kafka-id",
                            PlatformHeaders.CORRELATION_ID, "published-group"));
                    e.getMessage().setBody("accepted");
                });
            }
        });
        return context;
    }

    private MessageStoreRoutePolicyFactory factory(List<MessageAttemptResult> rows, String selection, String mode, long cap, boolean replay) {
        return new MessageStoreRoutePolicyFactory(rows::add, "file-pipeline", mode, cap, 30, replay, "", selection);
    }
}
