package no.basis.integrationplatform.camel.lifecycle;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
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

class DeliveryAuditTest {
    @Test
    void separateSiblingRequestsRetainOutgoingPayloadAndCorrelationWithoutReplay() throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        try (var context = context(rows, "postgres-inline", 1000, false)) {
            context.start();
            try (var producer = context.createProducerTemplate()) {
                for (String body : List.of("<Delivery>one</Delivery>", "<Delivery>two</Delivery>")) {
                    Exchange reply = producer.request("direct:delivery", e -> {
                        e.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "original-input");
                        e.getMessage().setBody(body);
                    });
                    assertNull(reply.getException());
                    assertEquals("receiver response", reply.getMessage().getBody());
                }
            }
        }
        assertEquals(2, rows.size());
        assertNotEquals(rows.get(0).messageId(), rows.get(1).messageId());
        assertArrayEquals("<Delivery>one</Delivery>".getBytes(), rows.get(0).payloadBytes());
        for (var row : rows) {
            assertNotEquals("original-input", row.messageId());
            assertEquals("original-input", row.correlationId());
            assertEquals("delivery", row.routeId());
            assertEquals(MessageStatus.COMPLETED, row.status());
            assertFalse(row.replayable());
            assertTrue(row.replayDisabledReason().contains("audit"));
            assertNotNull(row.payloadExpiresAt());
        }
    }

    @Test
    void receiverFailureRemainsFailedWithOriginalException() throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        var rejection = new IllegalArgumentException("receiver rejected request");
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(rows, "delivery", "postgres-inline", 1000, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    errorHandler(noErrorHandler());
                    from("direct:delivery").routeId("delivery").process(e -> { throw rejection; });
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                Exchange reply = producer.request("direct:delivery", e -> e.getMessage().setBody("outgoing"));
                assertSame(rejection, reply.getException());
            }
        }
        assertEquals(1, rows.size());
        assertEquals(MessageStatus.FAILED, rows.get(0).status());
        assertEquals("receiver rejected request", rows.get(0).error());
        assertFalse(rows.get(0).replayable());
    }

    @Test
    void captureLimitsAndMetadataOnlyApplyWithoutMakingAuditReplayable() throws Exception {
        for (String mode : List.of("postgres-inline", "metadata-only")) {
            List<MessageAttemptResult> rows = new ArrayList<>();
            try (var context = context(rows, mode, 2, true)) {
                context.start();
                try (var producer = context.createProducerTemplate()) {
                    producer.requestBody("direct:delivery", "too large");
                }
            }
            assertEquals("metadata_only", rows.get(0).payloadStorage());
            assertNull(rows.get(0).payloadBytes());
            assertFalse(rows.get(0).replayable());
        }
    }

    @Test
    void sharedInputExchangeIsRejectedWithoutWritingOrCallingReceiver() throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        AtomicInteger deliveries = new AtomicInteger();
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(rows, "delivery", "postgres-inline", 1000, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    errorHandler(noErrorHandler());
                    from("direct:delivery").routeId("delivery").process(e -> deliveries.incrementAndGet());
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                Exchange reply = producer.request("direct:delivery", e -> {
                    e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "original-input");
                    e.getMessage().setBody("input");
                });
                assertNotNull(reply.getException());
                assertEquals("original-input", reply.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
            }
        }
        assertTrue(rows.isEmpty());
        assertEquals(0, deliveries.get());
    }

    @Test
    void auditCompletionNeverCommitsEvenIfReceiverAddsACommitHandle() throws Exception {
        AtomicInteger commits = new AtomicInteger();
        KafkaManualCommit commit = (KafkaManualCommit) java.lang.reflect.Proxy.newProxyInstance(
                KafkaManualCommit.class.getClassLoader(), new Class<?>[]{KafkaManualCommit.class},
                (proxy, method, args) -> { if (method.getName().equals("commit")) commits.incrementAndGet(); return null; });
        List<MessageAttemptResult> rows = new ArrayList<>();
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(rows, "delivery", "postgres-inline", 1000, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:delivery").routeId("delivery").process(e -> {
                        e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, commit);
                        e.setProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT, commit);
                    });
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) { producer.requestBody("direct:delivery", "outgoing"); }
        }
        assertEquals(1, rows.size());
        assertEquals(0, commits.get());
    }

    @Test
    void journalFailureFailsRequestAndPreservesReceiverFailure() throws Exception {
        for (boolean receiverFails : List.of(false, true)) {
            var rejection = new IllegalArgumentException("receiver rejection");
            try (var context = new DefaultCamelContext()) {
                context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(
                        r -> { throw new IllegalStateException("journal unavailable"); },
                        "order-pipeline", "postgres-inline", 1000, 30, true, "delivery"));
                context.addRoutes(new RouteBuilder() {
                    public void configure() {
                        errorHandler(noErrorHandler());
                        from("direct:delivery").routeId("delivery").process(e -> {
                            if (receiverFails) throw rejection;
                        });
                    }
                });
                context.start();
                try (var producer = context.createProducerTemplate()) {
                    Exchange reply = producer.request("direct:delivery", e -> e.getMessage().setBody("outgoing"));
                    assertNotNull(reply.getException());
                    if (receiverFails) {
                        assertSame(rejection, reply.getException());
                        assertEquals(1, rejection.getSuppressed().length);
                    }
                }
            }
        }
    }

    @Test
    void auditJournalFailureAfterReceiverSuccessReachesOriginalInputRecovery() throws Exception {
        for (boolean inputJournalFails : List.of(false, true)) {
            List<MessageAttemptResult> inputs = new ArrayList<>();
            List<String> parked = new ArrayList<>();
            AtomicInteger deliveries = new AtomicInteger();
            AtomicInteger commits = new AtomicInteger();
            try (var context = new DefaultCamelContext()) {
                var properties = new java.util.Properties();
                properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
                context.getPropertiesComponent().setOverrideProperties(properties);
                context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(
                        r -> { throw new IllegalStateException("audit journal unavailable"); },
                        "order-pipeline", "postgres-inline", 1000, 30, true, "delivery"));
                context.addRoutes(new PlatformRouteConfiguration());
                context.addRoutes(new PlatformDeadLetterRoute());
                context.addRoutes(new RouteBuilder() {
                    public void configure() {
                        from("direct:input").routeId("input")
                            .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .routePolicy(new MessageStoreRoutePolicy(r -> {
                                if (inputJournalFails) throw new IllegalStateException("input journal unavailable");
                                inputs.add(r);
                            }, "order-pipeline"))
                            .process(input -> {
                                try (var producer = context.createProducerTemplate()) {
                                    Exchange reply = producer.request("direct:delivery", child -> {
                                        child.getMessage().setBody("<Delivery/>");
                                        child.getMessage().setHeader(PlatformHeaders.CORRELATION_ID,
                                                input.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
                                    });
                                    assertNotNull(reply.getException());
                                    input.setException(reply.getException());
                                }
                            });
                        from("direct:delivery").routeId("delivery")
                            .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .process(e -> deliveries.incrementAndGet()).setBody(constant("accepted"));
                        from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                            .process(e -> parked.add(e.getMessage().getBody(String.class)));
                    }
                });
                context.start();
                try (var producer = context.createProducerTemplate()) {
                    producer.request("direct:input", e -> {
                        e.getMessage().setBody("<OriginalInput/>");
                        e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "original-input");
                        e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT,
                                (KafkaManualCommit) commits::incrementAndGet);
                    });
                }
            }
            assertEquals(1, deliveries.get());
            assertEquals(List.of("<OriginalInput/>"), parked);
            assertEquals(inputJournalFails ? 0 : 1, commits.get());
            if (inputJournalFails) {
                assertTrue(inputs.isEmpty());
            } else {
                assertEquals(1, inputs.size());
                assertEquals("original-input", inputs.getFirst().messageId());
                assertEquals(MessageStatus.DLQ, inputs.getFirst().status());
                assertEquals("Failed to record delivery audit", inputs.getFirst().error());
            }
        }
    }

    @Test
    void requestIdentitySurvivesResponseHeaderReplacementAndNeverClaimsParking() throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(rows, "delivery", "postgres-inline", 1000, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:delivery").routeId("delivery").process(e -> {
                        e.getMessage().setHeaders(java.util.Map.of(PlatformHeaders.MESSAGE_ID, "receiver-id"));
                        e.setProperty(PlatformExchangeProperties.DLQ_PARKED, true);
                    });
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.request("direct:delivery", e -> {
                    e.getMessage().setBody("outgoing");
                    e.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "original");
                    e.getMessage().setHeader(Exchange.CONTENT_TYPE, "application/xml");
                });
            }
        }
        assertNotEquals("receiver-id", rows.getFirst().messageId());
        assertEquals("original", rows.getFirst().correlationId());
        assertEquals("application/xml", rows.getFirst().contentType());
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
    }

    @Test
    void byteArrayPayloadIsASnapshotOfTheOutgoingRequest() throws Exception {
        List<MessageAttemptResult> rows = new ArrayList<>();
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory(rows, "delivery", "postgres-inline", 1000, true));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:delivery").routeId("delivery").process(e -> {
                        e.getMessage().getBody(byte[].class)[0] = 'X';
                    });
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                producer.requestBody("direct:delivery", "outgoing".getBytes(java.nio.charset.StandardCharsets.UTF_8));
            }
        }
        assertArrayEquals("outgoing".getBytes(java.nio.charset.StandardCharsets.UTF_8), rows.getFirst().payloadBytes());
    }

    @Test
    void invalidOrMissingSelectionsFailRatherThanSilentlyLosingAudit() throws Exception {
        for (String selection : List.of("*", "delivery,", "delivery,?")) {
            assertThrows(IllegalArgumentException.class, () -> factory(new ArrayList<>(), selection, "postgres-inline", 1000, true));
        }
        try (var context = new DefaultCamelContext()) {
            var factory = factory(new ArrayList<>(), "delivery", "postgres-inline", 1000, true);
            assertThrows(IllegalArgumentException.class, () -> factory.createRoutePolicy(
                    context, "delivery", new RouteDefinition().from("kafka:input")));
            assertThrows(IllegalArgumentException.class, () -> factory.validateAuditRoutes(null));
            assertNotNull(factory.createRoutePolicy(context, "delivery", new RouteDefinition().from("direct:delivery")));
            assertDoesNotThrow(() -> factory.validateAuditRoutes(null));
        }
    }

    private DefaultCamelContext context(List<MessageAttemptResult> rows, String mode, long maxBytes,
            boolean replayEnabled) throws Exception {
        var context = new DefaultCamelContext();
        context.addRoutePolicyFactory(factory(rows, "delivery", mode, maxBytes, replayEnabled));
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:delivery").routeId("delivery").setBody(constant("receiver response"));
                from("direct:unselected").routeId("unselected").setBody(constant("unrecorded"));
            }
        });
        return context;
    }

    private MessageStoreRoutePolicyFactory factory(List<MessageAttemptResult> rows, String selection,
            String mode, long maxBytes, boolean replayEnabled) {
        return new MessageStoreRoutePolicyFactory(rows::add, "order-pipeline", mode, maxBytes, 30, replayEnabled, selection);
    }
}
