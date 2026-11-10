package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.step.StepOutcomeFailure;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RedeliveryPolicyDefinition;
import org.apache.camel.model.RouteConfigurationDefinition;
import org.apache.camel.model.errorhandler.DefaultErrorHandlerDefinition;
import org.apache.camel.processor.errorhandler.RedeliveryPolicy;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class PlatformRouteConfigurationTest {
    private final PlatformRouteConfiguration configuration = new PlatformRouteConfiguration();

    @ParameterizedTest
    @EnumSource(MessageFailureMode.class)
    void slowBackendRedeliveriesWaitWellUnderTheKafkaPollInterval(MessageFailureMode mode) throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.addRoutesConfigurations(new PlatformRouteConfiguration(mode));
            RedeliveryPolicyDefinition policy = redeliveryPolicy(context, PlatformRouteConfiguration.SLOW_BACKEND);

            // Camel's own policy computes the waits, including its default cap when none is set.
            var camel = new RedeliveryPolicy();
            camel.setMaximumRedeliveries(Integer.parseInt(policy.getMaximumRedeliveries()));
            camel.setRedeliveryDelay(Long.parseLong(policy.getRedeliveryDelay()));
            camel.setUseExponentialBackOff(Boolean.parseBoolean(policy.getUseExponentialBackOff()));
            camel.setBackOffMultiplier(Double.parseDouble(policy.getBackOffMultiplier()));
            if (policy.getMaximumRedeliveryDelay() != null) {
                camel.setMaximumRedeliveryDelay(Long.parseLong(policy.getMaximumRedeliveryDelay()));
            }
            int redeliveries = camel.getMaximumRedeliveries();
            long delay = 0;
            long total = 0;
            for (int redelivery = 1; redelivery <= redeliveries; redelivery++) {
                delay = camel.calculateRedeliveryDelay(delay, redelivery);
                total += delay;
            }

            // Still patient: six redeliveries, the first after five seconds.
            assertEquals(6, redeliveries);
            assertEquals(5_000, Long.parseLong(policy.getRedeliveryDelay()));
            // The waits run on the Kafka consumer thread. With Camel's default 60 s cap they took
            // 195 s of the 300 s max.poll.interval.ms; capped at 20 s, they leave most of it to the attempts.
            assertEquals(95_000, total);
        }
    }

    private static RedeliveryPolicyDefinition redeliveryPolicy(DefaultCamelContext context, String id) {
        for (RouteConfigurationDefinition definition : context.getRouteConfigurationDefinitions()) {
            if (id.equals(definition.getId())) {
                return ((DefaultErrorHandlerDefinition) definition.getErrorHandler().getErrorHandlerType())
                        .getRedeliveryPolicy();
            }
        }
        return fail("No route configuration " + id);
    }

    @Test
    void dlqParkingRestoresOriginalPayloadAndSetsDiagnosticsHeaders() {
        // Given a message whose body was transformed mid-route before the failure.
        byte[] originalPayload = "{\"original\":true}".getBytes(StandardCharsets.UTF_8);
        Exchange exchange = exchange();
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_BYTES, originalPayload);
        exchange.getMessage().setBody("half-transformed body");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("vendor unavailable"));

        Instant before = Instant.now();

        // When the dead letter channel prepares the exchange for parking.
        PlatformRouteConfiguration.prepareDlq(exchange);

        // Then the DLQ record carries the original consumed payload plus diagnostics. It is not
        // parked until the dead-letter route has published it.
        assertEquals(Boolean.FALSE, exchange.getProperty(PlatformExchangeProperties.DLQ_PARKED, Boolean.class));
        assertEquals("{\"original\":true}",
                new String(exchange.getMessage().getBody(byte[].class), StandardCharsets.UTF_8));
        assertNotSame(originalPayload, exchange.getMessage().getBody(),
                "The parking endpoint gets its own array; the capture stays the input for replay storage");
        assertEquals("vendor unavailable", exchange.getMessage().getHeader(PlatformHeaders.ERROR));

        String dlqAt = exchange.getMessage().getHeader(PlatformHeaders.DLQ_AT, String.class);
        assertNotNull(dlqAt);
        Instant parkedAt = Instant.parse(dlqAt);
        assertTrue(!parkedAt.isBefore(before) && !parkedAt.isAfter(Instant.now()),
                "x-bip-dlq-at must be an RFC 3339 timestamp taken at parking time");
    }

    @Test
    void dlqParkingDoesNotPublishAReplaysRedoList() {
        // A handler that restored the original message gave the header back.
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.REDO_STEPS, "[\"post-receiver\"]".getBytes(StandardCharsets.UTF_8));
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("vendor unavailable"));

        PlatformRouteConfiguration.prepareDlq(exchange);

        assertNull(exchange.getMessage().getHeader(PlatformHeaders.REDO_STEPS));
    }

    @Test
    void dlqParkingRestoresTheParkingCopyWhenReplayStorageDidNotKeepThePayload() {
        Exchange exchange = exchange();
        exchange.setProperty(PlatformExchangeProperties.PARKING_PAYLOAD, "<Input/>");
        exchange.getMessage().setBody("<Transformed/>");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("vendor unavailable"));

        PlatformRouteConfiguration.prepareDlq(exchange);

        assertEquals("<Input/>", exchange.getMessage().getBody());
    }

    @Test
    void aMessageWhoseStepOutcomeWasNotRecordedIsLoggedAsNotParked() {
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        var failure = new IllegalStateException("step outcome store unavailable");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, failure);
        StepOutcomeFailure.begin(exchange);
        exchange.getProperty(PlatformExchangeProperties.STEP_OUTCOME_FAILURE,
                java.util.concurrent.atomic.AtomicReference.class).set(failure);

        try (var logs = LogCapture.of(PlatformRouteConfiguration.class)) {
            PlatformRouteConfiguration.prepareDlq(exchange);

            var errors = logs.at(java.util.logging.Level.SEVERE);
            assertEquals(1, errors.size());
            assertSame(failure, errors.getFirst().getThrown());
            assertEquals("Message message-123 exhausted retries and is not parked: a step outcome "
                    + "was not checked or recorded", LogCapture.text(errors.getFirst()));
        }
    }

    @Test
    void dlqParkingLeavesBodyAsIsWhenPayloadWasNotCaptured() {
        // Given an uncapturable body: neither the replay nor the parking copy was recorded.
        Exchange exchange = exchange();
        exchange.getMessage().setBody("stream-derived body");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("vendor unavailable"));

        PlatformRouteConfiguration.prepareDlq(exchange);

        assertEquals("stream-derived body", exchange.getMessage().getBody());
        assertEquals("vendor unavailable", exchange.getMessage().getHeader(PlatformHeaders.ERROR));
        assertNotNull(exchange.getMessage().getHeader(PlatformHeaders.DLQ_AT));
    }

    @Test
    void dlqParkingUsesClaimCheckForObjectBackedPayload() {
        byte[] claimCheck = "{\"storage\":\"s3_object\"}".getBytes(StandardCharsets.UTF_8);
        Exchange exchange = exchange();
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_CLAIM_CHECK, claimCheck);
        exchange.getMessage().setBody("transformed body");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("target unavailable"));

        PlatformRouteConfiguration.prepareDlq(exchange);

        assertEquals("{\"storage\":\"s3_object\"}",
                new String(exchange.getMessage().getBody(byte[].class), StandardCharsets.UTF_8));
        assertNotSame(claimCheck, exchange.getMessage().getBody(), "The parking endpoint gets its own array");
        assertEquals(no.basis.integrationplatform.camel.payload.PayloadReference.CONTENT_TYPE,
                exchange.getMessage().getHeader(PlatformHeaders.TRANSPORT_CONTENT_TYPE));
    }

    @Test
    void dlqParkingMasksCredentialHeaderValuesButKeepsTheirNames() {
        Exchange exchange = exchange();
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("after receiver success"));
        var message = exchange.getMessage();
        for (String name : java.util.List.of("Authorization", "client_secret", "Ocp-Apim-Subscription-Key", "X-API-Key",
                "x-functions-key", "X-Access-Key", "x_client_key", "Set-Cookie", "WWW-Authenticate", "X-Request-Token-Echo")) {
            message.setHeader(name, "credential-value");
        }
        message.setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        message.setHeader(PlatformHeaders.IDEMPOTENCY_KEY, "message-123:deliver");
        message.setHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY, "message-123:deliver");
        message.setHeader("CamelKafkaKey", "order-1");
        message.setHeader("kafka.KEY", "order-1");
        message.setHeader("Content-Type", "application/xml");

        PlatformRouteConfiguration.prepareDlq(exchange);

        for (String name : java.util.List.of("Authorization", "client_secret", "Ocp-Apim-Subscription-Key", "X-API-Key",
                "x-functions-key", "X-Access-Key", "x_client_key", "Set-Cookie", "WWW-Authenticate", "X-Request-Token-Echo")) {
            assertEquals("xxxxxx", message.getHeader(name), name);
        }
        assertEquals("message-123", message.getHeader(PlatformHeaders.MESSAGE_ID));
        assertEquals("message-123:deliver", message.getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertEquals("message-123:deliver", message.getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY));
        assertEquals("order-1", message.getHeader("CamelKafkaKey"));
        assertEquals("order-1", message.getHeader("kafka.KEY"));
        assertEquals("application/xml", message.getHeader("Content-Type"));
        assertEquals("after receiver success", message.getHeader(PlatformHeaders.ERROR));
    }

    @Test
    void dlqParkingRestoresTheInputMessageIdAfterAFailedSendUnderAnotherId() {
        Exchange exchange = exchange();
        exchange.setProperty(PlatformExchangeProperties.INPUT_MESSAGE_ID, "input-message");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "derived-downstream-id");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("broker unavailable"));

        PlatformRouteConfiguration.prepareDlq(exchange);

        assertEquals("input-message", exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
    }

    @Test
    void dlqParkingWithoutACapturedInputKeepsTheMessageId() {
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        PlatformRouteConfiguration.prepareDlq(exchange);
        assertEquals("message-123", exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
    }

    @Test
    void dlqParkingPrefersExchangeExceptionAndFallsBackToClassName() {
        Exchange exchange = exchange();
        exchange.setException(new IllegalStateException((String) null));

        PlatformRouteConfiguration.prepareDlq(exchange);

        assertEquals(IllegalStateException.class.getName(), exchange.getMessage().getHeader(PlatformHeaders.ERROR));
    }

    @Test
    void redeliveryCapturesDiagnosticsIntoExchangeProperties() {
        // The redelivery headers are only present while a
        // redelivery attempt is in flight (Camel strips them before dead-letter
        // delivery), so onRedelivery must snapshot them into properties.
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(Exchange.REDELIVERED, true);
        exchange.getMessage().setHeader(Exchange.REDELIVERY_COUNTER, 3);
        exchange.getMessage().setHeader(Exchange.REDELIVERY_MAX_COUNTER, 3);

        configuration.captureRedeliveryDiagnostics(exchange);

        assertTrue(Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERED, Boolean.class)));
        assertEquals(3, exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERY_COUNT, Integer.class));
        assertEquals(3, exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERY_MAX, Integer.class));
    }

    @Test
    void redeliveryCaptureIsANoopWithoutRedeliveryHeaders() {
        Exchange exchange = exchange();

        configuration.captureRedeliveryDiagnostics(exchange);

        assertNull(exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERED));
        assertNull(exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERY_COUNT));
        assertNull(exchange.getProperty(PlatformExchangeProperties.AUTO_REDELIVERY_MAX));
    }

    private Exchange exchange() {
        return new DefaultExchange(new DefaultCamelContext());
    }
}
