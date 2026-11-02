package no.basis.integrationplatform.camel;

import no.basis.integrationplatform.camel.step.StepOutcomeFailure;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlatformRouteConfigurationTest {
    private final PlatformRouteConfiguration configuration = new PlatformRouteConfiguration();

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
        configuration.prepareDlq(exchange);

        // Then the DLQ record carries the original consumed payload plus diagnostics.
        assertEquals(false, exchange.getProperty(PlatformExchangeProperties.DLQ_PARKED, Boolean.class));
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
    void dlqParkingRestoresTheParkingCopyWhenReplayStorageDidNotKeepThePayload() {
        Exchange exchange = exchange();
        exchange.setProperty(PlatformExchangeProperties.PARKING_PAYLOAD, "<Input/>");
        exchange.getMessage().setBody("<Transformed/>");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("vendor unavailable"));

        configuration.prepareDlq(exchange);

        assertEquals("<Input/>", exchange.getMessage().getBody());
    }

    @Test
    void dlqParkingLeavesBodyAsIsWhenPayloadWasNotCaptured() {
        // Given an uncapturable body: neither the replay nor the parking copy was recorded.
        Exchange exchange = exchange();
        exchange.getMessage().setBody("stream-derived body");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("vendor unavailable"));

        configuration.prepareDlq(exchange);

        assertEquals("stream-derived body", exchange.getMessage().getBody());
        assertEquals("vendor unavailable", exchange.getMessage().getHeader(PlatformHeaders.ERROR));
        assertNotNull(exchange.getMessage().getHeader(PlatformHeaders.DLQ_AT));
    }

    @Test
    void dlqParkingLogsTheFailureWithItsStackTrace() {
        Exchange exchange = exchange();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        var failure = new IllegalStateException("vendor unavailable");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, failure);

        try (var logs = LogCapture.of(PlatformRouteConfiguration.class)) {
            configuration.prepareDlq(exchange);

            var errors = logs.at(java.util.logging.Level.SEVERE);
            assertEquals(1, errors.size());
            assertSame(failure, errors.getFirst().getThrown());
            assertEquals("Message message-123 failed after its configured retries and is being sent to the dead letter queue",
                    LogCapture.text(errors.getFirst()));
        }
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
            configuration.prepareDlq(exchange);

            var errors = logs.at(java.util.logging.Level.SEVERE);
            assertEquals(1, errors.size());
            assertSame(failure, errors.getFirst().getThrown());
            assertEquals("Message message-123 failed after its configured retries and is not parked: a step outcome "
                    + "was not checked or recorded, so it is consumed again", LogCapture.text(errors.getFirst()));
        }
    }

    @Test
    void dlqParkingMasksCredentialHeaderValuesButKeepsTheirNames() {
        Exchange exchange = exchange();
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("after receiver success"));
        var message = exchange.getMessage();
        // Request headers a route set for a receiver.
        message.setHeader("Authorization", "Bearer request-secret-token");
        message.setHeader("client_secret", "request-client-secret");
        message.setHeader("Ocp-Apim-Subscription-Key", "request-subscription-key");
        message.setHeader("X-API-Key", "key-value");
        message.setHeader("x-functions-key", "functions-key-value");
        message.setHeader("X-Access-Key", "access-key-value");
        message.setHeader("x_client_key", "client-key-value");
        // Kafka record-key headers name the record key, not a credential.
        message.setHeader("CamelKafkaKey", "order-1");
        message.setHeader("kafka.KEY", "order-1");
        // Response headers copied back from a successful receiver call.
        message.setHeader("Set-Cookie", "SESSION=receiver-session-cookie");
        message.setHeader("WWW-Authenticate", "Bearer error=\"invalid_token\"");
        message.setHeader("X-Request-Token-Echo", "receiver-echoed-token");
        // Envelope and diagnostics that must stay readable.
        message.setHeader(PlatformHeaders.MESSAGE_ID, "message-123");
        message.setHeader(PlatformHeaders.IDEMPOTENCY_KEY, "message-123:deliver");
        message.setHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY, "message-123:deliver");
        message.setHeader("Content-Type", "application/xml");
        message.setHeader("CamelHttpResponseCode", 200);

        configuration.prepareDlq(exchange);

        for (String name : List.of("Authorization", "client_secret", "Ocp-Apim-Subscription-Key", "X-API-Key",
                "x-functions-key", "X-Access-Key", "x_client_key",
                "Set-Cookie", "WWW-Authenticate", "X-Request-Token-Echo")) {
            assertEquals("xxxxxx", message.getHeader(name), name);
        }
        assertEquals("message-123", message.getHeader(PlatformHeaders.MESSAGE_ID));
        assertEquals("message-123:deliver", message.getHeader(PlatformHeaders.IDEMPOTENCY_KEY));
        assertEquals("message-123:deliver", message.getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY));
        assertEquals("application/xml", message.getHeader("Content-Type"));
        assertEquals("order-1", message.getHeader("CamelKafkaKey"));
        assertEquals("order-1", message.getHeader("kafka.KEY"));
        assertEquals(200, message.getHeader("CamelHttpResponseCode"));
        assertEquals("after receiver success", message.getHeader(PlatformHeaders.ERROR));
    }

    @Test
    void dlqParkingWithoutAFailureLogsNoError() {
        try (var logs = LogCapture.of(PlatformRouteConfiguration.class)) {
            configuration.prepareDlq(exchange());

            assertTrue(logs.records().isEmpty());
        }
    }

    @Test
    void dlqParkingPrefersExchangeExceptionAndFallsBackToClassName() {
        Exchange exchange = exchange();
        exchange.setException(new IllegalStateException((String) null));

        configuration.prepareDlq(exchange);

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
