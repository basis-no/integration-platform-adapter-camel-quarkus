package no.basis.integrationplatform.camel;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PlatformRouteConfigurationTest {
    private final PlatformRouteConfiguration configuration = new PlatformRouteConfiguration();

    @Test
    void dlqParkingRestoresOriginalPayloadAndSetsDiagnosticsHeaders() {
        // Given a message whose body was transformed mid-route before the failure (contract §8).
        byte[] originalPayload = "{\"original\":true}".getBytes(StandardCharsets.UTF_8);
        Exchange exchange = exchange();
        exchange.setProperty(PlatformExchangeProperties.PAYLOAD_BYTES, originalPayload);
        exchange.getMessage().setBody("half-transformed body");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("vendor unavailable"));

        Instant before = Instant.now();

        // When the dead letter channel prepares the exchange for parking.
        configuration.markDlqParked(exchange);

        // Then the DLQ record carries the original consumed payload plus diagnostics.
        assertTrue(Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.DLQ_PARKED, Boolean.class)));
        assertEquals("{\"original\":true}",
                new String(exchange.getMessage().getBody(byte[].class), StandardCharsets.UTF_8));
        assertEquals("vendor unavailable", exchange.getMessage().getHeader(PlatformHeaders.ERROR));

        String dlqAt = exchange.getMessage().getHeader(PlatformHeaders.DLQ_AT, String.class);
        assertNotNull(dlqAt);
        Instant parkedAt = Instant.parse(dlqAt);
        assertTrue(!parkedAt.isBefore(before) && !parkedAt.isAfter(Instant.now()),
                "x-bip-dlq-at must be an RFC 3339 timestamp taken at parking time");
    }

    @Test
    void dlqParkingLeavesBodyAsIsWhenPayloadWasNotCaptured() {
        // Given a metadata_only / uncapturable message: no PAYLOAD_BYTES property was recorded.
        Exchange exchange = exchange();
        exchange.getMessage().setBody("stream-derived body");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("vendor unavailable"));

        configuration.markDlqParked(exchange);

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

        configuration.markDlqParked(exchange);

        assertEquals("{\"storage\":\"s3_object\"}",
                new String(exchange.getMessage().getBody(byte[].class), StandardCharsets.UTF_8));
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

        configuration.markDlqParked(exchange);

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
    void dlqParkingPrefersExchangeExceptionAndFallsBackToClassName() {
        Exchange exchange = exchange();
        exchange.setException(new IllegalStateException((String) null));

        configuration.markDlqParked(exchange);

        assertEquals(IllegalStateException.class.getName(), exchange.getMessage().getHeader(PlatformHeaders.ERROR));
    }

    @Test
    void redeliveryCapturesDiagnosticsIntoExchangeProperties() {
        // Contract §5: the redelivery headers are only present while a
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
