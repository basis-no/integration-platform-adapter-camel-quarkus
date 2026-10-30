package no.basis.integrationplatform.camel.ingress;

import no.basis.integrationplatform.camel.PlatformHeaders;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IngressMessageIdentityTest {
    private final IngressMessageIdentity identity = new IngressMessageIdentity(
            new IngressNormalizationConfig("source-to-kafka"), "orders-integration");

    @Test
    void derivesSameMessageIdForSameExplicitSourceEvent() {
        Exchange first = exchange("source-to-kafka");
        first.getMessage().setHeader(PlatformHeaders.SOURCE_EVENT_ID, "order-42/version-7");
        Exchange redelivery = exchange("source-to-kafka");
        redelivery.getMessage().setHeader(PlatformHeaders.SOURCE_EVENT_ID, "order-42/version-7");

        identity.prepareKafkaPublish(first, "kafka:bip.orders.in");
        identity.prepareKafkaPublish(redelivery, "kafka:bip.orders.in");

        String messageId = first.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
        assertEquals(messageId, redelivery.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
        assertDoesNotThrow(() -> UUID.fromString(messageId));
        assertEquals("order-42/version-7", first.getMessage().getHeader(PlatformHeaders.SOURCE_EVENT_ID));
        assertEquals("1", first.getMessage().getHeader(PlatformHeaders.SCHEMA_VERSION));
    }

    @Test
    void namespacesIdentityByIntegrationAndRoute() {
        Exchange firstRoute = exchange("source-to-kafka");
        firstRoute.getMessage().setHeader(PlatformHeaders.SOURCE_EVENT_ID, "event-1");
        Exchange secondRoute = exchange("another-source-route");
        secondRoute.getMessage().setHeader(PlatformHeaders.SOURCE_EVENT_ID, "event-1");
        IngressMessageIdentity bothRoutes = new IngressMessageIdentity(
                new IngressNormalizationConfig("source-to-kafka,another-source-route"), "orders-integration");

        bothRoutes.prepareKafkaPublish(firstRoute, "kafka:bip.orders.in");
        bothRoutes.prepareKafkaPublish(secondRoute, "kafka:bip.orders.in");

        assertNotEquals(firstRoute.getMessage().getHeader(PlatformHeaders.MESSAGE_ID),
                secondRoute.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
    }

    @Test
    void derivesIdentityFromFileMetadata() {
        Exchange exchange = exchange("source-to-kafka");
        exchange.getMessage().setHeader("CamelFileRelativePath", "incoming/orders-42.json");
        exchange.getMessage().setHeader("CamelFileLength", 128L);
        exchange.getMessage().setHeader("CamelFileLastModified", 1_723_456_789L);

        identity.prepareKafkaPublish(exchange, "kafka:bip.orders.in");

        assertTrue(exchange.getMessage().getHeader(PlatformHeaders.SOURCE_EVENT_ID, String.class)
                .contains("incoming/orders-42.json"));
        assertDoesNotThrow(() -> UUID.fromString(
                exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class)));
    }

    @Test
    void derivesIdentityFromJmsMessageId() {
        Exchange exchange = exchange("source-to-kafka");
        exchange.getMessage().setHeader("JMSMessageID", "ID:broker-123");

        identity.prepareKafkaPublish(exchange, "kafka:bip.orders.in");

        assertEquals("ID:broker-123", exchange.getMessage().getHeader(PlatformHeaders.SOURCE_EVENT_ID));
        assertDoesNotThrow(() -> UUID.fromString(
                exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class)));
    }

    @Test
    void preservesExistingPlatformMessageId() {
        Exchange exchange = exchange("source-to-kafka");
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "upstream-message-id");

        identity.prepareKafkaPublish(exchange, "kafka:bip.orders.in");

        assertEquals("upstream-message-id", exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
        assertNull(exchange.getMessage().getHeader(PlatformHeaders.SOURCE_EVENT_ID));
    }

    @Test
    void rejectsConfiguredRouteWithoutStableSourceIdentity() {
        Exchange exchange = exchange("source-to-kafka");

        IngressNormalizationException failure = assertThrows(
                IngressNormalizationException.class,
                () -> identity.prepareKafkaPublish(exchange, "kafka:bip.orders.in"));

        assertTrue(failure.getMessage().contains(PlatformHeaders.SOURCE_EVENT_ID));
    }

    @Test
    void ignoresOtherRoutesAndNonKafkaSends() {
        Exchange otherRoute = exchange("unconfigured-route");
        Exchange httpSend = exchange("source-to-kafka");

        identity.prepareKafkaPublish(otherRoute, "kafka:bip.orders.in");
        identity.prepareKafkaPublish(httpSend, "https://example.test/orders");

        assertNull(otherRoute.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
        assertNull(httpSend.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
    }

    private Exchange exchange(String routeId) {
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.setProperty(Exchange.INTERCEPTED_ROUTE_ID, routeId);
        return exchange;
    }
}
