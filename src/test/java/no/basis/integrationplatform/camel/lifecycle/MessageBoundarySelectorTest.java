package no.basis.integrationplatform.camel.lifecycle;

import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageBoundarySelectorTest {
    @Test
    void autoSelectsKafkaConsumersButNotInternalOrScheduledRoutes() {
        MessageBoundarySelector selector = MessageBoundarySelector.fromConfig("auto");

        assertTrue(selector.includes("orders", route("kafka:bip.orders.in")));
        assertFalse(selector.includes("internal", route("direct:transform")));
        assertFalse(selector.includes("poll", route("timer:poll")));
    }

    @Test
    void explicitRouteIdsExtendAutoSelection() {
        MessageBoundarySelector selector = MessageBoundarySelector.fromConfig("auto, settlement-poll");

        assertTrue(selector.includes("orders", route("kafka:bip.orders.in")));
        assertTrue(selector.includes("settlement-poll", route("timer:poll")));
        assertFalse(selector.includes("internal", route("direct:transform")));
    }

    @Test
    void explicitRouteIdsCanReplaceAutomaticSelection() {
        MessageBoundarySelector selector = MessageBoundarySelector.fromConfig("settlement-poll");

        assertTrue(selector.includes("settlement-poll", route("timer:poll")));
        assertFalse(selector.includes("orders", route("kafka:bip.orders.in")));
    }

    @Test
    void allSelectsEveryRoute() {
        MessageBoundarySelector selector = MessageBoundarySelector.fromConfig("all");

        assertTrue(selector.includes("internal", route("direct:transform")));
    }

    @Test
    void identifiesBoundaryTransportKinds() {
        MessageBoundarySelector selector = MessageBoundarySelector.fromConfig("auto");

        assertEquals(MessageBoundaryKind.KAFKA, selector.kind(route("kafka:bip.orders.in")));
        assertEquals(MessageBoundaryKind.HTTP, selector.kind(route("platform-http:/orders")));
        assertEquals(MessageBoundaryKind.OTHER, selector.kind(route("timer:poll")));
    }

    private RouteDefinition route(String endpointUri) {
        return new RouteDefinition().from(endpointUri);
    }
}
