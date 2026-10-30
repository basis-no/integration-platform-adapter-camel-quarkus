package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.ingress.IngressNormalizationConfig;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageStoreRoutePolicyFactoryTest {
    @Test
    void resolvesPayloadThresholdWithBackwardCompatiblePrecedence() {
        assertEquals(2048, MessageStoreRoutePolicyFactory.resolveInlineThreshold(
                java.util.Optional.of(2048L), java.util.Optional.of(4096L)));
        assertEquals(4096, MessageStoreRoutePolicyFactory.resolveInlineThreshold(
                java.util.Optional.empty(), java.util.Optional.of(4096L)));
        assertEquals(PayloadCapturePolicy.DEFAULT_INLINE_THRESHOLD_BYTES,
                MessageStoreRoutePolicyFactory.resolveInlineThreshold(
                        java.util.Optional.empty(), java.util.Optional.empty()));
    }

    @Test
    void installsPolicyOnlyOnSelectedBusinessMessageBoundaries() {
        MessageStoreRoutePolicyFactory factory = factory("auto", true, "dlq");

        assertInstanceOf(MessageStoreRoutePolicy.class,
                factory.createRoutePolicy(null, "orders", route("kafka:bip.orders.in")));
        assertNull(factory.createRoutePolicy(null, "transform", route("direct:transform")));
    }

    @Test
    void acceptsExplicitHttpAuditBoundary() {
        MessageStoreRoutePolicyFactory factory = factory("receive-order", false, "record-failed");

        assertInstanceOf(MessageStoreRoutePolicy.class,
                factory.createRoutePolicy(null, "receive-order", route("platform-http:/orders")));
    }

    @Test
    void rejectsNonKafkaSourceWithoutSupportedProfile() {
        MessageStoreRoutePolicyFactory factory = factory("poll-settlements", false, "record-failed");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> factory.createRoutePolicy(null, "poll-settlements", route("timer:poll")));

        assertEquals(
                "Non-Kafka boundary route 'poll-settlements' uses a source that has no supported message-store profile",
                thrown.getMessage());
    }

    @Test
    void rejectsReplayableNonKafkaBoundaryWithoutReplayTarget() {
        MessageStoreRoutePolicyFactory factory = factory("receive-order", true, "record-failed");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> factory.createRoutePolicy(null, "receive-order", route("platform-http:/orders")));

        assertEquals(
                "Non-Kafka boundary route 'receive-order' requires basis.message-store.replay.enabled=false until a replay target is configured",
                thrown.getMessage());
    }

    @Test
    void rejectsKafkaDlqModeForNonKafkaBoundary() {
        MessageStoreRoutePolicyFactory factory = factory("receive-order", false, "dlq");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> factory.createRoutePolicy(null, "receive-order", route("platform-http:/orders")));

        assertEquals(
                "Non-Kafka boundary route 'receive-order' requires basis.message-store.failure-mode=record-failed",
                thrown.getMessage());
    }

    @Test
    void rejectsRecordFailedModeForKafkaBoundary() {
        MessageStoreRoutePolicyFactory factory = factory("auto", false, "record-failed");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> factory.createRoutePolicy(null, "orders", route("kafka:bip.orders.in")));

        assertEquals(
                "Kafka boundary route 'orders' requires basis.message-store.failure-mode=dlq",
                thrown.getMessage());
    }

    @Test
    void acceptsSourceSafeIngressNormalizationOutsideLifecycleBoundaries() {
        MessageStoreRoutePolicyFactory factory = normalizationFactory("auto", "file-to-kafka");
        RouteDefinition route = route("file:/input");
        route.routeConfigurationId(PlatformRouteConfiguration.INGRESS_NORMALIZATION);

        assertNull(factory.createRoutePolicy(null, "file-to-kafka", route));
        factory.validateConfiguredNormalizationRoutes(null);
    }

    @Test
    void rejectsNormalizationRouteWithoutSourceSafeErrorHandling() {
        MessageStoreRoutePolicyFactory factory = normalizationFactory("auto", "file-to-kafka");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> factory.createRoutePolicy(null, "file-to-kafka", route("file:/input")));

        assertTrue(thrown.getMessage().contains(PlatformRouteConfiguration.INGRESS_NORMALIZATION));
    }

    @Test
    void rejectsNormalizationRouteSelectedAsLifecycleBoundary() {
        MessageStoreRoutePolicyFactory factory = normalizationFactory("all", "file-to-kafka");
        RouteDefinition route = route("file:/input");
        route.routeConfigurationId(PlatformRouteConfiguration.INGRESS_NORMALIZATION);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> factory.createRoutePolicy(null, "file-to-kafka", route));

        assertTrue(thrown.getMessage().contains("downstream Kafka consumer"));
    }

    @Test
    void rejectsConfiguredNormalizationRouteIdThatDoesNotExist() {
        MessageStoreRoutePolicyFactory factory = normalizationFactory("auto", "missing-route");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> factory.validateConfiguredNormalizationRoutes(null));

        assertTrue(thrown.getMessage().contains("missing-route"));
    }

    private MessageStoreRoutePolicyFactory factory(String boundaryRoutes, boolean replayEnabled, String failureMode) {
        MessageStoreSink sink = result -> {
        };
        return new MessageStoreRoutePolicyFactory(
                sink,
                "adapter-test",
                "postgres-inline",
                PayloadCapturePolicy.DEFAULT_MAX_BYTES,
                30,
                replayEnabled,
                failureMode,
                boundaryRoutes);
    }

    private MessageStoreRoutePolicyFactory normalizationFactory(String boundaryRoutes, String normalizationRoutes) {
        MessageStoreSink sink = result -> {
        };
        return new MessageStoreRoutePolicyFactory(
                sink,
                "adapter-test",
                "postgres-inline",
                PayloadCapturePolicy.DEFAULT_MAX_BYTES,
                30,
                true,
                "dlq",
                boundaryRoutes,
                new IngressNormalizationConfig(normalizationRoutes));
    }

    private RouteDefinition route(String endpointUri) {
        return new RouteDefinition().from(endpointUri);
    }
}
