package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.ingress.IngressNormalizationConfig;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.ParameterizedTest;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
        // Not recorded: such a route only reads a replay's redo list, so it cannot forward it.
        assertInstanceOf(RedoStepsRoutePolicy.class, factory.createRoutePolicy(null, "transform", route("direct:transform")));
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

    @ParameterizedTest
    @CsvSource({"true,dlq", "false,dlq", "true,record-failed", "false,record-failed"})
    void anHttpBoundaryBehavesAlikeWithAnyGlobalFailureModeAndReplaySetting(boolean replayEnabled, String failureMode) {
        // The failure mode follows the input type: an HTTP boundary records FAILED and answers
        // its caller, and is never replayable, whatever the global settings say.
        var rows = new java.util.ArrayList<no.basis.integrationplatform.camel.model.MessageAttemptResult>();
        var factory = new MessageStoreRoutePolicyFactory(rows::add, "adapter-test", "postgres-inline",
                PayloadCapturePolicy.DEFAULT_MAX_BYTES, 30, replayEnabled, failureMode, "receive-order");
        var policy = assertInstanceOf(MessageStoreRoutePolicy.class,
                factory.createRoutePolicy(null, "receive-order", route("platform-http:/orders")));
        var exchange = new org.apache.camel.support.DefaultExchange(new org.apache.camel.impl.DefaultCamelContext());
        exchange.getMessage().setBody("<Order/>");
        exchange.setException(new IllegalStateException("downstream unavailable"));
        org.apache.camel.Route route = (org.apache.camel.Route) java.lang.reflect.Proxy.newProxyInstance(
                getClass().getClassLoader(), new Class<?>[]{org.apache.camel.Route.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getRouteId", "getId" -> "receive-order";
                    case "toString" -> "route(receive-order)";
                    default -> throw new UnsupportedOperationException(method.getName());
                });

        policy.onExchangeBegin(route, exchange);
        policy.onExchangeDone(route, exchange);

        assertEquals(1, rows.size());
        assertEquals(no.basis.integrationplatform.camel.model.MessageStatus.FAILED, rows.getFirst().status());
        assertEquals("metadata_only", rows.getFirst().payloadStorage());
        assertEquals(false, rows.getFirst().replayable());
        assertEquals("a synchronous HTTP boundary is not replayable", rows.getFirst().replayDisabledReason());
        assertEquals("downstream unavailable", exchange.getException().getMessage(),
                "The failure stays on the exchange for the caller");
    }

    @Test
    void kafkaAndHttpBoundariesStartTogetherInOneApplication() {
        MessageStoreRoutePolicyFactory factory = factory("auto,receive-order", true, "dlq");

        assertInstanceOf(MessageStoreRoutePolicy.class,
                factory.createRoutePolicy(null, "orders", route("kafka:bip.orders.in")));
        assertInstanceOf(MessageStoreRoutePolicy.class,
                factory.createRoutePolicy(null, "receive-order", route("platform-http:/orders")));
    }

    @Test
    void rejectsRecordFailedModeForKafkaBoundary() {
        MessageStoreRoutePolicyFactory factory = factory("auto", false, "record-failed");

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> factory.createRoutePolicy(null, "orders", route("kafka:bip.orders.in")));

        assertEquals(
                "Kafka boundary route 'orders' parks a failed message in the DLQ, but "
                        + "basis.message-store.failure-mode=record-failed turns parking off for the whole "
                        + "application. Remove the setting (the default is dlq): an HTTP audit-only boundary "
                        + "records FAILED and answers its caller with any setting",
                thrown.getMessage());
    }

    @Test
    void acceptsSourceSafeIngressNormalizationOutsideLifecycleBoundaries() {
        MessageStoreRoutePolicyFactory factory = normalizationFactory("auto", "file-to-kafka");
        RouteDefinition route = route("file:/input");
        route.routeConfigurationId(PlatformRouteConfiguration.INGRESS_NORMALIZATION);

        assertInstanceOf(RedoStepsRoutePolicy.class, factory.createRoutePolicy(null, "file-to-kafka", route));
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

    @Test
    void theDeadLetterRouteIsNotABoundaryEvenWithAllRoutesSelected() throws Exception {
        try (var context = new org.apache.camel.impl.DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory("all", true, "dlq"));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.start();
            assertTrue(context.getRoute(PlatformDeadLetterRoute.ROUTE_ID).getRoutePolicyList().isEmpty());
        }
    }

    @Test
    void countsKafkaAdmissionByDefaultOnlyWithAKafkaBoundary() {
        var withoutKafka = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        MessageStoreRoutePolicyFactory http = factory("receive-order", false, "record-failed");
        http.trafficSignals = defaultSignals(withoutKafka);
        http.createRoutePolicy(null, "receive-order", route("platform-http:/orders"));
        assertTrue(withoutKafka.getMeters().isEmpty(), "An application without a Kafka boundary exports no activity metrics");

        var withKafka = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        MessageStoreRoutePolicyFactory kafka = factory("auto", true, "dlq");
        kafka.trafficSignals = defaultSignals(withKafka);
        kafka.createRoutePolicy(null, "orders", route("kafka:bip.orders.in"));
        kafka.createRoutePolicy(null, "transform", route("direct:transform"));
        assertEquals(1.0, withKafka.get("integration_traffic_signal_supported")
                .tag("signal", "record_admitted").gauge().value());
        assertEquals(0.0, withKafka.get("integration_traffic_signal_supported")
                .tag("signal", "record_published").gauge().value());
    }

    @Test
    void theAdapterShipsEarliestAsTheKafkaOffsetReset() throws Exception {
        var defaults = new java.util.Properties();
        try (var in = MessageStoreRoutePolicyFactoryTest.class.getResourceAsStream("/application.properties")) {
            defaults.load(in);
        }
        assertEquals("earliest", defaults.getProperty("camel.component.kafka.auto-offset-reset"));
    }

    @Test
    void acceptsAKafkaBoundaryThatResetsToEarliest() throws Exception {
        try (var context = kafkaContext("earliest")) {
            assertInstanceOf(MessageStoreRoutePolicy.class, factory("auto", true, "dlq")
                    .createRoutePolicy(context, "orders", route("kafka:bip.orders.in?brokers=localhost:9092")));
        }
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource({
            // Camel's own default, without the adapter's
            ",kafka:bip.orders.in?brokers=localhost:9092,latest",
            // the component default
            "latest,kafka:bip.orders.in?brokers=localhost:9092,latest",
            "none,kafka:bip.orders.in?brokers=localhost:9092,none",
            // the endpoint option
            "earliest,kafka:bip.orders.in?brokers=localhost:9092&autoOffsetReset=latest,latest",
            // an additional consumer property, which Camel applies last
            "earliest,kafka:bip.orders.in?brokers=localhost:9092&additionalProperties.auto.offset.reset=latest,latest"})
    void refusesAKafkaBoundaryThatDoesNotResetToEarliest(String component, String uri, String effective) throws Exception {
        try (var context = kafkaContext(component)) {
            IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                    () -> factory("auto", true, "dlq").createRoutePolicy(context, "orders", route(uri)));
            assertTrue(thrown.getMessage().startsWith("Kafka boundary route 'orders' (kafka://bip.orders.in) requires "
                    + "autoOffsetReset=earliest, but its consumer uses auto.offset.reset=" + effective + "."),
                    thrown.getMessage());
        }
    }

    @Test
    void doesNotCheckAKafkaRouteThatIsNotABoundary() throws Exception {
        try (var context = kafkaContext("latest")) {
            assertInstanceOf(RedoStepsRoutePolicy.class, factory("receive-order", false, "record-failed")
                    .createRoutePolicy(context, "orders", route("kafka:bip.orders.in?brokers=localhost:9092")));
        }
    }

    @Test
    void aKafkaBoundaryWithAnotherOffsetResetDoesNotStart() throws Exception {
        try (var context = kafkaContext("latest")) {
            context.addRoutePolicyFactory(factory("auto", true, "dlq"));
            context.addRoutes(new org.apache.camel.builder.RouteBuilder() {
                public void configure() {
                    from("kafka:bip.orders.in?brokers=localhost:9092").routeId("orders").autoStartup(false).log("${body}");
                }
            });
            Exception thrown = assertThrows(Exception.class, context::start);
            Throwable cause = thrown;
            while (cause != null && !(cause instanceof IllegalArgumentException)) cause = cause.getCause();
            assertInstanceOf(IllegalArgumentException.class, cause, thrown.toString());
            assertTrue(cause.getMessage().contains("requires autoOffsetReset=earliest"), cause.getMessage());
        }
    }

    private static org.apache.camel.impl.DefaultCamelContext kafkaContext(String componentOffsetReset) {
        var context = new org.apache.camel.impl.DefaultCamelContext();
        if (componentOffsetReset != null) {
            context.getComponent("kafka", org.apache.camel.component.kafka.KafkaComponent.class)
                    .getConfiguration().setAutoOffsetReset(componentOffsetReset);
        }
        return context;
    }

    @SuppressWarnings("unchecked")
    private static no.basis.integrationplatform.camel.traffic.TrafficSignals defaultSignals(
            io.micrometer.core.instrument.MeterRegistry registry) {
        var registries = (jakarta.enterprise.inject.Instance<io.micrometer.core.instrument.MeterRegistry>)
                java.lang.reflect.Proxy.newProxyInstance(MessageStoreRoutePolicyFactoryTest.class.getClassLoader(),
                        new Class<?>[]{jakarta.enterprise.inject.Instance.class},
                        (proxy, method, args) -> switch (method.getName()) {
                            case "isResolvable" -> true;
                            case "get" -> registry;
                            default -> throw new UnsupportedOperationException(method.getName());
                        });
        return new no.basis.integrationplatform.camel.traffic.TrafficSignals(
                registries, java.util.Optional.empty(), java.util.Optional.empty());
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
