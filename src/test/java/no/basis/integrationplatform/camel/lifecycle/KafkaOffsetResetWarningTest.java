package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.LogCapture;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A managed Kafka input whose consumer does not reset to {@code earliest}, or has no group id,
 * starts with a warning. In 0.3.x it is not refused.
 */
class KafkaOffsetResetWarningTest {

    @Test
    void theAdapterShipsEarliestAsTheKafkaOffsetReset() throws Exception {
        var defaults = new Properties();
        try (InputStream in = KafkaOffsetResetWarningTest.class.getResourceAsStream("/application.properties")) {
            defaults.load(in);
        }
        assertEquals("earliest", defaults.getProperty("camel.component.kafka.auto-offset-reset"));
    }

    @Test
    void anInputThatResetsToEarliestWithAGroupStartsWithoutAWarning() throws Exception {
        try (var context = kafkaContext("earliest"); var log = LogCapture.of(MessageStoreRoutePolicyFactory.class)) {
            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "orders",
                    route("kafka:bip.orders.in?brokers=localhost:9092&groupId=orders")));
            assertEquals(List.of(), log.at(Level.WARNING).stream().map(LogCapture::text).toList());
        }
    }

    @ParameterizedTest
    @CsvSource({
            // Camel's own default, without the adapter's
            ",kafka:bip.orders.in?brokers=localhost:9092&groupId=orders,latest",
            // the component default
            "latest,kafka:bip.orders.in?brokers=localhost:9092&groupId=orders,latest",
            "none,kafka:bip.orders.in?brokers=localhost:9092&groupId=orders,none",
            // the endpoint option
            "earliest,kafka:bip.orders.in?brokers=localhost:9092&groupId=orders&autoOffsetReset=latest,latest",
            // an additional consumer property, which Camel applies last
            "earliest,kafka:bip.orders.in?brokers=localhost:9092&groupId=orders"
                    + "&additionalProperties.auto.offset.reset=latest,latest"})
    void anInputThatDoesNotResetToEarliestStartsWithAWarning(String component, String uri, String effective)
            throws Exception {
        try (var context = kafkaContext(component); var log = LogCapture.of(MessageStoreRoutePolicyFactory.class)) {
            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "orders", route(uri)),
                    "Not refused in 0.3.x");
            List<String> warnings = log.at(Level.WARNING).stream().map(LogCapture::text).toList();
            assertEquals(1, warnings.size(), warnings.toString());
            assertTrue(warnings.getFirst().startsWith("Kafka input route 'orders' (kafka://bip.orders.in) uses "
                    + "auto.offset.reset=" + effective + ". "), warnings.getFirst());
            assertTrue(warnings.getFirst().contains("or set it to earliest"), warnings.getFirst());
        }
    }

    @Test
    void aGroupIdInTheAdditionalPropertiesCounts() throws Exception {
        try (var context = kafkaContext("earliest"); var log = LogCapture.of(MessageStoreRoutePolicyFactory.class)) {
            factory().createRoutePolicy(context, "orders",
                    route("kafka:bip.orders.in?brokers=localhost:9092&additionalProperties.group.id=orders"));
            assertEquals(List.of(), log.at(Level.WARNING).stream().map(LogCapture::text).toList());
        }
    }

    @Test
    void anInputWithoutAGroupIdStartsWithAWarning() throws Exception {
        try (var context = kafkaContext("earliest"); var log = LogCapture.of(MessageStoreRoutePolicyFactory.class)) {
            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "orders",
                    route("kafka:bip.orders.in?brokers=localhost:9092")));
            List<String> warnings = log.at(Level.WARNING).stream().map(LogCapture::text).toList();
            assertEquals(1, warnings.size(), warnings.toString());
            assertTrue(warnings.getFirst().startsWith("Kafka input route 'orders' (kafka://bip.orders.in) has no "
                    + "groupId"), warnings.getFirst());
        }
    }

    @Test
    void aRouteThatIsNotAKafkaInputIsNotChecked() throws Exception {
        try (var context = kafkaContext("latest"); var log = LogCapture.of(MessageStoreRoutePolicyFactory.class)) {
            assertInstanceOf(RedoStepsRoutePolicy.class,
                    factory().createRoutePolicy(context, "transform", route("direct:transform")));
            assertEquals(List.of(), log.at(Level.WARNING));
        }
    }

    @Test
    void aContextWithSuchAnInputStarts() throws Exception {
        try (var context = kafkaContext("latest"); var log = LogCapture.of(MessageStoreRoutePolicyFactory.class)) {
            context.addRoutePolicyFactory(factory());
            context.addRoutes(new org.apache.camel.builder.RouteBuilder() {
                public void configure() {
                    from("kafka:bip.orders.in?brokers=localhost:9092").routeId("orders").autoStartup(false)
                            .log("${body}");
                }
            });
            context.start();
            assertTrue(context.getRouteController().getRouteStatus("orders").isStopped());
            assertEquals(2, log.at(Level.WARNING).size(), log.records().toString());
        }
    }

    private static MessageStoreRoutePolicyFactory factory() {
        return new MessageStoreRoutePolicyFactory(result -> { }, "order-pipeline", "postgres-inline", 100_000, 30, true);
    }

    private static RouteDefinition route(String uri) {
        return new RouteDefinition().from(uri);
    }

    private static DefaultCamelContext kafkaContext(String componentOffsetReset) {
        var context = new DefaultCamelContext();
        if (componentOffsetReset != null) {
            context.getComponent("kafka", KafkaComponent.class).getConfiguration().setAutoOffsetReset(componentOffsetReset);
        }
        return context;
    }
}
