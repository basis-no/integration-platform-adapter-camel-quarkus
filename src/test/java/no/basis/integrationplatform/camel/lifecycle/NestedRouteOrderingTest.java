package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.RoutePolicySupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Adapter 0.3.1 enrolled every route, so a nested fallback route could record and commit
 * before its caller finished. These tests show the 0.3.6 behaviour: the factory enrolls only
 * the Kafka input; a nested route's completion callback still runs before the input's, but no
 * nested route records the input or commits its offset.
 */
class NestedRouteOrderingTest {
    private final List<String> events = Collections.synchronizedList(new ArrayList<>());
    private DefaultCamelContext context;

    @AfterEach
    void stop() throws Exception {
        if (context != null) context.close();
    }

    @Test
    void factoryEnrollsOnlyTheKafkaInputNotItsFallbackRoutes() throws Exception {
        start();
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("kafka:bip.order-pipeline.in?brokers=localhost:1").routeId("source").autoStartup(false)
                        .to("direct:fallback");
            }
        });

        assertTrue(context.getRoute("source").getRoutePolicyList().stream()
                .anyMatch(MessageStoreRoutePolicy.class::isInstance));
        assertTrue(context.getRoute("fallback").getRoutePolicyList().stream()
                .noneMatch(MessageStoreRoutePolicy.class::isInstance));
        assertTrue(context.getRoute("audited-request").getRoutePolicyList().stream()
                .anyMatch(MessageStoreAuditRoutePolicy.class::isInstance));
    }

    @Test
    void nestedFallbackFinishesFirstButOnlyTheInputRecordsAndCommits() throws Exception {
        start();

        input("direct:fallback");

        assertEquals(List.of(
                "fallback ran",
                "fallback route done",
                "input continued",
                "record message-1 from input",
                "commit"), events);
    }

    @Test
    void separateAuditRequestRecordsItsOwnRowFirstButNeverCommits() throws Exception {
        start();

        input("audited");

        assertEquals(4, events.size(), events.toString());
        assertTrue(events.get(0).startsWith("record ") && events.get(0).endsWith(" from audited-request"), events.get(0));
        assertNotEquals("record message-1 from audited-request", events.get(0), "The request has its own message id");
        assertEquals(List.of("input continued", "record message-1 from input", "commit"), events.subList(1, 4));
    }

    private void start() throws Exception {
        context = new DefaultCamelContext();
        context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(
                this::record, "order-pipeline", "postgres-inline", 1000, 30, true, "audited-request"));
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                // The explicit policy stands in for the Kafka input lifecycle with a local source.
                from("direct:input").routeId("input")
                        .routePolicy(new MessageStoreRoutePolicy(NestedRouteOrderingTest.this::record, "order-pipeline"))
                        .choice()
                            .when(header("target").isEqualTo("audited"))
                                .process(e -> {
                                    // A separate request exchange, as docs/delivery-audit.md requires.
                                    try (var producer = getContext().createProducerTemplate()) {
                                        producer.request("direct:audited-request", r -> r.getMessage().setBody("<Request/>"));
                                    }
                                })
                            .otherwise()
                                .to("direct:fallback")
                        .end()
                        .process(e -> events.add("input continued"));
                from("direct:fallback").routeId("fallback")
                        .routePolicy(new RoutePolicySupport() {
                            @Override public void onExchangeDone(Route route, Exchange exchange) {
                                events.add("fallback route done");
                            }
                        })
                        .process(e -> events.add("fallback ran"));
                from("direct:audited-request").routeId("audited-request")
                        .setBody(constant("accepted"));
            }
        });
        context.start();
    }

    private void input(String target) {
        try (var producer = context.createProducerTemplate()) {
            producer.send("direct:input", e -> {
                e.getMessage().setHeader("target", target);
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> events.add("commit"));
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                e.getMessage().setBody("<Input/>");
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private void record(MessageAttemptResult result) {
        events.add("record " + result.messageId() + " from " + result.routeId());
    }
}
