package no.basis.integrationplatform.camel.lifecycle;

import org.apache.camel.AggregationStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.AggregationStrategies;
import org.apache.camel.builder.Builder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.processor.aggregate.MemoryAggregationRepository;
import org.apache.camel.spi.AggregationRepository;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A Kafka boundary route may not hold records in memory after its input completes. */
class InMemoryHoldGuardTest {
    private static final AggregationStrategy STRATEGY = AggregationStrategies.groupedBody();

    @Test
    void refusesAnInMemoryAggregateOnAKafkaBoundary() throws Exception {
        RouteDefinition route = kafka("orders");
        route.aggregate(Builder.constant("all"), STRATEGY).completionSize(10).id("batch-orders").to("log:batch");

        IllegalArgumentException refused = refuse("orders", route);

        assertEquals("Kafka boundary route 'orders' holds records in memory after its input completes: aggregate "
                + "'batch-orders' in route 'orders' (in-memory repository). The adapter commits a completed input, so "
                + "a restart before such a record is released loses it. Give an aggregate a persistent "
                + "aggregationRepository; a resequence has no durable form yet.", refused.getMessage());
    }

    @Test
    void theApplicationDoesNotStart() throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.addRoutePolicyFactory(factory());
            context.addRoutes(new org.apache.camel.builder.RouteBuilder() {
                public void configure() {
                    from("kafka:bip.orders.in?brokers=localhost:1&autoOffsetReset=earliest").routeId("orders")
                            .aggregate(constant("all"), STRATEGY).completionSize(10).to("direct:batched");
                }
            });

            Exception refused = assertThrows(Exception.class, context::start);

            Throwable cause = refused;
            while (cause != null && !(cause instanceof IllegalArgumentException)) cause = cause.getCause();
            assertInstanceOf(IllegalArgumentException.class, cause, () -> "Refused for another reason: " + refused);
            assertEquals(true, cause.getMessage().startsWith("Kafka boundary route 'orders' holds records in memory"));
        }
    }

    @Test
    void refusesAnExplicitMemoryRepository() throws Exception {
        RouteDefinition route = kafka("orders");
        route.aggregate(Builder.constant("all"), STRATEGY).aggregationRepository(new MemoryAggregationRepository())
                .completionSize(10).to("log:batch");

        refuse("orders", route);
    }

    @Test
    void refusesAMemoryRepositoryNamedInTheRegistry() throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.getRegistry().bind("batches", new MemoryAggregationRepository());
            RouteDefinition route = kafka("orders");
            route.aggregate(Builder.constant("all"), STRATEGY).aggregationRepository("#batches").completionSize(10)
                    .to("log:batch");
            context.addRouteDefinition(route);

            IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                    () -> factory().createRoutePolicy(context, "orders", route));
            assertEquals(true, refused.getMessage().contains("in route 'orders' (in-memory repository)"),
                    refused.getMessage());
        }
    }

    @Test
    void acceptsAPersistentRepository() throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.getRegistry().bind("batches", new PersistentRepository());
            RouteDefinition byBean = kafka("orders");
            byBean.aggregate(Builder.constant("all"), STRATEGY).aggregationRepository(new PersistentRepository())
                    .completionSize(10).to("log:batch");
            RouteDefinition byName = kafka("invoices");
            byName.aggregate(Builder.constant("all"), STRATEGY).aggregationRepository("batches").completionSize(10)
                    .to("log:batch");
            context.addRouteDefinitions(List.of(byBean, byName));

            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "orders", byBean));
            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "invoices", byName));
        }
    }

    @Test
    void refusesAResequencerOnAKafkaBoundary() throws Exception {
        RouteDefinition batch = kafka("orders");
        batch.resequence(Builder.header("seq")).batch().id("in-order").to("log:ordered");
        RouteDefinition stream = kafka("invoices");
        stream.resequence(Builder.header("seq")).stream().to("log:ordered");

        assertEquals("Kafka boundary route 'orders' holds records in memory after its input completes: resequence "
                        + "'in-order' in route 'orders'",
                refuse("orders", batch).getMessage().split("\\. The adapter")[0]);
        refuse("invoices", stream);
    }

    @Test
    void refusesAHoldInsideAChoiceOrASplit() throws Exception {
        RouteDefinition route = kafka("orders");
        route.choice().when(Builder.header("batch").isEqualTo("yes"))
                .split(Builder.body()).aggregate(Builder.constant("all"), STRATEGY).completionSize(10).to("log:batch");

        refuse("orders", route);
    }

    @Test
    void refusesAHoldInADirectRouteTheBoundarySendsTo() throws Exception {
        try (var context = new DefaultCamelContext()) {
            RouteDefinition input = kafka("orders");
            input.to("direct:enrich");
            RouteDefinition enrich = new RouteDefinition().from("direct:enrich").routeId("enrich");
            enrich.to("direct://batch?timeout=1000");
            RouteDefinition batch = new RouteDefinition().from("direct:batch").routeId("batch");
            batch.aggregate(Builder.constant("all"), STRATEGY).id("batch-orders").completionSize(10).to("log:batch");
            context.addRouteDefinitions(List.of(input, enrich, batch));

            IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                    () -> factory().createRoutePolicy(context, "orders", input));

            assertEquals("Kafka boundary route 'orders' holds records in memory after its input completes: aggregate "
                    + "'batch-orders' in route 'batch' (in-memory repository)", refused.getMessage().split("\\. The adapter")[0]);
        }
    }

    @Test
    void acceptsAHoldOutsideKafkaBoundaries() throws Exception {
        try (var context = new DefaultCamelContext()) {
            RouteDefinition input = kafka("orders");
            input.to("kafka:orders.batched");
            RouteDefinition batched = new RouteDefinition().from("direct:unrelated").routeId("unrelated");
            batched.aggregate(Builder.constant("all"), STRATEGY).completionSize(10).to("log:batch");
            context.addRouteDefinitions(List.of(input, batched));

            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "orders", input));
            // A route that is not a boundary is not recorded or committed by the adapter.
            assertInstanceOf(RedoStepsRoutePolicy.class, factory().createRoutePolicy(context, "unrelated", batched));
        }
    }

    @Test
    void followsPropertyPlaceholdersInDirectUris() throws Exception {
        try (var context = new DefaultCamelContext()) {
            var properties = new java.util.Properties();
            properties.setProperty("target", "batch");
            context.getPropertiesComponent().setOverrideProperties(properties);
            RouteDefinition input = kafka("orders");
            input.to("direct:{{target}}");
            RouteDefinition batch = new RouteDefinition().from("direct:batch").routeId("batch");
            batch.aggregate(Builder.constant("all"), STRATEGY).completionSize(10).to("log:batch");
            RouteDefinition invoices = kafka("invoices");
            invoices.to("direct:invoice-batch");
            RouteDefinition invoiceBatch = new RouteDefinition().from("direct:{{target}}-invoices").routeId("invoice-batch");
            invoiceBatch.resequence(Builder.header("seq")).to("log:batch");
            context.addRouteDefinitions(List.of(input, batch, invoices, invoiceBatch));

            assertThrows(IllegalArgumentException.class, () -> factory().createRoutePolicy(context, "orders", input));
            // The input URI resolves to direct:batch-invoices, not the invoices route's target.
            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "invoices", invoices));
            invoices.getOutputs().clear();
            invoices.to("direct:batch-invoices");
            assertThrows(IllegalArgumentException.class, () -> factory().createRoutePolicy(context, "invoices", invoices));
        }
    }

    @Test
    void followsEnrichAndToDWithAFixedUri() throws Exception {
        for (var send : List.<java.util.function.Consumer<RouteDefinition>>of(
                route -> route.enrich("direct:batch"), route -> route.toD("direct:batch"))) {
            try (var context = new DefaultCamelContext()) {
                RouteDefinition input = kafka("orders");
                send.accept(input);
                RouteDefinition batch = new RouteDefinition().from("direct:batch").routeId("batch");
                batch.aggregate(Builder.constant("all"), STRATEGY).completionSize(10).to("log:batch");
                context.addRouteDefinitions(List.of(input, batch));

                assertThrows(IllegalArgumentException.class, () -> factory().createRoutePolicy(context, "orders", input));
            }
        }
        try (var context = new DefaultCamelContext()) {
            RouteDefinition input = kafka("orders");
            input.toD("direct:${header.target}");
            RouteDefinition batch = new RouteDefinition().from("direct:batch").routeId("batch");
            batch.aggregate(Builder.constant("all"), STRATEGY).completionSize(10).to("log:batch");
            context.addRouteDefinitions(List.of(input, batch));

            // A dynamic URI is not followed (documented).
            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "orders", input));
        }
    }

    @Test
    void refusesAHoldInACircuitBreakerFallback() throws Exception {
        RouteDefinition route = kafka("orders");
        route.circuitBreaker().to("log:call").onFallback()
                .aggregate(Builder.constant("all"), STRATEGY).completionSize(10).to("log:batch");

        refuse("orders", route);
    }

    @Test
    void resolvesRepositoryReferencesAsCamelDoes() throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.getRegistry().bind("durable", new PersistentRepository());
            RouteDefinition byClass = aggregating("by-class", "#class:" + PersistentRepository.class.getName());
            RouteDefinition byType = aggregating("by-type", "#type:" + AggregationRepository.class.getName());
            RouteDefinition memoryByClass = aggregating("memory-by-class", "#class:" + MemoryAggregationRepository.class.getName());
            RouteDefinition missing = aggregating("missing", "#bean:absent");
            context.addRouteDefinitions(List.of(byClass, byType, memoryByClass, missing));

            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "by-class", byClass));
            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "by-type", byType));
            assertThrows(IllegalArgumentException.class,
                    () -> factory().createRoutePolicy(context, "memory-by-class", memoryByClass));
            IllegalArgumentException unresolved = assertThrows(IllegalArgumentException.class,
                    () -> factory().createRoutePolicy(context, "missing", missing));
            assertEquals(true, unresolved.getMessage().contains(
                    "repository '#bean:absent' could not be resolved to show that it is not in memory"));
        }
    }

    @Test
    void acceptsAPersistentRepositoryByHashNameAndPlaceholder() throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.getRegistry().bind("durable", new PersistentRepository());
            context.getRegistry().bind("memory", new MemoryAggregationRepository());
            var properties = new java.util.Properties();
            properties.setProperty("repo", "durable");
            properties.setProperty("memory-repo", "memory");
            context.getPropertiesComponent().setOverrideProperties(properties);
            RouteDefinition hash = aggregating("hash", "#durable");
            RouteDefinition placeholder = aggregating("placeholder", "#{{repo}}");
            RouteDefinition memory = aggregating("memory", "#{{memory-repo}}");
            context.addRouteDefinitions(List.of(hash, placeholder, memory));

            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "hash", hash));
            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "placeholder", placeholder));
            IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                    () -> factory().createRoutePolicy(context, "memory", memory));
            assertEquals(true, refused.getMessage().contains("(in-memory repository)"), refused.getMessage());
        }
    }

    @Test
    void doesNotCreateARepositoryNamedByClass() throws Exception {
        try (var context = new DefaultCamelContext()) {
            RouteDefinition route = aggregating("by-class", "#class:" + UncreatableRepository.class.getName());
            context.addRouteDefinition(route);

            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "by-class", route));
        }
    }

    @Test
    void followsAToDWhoseTargetIsFixedAndOnlyItsOptionsAreDynamic() throws Exception {
        try (var context = new DefaultCamelContext()) {
            RouteDefinition input = kafka("orders");
            input.toD("direct:batch?timeout=${header.timeout}");
            RouteDefinition batch = new RouteDefinition().from("direct:batch").routeId("batch");
            batch.aggregate(Builder.constant("all"), STRATEGY).completionSize(10).to("log:batch");
            context.addRouteDefinitions(List.of(input, batch));

            assertThrows(IllegalArgumentException.class, () -> factory().createRoutePolicy(context, "orders", input));
        }
    }

    private static RouteDefinition aggregating(String routeId, String repository) {
        RouteDefinition route = kafka(routeId);
        route.aggregate(Builder.constant("all"), STRATEGY).aggregationRepository(repository).completionSize(10)
                .to("log:batch");
        return route;
    }

    @Test
    void stopsAtACycleOfDirectRoutes() throws Exception {
        try (var context = new DefaultCamelContext()) {
            RouteDefinition input = kafka("orders");
            input.to("direct:a");
            RouteDefinition a = new RouteDefinition().from("direct:a").routeId("a");
            a.to("direct:a");
            context.addRouteDefinitions(List.of(input, a));

            assertInstanceOf(MessageStoreRoutePolicy.class, factory().createRoutePolicy(context, "orders", input));
        }
    }

    private static IllegalArgumentException refuse(String routeId, RouteDefinition route) throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.addRouteDefinition(route);
            return assertThrows(IllegalArgumentException.class, () -> factory().createRoutePolicy(context, routeId, route));
        }
    }

    private static RouteDefinition kafka(String routeId) {
        return new RouteDefinition().from("kafka:bip." + routeId + ".in?autoOffsetReset=earliest").routeId(routeId);
    }

    private static MessageStoreRoutePolicyFactory factory() {
        return new MessageStoreRoutePolicyFactory(row -> { }, "order-pipeline", "postgres-inline", 100_000, 30, true,
                "dlq", "auto");
    }

    /** A persistent repository that cannot be created; the guard must only inspect its class. */
    public static final class UncreatableRepository extends PersistentRepositoryBase {
        public UncreatableRepository() {
            throw new IllegalStateException("the guard must not create the repository");
        }
    }

    /** Base for test repositories. */
    public abstract static class PersistentRepositoryBase implements AggregationRepository {
        @Override public Exchange add(org.apache.camel.CamelContext camelContext, String key, Exchange exchange) { return null; }
        @Override public Exchange get(org.apache.camel.CamelContext camelContext, String key) { return null; }
        @Override public void remove(org.apache.camel.CamelContext camelContext, String key, Exchange exchange) { }
        @Override public void confirm(org.apache.camel.CamelContext camelContext, String exchangeId) { }
        @Override public Set<String> getKeys() { return Set.of(); }
    }

    /** Stands for a persistent repository, such as a database-backed one. */
    public static final class PersistentRepository implements AggregationRepository {
        @Override public Exchange add(org.apache.camel.CamelContext camelContext, String key, Exchange exchange) { return null; }
        @Override public Exchange get(org.apache.camel.CamelContext camelContext, String key) { return null; }
        @Override public void remove(org.apache.camel.CamelContext camelContext, String key, Exchange exchange) { }
        @Override public void confirm(org.apache.camel.CamelContext camelContext, String exchangeId) { }
        @Override public Set<String> getKeys() { return Set.of(); }
    }
}
