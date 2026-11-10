package no.basis.integrationplatform.camel.step;

import com.sun.net.httpserver.HttpServer;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Producer;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.rest.RestEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.HeaderFilterStrategy;
import org.apache.camel.spi.HeaderFilterStrategyAware;
import org.apache.camel.spi.PropertyConfigurerGetter;
import org.apache.camel.spi.RestConfiguration;
import org.apache.camel.spi.RestProducerFactory;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.support.DefaultHeaderFilterStrategy;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A {@code rest:} send keeps a consumed record's credential headers and Camel's Kafka consumer
 * headers out of its request through each covered producer component and each EIP, while its
 * path and query still bind from the exchange's headers. The last tests pin the Camel internals
 * this relies on, so that a Camel upgrade that changes them fails here instead of sending the
 * headers.
 */
class RestSendRecordHeadersTest {
    private static final byte[] CALLER_AUTHORIZATION = "Bearer synthetic-caller".getBytes(StandardCharsets.UTF_8);
    private static final String ROUTE_API_KEY = "synthetic-key";

    private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
    private HttpServer server;
    private DefaultCamelContext context;
    private String host;

    private record Request(String uri, Set<String> headerNames, String authorization, String routeToken) {
    }

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", x -> {
            x.getRequestBody().readAllBytes();
            var names = new TreeSet<String>();
            x.getRequestHeaders().keySet().forEach(name -> names.add(name.toLowerCase(Locale.ROOT)));
            requests.add(new Request(x.getRequestURI().toString(), names, x.getRequestHeaders().getFirst("Authorization"),
                    x.getRequestHeaders().getFirst("X-Route-Token")));
            x.sendResponseHeaders(200, -1);
            x.close();
        });
        server.start();
        host = "127.0.0.1:" + server.getAddress().getPort();
        context = new DefaultCamelContext();
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new NoStepTracker()));
    }

    @AfterEach
    void stop() throws Exception {
        context.close();
        server.stop(0);
    }

    /**
     * The request URIs the same routes send without the adapter: the adapter must not change
     * how the rest: producer binds its path and query, whatever Camel does with them.
     */
    private List<Request> withoutTheAdapter(Supplier<RouteBuilder> routes, Map<String, Object> extra) throws Exception {
        try (var plain = new DefaultCamelContext()) {
            plain.addRoutes(routes.get());
            plain.start();
            send(plain, extra);
        }
        List<Request> sent = List.copyOf(requests);
        requests.clear();
        return sent;
    }

    /**
     * The adapter leaves out the record's headers, and only them: the requests go where they
     * went without it, with the same other headers.
     */
    private void assertOnlyTheRecordHeadersLeftOut(List<Request> before) {
        assertEquals(before.stream().map(Request::uri).toList(), requests.stream().map(Request::uri).toList(),
                "The path and query bind as without the adapter");
        // (vertx-http sends the second of two rest: sends through one endpoint without binding
        // or headers, with or without the adapter.)
        assertTrue(before.stream().anyMatch(r -> r.headerNames().contains("authorization")),
                "Without the adapter the record's credential is sent: " + before);
        for (int i = 0; i < before.size(); i++) {
            var expected = new TreeSet<>(before.get(i).headerNames());
            expected.removeIf(name -> name.equals("authorization") || name.startsWith("kafka.") || name.startsWith("camelkafka"));
            assertEquals(expected, requests.get(i).headerNames(), requests.get(i).uri());
        }
    }

    @ParameterizedTest
    @CsvSource({"http,to", "http,toD", "http,enrich", "http,template",
            "vertx-http,to", "vertx-http,toD", "vertx-http,enrich", "vertx-http,template",
            "netty-http,to", "netty-http,toD", "netty-http,enrich", "netty-http,template"})
    void aRestSendLeavesOutTheRecordHeadersAndStillBindsItsPathAndQuery(String component, String eip) throws Exception {
        String rest = "rest:post:orders/{id}?queryParameters=key={apiKey}";
        Supplier<RouteBuilder> routes = () -> new RouteBuilder() {
            public void configure() {
                restConfiguration().producerComponent(component).host(host);
                var route = from("direct:record").routeId("record");
                switch (eip) {
                    case "to" -> route.to(rest).id("post-rest");
                    case "toD" -> route.toD(rest).id("post-rest");
                    case "enrich" -> route.enrich(rest);
                    default -> route.process(e -> e.getContext().createProducerTemplate().send(rest, e));
                }
            }
        };
        Map<String, Object> extra = Map.of("Authorization", CALLER_AUTHORIZATION);
        List<Request> before = withoutTheAdapter(routes, extra);
        context.addRoutes(routes.get());
        context.start();
        send(context, extra);

        assertEquals(1, requests.size(), requests.toString());
        Request request = requests.getFirst();
        assertEquals("/orders/o-1?key=synthetic-key", request.uri(), "The path and query bind from the headers");
        assertOnlyTheRecordHeadersLeftOut(before);
        assertFalse(request.headerNames().contains("id"), "The component's rest filter still applies");
        assertEquals("route-own", request.routeToken(), "Other headers are sent as before");
    }

    @ParameterizedTest
    @ValueSource(strings = {"http", "vertx-http", "netty-http"})
    void aCredentialTheRouteSetsItselfIsSent(String component) throws Exception {
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                restConfiguration().producerComponent(component).host(host);
                from("direct:record").routeId("record").to("rest:post:orders/{id}").id("post-rest");
            }
        });
        context.start();
        send(context, Map.of("Authorization", "Bearer route-own"));

        assertEquals("Bearer route-own", requests.getFirst().authorization());
    }

    @ParameterizedTest
    @ValueSource(strings = {"http", "vertx-http", "netty-http"})
    void anApplicationsOwnFilterIsKeptAndComposed(String component) throws Exception {
        var own = new DefaultHeaderFilterStrategy();
        own.setOutFilter(Set.of("X-Route-Token"));
        // The rest component hands its filter to every rest: producer as the producer parameter.
        context.getComponent("rest", org.apache.camel.component.rest.RestComponent.class).setHeaderFilterStrategy(own);
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                restConfiguration().producerComponent(component).host(host);
                from("direct:record").routeId("record").to("rest:post:orders/{id}").id("post-rest");
            }
        });
        context.start();
        send(context, Map.of("Authorization", CALLER_AUTHORIZATION));

        Request request = requests.getFirst();
        // Camel also appends the component's filter to the query; that is unchanged.
        assertTrue(request.uri().startsWith("/orders/o-1"), request.uri());
        assertFalse(request.headerNames().contains("x-route-token"), "The application's filter applies");
        assertFalse(request.headerNames().contains("authorization"), "The record's credential is not sent");
        assertTrue(request.headerNames().stream().noneMatch(name -> name.startsWith("kafka.")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"http", "vertx-http", "netty-http"})
    void twoRestSendsThroughOneHttpEndpointAreBothFiltered(String component) throws Exception {
        // Same method, host and path: the factory of the second reuses the first's HTTP endpoint.
        Supplier<RouteBuilder> routes = () -> new RouteBuilder() {
            public void configure() {
                restConfiguration().producerComponent(component).host(host);
                from("direct:record").routeId("record")
                        .to("rest:post:orders/{id}?queryParameters=key={apiKey}").id("post-key")
                        .to("rest:post:orders/{id}?queryParameters=token={apiKey}").id("post-token");
            }
        };
        Map<String, Object> extra = Map.of("Authorization", CALLER_AUTHORIZATION);
        List<Request> before = withoutTheAdapter(routes, extra);
        context.addRoutes(routes.get());
        context.start();
        send(context, extra);

        assertEquals(2, requests.size());
        assertOnlyTheRecordHeadersLeftOut(before);
    }

    @Test
    void aRestSendThroughAnotherComponentDoesNotStart() throws Exception {
        context.addComponent("other-http", new OtherHttpComponent());
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                restConfiguration().producerComponent("other-http").host(host);
                from("direct:record").routeId("record").to("rest:post:orders/{id}").id("post-rest");
            }
        });

        Exception failure = assertThrows(Exception.class, context::start);
        assertTrue(causes(failure).contains("is not covered"), causes(failure));
        assertTrue(requests.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(strings = {"platform-http", "netty-http"})
    void aRestDslServiceIsLeftAsItIs(String component) throws Exception {
        Consumed without = restService(new DefaultCamelContext(), component);
        Consumed with = restService(context, component);

        assertEquals(without.filterClass(), with.filterClass(), "The consumer keeps the component's own filter");
        assertFalse(with.filterClass().contains("no.basis"), with.filterClass());
        assertEquals(without.status(), with.status());
        assertEquals(without.headerNames(), with.headerNames(), "The response carries the same headers");
        assertEquals(without.body(), with.body());
    }

    private record Consumed(String filterClass, int status, Set<String> headerNames, String body) {
    }

    /**
     * A REST DSL service through the component, and a rest: send to it through http in the same
     * context; returns what an HTTP client gets from the service directly.
     */
    private Consumed restService(DefaultCamelContext target, String component) throws Exception {
        int port;
        try (var socket = new java.net.ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        if (component.equals("platform-http")) {
            var configuration = new org.apache.camel.component.platform.http.vertx.VertxPlatformHttpServerConfiguration();
            configuration.setBindPort(port);
            target.addService(new org.apache.camel.component.platform.http.vertx.VertxPlatformHttpServer(configuration));
        }
        int servicePort = port;
        try (target) {
            target.addRoutes(new RouteBuilder() {
                public void configure() {
                    restConfiguration().component(component).port(servicePort)
                            .producerComponent("http").host("127.0.0.1");
                    rest("/svc").post("/orders/{id}").to("direct:svc");
                    from("direct:svc").routeId("svc").setHeader("X-Reply", constant("r")).setBody(constant("ok"));
                    from("direct:record").routeId("record").to("rest:post:svc/orders/{id}").id("post-rest");
                }
            });
            target.start();
            send(target, Map.of("Authorization", CALLER_AUTHORIZATION));

            var client = java.net.http.HttpClient.newHttpClient();
            var response = client.send(java.net.http.HttpRequest.newBuilder(
                            java.net.URI.create("http://127.0.0.1:" + servicePort + "/svc/orders/o-1"))
                    .header("X-Route-Token", "route-own").POST(java.net.http.HttpRequest.BodyPublishers.ofString("{}")).build(),
                    java.net.http.HttpResponse.BodyHandlers.ofString());
            var names = new TreeSet<String>();
            response.headers().map().keySet().forEach(name -> names.add(name.toLowerCase(Locale.ROOT)));
            names.remove("date");
            String filterClass = target.getEndpoints().stream()
                    .filter(e -> e.getEndpointUri().startsWith(component + ":"))
                    .map(RestSendRecordHeadersTest::filterClassOf).findFirst().orElse("no consumer endpoint");
            return new Consumed(filterClass, response.statusCode(), names, response.body());
        }
    }

    private static String filterClassOf(Endpoint endpoint) {
        Object filter = null;
        if (endpoint.getComponent().getEndpointPropertyConfigurer() instanceof PropertyConfigurerGetter getter) {
            filter = getter.getOptionValue(endpoint, "headerFilterStrategy", true);
        }
        if (filter == null && endpoint instanceof HeaderFilterStrategyAware aware) {
            filter = aware.getHeaderFilterStrategy();
        }
        return filter == null ? "none" : filter.getClass().getName();
    }

    // The Camel internals this relies on, per component, without the adapter installed.

    @ParameterizedTest
    @CsvSource({"http,org.apache.camel.http.common.HttpRestHeaderFilterStrategy",
            "vertx-http,org.apache.camel.component.vertx.http.VertxHttpRestHeaderFilterStrategy",
            "netty-http,org.apache.camel.component.netty.http.NettyHttpRestHeaderFilterStrategy"})
    void camelsRestProducerInstallsTheRestFilterTheAdapterBuilds(String component, String filterClass) throws Exception {
        try (var plain = new DefaultCamelContext()) {
            plain.getRestConfiguration().setProducerComponent(component);
            plain.getRestConfiguration().setHost(host);
            plain.start();
            String uri = "rest:post:orders/{id}?queryParameters=key={apiKey}";
            RestEndpoint rest = plain.getEndpoint(uri, RestEndpoint.class);
            Endpoint http = producerEndpoint(plain, uri);

            HeaderFilterStrategy filter = filterOf(http);
            assertEquals(filterClass, filter.getClass().getName(),
                    "The filter the adapter builds by name must be the one the component installs");
            // Built as the adapter builds it, it filters as the one the component installed.
            var built = (HeaderFilterStrategy) filter.getClass().getConstructor(String.class, String.class)
                    .newInstance(rest.getUriTemplate() != null ? rest.getUriTemplate() : rest.getPath(), rest.getQueryParameters());
            for (String name : List.of("id", "apiKey", "Content-Type", "X-Route-Token", "CamelHttpUri", "Host")) {
                assertEquals(filter.applyFilterToCamelHeaders(name, "v", null), built.applyFilterToCamelHeaders(name, "v", null), name);
                assertEquals(filter.applyFilterToExternalHeaders(name, "v", null), built.applyFilterToExternalHeaders(name, "v", null), name);
            }
            assertTrue(filter.applyFilterToCamelHeaders("id", "o-1", null), "It leaves out a path placeholder");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"http", "vertx-http", "netty-http"})
    void camelsRestProducerUsesTheHeaderFilterStrategyParameter(String component) throws Exception {
        try (var plain = new DefaultCamelContext()) {
            var marker = new DefaultHeaderFilterStrategy();
            plain.getRegistry().bind("marker", marker);
            plain.getRestConfiguration().setProducerComponent(component);
            plain.getRestConfiguration().setHost(host);
            plain.start();
            RestEndpoint rest = plain.getEndpoint("rest:post:orders/{id}", RestEndpoint.class);
            rest.getParameters().put("headerFilterStrategy", marker);
            try (Producer producer = rest.createProducer()) {
                producer.start();
                assertSame(marker, filterOf(httpEndpoint(plain)),
                        "The component puts the producer parameter on the endpoint it sends through");
            }
        }
    }

    @Test
    void camelsRestEndpointHasTheGettersTheAdapterCalls() throws Exception {
        try (var plain = new DefaultCamelContext()) {
            plain.getRestConfiguration().setHost(host);
            plain.start();
            RestEndpoint rest = plain.getEndpoint("rest:post:orders:{id}?queryParameters=key={apiKey}", RestEndpoint.class);
            assertInstanceOf(Map.class, RestEndpoint.class.getMethod("getParameters").invoke(rest));
            assertEquals("orders", RestEndpoint.class.getMethod("getPath").invoke(rest));
            assertEquals("{id}", RestEndpoint.class.getMethod("getUriTemplate").invoke(rest));
            assertEquals("key={apiKey}", RestEndpoint.class.getMethod("getQueryParameters").invoke(rest));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"http", "vertx-http", "netty-http"})
    void eachComponentsConfigurerReadsAndWritesTheHeaderFilter(String component) throws Exception {
        try (var plain = new DefaultCamelContext()) {
            plain.start();
            Endpoint http = plain.getEndpoint((component.equals("http") ? "" : component + ":") + "http://" + host + "/orders");
            var getter = assertInstanceOf(PropertyConfigurerGetter.class, http.getComponent().getEndpointPropertyConfigurer());
            assertEquals(HeaderFilterStrategy.class, getter.getOptionType("headerFilterStrategy", true));
            var replacement = new DefaultHeaderFilterStrategy();
            assertTrue(http.getComponent().getEndpointPropertyConfigurer()
                    .configure(plain, http, "headerFilterStrategy", replacement, true));
            assertSame(replacement, getter.getOptionValue(http, "headerFilterStrategy", true));
        }
    }

    private static Endpoint producerEndpoint(CamelContext plain, String uri) throws Exception {
        try (Producer producer = plain.getEndpoint(uri).createProducer()) {
            producer.start();
            return httpEndpoint(plain);
        }
    }

    private static Endpoint httpEndpoint(CamelContext plain) {
        return plain.getEndpoints().stream().filter(e -> !e.getEndpointUri().startsWith("rest:")).findFirst().orElseThrow();
    }

    private static HeaderFilterStrategy filterOf(Endpoint endpoint) {
        var getter = (PropertyConfigurerGetter) endpoint.getComponent().getEndpointPropertyConfigurer();
        return (HeaderFilterStrategy) getter.getOptionValue(endpoint, "headerFilterStrategy", true);
    }

    private static String causes(Throwable failure) {
        var text = new StringBuilder();
        for (Throwable t = failure; t != null; t = t.getCause()) {
            text.append(t.getMessage()).append('\n');
        }
        return text.toString();
    }

    private static void send(CamelContext target, Map<String, Object> extra) {
        var recordHeaders = new RecordHeaders();
        recordHeaders.add("Authorization", CALLER_AUTHORIZATION);
        try (var producer = target.createProducerTemplate()) {
            Exchange result = producer.send("direct:record", e -> {
                // As Camel 4.18.0's Kafka consumer sets them; later versions name them CamelKafka*.
                e.getMessage().setHeader("kafka.HEADERS", recordHeaders);
                e.getMessage().setHeader("CamelKafkaHeaders", recordHeaders);
                e.getMessage().setHeader("kafka.TOPIC", "orders-in");
                // A credential-like header the route binds into the query; text, as a route sets it.
                // (The rest: producer binds a byte array as Java's array text, with or without the adapter.)
                e.getMessage().setHeader("apiKey", ROUTE_API_KEY);
                e.getMessage().setHeader("id", "o-1");
                e.getMessage().setHeader("X-Route-Token", "route-own");
                e.getMessage().getHeaders().putAll(extra);
                e.getMessage().setBody("{}");
            });
            if (result.getException() != null) {
                throw new AssertionError(result.getException());
            }
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /** A rest producer component the adapter does not know, which honours the producer parameter. */
    private static final class OtherHttpComponent extends DefaultComponent implements RestProducerFactory {
        @Override
        protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
            var endpoint = new OtherHttpEndpoint(uri, this);
            if (parameters.remove("headerFilterStrategy") instanceof HeaderFilterStrategy filter) {
                endpoint.setHeaderFilterStrategy(filter);
            }
            parameters.clear();
            return endpoint;
        }

        @Override
        public Producer createProducer(CamelContext camelContext, String host, String verb, String basePath,
                String uriTemplate, String queryParameters, String consumes, String produces,
                RestConfiguration configuration, Map<String, Object> parameters) throws Exception {
            return camelContext.getEndpoint("other-http:" + basePath, parameters).createProducer();
        }
    }

    private static final class OtherHttpEndpoint extends DefaultEndpoint implements HeaderFilterStrategyAware {
        private HeaderFilterStrategy filter;

        OtherHttpEndpoint(String uri, DefaultComponent component) {
            super(uri, component);
        }

        @Override
        public HeaderFilterStrategy getHeaderFilterStrategy() {
            return filter;
        }

        @Override
        public void setHeaderFilterStrategy(HeaderFilterStrategy filter) {
            this.filter = filter;
        }

        @Override
        public Producer createProducer() {
            throw new UnsupportedOperationException("Never reached: the adapter refuses the endpoint");
        }

        @Override
        public org.apache.camel.Consumer createConsumer(org.apache.camel.Processor processor) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class NoStepTracker implements StepTracker {
        public void beforeSend(Exchange e) {}
        public void recordDelivered(Exchange e) {}
        public void recordSkipped(Exchange e) {}
        public void recordFailed(Exchange e) {}
        public void recordFailedAndRethrow(Exchange e) {}
    }
}
