package no.basis.integrationplatform.camel;

import com.sun.net.httpserver.HttpServer;
import no.basis.integrationplatform.camel.step.StepTracker;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.http.HttpEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Camel 4's http component accepts some timeout options without applying them, and a call
 * then waits up to three minutes. The adapter warns about each one when the route is created.
 */
class HttpTimeoutOptionsTest {
    private static final String BASE = "http://127.0.0.1:1/orders";

    @Test
    void warnsOnceForEachIgnoredOptionAndNamesItsReplacement() throws Exception {
        try (var logs = LogCapture.of(HttpTimeoutOptions.class);
             var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("receiver.base", BASE);
            context.getPropertiesComponent().setInitialProperties(properties);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new NoStepTracker()));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:static").routeId("static")
                            .to(BASE + "?socketTimeout=1000").id("to-socket")
                            .to(BASE + "?httpConnection.socketTimeout=1000&httpConnection.connectTimeout=1000"
                                    + "&httpConnection.soTimeout=1000").id("to-connection")
                            .to(BASE + "?responseTimeout=1000&connectTimeout=1000&soTimeout=1000"
                                    + "&connectionRequestTimeout=1000&httpClient.responseTimeout=1000").id("to-valid")
                            .to("https://127.0.0.1:1/orders?socketTimeout=1000").id("to-https");
                    from("direct:dynamic").routeId("dynamic")
                            .toD(BASE + "/${header.id}?responseTimeout=1000&connectTimeout=1000"
                                    + "&connectionRequestTimeout=1000&soTimeout=1000&socketTimeout=1000"
                                    + "&httpClient.responseTimeout=1000&httpConnection.soTimeout=1000"
                                    + "&httpClient.socketTimeout=1000").id("toD-all")
                            .toD().uri(BASE + "/${header.id}?responseTimeout=1000").allowOptimisedComponents(false).id("toD-plain")
                            .toD("{{receiver.base}}/${header.id}?responseTimeout=1000").id("toD-placeholder")
                            .enrich(BASE + "?responseTimeout=1000").id("enrich-response")
                            .wireTap(BASE + "/${header.id}?soTimeout=1000").id("tap-dynamic")
                            .wireTap(BASE + "?soTimeout=1000").id("tap-static")
                            .toD("direct:elsewhere?socketTimeout=1000").id("not-http")
                            .toD("${header.target}?socketTimeout=1000").id("scheme-unknown");
                }
            });
            context.start();

            List<String> warnings = logs.at(Level.WARNING).stream().map(LogCapture::text).toList();
            assertEquals(java.util.stream.Stream.of(
                    "socketTimeout to-socket static responseTimeout camel.component.http.response-timeout",
                    "httpConnection.socketTimeout to-connection static responseTimeout camel.component.http.response-timeout",
                    "httpConnection.connectTimeout to-connection static connectTimeout camel.component.http.connect-timeout",
                    "socketTimeout to-https static responseTimeout camel.component.https.response-timeout",
                    "responseTimeout toD-all dynamic httpClient.responseTimeout camel.component.http.response-timeout",
                    "connectTimeout toD-all dynamic httpClient.connectTimeout camel.component.http.connect-timeout",
                    "connectionRequestTimeout toD-all dynamic httpClient.connectionRequestTimeout camel.component.http.connection-request-timeout",
                    "soTimeout toD-all dynamic httpConnection.soTimeout camel.component.http.so-timeout",
                    "socketTimeout toD-all dynamic httpClient.responseTimeout camel.component.http.response-timeout",
                    "responseTimeout toD-placeholder dynamic httpClient.responseTimeout camel.component.http.response-timeout",
                    "responseTimeout enrich-response dynamic httpClient.responseTimeout camel.component.http.response-timeout",
                    "soTimeout tap-dynamic dynamic httpConnection.soTimeout camel.component.http.so-timeout").sorted().toList(),
                    warnings.stream().map(HttpTimeoutOptionsTest::summary).sorted().toList());
            String first = warnings.stream().filter(w -> w.contains(" to-socket ")).findFirst().orElseThrow();
            assertEquals("HTTP option socketTimeout on to to-socket in route static has no effect: Camel 4's http "
                    + "component does not know it and sends it to the receiver as a query parameter. The call keeps "
                    + "the component's timeout, three minutes unless configured. Use responseTimeout on this endpoint, "
                    + "or camel.component.http.response-timeout for every call through the component", first);
            assertTrue(warnings.stream().noneMatch(w -> w.contains("1000") || w.contains("127.0.0.1")),
                    "Names only: no URI and no value");

            // Each occurrence is reported once, also when Camel creates the route again.
            context.getRouteController().stopRoute("dynamic");
            context.removeRoute("dynamic");
            int before = logs.at(Level.WARNING).size();
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:dynamic").routeId("dynamic")
                            .toD(BASE + "/${header.id}?soTimeout=1000").id("toD-all");
                }
            });
            assertEquals(before, logs.at(Level.WARNING).size());
        }
    }

    /** Option, step, static or dynamic, replacement and component setting of one warning. */
    private static String summary(String warning) {
        String option = between(warning, "HTTP option ", " on ");
        String step = between(warning, " on ", " in route ").split(" ")[1];
        String kind = warning.contains("toD, enrich or dynamic wireTap")
                || warning.contains("Use httpClient.") || warning.contains("Use httpConnection.") ? "dynamic" : "static";
        String replacement = between(warning, "Use ", " on this endpoint");
        String component = between(warning, ", or ", " for every call");
        return option + " " + step + " " + kind + " " + replacement + " " + component;
    }

    private static String between(String text, String start, String end) {
        int from = text.indexOf(start) + start.length();
        return text.substring(from, text.indexOf(end, from));
    }

    /**
     * Pins the Camel behaviour the warnings describe. The receiver answers after 1.5 s and the
     * option asks for 200 ms: an applied option fails the call, an ignored one does not.
     */
    @ParameterizedTest(name = "{0} {1}: applied {2}, sent to the receiver {3}")
    @CsvSource({
            "to,        socketTimeout=200,                  false, true",
            "to,        httpConnection.socketTimeout=200,   false, false",
            "to,        responseTimeout=200,                true,  false",
            "to,        httpConnection.soTimeout=200,       true,  false",
            "toD,       responseTimeout=200,                false, true",
            "toD,       soTimeout=200,                      false, true",
            "toD,       connectTimeout=200,                 false, true",
            "toD,       connectionRequestTimeout=200,       false, true",
            "toD,       httpClient.responseTimeout=200,     true,  false",
            "toD,       httpConnection.soTimeout=200,       true,  false",
            "toD-plain, responseTimeout=200,                true,  false",
            "enrich,    responseTimeout=200,                false, true",
            "enrich-plain, responseTimeout=200,             true,  false"})
    void camelAppliesOrIgnoresTheOptionAsTheWarningSays(String kind, String option, boolean applied, boolean sent)
            throws Exception {
        List<String> queries = new CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
        server.createContext("/", x -> {
            queries.add(String.valueOf(x.getRequestURI().getRawQuery()));
            try {
                Thread.sleep(1500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            x.sendResponseHeaders(200, -1);
            x.close();
        });
        server.start();
        try (var context = new DefaultCamelContext()) {
            String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/orders";
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    var route = from("direct:send").errorHandler(noErrorHandler());
                    switch (kind) {
                        case "to" -> route.to(base + "/1?" + option);
                        case "toD" -> route.toD(base + "/${header.id}?" + option);
                        case "toD-plain" -> route.toD().uri(base + "/${header.id}?" + option).allowOptimisedComponents(false);
                        case "enrich" -> route.enrich(base + "/1?" + option);
                        case "enrich-plain" -> route.enrich().constant(base + "/1?" + option).allowOptimisedComponents(false);
                        default -> throw new IllegalArgumentException(kind);
                    }
                }
            });
            context.start();
            Exchange result = context.createProducerTemplate().send("direct:send", e -> e.getMessage().setHeader("id", "1"));
            assertEquals(applied, result.getException() != null, "Timed out after 200 ms");
            String name = option.substring(0, option.indexOf('='));
            assertEquals(sent, queries.getFirst().contains(name), "Query " + queries);
        } finally {
            server.stop(0);
        }
    }

    @Test
    void httpConnectionBindsOnlySoTimeout() throws Exception {
        try (var context = new DefaultCamelContext()) {
            context.start();
            var endpoint = context.getEndpoint(BASE + "?httpConnection.soTimeout=200&httpConnection.socketTimeout=200"
                    + "&httpConnection.connectTimeout=200&httpConnection.connectionRequestTimeout=200"
                    + "&httpConnection.responseTimeout=200", HttpEndpoint.class);
            // Camel removes the options it applied; the rest stay unused.
            assertEquals(java.util.Set.of("socketTimeout", "connectTimeout", "connectionRequestTimeout", "responseTimeout"),
                    endpoint.getHttpConnectionOptions().keySet());
            assertNull(endpoint.getHttpUri().getQuery(), "Not sent to the receiver either");
        }
    }

    @Test
    void anUnusualDefinitionNeverFailsRouteCreation() throws Exception {
        try (var logs = LogCapture.of(HttpTimeoutOptions.class);
             var context = new DefaultCamelContext()) {
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new NoStepTracker()));
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:odd").routeId("odd")
                            .toD("http:?").id("empty-query")
                            .toD("http://host/${header.a}?=&&socketTimeout&responseTimeout=${header.t}").id("odd-query")
                            .toD("{{missing.base:http://host}}/x?socketTimeout=1").id("default-placeholder");
                }
            });
            context.start();
            List<String> steps = logs.at(Level.WARNING).stream()
                    .map(w -> between(LogCapture.text(w), " on ", " in route ")).toList();
            assertEquals(List.of("toD odd-query", "toD odd-query", "toD default-placeholder"), steps);
            assertFalse(logs.at(Level.WARNING).stream().anyMatch(w -> LogCapture.text(w).contains("header.t")));
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
