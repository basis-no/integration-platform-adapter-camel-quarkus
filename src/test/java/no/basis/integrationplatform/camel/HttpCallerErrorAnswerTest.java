package no.basis.integrationplatform.camel;

import com.sun.net.httpserver.HttpServer;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicyFactory;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.DefaultStepClassifier;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.platform.http.vertx.VertxPlatformHttpServer;
import org.apache.camel.component.platform.http.vertx.VertxPlatformHttpServerConfiguration;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An HTTP caller whose request fails after a send to a receiver URI with credentials gets an
 * error status without the failure: no stack trace, exception text or receiver URI in the body or
 * the headers. Camel's platform-http consumer copies the exchange's headers into its answer, so
 * the parking headers must stay off an input that is not parked. Operators keep the details,
 * masked, in the Message Store and the log. An answer the application makes itself still wins.
 * The dead-letter route hands such an input its failure back before anything sets the parking
 * headers; released versions up to 0.3.6 and 0.4.0-rc1 left them on the answer.
 */
class HttpCallerErrorAnswerTest {
    private static final String PASSWORD = "pw-secret-1";
    private static final String KEY = "key-secret-2";
    private static final List<String> SECRETS = List.of(PASSWORD, KEY);

    private final List<MessageAttemptResult> rows = Collections.synchronizedList(new ArrayList<>());
    private final List<StepResult> failedSteps = Collections.synchronizedList(new ArrayList<>());
    private HttpServer receiver;

    @BeforeEach
    void startReceiver() throws Exception {
        receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiver.createContext("/", x -> {
            x.getRequestBody().readAllBytes();
            x.sendResponseHeaders(500, -1);
            x.close();
        });
        receiver.start();
    }

    @AfterEach
    void stopReceiver() {
        receiver.stop(0);
    }

    // vertx-http names the receiver URI in its failure with the query in clear; camel-http
    // refuses user info, so it gets the keyed query only.
    @ParameterizedTest(name = "{0}, selected as a boundary: {1}")
    @CsvSource({"vertx-http:http://partner:" + PASSWORD + "@, true", "vertx-http:http://partner:" + PASSWORD + "@, false",
            "http://, true", "http://, false"})
    void aFailedSendAnswersTheCallerWithoutTheFailureOrTheReceiverUri(String receiverPrefix, boolean inspected)
            throws Exception {
        String receiverUri = receiverPrefix + "127.0.0.1:" + receiver.getAddress().getPort() + "/hook?key=" + KEY + "&page=2";
        HttpResponse<String> response = call(inspected ? "auto,receive-order" : "auto", "/orders", new RouteBuilder() {
            @Override
            public void configure() {
                from("platform-http:/orders?httpMethodRestrict=POST").routeId("receive-order")
                        .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .removeHeaders("CamelHttp*")
                        .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                        .to(receiverUri).id("post-hook");
            }
        });

        assertEquals(500, response.statusCode());
        assertNoFailureDetails(response);
        // The send was tracked and failed against the receiver.
        assertEquals(1, failedSteps.size());
        assertNoSecret(failedSteps.getFirst().error());
        if (inspected) {
            // The operator keeps the failure, masked, in the Message Store.
            String error = rows.getLast().error();
            assertTrue(error.contains("statusCode: 500"), error);
            assertTrue(error.contains("key=xxxxxx"), error);
            assertNoSecret(error);
        } else {
            assertTrue(rows.isEmpty(), "Only a selected boundary is recorded");
        }
    }

    @Test
    void aRestDslInputAnswersWithoutTheFailure() throws Exception {
        String receiverUri = "vertx-http:http://partner:" + PASSWORD + "@127.0.0.1:" + receiver.getAddress().getPort()
                + "/hook?key=" + KEY + "&page=2";
        HttpResponse<String> response = call("auto", "/api/orders", new RouteBuilder() {
            @Override
            public void configure() {
                restConfiguration().component("platform-http");
                rest("/api").post("/orders").to("direct:send-order");
                from("direct:send-order").routeId("send-order")
                        .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .removeHeaders("CamelHttp*")
                        .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                        .to(receiverUri).id("post-hook");
            }
        });

        assertEquals(500, response.statusCode());
        assertNoFailureDetails(response);
    }

    @Test
    void anAnswerTheApplicationMakesItselfStillWins() throws Exception {
        String receiverUri = "http://127.0.0.1:" + receiver.getAddress().getPort() + "/hook?key=" + KEY;
        HttpResponse<String> response = call("auto,receive-order", "/orders", new RouteBuilder() {
            @Override
            public void configure() {
                onException(HttpOperationFailedException.class).handled(true)
                        .setHeader(Exchange.HTTP_RESPONSE_CODE, constant(502))
                        .setBody(constant("The partner did not accept the order"));
                from("platform-http:/orders?httpMethodRestrict=POST").routeId("receive-order")
                        .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                        .removeHeaders("CamelHttp*")
                        .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                        .to(receiverUri).id("post-hook");
            }
        });

        assertEquals(502, response.statusCode());
        assertEquals("The partner did not accept the order", response.body());
        assertFalse(response.headers().map().containsKey(PlatformHeaders.ERROR), response.headers().map().toString());
    }

    private HttpResponse<String> call(String boundaryRoutes, String path, RouteBuilder routes) throws Exception {
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return false; }
            public void recordDelivered(StepResult step) { }
            public void recordFailed(StepResult step) { failedSteps.add(step); }
        };
        try (var context = new DefaultCamelContext()) {
            var configuration = new VertxPlatformHttpServerConfiguration();
            configuration.setBindHost("127.0.0.1");
            configuration.setBindPort(0);
            var server = new VertxPlatformHttpServer(configuration);
            context.addService(server);
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.getCamelContextExtension().addInterceptStrategy(
                    new StepTrackingInterceptStrategy(new DefaultStepTracker(store, new DefaultStepClassifier(
                            Optional.empty(), Optional.empty(), Optional.of("post-hook"), Optional.empty()))));
            context.addRoutePolicyFactory(new MessageStoreRoutePolicyFactory(
                    rows::add, "order-pipeline", "postgres-inline", 1_048_576, 30, true, "dlq", boundaryRoutes));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler()).log("parked");
                }
            });
            context.addRoutes(routes);
            context.start();
            int port = server.getServer().actualPort();
            try (var client = HttpClient.newHttpClient()) {
                return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + path))
                                .header(PlatformHeaders.MESSAGE_ID, "order-1")
                                .POST(HttpRequest.BodyPublishers.ofString("<Order/>"))
                                .build(),
                        HttpResponse.BodyHandlers.ofString());
            }
        }
    }

    private static void assertNoFailureDetails(HttpResponse<String> response) {
        String answer = response.body() + "\n" + response.headers().map();
        assertNoSecret(answer);
        for (String detail : List.of("Exception", "\tat ", "operation failed", "127.0.0.1", "/hook")) {
            assertFalse(answer.contains(detail), "The caller got failure details: " + answer);
        }
        Map<String, List<String>> headers = response.headers().map();
        assertFalse(headers.containsKey(PlatformHeaders.ERROR), headers.toString());
        assertFalse(headers.containsKey(PlatformHeaders.DLQ_AT), headers.toString());
        assertEquals("", response.body());
    }

    private static void assertNoSecret(String text) {
        for (String secret : SECRETS) {
            assertFalse(String.valueOf(text).contains(secret), "A credential in: " + text);
        }
    }
}
