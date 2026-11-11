package no.basis.integrationplatform.camel.lifecycle;

import com.sun.net.httpserver.HttpServer;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.DefaultStepClassifier;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A send that fails against a receiver URL with credentials in its query: the credentials reach
 * neither the Message Store's error texts, the parked record's x-bip-error nor any log line.
 */
class CredentialsInErrorTextsTest {
    private static final List<String> SECRETS = List.of("fn-secret-1", "sas-secret-2", "key-secret-3");

    @Test
    void aFailedSendsReceiverUrlKeepsItsCredentialsOutOfStoredParkedAndLoggedTexts() throws Exception {
        List<MessageAttemptResult> rows = Collections.synchronizedList(new ArrayList<>());
        List<StepResult> failedSteps = Collections.synchronizedList(new ArrayList<>());
        List<Exchange> parked = Collections.synchronizedList(new ArrayList<>());
        List<LogRecord> logs = Collections.synchronizedList(new ArrayList<>());
        Handler capture = new Handler() {
            @Override public void publish(LogRecord record) { logs.add(record); }
            @Override public void flush() { }
            @Override public void close() { }
        };
        capture.setLevel(Level.ALL);
        Logger root = Logger.getLogger("");
        root.addHandler(capture);
        HttpServer receiver = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        receiver.createContext("/", x -> {
            x.getRequestBody().readAllBytes();
            x.sendResponseHeaders(500, -1);
            x.close();
        });
        receiver.start();
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return false; }
            public void recordDelivered(StepResult step) { }
            public void recordFailed(StepResult step) { failedSteps.add(step); }
        };
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(
                    new DefaultStepTracker(store, new DefaultStepClassifier(Optional.empty(), Optional.empty(),
                            Optional.of("post-hook"), Optional.empty()))));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            String url = "http://127.0.0.1:" + receiver.getAddress().getPort()
                    + "/hook?code=fn-secret-1&sig=sas-secret-2&x-functions-key=key-secret-3&page=2";
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                            .process(e -> parked.add(e.copy()));
                    from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .routePolicy(new MessageStoreRoutePolicy(rows::add, "order-pipeline"))
                            .setHeader(Exchange.HTTP_METHOD, constant("POST"))
                            .to(url).id("post-hook");
                }
            });
            context.start();
            context.createProducerTemplate().send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> { });
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                e.getMessage().setBody("order");
            });
        } finally {
            receiver.stop(0);
            root.removeHandler(capture);
        }

        assertEquals(1, parked.size());
        String error = parked.getFirst().getMessage().getHeader(PlatformHeaders.ERROR, String.class);
        // Camel's exception names the URI with its query sorted.
        for (String masked : List.of("code=xxxxxx", "sig=xxxxxx", "x-functions-key=xxxxxx", "page=2")) {
            assertTrue(error.contains(masked), error);
        }
        assertEquals(1, rows.size());
        List<String> texts = new ArrayList<>();
        texts.add(error);
        texts.add(rows.getFirst().error());
        rows.getFirst().steps().forEach(step -> { texts.add(step.error()); texts.add(step.endpointUri()); });
        failedSteps.forEach(step -> { texts.add(step.error()); texts.add(step.endpointUri()); });
        // The adapter's own lines at any level, and every line at WARN or above, as a production log
        // keeps them. Camel's own DEBUG lines, such as its route dump, are not the adapter's.
        for (LogRecord record : logs) {
            boolean adapter = record.getLoggerName() != null && record.getLoggerName().startsWith("no.basis.");
            if (!adapter && record.getLevel().intValue() < Level.WARNING.intValue()) {
                continue;
            }
            texts.add(record.getMessage());
            if (record.getParameters() != null) {
                for (Object parameter : record.getParameters()) {
                    texts.add(String.valueOf(parameter));
                }
            }
            for (Throwable thrown = record.getThrown(); thrown != null; thrown = thrown.getCause()) {
                texts.add(thrown.toString());
                if (thrown.getCause() == thrown) {
                    break;
                }
            }
        }
        for (String text : texts) {
            for (String secret : SECRETS) {
                assertFalse(String.valueOf(text).contains(secret), "A credential in: " + text);
            }
        }
    }
}
