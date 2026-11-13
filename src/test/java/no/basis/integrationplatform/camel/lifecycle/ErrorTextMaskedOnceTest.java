package no.basis.integrationplatform.camel.lifecycle;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import no.basis.integrationplatform.camel.ErrorTextMasking;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.ReceiverComponent;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import no.basis.integrationplatform.camel.replay.MessageReplayAdminResource;
import no.basis.integrationplatform.camel.replay.MessageReplayException;
import no.basis.integrationplatform.camel.replay.MessageReplayRequest;
import no.basis.integrationplatform.camel.replay.MessageReplayResult;
import no.basis.integrationplatform.camel.replay.MessageReplayService;
import no.basis.integrationplatform.camel.step.DefaultStepClassifier;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import no.basis.integrationplatform.camel.store.MessageStatusEvent;
import no.basis.integrationplatform.camel.store.MessageStatusEventJson;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every sink of a failed send's error text gets the text masked once, as the .NET adapter
 * stores it: the step outcome ledger, the attempt's steps, the attempt and its status event,
 * the parked record's {@code x-bip-error}, the resend answer and the log lines. The text names a
 * URI that a second pass masks further, so a sink that masked it again would differ.
 */
class ErrorTextMaskedOnceTest {
    // A second pass reads "host:xxxxxx" as the user info of this URI.
    private static final String NOT_IDEMPOTENT = "http://host:8080/a?b=c@d";
    private static final String SECRET = "fn-secret-1";
    private static final String RAW = "Receiver refused the call to " + NOT_IDEMPOTENT
            + " after https://receiver.example/hook?code=" + SECRET + "&page=2";
    private static final String ONCE = ErrorTextMasking.mask(RAW);
    private static final String URI_ONCE = ErrorTextMasking.mask(NOT_IDEMPOTENT);
    private static final String URI_TWICE = ErrorTextMasking.mask(URI_ONCE);

    @Test
    void aSecondPassMasksTheExampleFurther() {
        assertEquals("http://host:xxxxxx@d", URI_ONCE);
        assertEquals("http://xxxxxx@d", URI_TWICE);
        assertNotEquals(ONCE, ErrorTextMasking.mask(ONCE));
        assertFalse(ONCE.contains(SECRET), ONCE);
    }

    @ParameterizedTest(name = "parking route rethrows: {0}")
    @ValueSource(booleans = {false, true})
    void everySinkOfAFailedSendGetsItsErrorTextMaskedOnce(boolean parkingRethrows) throws Exception {
        List<MessageAttemptResult> rows = Collections.synchronizedList(new ArrayList<>());
        List<StepResult> ledger = Collections.synchronizedList(new ArrayList<>());
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
        StepOutcomeStore store = new StepOutcomeStore() {
            public boolean wasDelivered(String key) { return false; }
            public void recordDelivered(StepResult step) { }
            public void recordFailed(StepResult step) { ledger.add(step); }
        };
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            context.addComponent("receiver", new ReceiverComponent(exchange -> {
                throw new IllegalStateException(RAW);
            }));
            context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(
                    new DefaultStepTracker(store, new DefaultStepClassifier(Optional.empty(), Optional.empty(),
                            Optional.of("post-hook"), Optional.empty()))));
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                            .process(e -> {
                                if (parkingRethrows) {
                                    // An application's own parking route that gives the failure back.
                                    throw e.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
                                }
                                parked.add(e.copy());
                            });
                    from("direct:input").routeId("input").routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                            .routePolicy(new MessageStoreRoutePolicy(rows::add, "order-pipeline"))
                            .to("receiver:hook").id("post-hook");
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
            root.removeHandler(capture);
        }

        // The step outcome ledger (bip_step_outcome.error).
        assertEquals(1, ledger.size());
        assertEquals(ONCE, ledger.getFirst().error());
        // The attempt and the summary (bip_message_attempt, bip_message_log), and its steps (steps_json).
        assertEquals(1, rows.size());
        MessageAttemptResult row = rows.getFirst();
        assertEquals(ONCE, row.error());
        StepResult step = row.steps().stream().filter(s -> s.status() == StepStatus.FAILED).findFirst().orElseThrow();
        assertEquals(ONCE, step.error());
        // The status event the Kafka sink publishes, as an object and as the JSON it sends.
        MessageStatusEvent event = MessageStatusEvent.from(row);
        assertEquals(ONCE, event.error());
        assertEquals(ONCE, event.steps().stream().filter(s -> s.status() == StepStatus.FAILED).findFirst()
                .orElseThrow().error());
        var json = new ObjectMapper().readTree(new MessageStatusEventJson().serialize(row));
        assertEquals(ONCE, json.get("error").asText());
        // The parked record's x-bip-error.
        if (parkingRethrows) {
            assertEquals(0, parked.size());
        } else {
            assertEquals(1, parked.size());
            assertEquals(ONCE, parked.getFirst().getMessage().getHeader(PlatformHeaders.ERROR, String.class));
        }

        // The log: the adapter's own lines and Camel's error handler lines that the adapter's logger
        // writes. Camel's fallback handler logs a failure of the parking route itself as it is
        // (documented), so its lines are not checked when that route throws.
        boolean adapterLine = false;
        boolean errorHandlerLine = false;
        for (LogRecord record : logs) {
            String logger = String.valueOf(record.getLoggerName());
            boolean adapter = logger.startsWith("no.basis.");
            if (!adapter && !logger.startsWith("org.apache.camel.processor")
                    || parkingRethrows && logger.equals("org.apache.camel.processor.FatalFallbackErrorHandler")) {
                continue;
            }
            for (String text : texts(record)) {
                assertFalse(text.contains(SECRET), logger + ": " + text);
                assertFalse(text.contains(URI_TWICE), "Masked twice in " + logger + ": " + text);
                if (text.contains("host:")) {
                    assertTrue(text.contains(URI_ONCE), logger + ": " + text);
                }
            }
            errorHandlerLine |= logger.startsWith("org.apache.camel.processor.errorhandler")
                    && texts(record).stream().anyMatch(text -> text.contains(URI_ONCE));
            if (adapter && record.getThrown() != null && record.getThrown().toString().contains("host:")) {
                assertEquals(IllegalStateException.class.getName() + ": " + ONCE, record.getThrown().toString());
                adapterLine = true;
            }
        }
        assertTrue(adapterLine, "The adapter logged the failure");
        assertTrue(errorHandlerLine, "The dead letter channel logged the failure through the adapter's logger");
    }

    @Test
    void theResendAnswerGetsTheErrorTextMaskedOnce() {
        MessageReplayService failing = new MessageReplayService() {
            public MessageReplayResult replay(String messageId) {
                throw new MessageReplayException(RAW);
            }

            public MessageReplayResult replay(MessageReplayRequest request) {
                throw new MessageReplayException(RAW);
            }
        };
        Response answer = new MessageReplayAdminResource(failing, new ObjectMapper())
                .resend("message-1", "operator", "test", null);

        assertEquals(ONCE, ((MessageReplayAdminResource.ReplayResponse) answer.getEntity()).error());
    }

    /** The texts a log line shows: its formatted message and each failure of its chain. */
    private static List<String> texts(LogRecord record) {
        List<String> texts = new ArrayList<>();
        String message = String.valueOf(record.getMessage());
        if (record.getParameters() != null) {
            for (Object parameter : record.getParameters()) {
                message = message.replaceFirst("\\{}", java.util.regex.Matcher.quoteReplacement(String.valueOf(parameter)));
            }
        }
        texts.add(message);
        for (Throwable thrown = record.getThrown(); thrown != null; thrown = thrown.getCause()) {
            texts.add(thrown.toString());
            if (thrown.getCause() == thrown) {
                break;
            }
        }
        return texts;
    }
}
