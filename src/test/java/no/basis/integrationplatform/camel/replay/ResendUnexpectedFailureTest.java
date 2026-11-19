package no.basis.integrationplatform.camel.replay;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.ws.rs.core.Response;
import no.basis.integrationplatform.camel.LogCapture;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A resend that fails in a way the endpoint does not expect answers a plain error: not the
 * failure's text, type or stack, which Quarkus's error page shows its caller in dev and test
 * mode, unmasked. The log keeps the failure, masked.
 */
class ResendUnexpectedFailureTest {
    private static final List<String> SECRETS = List.of("pw-secret-1", "key-secret-2");
    private static final String FAILURE =
            "Kafka publish to http://partner:pw-secret-1@broker.example:9092/?key=key-secret-2 failed";

    // An Error, and a checked exception thrown unchecked, as a client library or driver may.
    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"IllegalStateException", "AssertionError", "IOException"})
    void anUnexpectedFailureAnswersAPlainErrorAndIsLoggedMasked(String type) {
        Throwable failure = switch (type) {
            case "AssertionError" -> new AssertionError(FAILURE);
            case "IOException" -> new IOException(FAILURE);
            default -> new IllegalStateException(FAILURE);
        };
        var resource = new MessageReplayAdminResource(failingWith(failure), new ObjectMapper());
        Response response;
        List<LogRecord> errors;
        try (LogCapture log = LogCapture.of(MessageReplayAdminResource.class)) {
            response = resource.resend("order-1", "operator", "probe", null);
            errors = log.at(Level.SEVERE);
        }

        assertEquals(500, response.getStatus());
        var answer = (MessageReplayAdminResource.ReplayResponse) response.getEntity();
        assertEquals("FAILED", answer.status());
        assertEquals("order-1", answer.messageId());
        assertEquals(MessageReplayAdminResource.RESEND_FAILED, answer.error());
        assertFalse(answer.toString().contains(type), answer.toString());
        assertNoSecret(answer.toString());

        assertEquals(1, errors.size());
        LogRecord line = errors.getFirst();
        assertFalse(LogCapture.text(line).contains("order-1"), "The caller's id is not logged: " + LogCapture.text(line));
        assertTrue(line.getThrown().toString().contains(type), line.getThrown().toString());
        assertTrue(line.getThrown().toString().contains("key=xxxxxx"), line.getThrown().toString());
        assertNoSecret(line.getThrown().toString());
    }

    @Test
    void aFailureWhoseTextCannotBeReadStillAnswersThePlainError() {
        var unreadable = new IllegalStateException(FAILURE) {
            @Override
            public String getMessage() {
                throw new UnsupportedOperationException("no text");
            }

            @Override
            public String toString() {
                throw new UnsupportedOperationException("no text");
            }
        };
        var resource = new MessageReplayAdminResource(failingWith(unreadable), new ObjectMapper());
        Response response;
        List<LogRecord> errors;
        try (LogCapture log = LogCapture.of(MessageReplayAdminResource.class)) {
            response = resource.resend("order-1", "operator", "probe", null);
            errors = log.at(Level.SEVERE);
        }

        assertEquals(500, response.getStatus());
        assertEquals(MessageReplayAdminResource.RESEND_FAILED,
                ((MessageReplayAdminResource.ReplayResponse) response.getEntity()).error());
        assertEquals(1, errors.size());
        assertTrue(LogCapture.text(errors.getFirst()).contains(unreadable.getClass().getName()),
                LogCapture.text(errors.getFirst()));
        assertNoSecret(LogCapture.text(errors.getFirst()));
    }

    @Test
    void aReplayFailureKeepsItsStatusAndReason() {
        var notFound = new MessageReplayAdminResource(
                failingWith(new MessageReplayException("Message order-1 was not found in bip_message_log")), new ObjectMapper())
                .resend("order-1", "operator", "probe", null);
        assertEquals(404, notFound.getStatus());
        assertEquals("Message order-1 was not found in bip_message_log",
                ((MessageReplayAdminResource.ReplayResponse) notFound.getEntity()).error());

        var refused = new MessageReplayAdminResource(failingWith(new IllegalStateException("not reached")), new ObjectMapper())
                .resend("order-1", "operator", "probe", "{not json");
        assertEquals(409, refused.getStatus());
        assertEquals("Invalid replay request body", ((MessageReplayAdminResource.ReplayResponse) refused.getEntity()).error());
    }

    private static MessageReplayService failingWith(Throwable failure) {
        return new MessageReplayService() {
            @Override
            public MessageReplayResult replay(String messageId) {
                return sneakyThrow(failure);
            }

            @Override
            public MessageReplayResult replay(MessageReplayRequest request) {
                return sneakyThrow(failure);
            }
        };
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable, R> R sneakyThrow(Throwable failure) throws T {
        throw (T) failure;
    }

    private static void assertNoSecret(String text) {
        for (String secret : SECRETS) {
            assertFalse(text.contains(secret), "A credential in: " + text);
        }
    }
}
