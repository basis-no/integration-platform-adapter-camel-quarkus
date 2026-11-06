package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertTrue;

class LoggingMessageStoreSinkTest {
    @Test
    void countsDeliveredSkippedAndFailedStepsSeparately() {
        var steps = List.of(StepResult.skipped("send-order", "message-1:send-order", "sftp://files/out"),
                StepResult.delivered("notify", "message-1:notify", "sftp://files/notify"));
        var result = new MessageAttemptResult(1, "message-1", "order-1", null, null, null, "metadata_only", null, null,
                null, null, null, null, false, "test", "order-pipeline", "input", 2, false, null, null, Instant.now(),
                MessageStatus.COMPLETED, steps, null);
        try (var logs = LogCapture.of(LoggingMessageStoreSink.class)) {
            new LoggingMessageStoreSink().record(result);
            String text = LogCapture.text(logs.at(Level.INFO).getFirst());
            assertTrue(text.endsWith("status=COMPLETED steps=2 delivered=1 skipped=1 failed=0"), text);
        }
    }
}
