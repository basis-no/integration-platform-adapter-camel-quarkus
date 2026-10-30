package no.basis.integrationplatform.camel;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MessageFailureModeTest {
    @Test
    void parsesSupportedModesCaseInsensitively() {
        assertEquals(MessageFailureMode.DLQ, MessageFailureMode.fromConfig(" DLQ "));
        assertEquals(MessageFailureMode.RECORD_FAILED, MessageFailureMode.fromConfig("record-failed"));
    }

    @Test
    void rejectsUnsupportedMode() {
        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> MessageFailureMode.fromConfig("ignore"));

        assertEquals(
                "Unsupported basis.message-store.failure-mode 'ignore'; expected one of: dlq, record-failed",
                thrown.getMessage());
    }
}
