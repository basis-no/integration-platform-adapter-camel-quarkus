package no.basis.integrationplatform.camel;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageStoreTimeoutsValidatorTest {
    @Test
    void startupRefusesATimeoutThatWouldNotBoundACall() {
        IllegalArgumentException zero = assertThrows(IllegalArgumentException.class,
                () -> new MessageStoreTimeoutsValidator(Duration.ZERO, Duration.ofSeconds(5)).validate(null));
        assertTrue(zero.getMessage().contains("basis.message-store.statement-timeout"), zero.getMessage());

        assertThrows(IllegalArgumentException.class,
                () -> new MessageStoreTimeoutsValidator(Duration.ofSeconds(5), Duration.ofSeconds(10)).validate(null));
    }

    @Test
    void startupAcceptsTheDefaults() {
        assertDoesNotThrow(() -> new MessageStoreTimeoutsValidator(
                MessageStoreTimeouts.DEFAULT_STATEMENT_TIMEOUT, MessageStoreTimeouts.DEFAULT_LOCK_TIMEOUT).validate(null));
    }
}
