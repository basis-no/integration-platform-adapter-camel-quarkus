package no.basis.integrationplatform.camel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OutputCheckpointHeaderValidatorTest {
    @Test
    void refusesCredentialLikeCheckpointHeadersAndNamesEach() {
        var refused = assertThrows(IllegalArgumentException.class, () -> new OutputCheckpointHeaderValidator(
                "Partner-Request-Id, Authorization, x-api-key, SET-COOKIE, Session-Token, client_key").validate());

        String message = refused.getMessage();
        assertTrue(message.startsWith("basis.message-store.step.output-checkpoint.header-names names "
                + "credential-like headers Authorization, client_key, Session-Token, SET-COOKIE, x-api-key."),
                message);
        assertFalse(message.contains("Partner-Request-Id"), message);
    }

    /** The rule is a name heuristic: harmless names that match it are refused too. */
    @ParameterizedTest
    @ValueSource(strings = {"X-Partition-Key", "X-Idempotency-Key", "X-Author", "X-Session-Count"})
    void refusesHarmlessNamesThatMatchTheRule(String name) {
        assertThrows(IllegalArgumentException.class, () -> new OutputCheckpointHeaderValidator(name).validate());
    }

    @Test
    void acceptsOrdinaryCheckpointHeadersTheAdaptersOwnAndNone() {
        new OutputCheckpointHeaderValidator(
                "Partner-Request-Id, Location, Content-Type, x-bip-correlation-id, Idempotency-Key").validate();
        new OutputCheckpointHeaderValidator(" , ").validate();
        new OutputCheckpointHeaderValidator(Optional.empty()).validate();
    }
}
