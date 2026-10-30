package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.ingress.IngressNormalizationConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StepOutcomeStoreModeValidatorTest {
    @Test
    void normalizationRequiresPostgresLedger() {
        StepOutcomeStoreModeValidator validator = new StepOutcomeStoreModeValidator(
                "noop", new IngressNormalizationConfig("file-to-kafka"));

        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class, () -> validator.validate(null));

        assertTrue(failure.getMessage().contains("step-outcome-store=postgres"));
    }

    @Test
    void normalizationAcceptsPostgresLedger() {
        StepOutcomeStoreModeValidator validator = new StepOutcomeStoreModeValidator(
                "postgres", new IngressNormalizationConfig("file-to-kafka"));

        assertDoesNotThrow(() -> validator.validate(null));
    }
}
