package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.TestDataSources;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A second DataSource from a @Named producer is also @Default. Before, the app started
 * and reported ready, and every tracked send failed with "requires a JDBC DataSource".
 */
class AmbiguousDataSourceTest {
    @Test
    void postgresStepStoreRefusesToStartWithTwoDefaultDataSources() {
        var validator = new StepOutcomeStoreModeValidator(StepOutcomeStoreModeValidator.POSTGRES, TestDataSources.ambiguous());

        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> validator.validate(null));

        assertTrue(refused.getMessage().contains("basis.message-store.step-outcome-store=postgres"), refused.getMessage());
        assertTrue(refused.getMessage().contains("more than one DataSource bean is @Default"), refused.getMessage());
        assertTrue(refused.getMessage().contains("@Identifier"), refused.getMessage());
    }

    @Test
    void noopStepStoreDoesNotNeedTheDataSource() {
        var validator = new StepOutcomeStoreModeValidator(StepOutcomeStoreModeValidator.NOOP, TestDataSources.ambiguous());

        assertDoesNotThrow(() -> validator.validate(null));
        assertInstanceOf(NoopStepOutcomeStore.class,
                new StepOutcomeStoreProducer().stepOutcomeStore(StepOutcomeStoreModeValidator.NOOP, TestDataSources.ambiguous()));
    }

    @Test
    void postgresStepStoreNamesTheAmbiguityWhenCreated() {
        IllegalStateException refused = assertThrows(IllegalStateException.class, () -> new StepOutcomeStoreProducer()
                .stepOutcomeStore(StepOutcomeStoreModeValidator.POSTGRES, TestDataSources.ambiguous()));

        assertTrue(refused.getMessage().contains("more than one DataSource bean is @Default"), refused.getMessage());
    }
}
