package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.TestDataSources;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A second @Named DataSource is also @Default; the journal must refuse to start, not fail per message. */
class AmbiguousSinkDataSourceTest {
    @Test
    void postgresSinkRefusesToStartWithTwoDefaultDataSources() {
        var gate = new ContractVersionStartupGate("postgres", TestDataSources.ambiguous());

        IllegalStateException refused = assertThrows(IllegalStateException.class, gate::verify);

        assertTrue(refused.getMessage().contains("basis.message-store.sink=postgres"), refused.getMessage());
        assertTrue(refused.getMessage().contains("more than one DataSource bean is @Default"), refused.getMessage());
    }

    @Test
    void logSinkDoesNotNeedTheDataSource() {
        assertDoesNotThrow(new ContractVersionStartupGate("log", TestDataSources.ambiguous())::verify);
    }
}
