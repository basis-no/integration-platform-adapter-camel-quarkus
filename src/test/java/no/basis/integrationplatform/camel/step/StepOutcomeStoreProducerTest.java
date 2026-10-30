package no.basis.integrationplatform.camel.step;

import jakarta.enterprise.inject.Instance;
import no.basis.integrationplatform.camel.MessageStoreTestProfileConfig;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class StepOutcomeStoreProducerTest {
    @Test
    void noPersistenceProfileDoesNotResolveConfiguredPostgresDataSource() {
        StepOutcomeStore store = new StepOutcomeStoreProducer().stepOutcomeStore(
                "postgres",
                unavailableDataSource(),
                new MessageStoreTestProfileConfig("no-persistence", true));

        assertInstanceOf(NoopStepOutcomeStore.class, store);
    }

    @SuppressWarnings("unchecked")
    private Instance<DataSource> unavailableDataSource() {
        return (Instance<DataSource>) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{Instance.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "isResolvable" -> false;
                    case "get" -> throw new AssertionError("Test profile must not resolve a DataSource");
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
