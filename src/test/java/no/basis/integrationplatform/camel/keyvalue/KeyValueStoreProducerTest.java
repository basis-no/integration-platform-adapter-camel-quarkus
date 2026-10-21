package no.basis.integrationplatform.camel.keyvalue;

import jakarta.enterprise.inject.Instance;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KeyValueStoreProducerTest {
    @Test
    void refusesStateWithoutAnIntegrationIdentity() {
        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> new KeyValueStoreProducer().keyValueStore(Optional.of(" "), 3600, unusedDataSource()));

        assertTrue(refused.getMessage().contains("app.name"), refused.getMessage());
    }

    @SuppressWarnings("unchecked")
    private Instance<DataSource> unusedDataSource() {
        return (Instance<DataSource>) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{Instance.class},
                (proxy, method, args) -> {
                    throw new AssertionError("State without an identity must not resolve a DataSource");
                });
    }
}
