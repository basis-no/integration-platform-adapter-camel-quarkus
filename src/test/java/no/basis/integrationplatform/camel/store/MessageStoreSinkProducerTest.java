package no.basis.integrationplatform.camel.store;

import jakarta.enterprise.inject.Instance;
import no.basis.integrationplatform.camel.MessageStoreTestProfileConfig;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class MessageStoreSinkProducerTest {
    @Test
    void noPersistenceProfileDoesNotResolveConfiguredPostgresDataSource() {
        MessageStoreSink sink = new MessageStoreSinkProducer().messageStoreSink(
                "postgres",
                Optional.empty(),
                KafkaMessageStoreSink.DEFAULT_STATUS_TOPIC,
                unavailableDataSource(),
                new MessageStoreTestProfileConfig("no-persistence", true));

        assertInstanceOf(LoggingMessageStoreSink.class, sink);
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
