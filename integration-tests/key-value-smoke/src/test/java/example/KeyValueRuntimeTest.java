package example;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStore;
import org.junit.jupiter.api.Test;
import javax.sql.DataSource;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(KeyValuePostgresResource.class)
class KeyValueRuntimeTest {
    @Inject KeyValueStore keys;
    @Inject DataSource dataSource;

    @Test
    void injectedStoreUsesTheMessageStoreIdentityAndKeepsContractMetadata() throws Exception {
        var first = JsonNodeFactory.instance.objectNode().put("after", 1);
        assertEquals(1, keys.put("cursor/runtime", first));
        var stored = keys.get("cursor/runtime").orElseThrow();
        assertEquals(first, stored.value());
        assertEquals("runtime", stored.updatedBy());
        assertEquals(2, keys.compareAndSet("cursor/runtime", stored.version(),
                JsonNodeFactory.instance.objectNode().put("after", 2)).orElseThrow());
        assertTrue(keys.compareAndSet("cursor/runtime", stored.version(), first).isEmpty());
        assertEquals(1, keys.list("cursor/", null, 10).size());
        assertTrue(keys.delete("cursor/runtime", 2));
        assertTrue(keys.get("cursor/runtime").isEmpty());
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            try (var result = statement.executeQuery("select contract_version from bip_contract where contract_major = 1")) {
                assertTrue(result.next());
                assertEquals("1.2.0-rc2", result.getString(1));
            }
            try (var result = statement.executeQuery("select value::text from bip_key_value where integration = 'other-integration' and key = 'cursor/runtime'")) {
                assertTrue(result.next());
                assertEquals("true", result.getString(1));
            }
        }
    }
}
