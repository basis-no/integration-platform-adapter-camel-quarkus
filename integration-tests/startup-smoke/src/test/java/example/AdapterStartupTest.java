package example;

import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStore;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.component.kafka.PollOnError;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
class AdapterStartupTest {
    @Inject CamelContext context;
    @Inject ProducerTemplate producer;
    @Inject Instance<KeyValueStore> keyValues;

    @Test
    void discoversAdapterAndAttachesPolicyOnlyToKafkaInput() {
        assertNotNull(context.getRoute("bip-confirmed-dead-letter"));
        assertTrue(context.getRoute("source").getRoutePolicyList().stream().anyMatch(MessageStoreRoutePolicy.class::isInstance));
        var internalPolicies = context.getRoute("receiver").getRoutePolicyList();
        assertTrue(internalPolicies == null || internalPolicies.stream().noneMatch(MessageStoreRoutePolicy.class::isInstance));
    }

    @Test
    void discoversKeyValueStoreWithoutStartingItsInactiveDataSource() {
        assertTrue(keyValues.isResolvable());
        // ApplicationScoped proxy creation must not invoke the producer or open JDBC.
        assertNotNull(keyValues.get());
        assertTrue(Thread.getAllStackTraces().keySet().stream()
                .noneMatch(thread -> thread.isAlive() && thread.getName().equals("bip-key-value-sweep")));
    }

    @Test
    void dependencyDefaultsPreserveManualCommitAndStopOnError() {
        var endpoint = (KafkaEndpoint) context.getRoute("source").getEndpoint();
        assertFalse(endpoint.getConfiguration().isAutoCommitEnable());
        assertTrue(endpoint.getConfiguration().isAllowManualCommit());
        assertTrue(endpoint.getConfiguration().isBreakOnFirstError());
        // A poll failure reconnects instead of stopping the consumer or skipping an offset.
        assertEquals(PollOnError.RECONNECT, endpoint.getConfiguration().getPollOnError());
    }

    @Test
    void internalRequestErrorReachesCallerWithoutKafkaOrDatabase() {
        var reply = producer.request("direct:receiver", e -> e.getMessage().setBody("<Transformed/>"));
        assertNotNull(reply.getException());
        assertInstanceOf(IllegalArgumentException.class, reply.getException());
        assertEquals("synthetic receiver rejection", reply.getException().getMessage());
    }

    @Test
    void syntheticSuccessReturnsWithoutCreatingAnotherLifecycleBoundary() {
        var reply = producer.request("direct:receiver", e -> e.getMessage().setBody("PASS sample"));
        assertNull(reply.getException());
        assertEquals("accepted", reply.getMessage().getBody(String.class));
    }
}
