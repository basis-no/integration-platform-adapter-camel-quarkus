package example;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreAuditRoutePolicy;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(DeliveryAuditStartupTest.AuditProfile.class)
class DeliveryAuditStartupTest {
    public static class AuditProfile implements QuarkusTestProfile {
        @Override
        public Map<String, String> getConfigOverrides() {
            return Map.of("basis.delivery-audit.routes", "receiver");
        }
    }

    @Inject CamelContext context;
    @Inject ProducerTemplate producer;

    @Test
    void discoversSelectedAuditPolicyAndPreservesReceiverResult() {
        assertTrue(context.getRoute("receiver").getRoutePolicyList().stream()
                .anyMatch(MessageStoreAuditRoutePolicy.class::isInstance));
        var reply = producer.request("direct:receiver", e -> {
            e.getMessage().setBody("PASS sample");
            e.getMessage().setHeader("x-bip-correlation-id", "original-input");
        });
        assertNull(reply.getException());
        assertEquals("original-input", reply.getMessage().getHeader("x-bip-correlation-id"));
        assertNotNull(reply.getMessage().getHeader("x-bip-message-id"));
        assertEquals("accepted", reply.getMessage().getBody(String.class));
    }
}
