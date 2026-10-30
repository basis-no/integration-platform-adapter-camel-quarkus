package example;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreInputAuditRoutePolicy;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(InputAuditStartupTest.InputProfile.class)
class InputAuditStartupTest {
    public static class InputProfile implements QuarkusTestProfile {
        public Map<String, String> getConfigOverrides() {
            return Map.of("basis.input-audit.routes", "${INPUT_AUDIT_ROUTES:receiver}");
        }
    }
    @Inject CamelContext context;
    @Inject ProducerTemplate producer;

    @Test
    void inputInspectionIsDiscoveredWithoutChangingCallerIdentity() {
        assertTrue(context.getRoute("receiver").getRoutePolicyList().stream()
                .anyMatch(MessageStoreInputAuditRoutePolicy.class::isInstance));
        var reply = producer.request("direct:receiver", e -> {
            e.getMessage().setBody("PASS sample");
            e.getMessage().setHeader("x-bip-message-id", "upstream-input");
        });
        assertNull(reply.getException());
        assertEquals("upstream-input", reply.getMessage().getHeader("x-bip-message-id"));
        assertNull(reply.getMessage().getHeader("x-bip-correlation-id"));
        assertNull(reply.getProperty(no.basis.integrationplatform.camel.PlatformExchangeProperties.PAYLOAD_STORAGE));
        assertEquals("accepted", reply.getMessage().getBody(String.class));
    }
}
