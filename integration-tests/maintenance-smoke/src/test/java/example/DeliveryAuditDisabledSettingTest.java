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
@TestProfile(DeliveryAuditDisabledSettingTest.Disabled.class)
class DeliveryAuditDisabledSettingTest {
    public static class Disabled implements QuarkusTestProfile {
        public Map<String, String> getConfigOverrides() {
            return Map.of("basis.delivery-audit.routes", "${DELIVERY_AUDIT_ROUTES:receiver}",
                    "DELIVERY_AUDIT_ROUTES", "");
        }
    }

    @Inject CamelContext context;
    @Inject ProducerTemplate producer;

    @Test
    void emptyApplicationSettingDisablesAuditEvenWithAnExpressionDefault() {
        var policies = context.getRoute("receiver").getRoutePolicyList();
        assertTrue(policies == null || policies.stream().noneMatch(MessageStoreAuditRoutePolicy.class::isInstance));
        assertEquals("accepted", producer.requestBody("direct:receiver", "PASS sample"));
    }
}
