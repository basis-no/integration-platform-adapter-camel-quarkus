package example;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreInputAuditRoutePolicy;
import org.apache.camel.CamelContext;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(InputAuditDisabledSettingTest.Disabled.class)
class InputAuditDisabledSettingTest {
    public static class Disabled implements QuarkusTestProfile {
        public Map<String, String> getConfigOverrides() {
            return Map.of("basis.input-audit.routes", "${INPUT_AUDIT_ROUTES:receiver}", "INPUT_AUDIT_ROUTES", "");
        }
    }
    @Inject CamelContext context;
    @Test
    void explicitEmptyInputSelectionIsDisabled() {
        var policies = context.getRoute("receiver").getRoutePolicyList();
        assertTrue(policies == null || policies.stream().noneMatch(MessageStoreInputAuditRoutePolicy.class::isInstance));
    }
}
