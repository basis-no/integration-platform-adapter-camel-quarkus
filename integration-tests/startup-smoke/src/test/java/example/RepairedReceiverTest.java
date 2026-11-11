package example;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.Test;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@TestProfile(RepairedReceiverTest.Repaired.class)
class RepairedReceiverTest {
    public static class Repaired implements QuarkusTestProfile {
        public Map<String, String> getConfigOverrides() {
            return Map.of("acceptance.accept-rejected", "true");
        }
    }

    @Inject ProducerTemplate producer;

    @Test
    void explicitRepairAllowsPreviouslyRejectedSyntheticInput() {
        var reply = producer.request("direct:receiver", e -> e.getMessage().setBody("FAIL rejected"));
        assertNull(reply.getException());
        assertEquals("accepted", reply.getMessage().getBody(String.class));
    }

    @Test
    void repairDoesNotAcceptUnrecognizedInput() {
        var reply = producer.request("direct:receiver", e -> e.getMessage().setBody("UNKNOWN"));
        assertNotNull(reply.getException());
    }
}
