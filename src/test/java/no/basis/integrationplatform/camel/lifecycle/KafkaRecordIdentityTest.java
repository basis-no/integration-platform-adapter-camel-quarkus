package no.basis.integrationplatform.camel.lifecycle;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/** The derived message id's test vectors, shared with the .NET adapter. */
class KafkaRecordIdentityTest {
    @Test
    void derivesTheSharedTestVectors() {
        assertEquals("d7431daf-3b19-51d8-968b-020679b335a5",
                KafkaRecordIdentity.derive("order-pipeline", "bip.order-pipeline.in", 0, 42));
        assertEquals("3421dcd0-bbee-5491-8683-1fc2b8cbfd84",
                KafkaRecordIdentity.derive("order-pipeline", "bip.order-pipeline.in", 3, 0));
        assertEquals("f3b505ba-d509-5a06-9d6f-fd595e79c844",
                KafkaRecordIdentity.derive("invoice-é", "bip.invoice.in", 11, 9007199254740993L));
    }

    @Test
    void anotherIntegrationOrPositionGetsAnotherId() {
        String id = KafkaRecordIdentity.derive("order-pipeline", "bip.order-pipeline.in", 0, 42);
        assertNotEquals(id, KafkaRecordIdentity.derive("billing", "bip.order-pipeline.in", 0, 42));
        assertNotEquals(id, KafkaRecordIdentity.derive("order-pipeline", "bip.order-pipeline.in", 1, 42));
        assertNotEquals(id, KafkaRecordIdentity.derive("order-pipeline", "bip.order-pipeline.in", 0, 43));
    }
}
