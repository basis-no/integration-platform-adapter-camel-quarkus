package no.basis.integrationplatform.camel.ingress;

import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IngressNormalizationConfigTest {
    @Test
    void parsesExactRouteIdsAndRemovesDuplicates() {
        IngressNormalizationConfig config = new IngressNormalizationConfig(
                "file-to-kafka, queue-to-kafka, file-to-kafka");

        assertTrue(config.enabled());
        assertTrue(config.includes("file-to-kafka"));
        assertTrue(config.includes("queue-to-kafka"));
        assertFalse(config.includes("file-to-kafka-child"));
        assertEquals(Set.of("file-to-kafka", "queue-to-kafka"), config.routeIds());
    }

    @Test
    void blankConfigurationDisablesNormalization() {
        IngressNormalizationConfig config = new IngressNormalizationConfig("  ");

        assertFalse(config.enabled());
        assertTrue(config.routeIds().isEmpty());
    }
}
