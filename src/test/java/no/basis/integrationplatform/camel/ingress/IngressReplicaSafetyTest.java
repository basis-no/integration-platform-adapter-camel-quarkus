package no.basis.integrationplatform.camel.ingress;

import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IngressReplicaSafetyTest {
    @Test
    void acceptsUncoordinatedSourceWithOneReplica() {
        IngressReplicaSafety safety = IngressReplicaSafety.declaredReplicas(1);

        assertDoesNotThrow(() -> safety.validate("poll-files", route("file:/input")));
    }

    @Test
    void rejectsUncoordinatedSourceWhenReplicaCountIsMissing() {
        IngressReplicaSafety safety = new IngressReplicaSafety(Optional.empty());

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> safety.validate("poll-files", route("file:/input")));

        assertTrue(thrown.getMessage().contains(IngressReplicaSafety.PROPERTY));
        assertTrue(thrown.getMessage().contains("prevent duplicate polling"));
    }

    @Test
    void rejectsUncoordinatedSourceWithMultipleReplicas() {
        IngressReplicaSafety safety = IngressReplicaSafety.declaredReplicas(2);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> safety.validate("poll-files", route("file:/input")));

        assertTrue(thrown.getMessage().contains("master:"));
        assertTrue(thrown.getMessage().contains("deploy one replica"));
    }

    @Test
    void acceptsMasterCoordinatedSourceWithMultipleReplicas() {
        IngressReplicaSafety safety = IngressReplicaSafety.declaredReplicas(3);

        assertDoesNotThrow(() -> safety.validate(
                "poll-files", route("master:ingress:file:/input")));
    }

    @Test
    void acceptsCompetingConsumerJmsQueueWithMultipleReplicas() {
        IngressReplicaSafety safety = IngressReplicaSafety.declaredReplicas(3);

        assertDoesNotThrow(() -> safety.validate(
                "receive-jms", route("jms:queue:orders")));
        assertDoesNotThrow(() -> safety.validate(
                "receive-sjms", route("sjms2:queue:orders")));
    }

    @Test
    void rejectsInvalidReplicaCount() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> IngressReplicaSafety.declaredReplicas(0));

        assertTrue(thrown.getMessage().contains("at least 1"));
    }

    private RouteDefinition route(String endpointUri) {
        return new RouteDefinition().from(endpointUri);
    }
}
