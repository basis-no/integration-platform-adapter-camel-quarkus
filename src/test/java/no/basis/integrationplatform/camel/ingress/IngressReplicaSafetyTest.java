package no.basis.integrationplatform.camel.ingress;

import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IngressReplicaSafetyTest {
    private final DefaultCamelContext context = new DefaultCamelContext();

    @AfterEach
    void stop() {
        context.stop();
    }

    @Test
    void acceptsUncoordinatedSourceWithOneReplica() {
        IngressReplicaSafety safety = IngressReplicaSafety.declaredReplicas(1);

        assertDoesNotThrow(() -> safety.validate(context, "receive-events", route("direct:events")));
    }

    @Test
    void rejectsUncoordinatedSourceWhenReplicaCountIsMissing() {
        IngressReplicaSafety safety = new IngressReplicaSafety(Optional.empty());

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> safety.validate(context, "receive-events", route("direct:events")));

        assertTrue(thrown.getMessage().contains(IngressReplicaSafety.PROPERTY));
        assertTrue(thrown.getMessage().contains("prevent duplicate polling"));
    }

    @Test
    void rejectsUncoordinatedSourceWithMultipleReplicas() {
        IngressReplicaSafety safety = IngressReplicaSafety.declaredReplicas(2);

        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> safety.validate(context, "receive-events", route("direct:events")));

        assertTrue(thrown.getMessage().contains("master:"));
        assertTrue(thrown.getMessage().contains("deploy one replica"));
    }

    @Test
    void requiresTheLeaseOfARouteItAcceptsOnlyBecauseOfTheLease() {
        var required = new java.util.ArrayList<String>();
        IngressReplicaSafety several = new IngressReplicaSafety(Optional.of(3), Optional.empty(), required::add);
        IngressReplicaSafety undeclared = new IngressReplicaSafety(Optional.empty(), Optional.empty(), required::add);
        IngressReplicaSafety single = new IngressReplicaSafety(Optional.of(1), Optional.empty(), required::add);

        several.validate(context, "poll-files", route("file:/input?move=.done"));
        undeclared.validate(context, "poll-ledger", route("timer:ledger?period=60000"));
        single.validate(context, "poll-one", route("file:/one"));
        several.validate(context, "poll-master", route("master:ingress:file:/input"));

        assertEquals(java.util.List.of("poll-files", "poll-ledger"), required,
                "only routes the check accepted because of their lease must have it; one replica, or master:, need none");
    }

    @Test
    void rejectsAPollingSourceExcludedFromTheLeasesWithMultipleReplicas() {
        IngressReplicaSafety safety = new IngressReplicaSafety(Optional.of(3), Optional.of("poll-files, other"), routeId -> { });

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> safety.validate(context, "poll-files", route("file:/input?move=.done")));

        assertTrue(error.getMessage().contains("poll-files"));
        assertDoesNotThrow(() -> safety.validate(context, "poll-ledger", route("timer:ledger?period=60000")));
    }

    @Test
    void acceptsPollingSourcesTheAdapterCoordinatesWithMultipleReplicas() {
        IngressReplicaSafety declared = IngressReplicaSafety.declaredReplicas(3);
        IngressReplicaSafety undeclared = new IngressReplicaSafety(Optional.empty());

        assertDoesNotThrow(() -> declared.validate(context, "poll-files", route("file:/input?move=.done")));
        assertDoesNotThrow(() -> declared.validate(context, "poll-ledger", route("timer:ledger?period=60000")));
        assertDoesNotThrow(() -> undeclared.validate(context, "poll-files", route("file:/input?move=.done")));
    }

    @Test
    void acceptsMasterCoordinatedSourceWithMultipleReplicas() {
        IngressReplicaSafety safety = IngressReplicaSafety.declaredReplicas(3);

        assertDoesNotThrow(() -> safety.validate(
                context, "poll-files", route("master:ingress:file:/input")));
    }

    @Test
    void acceptsCompetingConsumerJmsQueueWithMultipleReplicas() {
        IngressReplicaSafety safety = IngressReplicaSafety.declaredReplicas(3);

        assertDoesNotThrow(() -> safety.validate(
                context, "receive-jms", route("jms:queue:orders")));
        assertDoesNotThrow(() -> safety.validate(
                context, "receive-sjms", route("sjms2:queue:orders")));
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
