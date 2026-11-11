package no.basis.integrationplatform.camel.lease;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.spi.ObserverMethod;
import no.basis.integrationplatform.camel.lifecycle.ContractVersionStartupGate;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The lease manager's startup observer must run after the contract gate's, so
 * that when the gate throws, the event never reaches the manager. Without a
 * booted Quarkus application this reads the declared priorities: an observer
 * without {@code @Priority} has {@link ObserverMethod#DEFAULT_PRIORITY}.
 */
class LeaseManagerObserverOrderTest {
    @Test
    void theLeaseObserverRunsAfterTheContractGateObserver() {
        int gate = startupObserverPriority(ContractVersionStartupGate.class);
        int manager = startupObserverPriority(LeaseManager.class);

        assertEquals(ObserverMethod.DEFAULT_PRIORITY, gate, "the gate observes at the default priority");
        assertTrue(manager > gate, "the lease observer (" + manager + ") must be ordered after the gate (" + gate + ")");
    }

    private static int startupObserverPriority(Class<?> type) {
        List<Method> observers = Arrays.stream(type.getDeclaredMethods())
                .filter(method -> Arrays.stream(method.getParameters()).anyMatch(
                        parameter -> parameter.isAnnotationPresent(Observes.class) && parameter.getType() == StartupEvent.class))
                .toList();
        assertEquals(1, observers.size(), type.getSimpleName() + " observes StartupEvent once");
        Parameter observed = Arrays.stream(observers.get(0).getParameters())
                .filter(parameter -> parameter.isAnnotationPresent(Observes.class)).findFirst().orElseThrow();
        Priority priority = observed.getAnnotation(Priority.class);
        return priority == null ? ObserverMethod.DEFAULT_PRIORITY : priority.value();
    }
}
