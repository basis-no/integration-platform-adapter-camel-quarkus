package no.basis.integrationplatform.camel.lease;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import no.basis.integrationplatform.camel.lifecycle.ContractVersionStartupGate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A lease the replica check relied on has no uncoordinated fallback; every lease reports its state as a gauge. */
class SingleActiveRequiredLeaseTest {
    private static final LeaseTiming FAST = PostgresLeaseTest.FAST;

    private final TestClock clock = new TestClock();
    private final FakeKeyValueStore state = new FakeKeyValueStore(clock);
    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final LeaseManager manager = new LeaseManager("pod-a", () -> state,
            () -> ContractVersionStartupGate.Outcome.PASSED, FAST, clock::nanoTime, clock, () -> meters);
    private final DefaultCamelContext context = new DefaultCamelContext();

    @AfterEach
    void stop() {
        context.stop();
    }

    @Test
    void aRequiredLeaseKeepsTheRouteStoppedWithoutTheTableAndAnOptionalOneRunsUncoordinated() throws Exception {
        state.provisioned = false;
        manager.require("files-to-kafka");
        AtomicInteger required = new AtomicInteger();
        AtomicInteger optional = new AtomicInteger();
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:required?period=20").routeId("files-to-kafka").process(exchange -> required.incrementAndGet());
                from("timer:optional?period=20").routeId("refresh").process(exchange -> optional.incrementAndGet());
            }
        });
        context.start();

        await(() -> optional.get() > 2);
        Thread.sleep(FAST.attemptInterval().multipliedBy(3).toMillis());

        assertTrue(optional.get() > 2, "without the table an ordinary polling route runs on every replica");
        assertEquals(0, required.get(), "a route the replica check accepted only because of its lease does nothing");
        assertEquals(3, gauge("files-to-kafka"), "stopped: it needs its lease and there is no Key-Value Store");
        assertEquals(2, gauge("refresh"), "uncoordinated");

        state.provisioned = true;
        await(() -> required.get() > 0);
        assertEquals(1, gauge("files-to-kafka"), "held once the table exists");
    }

    @Test
    void aRemovedRouteLosesItsGaugeAndARouteAddedAgainReportsItsNewLease() throws Exception {
        install();
        RouteBuilder tick = new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("tick").log("tick");
            }
        };
        context.addRoutes(tick);
        context.start();
        await(() -> gauge("tick") == 1);

        context.getRouteController().stopRoute("tick");
        context.removeRoute("tick");
        assertTrue(meters.find("integration_single_active_lease").tag("route", "tick").gauge() == null,
                "the gauge of a removed route is removed");

        context.addRoutes(tick);
        await(() -> meters.find("integration_single_active_lease").tag("route", "tick").gauge() != null
                && gauge("tick") == 1);
        assertEquals(1, gauge("tick"), "the route added again reports its own lease");
    }

    private double gauge(String route) {
        return meters.get("integration_single_active_lease").tag("route", route).gauge().value();
    }

    private void install() throws Exception {
        context.addService(manager);
        context.addRoutePolicyFactory(new SingleActiveRoutePolicyFactory(manager));
        context.addLifecycleStrategy(new SingleActiveConsumerStrategy(manager));
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(condition.getAsBoolean(), "condition not met in time");
    }
}
