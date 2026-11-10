package no.basis.integrationplatform.camel.lease;

import no.basis.integrationplatform.camel.lifecycle.ContractVersionStartupGate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A polling route listed in the exclusion property runs on every replica; a wrong id refuses the start. */
class SingleActiveExclusionTest {
    private static final LeaseTiming FAST = PostgresLeaseTest.FAST;

    private final TestClock clock = new TestClock();
    private final FakeKeyValueStore state = new FakeKeyValueStore(clock);
    private final LeaseManager manager = new LeaseManager(
            "pod-a", () -> state, () -> ContractVersionStartupGate.Outcome.PASSED, FAST, clock::nanoTime, clock);
    private final DefaultCamelContext context = new DefaultCamelContext();

    @AfterEach
    void stop() {
        context.stop();
    }

    @Test
    void parsesTheExcludedRouteIds() {
        assertEquals(Set.of("heartbeat", "poll-files"),
                SingleActiveRoutes.excludedRoutes(Optional.of(" heartbeat, ,poll-files ")));
        assertEquals(Set.of(), SingleActiveRoutes.excludedRoutes(Optional.empty()));
    }

    @Test
    void anExcludedPollingRouteRunsWhileAnotherReplicaHoldsItsLeaseAndTakesNone() throws Exception {
        // Another replica holds the lease: a coordinated route would not run here.
        state.hold("lease/heartbeat", "pod-b", Duration.ofMinutes(5));
        state.hold("lease/tick", "pod-b", Duration.ofMinutes(5));
        AtomicInteger heartbeats = new AtomicInteger();
        AtomicInteger ticks = new AtomicInteger();
        SingleActiveRoutePolicyFactory factory = install(Optional.of("heartbeat"));
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:heartbeat?period=20").routeId("heartbeat").process(exchange -> heartbeats.incrementAndGet());
                from("timer:tick?period=20").routeId("tick").process(exchange -> ticks.incrementAndGet());
            }
        });
        context.start();
        factory.verifyExclusions(context);

        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (heartbeats.get() < 3 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        Thread.sleep(FAST.attemptInterval().multipliedBy(3).toMillis());

        assertTrue(heartbeats.get() >= 3, "the excluded route runs on this replica although pod-b holds its lease");
        assertNull(manager.current("heartbeat"), "the excluded route has no lease registration");
        assertEquals("pod-b", state.live("lease/heartbeat").value().get("holder").asText(),
                "the excluded route never touches its lease key");
        assertEquals(0, ticks.get(), "a route that is not excluded stays coordinated");
        assertFalse(manager.current("tick").isOpen());
    }

    @Test
    void aFiniteTimerRunsOnEveryReplicaWithoutALease() throws Exception {
        state.hold("lease/init", "pod-b", Duration.ofMinutes(5));
        AtomicInteger ticks = new AtomicInteger();
        install(Optional.empty());
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:init?delay=0&repeatCount=1").routeId("init").process(exchange -> ticks.incrementAndGet());
            }
        });
        context.start();

        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (ticks.get() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertEquals(1, ticks.get(), "the one-shot tick runs although another replica holds a lease of that name");
        assertNull(manager.current("init"), "a finite timer gets no lease");
    }

    @Test
    void anExcludedFiniteTimerIsAccepted() throws Exception {
        SingleActiveRoutePolicyFactory factory = install(Optional.of("init"));
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:init?repeatCount=1").routeId("init").log("x");
            }
        });
        context.start();

        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> factory.verifyExclusions(context),
                "a finite timer is never leased, so listing it is harmless");
    }

    @Test
    void aPollingEndpointWhoseConsumerTheAdapterCannotGateRefusesToStartUnlessExcluded() throws Exception {
        context.addComponent("pushpoll", new PushConsumerPollComponent());
        install(Optional.empty());
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("pushpoll:items").routeId("push-items").log("x");
            }
        });

        Exception error = assertThrows(Exception.class, context::start);
        StringBuilder chain = new StringBuilder();
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            chain.append(cause.getMessage()).append(' ');
        }
        String messages = chain.toString();
        assertTrue(messages.contains("push-items"), messages);
        assertTrue(messages.contains(SingleActiveRoutes.EXCLUDED_ROUTES), messages);
    }

    @Test
    void anExcludedPollingEndpointWithAnUngateableConsumerStarts() throws Exception {
        context.addComponent("pushpoll", new PushConsumerPollComponent());
        install(Optional.of("push-items"));
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("pushpoll:items").routeId("push-items").log("x");
            }
        });

        context.start();
        assertEquals(org.apache.camel.ServiceStatus.Started, context.getRouteController().getRouteStatus("push-items"));
    }

    /** A polling endpoint whose consumer is event-driven, not a scheduled poll consumer. */
    static final class PushConsumerPollComponent extends org.apache.camel.support.DefaultComponent {
        @Override
        protected org.apache.camel.Endpoint createEndpoint(String uri, String remaining,
                                                           java.util.Map<String, Object> parameters) {
            return new org.apache.camel.support.ScheduledPollEndpoint(uri, this) {
                @Override
                public org.apache.camel.Producer createProducer() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public org.apache.camel.Consumer createConsumer(org.apache.camel.Processor processor) {
                    return new org.apache.camel.support.DefaultConsumer(this, processor);
                }
            };
        }
    }

    @Test
    void anExcludedIdThatIsNotAPollingRouteRefusesTheStart() throws Exception {
        SingleActiveRoutePolicyFactory factory = install(Optional.of("transform"));
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:transform").routeId("transform").log("x");
            }
        });
        context.start();

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> factory.verifyExclusions(context));
        assertTrue(error.getMessage().contains("'transform'"), error.getMessage());
        assertTrue(error.getMessage().contains(SingleActiveRoutes.EXCLUDED_ROUTES), error.getMessage());
    }

    @Test
    void anExcludedIdOfNoRouteAtStartupOnlyWarns() throws Exception {
        SingleActiveRoutePolicyFactory factory = install(Optional.of("added-later"));
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:heartbeat?period=1000").routeId("heartbeat").log("x");
            }
        });
        context.start();

        // A route can still be added later, for example from a route template; the id then applies to it.
        org.junit.jupiter.api.Assertions.assertDoesNotThrow(() -> factory.verifyExclusions(context));
        AtomicInteger ticks = new AtomicInteger();
        state.hold("lease/added-later", "pod-b", Duration.ofMinutes(5));
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:later?period=20").routeId("added-later").process(exchange -> ticks.incrementAndGet());
            }
        });
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (ticks.get() == 0 && System.nanoTime() < deadline) {
            Thread.sleep(20);
        }
        assertTrue(ticks.get() > 0, "the route added later is excluded, so it runs without the lease");
    }

    private SingleActiveRoutePolicyFactory install(Optional<String> excluded) throws Exception {
        SingleActiveRoutePolicyFactory factory = new SingleActiveRoutePolicyFactory(manager, excluded);
        context.addService(manager);
        context.addRoutePolicyFactory(factory);
        context.addLifecycleStrategy(new SingleActiveConsumerStrategy(manager));
        return factory;
    }
}
