package no.basis.integrationplatform.camel.lease;

import no.basis.integrationplatform.camel.lifecycle.ContractVersionStartupGate;
import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Route;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.RoutePolicySupport;
import org.apache.camel.support.ScheduledPollConsumer;
import org.apache.camel.support.ScheduledPollEndpoint;
import org.apache.camel.Component;
import org.apache.camel.Endpoint;
import org.apache.camel.Processor;
import org.apache.camel.component.file.cluster.FileLockClusterService;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.ScheduledPollConsumer;
import org.apache.camel.support.ScheduledPollEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.CamelEvent;
import org.apache.camel.spi.PollingConsumerPollStrategy;
import org.apache.camel.support.EventNotifierSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static no.basis.integrationplatform.camel.lease.PostgresLeaseTest.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The gates inside coordinated routes, against an in-memory store on a test
 * clock: a scheduled poll does not begin while the gate is closed and work in
 * flight finishes, a trigger runs no processor while the gate is closed,
 * disabled routes take no lease, routes added later take theirs, a
 * {@code master:} route keeps its own cluster service, and shutdown completes
 * while a lease is being lost.
 */
class SingleActiveGateTest {
    private static final LeaseTiming FAST = PostgresLeaseTest.FAST;
    private static final Duration SLACK = Duration.ofMillis(1_500);

    @TempDir
    Path directory;

    private final TestClock clock = new TestClock();
    private final FakeKeyValueStore state = new FakeKeyValueStore(clock);
    private final AtomicReference<ContractVersionStartupGate.Outcome> contractCheck =
            new AtomicReference<>(ContractVersionStartupGate.Outcome.PASSED);
    private final LeaseManager manager = new LeaseManager(
            "pod-a", () -> state, contractCheck::get, FAST, clock::nanoTime, clock);
    private final DefaultCamelContext context = new DefaultCamelContext();
    private final AtomicInteger processed = new AtomicInteger();
    private final CountDownLatch inFlight = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);

    @AfterEach
    void stop() {
        release.countDown();
        context.stop();
    }

    @Test
    void aPollDoesNotBeginWhileTheGateIsClosedAndWorkInFlightFinishes() throws Exception {
        RecordingPollStrategy recording = new RecordingPollStrategy();
        context.getRegistry().bind("recording", recording);
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("file:" + directory + "?pollStrategy=#recording&initialDelay=0&delay=50&delete=true")
                        .routeId("poll-files")
                        .process(exchange -> {
                            processed.incrementAndGet();
                            inFlight.countDown();
                            release.await(30, TimeUnit.SECONDS);
                        });
            }
        });
        context.start();
        Lease lease = manager.lease("poll-files");
        assertTrue(lease.isHeld(), "the lease is acquired before the routes start");
        Files.writeString(directory.resolve("first.csv"), "1");
        await("the first file to be in flight", () -> inFlight.getCount() == 0, SLACK);
        int beginsBefore = recording.begins.get();
        assertTrue(beginsBefore > 0);

        state.reachable = false;
        clock.advance(FAST.stopDeadline().plusMillis(1));
        await("the deadline check to close the gate", () -> !lease.isOpen(), SLACK);
        int commitsAtClose = recording.commits.get();

        release.countDown();
        await("the exchange in flight to finish", () -> recording.commits.get() == commitsAtClose + 1, SLACK);
        assertFalse(Files.exists(directory.resolve("first.csv")), "the in-flight exchange completed normally");
        assertEquals(0, recording.rollbacks.get());
        assertEquals(1, processed.get());

        // The poller is free again: with the gate closed, its scheduled polls must not begin.
        int beginsAfterCompletion = recording.begins.get();
        Files.writeString(directory.resolve("second.csv"), "2");
        Thread.sleep(600);
        assertEquals(beginsAfterCompletion, recording.begins.get(), "later scheduled polls do not begin while closed");
        assertEquals(commitsAtClose + 1, recording.commits.get(), "and nothing more is committed");
        assertTrue(Files.exists(directory.resolve("second.csv")), "the second file is not picked up");
        assertEquals(1, processed.get());

        state.reachable = true;
        clock.advance(FAST.leaseDuration());
        await("the lease to be acquired again once the old one expired", lease::isOpen,
                FAST.attemptInterval().plus(SLACK));
        await("the second file to be processed", () -> processed.get() == 2, SLACK);
        assertFalse(Files.exists(directory.resolve("second.csv")));
    }

    @Test
    void aTriggerRunsNoProcessorWhileTheGateIsClosed() throws Exception {
        state.hold("lease/tick", "pod-b", FAST.leaseDuration());
        AtomicInteger created = new AtomicInteger();
        context.getManagementStrategy().addEventNotifier(new EventNotifierSupport() {
            @Override
            public void notify(CamelEvent event) {
                if (event instanceof CamelEvent.ExchangeCreatedEvent) {
                    created.incrementAndGet();
                }
            }
        });
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("tick").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();

        await("the timer to fire", () -> created.get() >= 5, SLACK);
        assertEquals(0, processed.get(), "a trigger that fires while the gate is closed runs no processor");
        assertEquals(ServiceStatus.Started, context.getRouteController().getRouteStatus("tick"));

        state.delete("lease/tick");
        await("the lease to be acquired", () -> processed.get() > 0, FAST.attemptInterval().plus(SLACK));
        assertTrue(manager.lease("tick").isHeld());
    }

    @Test
    void aDisabledRouteStaysStoppedAndTakesNoLeaseUntilItIsStarted() throws Exception {
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:nightly?period=20").routeId("nightly").autoStartup(false)
                        .process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        Thread.sleep(FAST.attemptInterval().plus(Duration.ofMillis(200)).toMillis());

        assertEquals(ServiceStatus.Stopped, context.getRouteController().getRouteStatus("nightly"));
        assertEquals(0, processed.get());
        assertNull(state.live("lease/nightly"), "no lease for a route Camel does not start");
        assertFalse(manager.lease("nightly").isOpen());

        context.getRouteController().startRoute("nightly");
        await("the started route to take its lease and run", () -> processed.get() > 0, FAST.attemptInterval().plus(SLACK));
        assertEquals("pod-a", state.live("lease/nightly").value().get("holder").asText());
    }

    @Test
    void aRouteAddedAfterStartupRunsOnceItHoldsItsLease() throws Exception {
        install();
        context.start();

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:late?period=20").routeId("late").process(exchange -> processed.incrementAndGet());
            }
        });

        await("the late route to run", () -> processed.get() > 0, FAST.attemptInterval().plus(SLACK));
        assertEquals("pod-a", state.live("lease/late").value().get("holder").asText());
    }

    @Test
    void aMasterRouteKeepsResolvingItsOwnClusterService() throws Exception {
        FileLockClusterService fileLock = new FileLockClusterService();
        fileLock.setRoot(directory.resolve("locks").toString());
        fileLock.setAcquireLockDelay(50, TimeUnit.MILLISECONDS);
        fileLock.setAcquireLockInterval(100, TimeUnit.MILLISECONDS);
        context.addService(fileLock);
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("master:reports:timer:report?period=20").routeId("report")
                        .process(exchange -> processed.incrementAndGet());
            }
        });

        context.start();

        await("the master route to run on the single member", () -> processed.get() > 0, Duration.ofSeconds(10));
        assertTrue(context.getRoute("report").getRoutePolicyList().stream()
                .noneMatch(SingleActiveRoutePolicy.class::isInstance), "a master: route is left alone");
        assertNull(state.live("lease/report"));
    }

    @Test
    void shutdownCompletesWhileALeaseIsBeingLostWithWorkInFlight() throws Exception {
        install();
        context.getShutdownStrategy().setTimeout(5);
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:slow?period=20").routeId("slow").process(exchange -> {
                    if (processed.incrementAndGet() == 1) {
                        inFlight.countDown();
                        release.await(30, TimeUnit.SECONDS);
                    }
                });
            }
        });
        context.start();
        Lease lease = manager.lease("slow");
        await("work to be in flight", () -> inFlight.getCount() == 0, SLACK);

        state.reachable = false;
        clock.advance(FAST.stopDeadline().plusMillis(1));
        Thread stopper = new Thread(context::stop, "test-stop");
        stopper.start();
        Thread.sleep(300);
        release.countDown();
        stopper.join(Duration.ofSeconds(20).toMillis());

        assertFalse(stopper.isAlive(), () -> "shutdown did not complete:\n" + threadDump());
        assertNull(ManagementFactory.getThreadMXBean().findDeadlockedThreads(), threadDump());
        assertFalse(lease.isOpen());
        assertEquals(ServiceStatus.Stopped, context.getStatus());
    }

    @Test
    void aRouteRemovedAndAddedAgainIsGatedByItsNewLeaseAndPollsAgain() throws Exception {
        install();
        RouteBuilder files = new RouteBuilder() {
            @Override
            public void configure() {
                from("file:" + directory + "?initialDelay=0&delay=50&delete=true").routeId("poll-files")
                        .process(exchange -> processed.incrementAndGet());
            }
        };
        context.addRoutes(files);
        context.start();
        Files.writeString(directory.resolve("first.csv"), "1");
        await("the first file", () -> processed.get() == 1, SLACK);
        Lease first = manager.lease("poll-files");

        context.getRouteController().stopRoute("poll-files");
        assertTrue(context.removeRoute("poll-files"));
        await("the removed route's lease to be released", () -> state.live("lease/poll-files") == null, SLACK);
        assertFalse(first.isOpen());
        context.addRoutes(files);

        await("the re-added route to hold a new lease", () -> {
            Lease current = manager.current("poll-files");
            return current != null && current != first && current.isOpen();
        }, FAST.attemptInterval().plus(SLACK));
        Files.writeString(directory.resolve("second.csv"), "2");
        await("the re-added route to poll", () -> processed.get() == 2, SLACK);

        // The gate lives in the route's own consumer; the shared endpoint carries no gate.
        ScheduledPollEndpoint endpoint = (ScheduledPollEndpoint) context.getRoute("poll-files").getEndpoint();
        assertFalse(endpoint.getPollStrategy() instanceof GatedPollStrategy, "the endpoint is left untouched");
        ScheduledPollConsumer consumer = (ScheduledPollConsumer) context.getRoute("poll-files").getConsumer();
        assertTrue(consumer.getPollStrategy() instanceof GatedPollStrategy, "the consumer is gated");
        assertEquals("poll-files", ((GatedPollStrategy) consumer.getPollStrategy()).routeId());
    }

    @Test
    void stoppingARouteWaitsForItsStateCallInFlightSoTheReleasedLeaseIsReallyGone() throws Exception {
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("tick").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        await("the route to run", () -> processed.get() > 0, SLACK);
        // The next renewal's compare-and-set hangs in the store while the route is stopped.
        CountDownLatch renewing = new CountDownLatch(1);
        CountDownLatch answer = new CountDownLatch(1);
        state.beforeCompareAndSet = () -> {
            renewing.countDown();
            FakeKeyValueStore.blockOn(answer);
        };
        assertTrue(renewing.await(5, TimeUnit.SECONDS));

        Thread stopper = new Thread(() -> {
            try {
                context.getRouteController().stopRoute("tick");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, "stop-route");
        stopper.start();
        Thread.sleep(300);
        assertTrue(stopper.isAlive(), "the stop waits for the call in flight");
        state.beforeCompareAndSet = () -> {
        };
        answer.countDown();
        stopper.join(Duration.ofSeconds(10).toMillis());

        assertFalse(stopper.isAlive());
        assertNull(state.live("lease/tick"), "the renewal that was in flight did not survive the release");
    }

    @Test
    void restartingARouteWhileItsAttemptIsBlockedLeavesOneRenewalLoop() throws Exception {
        install();
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch answer = new CountDownLatch(1);
        state.beforePutIfAbsent = () -> {
            blocked.countDown();
            FakeKeyValueStore.blockOn(answer);
        };
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("tick").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        assertTrue(blocked.await(5, TimeUnit.SECONDS));

        context.getRouteController().stopRoute("tick");
        state.beforePutIfAbsent = () -> {
        };
        context.getRouteController().startRoute("tick");
        answer.countDown();
        await("the restarted route to run", () -> processed.get() > 0, FAST.attemptInterval().plus(SLACK));
        Thread.sleep(FAST.attemptInterval().toMillis());

        int before = state.calls.get();
        Thread.sleep(FAST.attemptInterval().multipliedBy(6).toMillis());
        int renewalCalls = state.calls.get() - before;
        // One loop: a read and a compare-and-set per attempt interval; two loops would make twice as many.
        assertTrue(renewalCalls <= 6 * 2 + 4, "one renewal loop, saw " + renewalCalls + " calls in six intervals");
        assertTrue(renewalCalls >= 6 * 2 - 4, "the loop is running, saw " + renewalCalls + " calls");
    }

    @Test
    void aResultForAPreviousActivationNeverTouchesTheCurrentOne() throws Exception {
        install();
        // The first acquisition creates its row, then its reply hangs well past the stop's wait.
        CountDownLatch created = new CountDownLatch(1);
        CountDownLatch reply = new CountDownLatch(1);
        AtomicInteger inserts = new AtomicInteger();
        state.afterPutIfAbsent = () -> {
            if (inserts.incrementAndGet() == 1) {
                created.countDown();
                FakeKeyValueStore.blockOn(reply);
            }
        };
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("tick").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        assertTrue(created.await(5, TimeUnit.SECONDS));
        Lease first = manager.current("tick");

        context.getRouteController().stopRoute("tick");
        context.getRouteController().startRoute("tick");
        Lease second = manager.current("tick");
        assertTrue(first != second, "a new activation has its own lease instance");
        assertTrue(first.isDetached());
        assertEquals(0, processed.get(), "the old row still blocks the new instance, nothing runs");

        reply.countDown();

        await("the new instance to acquire once the stale row is released", second::isOpen,
                FAST.attemptInterval().multipliedBy(2).plus(SLACK));
        assertFalse(first.isOpen(), "the stale grant landed on the dead instance only");
        assertTrue(first.isDetached());
        assertEquals(manager.current("tick"), second);
        await("the route to run on the new activation", () -> processed.get() > 0, SLACK);
        assertEquals("pod-a", state.live("lease/tick").value().get("holder").asText());
    }

    @Test
    void aConsumerThatFailsToStartTakesNoLease() throws Exception {
        context.addComponent("failing", new FailingPollComponent());
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("failing:orders").routeId("poll-orders").autoStartup(false)
                        .process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();

        assertThrows(Exception.class, () -> context.getRouteController().startRoute("poll-orders"));

        assertEquals(ServiceStatus.Stopped, context.getRouteController().getRouteStatus("poll-orders"));
        Thread.sleep(FAST.attemptInterval().multipliedBy(2).toMillis());
        assertNull(state.live("lease/poll-orders"), "a consumer that did not start takes no lease");
        assertFalse(manager.current("poll-orders").isOpen());
        assertEquals(0, state.calls.get(), "no attempt was made at all");
    }

    @Test
    void aReleaseNeverRunsNextToAnOutstandingRenewal() throws Exception {
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("tick").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        await("the route to run", () -> processed.get() > 0, SLACK);
        // The next renewal's compare-and-set hangs longer than the stop waits; it writes when let go.
        CountDownLatch casStarted = new CountDownLatch(1);
        CountDownLatch casGo = new CountDownLatch(1);
        CountDownLatch casDone = new CountDownLatch(1);
        state.beforeCompareAndSet = () -> {
            casStarted.countDown();
            FakeKeyValueStore.blockOn(casGo);
        };
        state.afterCompareAndSet = casDone::countDown;
        // A release that reads before the renewal wrote would delete at a stale version: let the renewal
        // land between the release's read and its delete, the worst case.
        state.beforeDelete = () -> {
            casGo.countDown();
            FakeKeyValueStore.blockOn(casDone);
        };
        assertTrue(casStarted.await(5, TimeUnit.SECONDS));

        context.getRouteController().stopRoute("tick");
        state.beforeCompareAndSet = () -> {
        };
        casGo.countDown();

        await("the lease to be released once the renewal settled", () -> state.live("lease/tick") == null,
                FAST.callTimeout().plus(SLACK));
        assertFalse(manager.current("tick").isOpen());
    }

    @Test
    void aPollRouteWithoutARegistrationFailsClosed() throws Exception {
        RecordingPollStrategy recording = new RecordingPollStrategy();
        context.getRegistry().bind("recording", recording);
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("file:" + directory + "?pollStrategy=#recording&initialDelay=0&delay=50&delete=true")
                        .routeId("poll-files").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        Files.writeString(directory.resolve("first.csv"), "1");
        await("the first file", () -> processed.get() == 1, SLACK);

        // The registration disappears while the route keeps running: an unexpected state.
        manager.release("poll-files");
        assertNull(manager.current("poll-files"));
        Thread.sleep(200);
        int beginsAfter = recording.begins.get();
        Files.writeString(directory.resolve("second.csv"), "2");
        Thread.sleep(600);

        assertEquals(ServiceStatus.Started, context.getRouteController().getRouteStatus("poll-files"));
        assertEquals(beginsAfter, recording.begins.get(), "no poll begins without a registration");
        assertTrue(Files.exists(directory.resolve("second.csv")));
        assertEquals(1, processed.get());
    }

    @Test
    void aTriggerRouteWithoutARegistrationFailsClosed() throws Exception {
        AtomicInteger created = new AtomicInteger();
        context.getManagementStrategy().addEventNotifier(new EventNotifierSupport() {
            @Override
            public void notify(CamelEvent event) {
                if (event instanceof CamelEvent.ExchangeCreatedEvent) {
                    created.incrementAndGet();
                }
            }
        });
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("tick").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        await("the route to run", () -> processed.get() > 0, SLACK);

        manager.release("tick");
        assertNull(manager.current("tick"));
        Thread.sleep(200);
        int processedAfter = processed.get();
        int createdAfter = created.get();
        Thread.sleep(600);

        assertTrue(created.get() > createdAfter + 5, "the timer keeps firing");
        assertEquals(processedAfter, processed.get(), "no processor runs without a registration");
        assertEquals(ServiceStatus.Started, context.getRouteController().getRouteStatus("tick"));
    }

    @Test
    void aManuallyStartedOneShotTriggerKeepsItsTickWhenAnotherPolicyDelaysOnStart() throws Exception {
        // A policy created before ours makes onStart late; the consumer has started by then.
        context.addRoutePolicyFactory((camelContext, routeId, route) -> new RoutePolicySupport() {
            @Override
            public void onStart(Route route) {
                try {
                    Thread.sleep(300);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:once?delay=0&repeatCount=1").routeId("once").autoStartup(false)
                        .process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        assertNull(state.live("lease/once"), "no lease while the route is stopped");

        context.getRouteController().startRoute("once");

        await("the only tick to run", () -> processed.get() == 1, FAST.startupWait().plus(SLACK));

        // Stopped and started again, the route is a new activation with a new lease instance.
        context.getRouteController().stopRoute("once");
        assertNull(state.live("lease/once"));
        context.getRouteController().startRoute("once");

        await("the restarted route's only tick to run", () -> processed.get() == 2, FAST.startupWait().plus(SLACK));
    }

    @Test
    void aDeadInstancesDeferredReleaseLeavesAReplacementAcquiredAtTheSameMillisecondAlone() throws Exception {
        // The test clock never moves: both activations write the same holder and acquiredAt.
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("tick").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        await("the route to run", () -> processed.get() > 0, SLACK);
        await("one renewal", () -> state.live("lease/tick").version() >= 2, FAST.attemptInterval().plus(SLACK));
        // The next renewal reads, then its compare-and-set hangs longer than the stop waits.
        CountDownLatch casStarted = new CountDownLatch(1);
        CountDownLatch casGo = new CountDownLatch(1);
        state.beforeCompareAndSet = () -> {
            casStarted.countDown();
            FakeKeyValueStore.blockOn(casGo);
        };
        assertTrue(casStarted.await(5, TimeUnit.SECONDS));

        context.getRouteController().stopRoute("tick");
        state.beforeCompareAndSet = () -> {
        };
        // An operator deletes the stale row; the restarted route acquires a replacement at once.
        state.delete("lease/tick");
        context.getRouteController().startRoute("tick");
        await("the replacement lease", () -> manager.current("tick").isHeld(), FAST.attemptInterval().plus(SLACK));
        FakeKeyValueStore.Row replacement = state.live("lease/tick");
        assertEquals(1, replacement.version());
        int deletesBefore = state.versionedDeletes.get();

        casGo.countDown();
        Thread.sleep(FAST.callTimeout().plus(Duration.ofMillis(500)).toMillis());

        // Had the dead instance deleted the replacement, the new instance would have lost and re-acquired
        // its lease meanwhile with a new value object; the row must still carry the replacement's own value.
        FakeKeyValueStore.Row survivor = state.live("lease/tick");
        assertNotNull(survivor, "the dead instance's release did not delete the replacement");
        assertTrue(survivor.value() == replacement.value(), "the replacement's lease was never deleted and re-acquired");
        assertEquals(deletesBefore, state.versionedDeletes.get(), "no lease was deleted since the replacement");
        assertTrue(manager.current("tick").isOpen());
    }

    @Test
    void aRouteCamelExcludesFromAutoStartupTakesNoLease() throws Exception {
        context.setAutoStartupExcludePattern("nightly");
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:nightly?period=20").routeId("nightly").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        Thread.sleep(FAST.attemptInterval().multipliedBy(2).toMillis());

        assertEquals(ServiceStatus.Stopped, context.getRouteController().getRouteStatus("nightly"));
        assertNull(state.live("lease/nightly"), "a route Camel does not start takes no lease, whatever its own flag says");
        assertFalse(manager.lease("nightly").isOpen());
        assertEquals(0, processed.get());
    }

    @Test
    void aPollEnrichOnTheEndpointOfADisabledRouteIsNotGated() throws Exception {
        install();
        String uri = "file:" + directory + "?initialDelay=0&delay=50&delete=true";
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from(uri).routeId("poll-files").autoStartup(false).process(exchange -> processed.incrementAndGet());
                from("direct:enrich").routeId("enrich").pollEnrich(uri, 2_000);
            }
        });
        context.start();
        Files.writeString(directory.resolve("order.csv"), "order");

        String enriched = context.createProducerTemplate().requestBody("direct:enrich", "", String.class);

        assertEquals("order", enriched, "the disabled route's closed gate does not apply to another consumer of the endpoint");
        assertEquals(0, processed.get());
        assertEquals(ServiceStatus.Stopped, context.getRouteController().getRouteStatus("poll-files"));
    }

    @Test
    void stoppingARouteReleasesItsLeaseAndStartingItAcquiresAgain() throws Exception {
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("tick").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        await("the route to run", () -> processed.get() > 0, SLACK);
        assertEquals("pod-a", state.live("lease/tick").value().get("holder").asText());

        context.getRouteController().stopRoute("tick");

        assertNull(state.live("lease/tick"), "a stopped route releases its lease so another replica can take over");
        assertFalse(manager.lease("tick").isOpen());
        Thread.sleep(FAST.attemptInterval().multipliedBy(2).toMillis());
        assertNull(state.live("lease/tick"), "and does not acquire it again while stopped");

        int before = processed.get();
        context.getRouteController().startRoute("tick");
        await("the started route to acquire and run again", () -> processed.get() > before, FAST.attemptInterval().plus(SLACK));
        assertEquals("pod-a", state.live("lease/tick").value().get("holder").asText());
    }

    @Test
    void aOneShotTriggerAddedAtRuntimeRunsItsOnlyTickOnceTheLeaseIsAcquired() throws Exception {
        install();
        context.start();
        // The store answers slowly: the only tick fires while this replica's acquisition is in flight.
        state.afterPutIfAbsent = () -> {
            try {
                Thread.sleep(400);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:once?delay=0&repeatCount=1").routeId("once").process(exchange -> processed.incrementAndGet());
            }
        });

        await("the one-shot tick to run", () -> processed.get() == 1, FAST.startupWait().plus(SLACK));
        assertEquals("pod-a", state.live("lease/once").value().get("holder").asText());
    }

    @Test
    void noGateOpensBeforeTheContractCheckPassedEvenWithoutTheStateTable() throws Exception {
        contractCheck.set(ContractVersionStartupGate.Outcome.PENDING);
        state.provisioned = false;
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("tick").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        Thread.sleep(FAST.attemptInterval().multipliedBy(2).toMillis());

        assertEquals(ServiceStatus.Started, context.getRouteController().getRouteStatus("tick"));
        assertEquals(0, processed.get(), "no work before the contract check, not even the uncoordinated fallback");
        assertFalse(manager.lease("tick").isOpen());
        assertTrue(state.rows.isEmpty(), "no lease attempted");

        state.provisioned = true;
        contractCheck.set(ContractVersionStartupGate.Outcome.PASSED);
        manager.contractVerified();

        await("the lease to be acquired once the check passed", () -> processed.get() > 0, SLACK);
        assertEquals("pod-a", state.live("lease/tick").value().get("holder").asText());
    }

    @Test
    void noGateEverOpensWhenTheContractCheckFailed() throws Exception {
        contractCheck.set(ContractVersionStartupGate.Outcome.PENDING);
        install();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("tick").process(exchange -> processed.incrementAndGet());
            }
        });
        context.start();
        contractCheck.set(ContractVersionStartupGate.Outcome.FAILED);
        manager.contractVerified();
        state.provisioned = false;
        Thread.sleep(FAST.attemptInterval().multipliedBy(3).toMillis());

        assertEquals(0, processed.get());
        assertFalse(manager.lease("tick").isOpen());
        assertTrue(state.rows.isEmpty());
    }

    private void install() throws Exception {
        context.addService(manager);
        context.addRoutePolicyFactory(new SingleActiveRoutePolicyFactory(manager));
        context.addLifecycleStrategy(new SingleActiveConsumerStrategy(manager));
    }

    private static String threadDump() {
        ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        return Arrays.stream(threads.dumpAllThreads(true, true))
                .map(ThreadInfo::toString)
                .collect(Collectors.joining());
    }

    /** A scheduled poll endpoint whose consumer cannot start, like an SFTP consumer that cannot connect. */
    static final class FailingPollComponent extends DefaultComponent {
        @Override
        protected Endpoint createEndpoint(String uri, String remaining, java.util.Map<String, Object> parameters) {
            return new ScheduledPollEndpoint(uri, this) {
                @Override
                public org.apache.camel.Producer createProducer() {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Consumer createConsumer(Processor processor) throws Exception {
                    ScheduledPollConsumer consumer = new ScheduledPollConsumer(this, processor) {
                        @Override
                        protected int poll() {
                            return 0;
                        }

                        @Override
                        protected void doStart() {
                            throw new IllegalStateException("cannot connect");
                        }
                    };
                    configureConsumer(consumer);
                    return consumer;
                }
            };
        }
    }

    static final class RecordingPollStrategy implements PollingConsumerPollStrategy {
        final AtomicInteger begins = new AtomicInteger();
        final AtomicInteger commits = new AtomicInteger();
        final AtomicInteger rollbacks = new AtomicInteger();

        @Override
        public boolean begin(Consumer consumer, Endpoint endpoint) {
            begins.incrementAndGet();
            return true;
        }

        @Override
        public void commit(Consumer consumer, Endpoint endpoint, int polledMessages) {
            commits.incrementAndGet();
        }

        @Override
        public boolean rollback(Consumer consumer, Endpoint endpoint, int retryCounter, Exception cause) {
            rollbacks.incrementAndGet();
            return false;
        }
    }
}
