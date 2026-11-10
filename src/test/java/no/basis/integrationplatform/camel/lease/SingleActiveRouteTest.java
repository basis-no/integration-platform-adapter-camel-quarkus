package no.basis.integrationplatform.camel.lease;

import no.basis.integrationplatform.camel.TestJdbc;
import no.basis.integrationplatform.camel.lifecycle.ContractVersionStartupGate;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStore;
import no.basis.integrationplatform.camel.keyvalue.TestKeyValueStores;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.support.PluginHelper;
import org.apache.camel.support.ResourceHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import javax.sql.DataSource;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static no.basis.integrationplatform.camel.lease.PostgresLeaseTest.FAST;
import static no.basis.integrationplatform.camel.lease.PostgresLeaseTest.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real routes on two Camel contexts that share one Message Store: the work
 * runs on the lease holder only, counted as route executions rather than
 * flags, through an orderly handover and through a holder that loses the
 * database while one of its routes has work in flight.
 */
@Testcontainers
class SingleActiveRouteTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    private static final Duration SLACK = Duration.ofMillis(1_500);
    private static final String ROUTES = """
            - route:
                id: poll-orders
                from:
                  uri: "timer:{{orders.timer}}?period=50"
                  steps:
                    - to: "direct:work-poll-orders"
            - route:
                id: nightly-report
                autoStartup: "{{nightly.enabled}}"
                from:
                  uri: "timer:nightly?period=50"
                  steps:
                    - to: "direct:work-nightly-report"
            """;

    private DataSource dataSource;
    private final List<Replica> replicas = new ArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        dataSource = dataSource();
        TestJdbc.awaitReachable(dataSource);
        execute("drop table if exists bip_key_value");
        execute("drop table if exists bip_contract");
        execute(schemaSql("bip_contract.sql"));
        execute(schemaSql("bip_key_value.sql"));
    }

    @AfterEach
    void tearDown() {
        for (Replica replica : replicas) {
            replica.release.countDown();
            replica.context.stop();
        }
    }

    @Test
    void theSameTimerRouteRunsOnOneReplicaAndHandsOverAfterItStopped() throws Exception {
        Replica a = replica("pod-a");
        Replica b = replica("pod-b");
        a.context.start();
        b.context.start();

        await("one replica to poll", () -> a.runs("poll-orders") > 0 ^ b.runs("poll-orders") > 0,
                FAST.attemptInterval().plus(SLACK));
        Replica leader = a.runs("poll-orders") > 0 ? a : b;
        Replica other = leader == a ? b : a;
        Thread.sleep(FAST.attemptInterval().multipliedBy(3).toMillis());

        assertEquals(0, other.runs("poll-orders"), "the route does no work on the replica without the lease");
        assertTrue(leader.runs("poll-orders") > 10);
        assertEquals(leader.holder, holderOf("lease/poll-orders"));
        for (Replica replica : List.of(a, b)) {
            assertEquals(ServiceStatus.Started, replica.status("poll-orders"), "the route is started everywhere");
            assertEquals(ServiceStatus.Stopped, replica.status("nightly-report"), "autoStartup=false stays stopped");
            assertEquals(0, replica.runs("nightly-report"));
            assertTrue(replica.policies("poll-orders").stream().anyMatch(SingleActiveRoutePolicy.class::isInstance));
            for (String routeId : List.of("transform", "consume", "work-poll-orders")) {
                assertTrue(replica.policies(routeId).stream().noneMatch(SingleActiveRoutePolicy.class::isInstance),
                        routeId + " is not coordinated");
            }
        }
        assertNull(holderOf("lease/nightly-report"), "a route Camel does not start takes no lease");

        long stopped = System.nanoTime();
        leader.context.stop();

        ServiceStatus statusAtRelease = leader.routeStatusAtRelease.get();
        assertTrue(statusAtRelease == ServiceStatus.Stopping || statusAtRelease == ServiceStatus.Stopped,
                "the lease is released after the route's consumer stopped, saw " + statusAtRelease);
        assertEquals(0, leader.inFlightAtRelease.get(), "and after its exchanges in flight finished");
        String holderAfterStop = holderOf("lease/poll-orders");
        assertTrue(holderAfterStop == null || other.holder.equals(holderAfterStop),
                "an orderly stop deletes the stopped replica's lease (the other may already hold a new one): " + holderAfterStop);
        long lastRunOnLeader = leader.lastStart("poll-orders");

        await("the other replica to take over", () -> other.runs("poll-orders") > 0, FAST.attemptInterval().plus(SLACK));
        long firstRunOnOther = other.firstStart("poll-orders");
        assertTrue(lastRunOnLeader < firstRunOnOther, "the work never runs on both replicas");
        assertTrue(Duration.ofNanos(firstRunOnOther - stopped).compareTo(FAST.leaseDuration()) < 0,
                "the handover happens at the next attempt, not after the expiry");
        assertEquals(other.holder, holderOf("lease/poll-orders"));
    }

    @Test
    void aHolderThatLosesTheDatabaseDoesNoNewWorkAfterItsDeadlineWhileWorkInFlightFinishes() throws Exception {
        Replica a = replica("pod-a");
        a.context.start();
        await("replica a to run both routes", () -> a.runs("poll-orders") > 0 && a.runs("poll-ledger") > 0,
                FAST.attemptInterval().plus(SLACK));
        Replica b = replica("pod-b");
        b.context.start();
        Thread.sleep(FAST.attemptInterval().multipliedBy(2).toMillis());
        assertEquals(0, b.runs("poll-orders") + b.runs("poll-ledger"), "replica b waits for both leases");

        // The next poll-ledger run on a blocks: work in flight while the database goes away.
        a.blockNext.set("poll-ledger");
        await("poll-ledger to be in flight on a", () -> a.inFlight.getCount() == 0, SLACK);
        int ledgerRunsAtBlock = a.runs("poll-ledger");
        long cut = System.nanoTime();
        a.state.reachable = false;

        await("replica b to take over poll-orders", () -> b.runs("poll-orders") > 0,
                FAST.leaseDuration().plus(FAST.attemptInterval()).plus(SLACK));
        Thread.sleep(FAST.attemptInterval().multipliedBy(2).toMillis());

        long deadline = cut + FAST.stopDeadline().toNanos() + Duration.ofMillis(300).toNanos();
        assertTrue(a.lastStart("poll-orders") < deadline,
                "a's poll-orders did no new work after its deadline although poll-ledger was blocked in flight");
        assertTrue(a.lastStart("poll-orders") < b.firstStart("poll-orders"), "poll-orders never ran on both");
        assertEquals(ledgerRunsAtBlock, a.runs("poll-ledger"), "no new poll-ledger run started on a after the block");
        assertEquals(1, a.inFlightStarts("poll-ledger"), "the blocked exchange is still in flight");

        a.release.countDown();
        await("the in-flight exchange to finish normally", () -> a.inFlightStarts("poll-ledger") == 0, SLACK);
        await("replica b to take over poll-ledger", () -> b.runs("poll-ledger") > 0,
                FAST.leaseDuration().plus(FAST.attemptInterval()).plus(SLACK));
        Thread.sleep(FAST.attemptInterval().toMillis());
        assertEquals(ledgerRunsAtBlock, a.runs("poll-ledger"), "a started no poll-ledger work after its deadline");
        assertTrue(a.lastStart("poll-ledger") < b.firstStart("poll-ledger"), "poll-ledger never ran on both");
        assertEquals(b.holder, holderOf("lease/poll-orders"));
        assertEquals(b.holder, holderOf("lease/poll-ledger"));
    }

    private Replica replica(String holder) throws Exception {
        Replica replica = new Replica(holder);
        replicas.add(replica);
        DefaultCamelContext context = replica.context;
        context.getShutdownStrategy().setTimeout(10);
        KeyValueStore state = new ReleaseObservingState(replica.state, replica);
        LeaseManager manager = new LeaseManager(holder, () -> state, () -> ContractVersionStartupGate.Outcome.PASSED, FAST, System::nanoTime, Clock.systemUTC());
        context.addService(manager);
        context.addRoutePolicyFactory(new SingleActiveRoutePolicyFactory(manager));
        context.addLifecycleStrategy(new SingleActiveConsumerStrategy(manager));
        context.getPropertiesComponent().addInitialProperty("orders.timer", "orders");
        context.getPropertiesComponent().addInitialProperty("nightly.enabled", "false");
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:ledger?period=50").routeId("poll-ledger").to("direct:work-poll-ledger");
                for (String routeId : List.of("poll-orders", "poll-ledger", "nightly-report")) {
                    from("direct:work-" + routeId).routeId("work-" + routeId).process(exchange -> replica.work(routeId));
                }
                from("direct:transform").routeId("transform").log("transform");
                from("kafka:orders?brokers=localhost:1").routeId("consume").autoStartup(false).log("consume");
            }
        });
        PluginHelper.getRoutesLoader(context).loadRoutes(ResourceHelper.fromString("orders.yaml", ROUTES));
        return replica;
    }

    private static final class Replica {
        private final String holder;
        private final DefaultCamelContext context = new DefaultCamelContext();
        private final PostgresLeaseTest.SwitchableState state;
        private final Map<String, List<Long>> starts = new ConcurrentHashMap<>();
        private final Map<String, List<Long>> completions = new ConcurrentHashMap<>();
        private final AtomicReference<String> blockNext = new AtomicReference<>();
        private final CountDownLatch inFlight = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicReference<ServiceStatus> routeStatusAtRelease = new AtomicReference<>();
        private final AtomicReference<Integer> inFlightAtRelease = new AtomicReference<>(-1);

        private Replica(String holder) {
            this.holder = holder;
            this.state = new PostgresLeaseTest.SwitchableState(TestKeyValueStores.postgres(dataSourceOf(), "orders-sync"));
        }

        private void work(String routeId) throws InterruptedException {
            starts.computeIfAbsent(routeId, id -> new CopyOnWriteArrayList<>()).add(System.nanoTime());
            if (routeId.equals(blockNext.getAndUpdate(current -> routeId.equals(current) ? null : current))) {
                inFlight.countDown();
                if (!release.await(30, TimeUnit.SECONDS)) {
                    throw new AssertionError("the in-flight exchange was never released");
                }
            }
            completions.computeIfAbsent(routeId, id -> new CopyOnWriteArrayList<>()).add(System.nanoTime());
        }

        private int runs(String routeId) {
            return starts.getOrDefault(routeId, List.of()).size();
        }

        private int completed(String routeId) {
            return completions.getOrDefault(routeId, List.of()).size();
        }

        private int inFlightStarts(String routeId) {
            return runs(routeId) - completed(routeId);
        }

        private long firstStart(String routeId) {
            return starts.get(routeId).stream().mapToLong(Long::longValue).min().orElseThrow();
        }

        private long lastStart(String routeId) {
            return starts.get(routeId).stream().mapToLong(Long::longValue).max().orElseThrow();
        }

        private ServiceStatus status(String routeId) {
            return context.getRouteController().getRouteStatus(routeId);
        }

        private List<RoutePolicy> policies(String routeId) {
            return context.getRoute(routeId).getRoutePolicyList();
        }
    }

    /** Records what the coordinated route was doing when its lease was deleted. */
    private static final class ReleaseObservingState extends DelegatingKeyValueStore {
        private final Replica replica;

        private ReleaseObservingState(KeyValueStore delegate, Replica replica) {
            super(delegate);
            this.replica = replica;
        }

        @Override
        protected void before(String operation) {
            if ("delete".equals(operation)) {
                // Camel calls onStop while the route service is still Stopping, after the graceful stop drained it.
                replica.routeStatusAtRelease.set(replica.status("poll-orders"));
                replica.inFlightAtRelease.set(replica.context.getInflightRepository().size("poll-orders"));
            }
        }
    }

    private String holderOf(String key) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "select value->>'holder' from bip_key_value where key = '" + key + "'")) {
            return resultSet.next() ? resultSet.getString(1) : null;
        }
    }

    private void execute(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private DataSource dataSource() {
        return dataSourceOf();
    }

    private static DataSource dataSourceOf() {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(POSTGRES.getJdbcUrl());
        dataSource.setUser(POSTGRES.getUsername());
        dataSource.setPassword(POSTGRES.getPassword());
        return dataSource;
    }

    private String schemaSql(String resource) throws Exception {
        try (InputStream input = getClass().getResourceAsStream((resource.equals("bip_key_value.sql") ? "/db/key-value/" : "/db/message-store/") + resource)) {
            if (input == null) {
                throw new AssertionError("Missing schema resource " + resource);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
