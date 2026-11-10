package no.basis.integrationplatform.camel.lease;

import no.basis.integrationplatform.camel.TestJdbc;
import no.basis.integrationplatform.camel.lifecycle.ContractVersionStartupGate;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStore;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStoreException;
import no.basis.integrationplatform.camel.keyvalue.TestKeyValueStores;
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
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Certification scenarios S39-S44 of the draft contract at the level of the
 * lease protocol: two replicas share one Message Store and the work is the
 * gate of the {@code poll-orders} lease. Timing is shortened to a 3 s lease,
 * 500 ms attempts and a 2 s deadline. Route-level behaviour, with real polls,
 * is covered by {@link SingleActiveRouteTest}.
 */
@Testcontainers
class PostgresLeaseTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");

    static final LeaseTiming FAST = new LeaseTiming(Duration.ofSeconds(3), Duration.ofMillis(500),
            Duration.ofMillis(200), Duration.ofSeconds(2), Duration.ofSeconds(2), Duration.ofSeconds(1));
    private static final Duration SLACK = Duration.ofMillis(1_500);
    private static final String INTEGRATION = "orders-sync";
    private static final String NAME = "poll-orders";
    private static final String KEY = "lease/" + NAME;

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
            replica.manager.stop();
        }
    }

    @Test
    void exactlyOneReplicaOpensItsGateAndTheLeaseNamesIt() throws Exception {
        Replica a = replica("pod-a");
        Replica b = replica("pod-b");

        await("one open gate", () -> a.isOpen() ^ b.isOpen(), FAST.attemptInterval().plus(SLACK));
        Replica leader = a.isOpen() ? a : b;
        Replica other = leader == a ? b : a;
        Thread.sleep(FAST.attemptInterval().multipliedBy(3).toMillis());

        assertTrue(leader.isOpen());
        assertFalse(other.isOpen());
        assertEquals(leader.holder(), holderOf(KEY));
        assertEquals(FAST.leaseDuration().toSeconds(), count(
                "select extract(epoch from expires_at - updated_at)::bigint from bip_key_value"),
                "the expiry is the lease duration after the last write");
        assertTrue(count("select version from bip_key_value") > 1, "the holder renews the lease");
        assertEquals(1, count("select count(*) from bip_key_value"));
    }

    @Test
    void anOrderlyStopReleasesTheLeaseAndTheOtherReplicaTakesOverAtItsNextAttempt() throws Exception {
        Replica a = replica("pod-a");
        Replica b = replica("pod-b");
        await("one open gate", () -> a.isOpen() ^ b.isOpen(), FAST.attemptInterval().plus(SLACK));
        Replica leader = a.isOpen() ? a : b;
        Replica other = leader == a ? b : a;

        long stopped = System.nanoTime();
        leader.manager.stop();

        assertEquals(0, count("select count(*) from bip_key_value"), "the lease is deleted on release");
        assertFalse(leader.isOpen());
        await("the other replica to take over", other::isOpen, FAST.attemptInterval().plus(SLACK));
        assertTrue(Duration.ofNanos(System.nanoTime() - stopped).compareTo(FAST.leaseDuration()) < 0,
                "the takeover happens at the next attempt, not after the expiry");
        assertEquals(other.holder(), holderOf(KEY));
    }

    @Test
    void aHolderThatStopsWritingClosesAtItsDeadlineAndIsReplacedOnlyAfterItsLeaseExpires() throws Exception {
        Replica a = replica("pod-a");
        Replica b = replica("pod-b");
        await("one open gate", () -> a.isOpen() ^ b.isOpen(), FAST.attemptInterval().plus(SLACK));
        Replica leader = a.isOpen() ? a : b;
        Replica other = leader == a ? b : a;

        long cut = System.nanoTime();
        leader.state.reachable = false;

        await("the holder to close at its deadline", () -> !leader.isOpen(), FAST.stopDeadline().plus(SLACK));
        long holderClosed = System.nanoTime();
        assertTrue(Duration.ofNanos(holderClosed - cut).compareTo(FAST.stopDeadline().plus(SLACK)) < 0);
        assertFalse(other.isOpen(), "the lease is honoured until it expires");

        await("the other replica to take over after the expiry", other::isOpen,
                FAST.leaseDuration().plus(FAST.attemptInterval()).plus(SLACK));
        long takeover = System.nanoTime();
        // The last successful write happened at most one attempt interval before the cut.
        assertTrue(Duration.ofNanos(takeover - cut).compareTo(FAST.leaseDuration().minus(FAST.attemptInterval())) >= 0);
        assertTrue(holderClosed < takeover, "the gates are never open on both replicas");
        assertEquals(other.holder(), holderOf(KEY));

        leader.state.reachable = true;
        Thread.sleep(FAST.attemptInterval().multipliedBy(3).toMillis());
        assertFalse(leader.isOpen(), "a recovered replica waits while the other holds the lease");
        assertTrue(other.isOpen());
    }

    @Test
    void anOperatorDeleteHandsTheWorkOverAtTheNextRenewal() throws Exception {
        Replica a = replica("pod-a");
        Replica b = replica("pod-b");
        await("one open gate", () -> a.isOpen() ^ b.isOpen(), FAST.attemptInterval().plus(SLACK));
        Replica leader = a.isOpen() ? a : b;
        long leaderVersionBefore = count("select version from bip_key_value");

        execute("delete from bip_key_value where key = '" + KEY + "'");

        await("a new lease", () -> count("select count(*) from bip_key_value") == 1,
                FAST.attemptInterval().multipliedBy(2).plus(SLACK));
        await("exactly one holder", () -> oneHolder(a, b), FAST.attemptInterval().plus(SLACK));
        Thread.sleep(FAST.attemptInterval().multipliedBy(2).toMillis());
        assertTrue(a.isOpen() ^ b.isOpen());
        assertEquals((a.isOpen() ? a : b).holder(), holderOf(KEY));
        assertTrue(leaderVersionBefore >= 1 && count("select version from bip_key_value") >= 1);
        if (leader.isOpen()) {
            assertTrue(leader.lease.isHeld(), "the same replica acquired a new lease rather than keeping the old one");
        }
    }

    @Test
    void withoutTheStateTableEveryReplicaOpensUntilTheTableExists() throws Exception {
        execute("drop table bip_key_value");
        Replica a = replica("pod-a");
        Replica b = replica("pod-b");

        await("both replicas to run uncoordinated", () -> a.isOpen() && b.isOpen(),
                FAST.attemptInterval().plus(SLACK));

        execute(schemaSql("bip_key_value.sql"));

        await("one holder once the table exists", () -> oneHolder(a, b),
                FAST.attemptInterval().multipliedBy(2).plus(SLACK));
        Replica leader = a.isOpen() ? a : b;
        Replica other = leader == a ? b : a;
        assertEquals(leader.holder(), holderOf(KEY));
        Thread.sleep(FAST.attemptInterval().multipliedBy(2).toMillis());
        assertTrue(leader.isOpen());
        assertFalse(other.isOpen());
    }

    private Replica replica(String holder) {
        SwitchableState state = new SwitchableState(TestKeyValueStores.postgres(dataSource, INTEGRATION));
        LeaseManager manager = new LeaseManager(holder, () -> state, () -> ContractVersionStartupGate.Outcome.PASSED, FAST, System::nanoTime, Clock.systemUTC());
        Replica replica = new Replica(manager, state, manager.lease(NAME));
        replicas.add(replica);
        manager.activate(NAME);
        manager.start();
        return replica;
    }

    /** Exactly one replica's gate is open and that replica holds the lease (not a fallback gate closing). */
    private static boolean oneHolder(Replica a, Replica b) {
        return (a.isOpen() ^ b.isOpen()) && (a.isOpen() ? a : b).lease.isHeld();
    }

    private record Replica(LeaseManager manager, SwitchableState state, Lease lease) {
        private boolean isOpen() {
            return lease.isOpen();
        }

        private String holder() {
            return manager.holder();
        }
    }

    static final class SwitchableState extends DelegatingKeyValueStore {
        volatile boolean reachable = true;

        SwitchableState(KeyValueStore delegate) {
            super(delegate);
        }

        @Override
        protected void before(String operation) {
            if (!reachable) {
                throw new KeyValueStoreException("Key-Value Store failed to " + operation,
                        new SQLException("connection refused", "08001"));
            }
        }
    }

    interface Condition {
        boolean test() throws Exception;
    }

    static void await(String description, Condition condition, Duration timeout) throws Exception {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.test()) {
                return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Timed out waiting for " + description);
    }

    private String holderOf(String key) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(
                     "select value->>'holder' from bip_key_value where key = '" + key + "'")) {
            return resultSet.next() ? resultSet.getString(1) : null;
        }
    }

    private long count(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getLong(1);
        }
    }

    private void execute(String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private DataSource dataSource() {
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
