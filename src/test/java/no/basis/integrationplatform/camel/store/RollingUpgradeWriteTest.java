package no.basis.integrationplatform.camel.store;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.TestJdbc;
import no.basis.integrationplatform.camel.lifecycle.MessageStoreRoutePolicy;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.RoutePolicy;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A rolling upgrade from the released 0.3.6 adapter: its pods and this build's pods write the
 * same messages to one Message Store. The released adapter runs from its Maven Central jar on a
 * class loader of its own, which loads the adapter's classes from that jar and everything else
 * (Camel, Kafka, JDBC) from this build, as the two versions share them at runtime.
 */
@Testcontainers
class RollingUpgradeWriteTest {
    @Container
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:16-alpine");
    @Container
    private static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    private static ReleasedAdapter released;
    private DataSource dataSource;

    @BeforeAll
    static void loadReleasedAdapter() throws Exception {
        released = ReleasedAdapter.load();
    }

    @BeforeEach
    void schema() throws Exception {
        PGSimpleDataSource pg = new PGSimpleDataSource();
        pg.setURL(POSTGRES.getJdbcUrl());
        pg.setUser(POSTGRES.getUsername());
        pg.setPassword(POSTGRES.getPassword());
        dataSource = pg;
        TestJdbc.awaitReachable(dataSource);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             InputStream in = getClass().getResourceAsStream("/db/message-store/bip_message_store.sql")) {
            statement.execute("drop table if exists bip_message_replay_audit, bip_message_attempt, bip_message_log cascade");
            for (String sql : new String(in.readAllBytes(), StandardCharsets.UTF_8).split(";")) {
                if (!sql.isBlank()) statement.execute(sql);
            }
        }
    }

    @Test
    void bothVersionsWritingTheSameAttemptAtOnceDoNotDeadlock() throws Exception {
        Object releasedSink = released.sink(dataSource);
        var current = new PostgresMessageStoreSink(dataSource);
        var failures = new CopyOnWriteArrayList<Throwable>();
        int messages = 300;
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            for (int i = 0; i < messages; i++) {
                String id = "upgrade-" + i;
                Instant at = Instant.parse("2026-11-10T12:00:00Z").plusSeconds(i);
                var barrier = new CyclicBarrier(2);
                List<Future<?>> writes = new ArrayList<>();
                writes.add(pool.submit(() -> {
                    await(barrier);
                    try {
                        released.record(releasedSink, id, at);
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                }));
                writes.add(pool.submit(() -> {
                    await(barrier);
                    try {
                        current.record(result(id, at));
                    } catch (Throwable e) {
                        failures.add(e);
                    }
                }));
                for (Future<?> write : writes) write.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        // Both versions take the attempt row first, then the summary, so neither waits for the other in a cycle.
        assertEquals(List.of(), failures.stream().map(RollingUpgradeWriteTest::rootMessage).toList());
        assertEquals(Integer.toString(messages), value("select count(*) from bip_message_log"));
        assertEquals(Integer.toString(messages), value("select count(*) from bip_message_attempt"));
        assertConsistentRows();
    }

    @Test
    void releasedAndCurrentConsumersInOneGroupRecordEveryMessageOnce() throws Exception {
        String topic = "orders-" + UUID.randomUUID();
        String group = "orders-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", brokers))) {
            admin.createTopics(List.of(new NewTopic(topic, 4, (short) 1))).all().get(20, TimeUnit.SECONDS);
        }

        var failures = new CopyOnWriteArrayList<Throwable>();
        var oldPodWrites = new AtomicInteger();
        var newPodWrites = new AtomicInteger();
        var current = new PostgresMessageStoreSink(dataSource);
        MessageStoreSink countingCurrent = row -> {
            try {
                current.record(row);
                newPodWrites.incrementAndGet();
            } catch (RuntimeException e) {
                failures.add(e);
                throw e;
            }
        };
        Object releasedSink = released.countingSink(released.sink(dataSource), oldPodWrites, failures);
        String uri = "kafka:" + topic + "?brokers=" + brokers + "&groupId=" + group
                + "&autoOffsetReset=earliest&autoCommitEnable=false&allowManualCommit=true&breakOnFirstError=true"
                + "&maxPollRecords=5&pollTimeoutMs=100";

        // Records keep arriving while the pods change, as in a live rolling upgrade, so the new pod
        // has work after its group join whenever the rebalance completes.
        int produced;
        try (var traffic = new Traffic(brokers, topic);
             var oldPod = consumer(uri, released.policy(releasedSink, "orders"));
             var newPod = consumer(uri, new MessageStoreRoutePolicy(countingCurrent, "orders"))) {
            oldPod.start();
            await(() -> oldPodWrites.get() >= 20, "the old pod to record messages");
            newPod.start();
            // Both versions write before the old pod leaves.
            await(() -> newPodWrites.get() > 0, "the new pod to record a message");
            oldPod.stop();
            int afterOldPodLeft = newPodWrites.get();
            await(() -> newPodWrites.get() > afterOldPodLeft, "the new pod to record after the old pod left");
            produced = traffic.stop();
            awaitRows(produced);
        }

        assertTrue(oldPodWrites.get() > 0 && newPodWrites.get() > 0,
                "old pod " + oldPodWrites.get() + ", new pod " + newPodWrites.get() + " writes");
        assertEquals(List.of(), failures.stream().map(RollingUpgradeWriteTest::rootMessage).toList());
        assertEquals(Integer.toString(produced), value("select count(*) from bip_message_log"));
        assertEquals(Integer.toString(produced), value("select count(*) from bip_message_log where status = 'COMPLETED'"));
        assertEquals("0", value("select count(*) from bip_message_log where integration <> 'orders'"));
        assertConsistentRows();
    }

    /** Publishes records with their own message ids, one every few milliseconds, until stopped. */
    private static final class Traffic implements AutoCloseable {
        private static final int LIMIT = 20_000;
        private final KafkaProducer<String, String> producer;
        private final String topic;
        private final AtomicInteger sent = new AtomicInteger();
        private final java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);
        private final java.util.concurrent.atomic.AtomicReference<Throwable> failure = new java.util.concurrent.atomic.AtomicReference<>();
        private final Thread thread;

        Traffic(String brokers, String topic) {
            var properties = new HashMap<String, Object>();
            properties.put("bootstrap.servers", brokers);
            properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            this.producer = new KafkaProducer<>(properties);
            this.topic = topic;
            this.thread = new Thread(this::run, "rolling-upgrade-traffic");
            thread.start();
        }

        private void run() {
            try {
                while (running.get() && sent.get() < LIMIT) {
                    int n = sent.get();
                    var record = new ProducerRecord<String, String>(topic, "key-" + (n % 16), "<Order n=\"" + n + "\"/>");
                    record.headers().add(PlatformHeaders.MESSAGE_ID, ("order-" + n).getBytes(StandardCharsets.UTF_8));
                    producer.send(record).get(10, TimeUnit.SECONDS);
                    sent.incrementAndGet();
                    Thread.sleep(5);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                failure.set(e);
            }
        }

        /** Stops publishing and returns how many records the broker acknowledged. */
        int stop() throws InterruptedException {
            running.set(false);
            thread.join(TimeUnit.SECONDS.toMillis(30));
            if (failure.get() != null) throw new AssertionError("Publishing failed", failure.get());
            assertTrue(sent.get() < LIMIT, "The pods did not change before " + LIMIT + " records");
            return sent.get();
        }

        @Override
        public void close() throws InterruptedException {
            running.set(false);
            thread.join(TimeUnit.SECONDS.toMillis(30));
            producer.close();
        }
    }

    private DefaultCamelContext consumer(String uri, RoutePolicy policy) throws Exception {
        var context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from(uri).routeId("orders-input").routePolicy(policy)
                        .process(e -> Thread.sleep(2))
                        .log("${header." + PlatformHeaders.MESSAGE_ID + "}");
            }
        });
        return context;
    }

    private static void await(java.util.function.BooleanSupplier condition, String what) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError("Timed out waiting for " + what);
            Thread.sleep(100);
        }
    }

    private void awaitRows(int expected) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
        while (System.nanoTime() < deadline) {
            if (Integer.parseInt(value("select count(*) from bip_message_log")) >= expected) return;
            Thread.sleep(100);
        }
        throw new AssertionError("Only " + value("select count(*) from bip_message_log") + " of " + expected
                + " messages were recorded");
    }

    /** Every summary row points at an attempt row with the same outcome and payload expiry. */
    private void assertConsistentRows() {
        assertEquals("0", value("""
                select count(*) from bip_message_log l
                left join bip_message_attempt a on a.message_id = l.message_id and a.attempt = l.last_attempt
                where a.message_id is null
                   or a.status <> l.status
                   or a.integration <> l.integration
                   or a.payload_expires_at is distinct from l.payload_expires_at
                """));
    }

    private static MessageAttemptResult result(String id, Instant at) {
        return new MessageAttemptResult(1, id, "order-" + id, "application/xml", null, null, "postgres_inline", 8L,
                "<Order/>".getBytes(StandardCharsets.UTF_8), null, null, at.plusSeconds(86_400), null, true, null,
                "orders", "orders-input", 1, false, null, null, at, MessageStatus.COMPLETED, List.of(), null);
    }

    private static void await(CyclicBarrier barrier) {
        try {
            barrier.await(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static String rootMessage(Throwable failure) {
        Throwable cause = failure;
        while (cause.getCause() != null) cause = cause.getCause();
        return cause.getMessage();
    }

    private String value(String sql) {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getString(1);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /** The released adapter's classes, from its jar; shared libraries from this build. */
    private static final class ReleasedAdapter {
        private static final String PACKAGE = "no.basis.integrationplatform.";
        private final ClassLoader loader;
        private final Class<?> sinkType;
        private final Method record;
        private final Constructor<?> result;
        private final Class<?> status;

        private ReleasedAdapter(ClassLoader loader) throws Exception {
            this.loader = loader;
            this.sinkType = loader.loadClass(PACKAGE + "camel.store.MessageStoreSink");
            Class<?> resultType = loader.loadClass(PACKAGE + "camel.model.MessageAttemptResult");
            this.record = sinkType.getMethod("record", resultType);
            // The record's canonical constructor: 23 components in 0.3.6.
            this.result = Arrays.stream(resultType.getConstructors()).filter(c -> c.getParameterCount() == 23)
                    .findFirst().orElseThrow();
            this.status = loader.loadClass(PACKAGE + "camel.model.MessageStatus");
        }

        static ReleasedAdapter load() throws Exception {
            String jar = System.getProperty("released.adapter.jar");
            assertTrue(jar != null && Files.isRegularFile(Path.of(jar)),
                    "The released adapter jar is copied by the build (maven-dependency-plugin): " + jar);
            return new ReleasedAdapter(new ChildFirst(Path.of(jar).toUri().toURL(),
                    RollingUpgradeWriteTest.class.getClassLoader()));
        }

        Object sink(DataSource dataSource) throws Exception {
            return loader.loadClass(PACKAGE + "camel.store.PostgresMessageStoreSink")
                    .getConstructor(DataSource.class).newInstance(dataSource);
        }

        Object countingSink(Object sink, AtomicInteger writes, List<Throwable> failures) {
            return Proxy.newProxyInstance(loader, new Class<?>[] {sinkType}, (proxy, method, args) -> {
                try {
                    Object returned = method.invoke(sink, args);
                    if (method.equals(record)) writes.incrementAndGet();
                    return returned;
                } catch (InvocationTargetException e) {
                    if (method.equals(record)) failures.add(e.getCause());
                    throw e.getCause();
                }
            });
        }

        RoutePolicy policy(Object sink, String integration) throws Exception {
            return (RoutePolicy) loader.loadClass(PACKAGE + "camel.lifecycle.MessageStoreRoutePolicy")
                    .getConstructor(sinkType, String.class).newInstance(sink, integration);
        }

        @SuppressWarnings({"unchecked", "rawtypes"})
        void record(Object sink, String id, Instant at) throws Throwable {
            // 0.3.6's record has no payload reference or digest.
            Object row = result.newInstance(1, id, "order-" + id, "application/xml", null, null, "postgres_inline", 8L,
                    "<Order/>".getBytes(StandardCharsets.UTF_8), at.plusSeconds(86_400), null, true, null, "orders",
                    "orders-input", 1, false, null, null, at, Enum.valueOf((Class) status, "COMPLETED"), List.of(), null);
            try {
                record.invoke(sink, row);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }
    }

    /**
     * Child-first for classes in the adapter's package only. Resources are parent-first, so
     * {@code getResource} finds this build's files (such as the schema SQL), not the jar's.
     */
    private static final class ChildFirst extends URLClassLoader {
        ChildFirst(URL jar, ClassLoader parent) {
            super(new URL[] {jar}, parent);
        }

        @Override
        protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
            if (!name.startsWith(ReleasedAdapter.PACKAGE)) return super.loadClass(name, resolve);
            synchronized (getClassLoadingLock(name)) {
                Class<?> loaded = findLoadedClass(name);
                if (loaded == null) loaded = findClass(name);
                if (resolve) resolveClass(loaded);
                return loaded;
            }
        }
    }
}
