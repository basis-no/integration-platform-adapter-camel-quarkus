package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Kafka record whose outcome was not made durable is read again at once the first time, and
 * then only after a wait that doubles up to its cap, so that an outage does not run the route
 * several times a second.
 */
class ReadAgainBackOffTest {
    private final DefaultCamelContext context = new DefaultCamelContext();
    private final List<Long> waits = new ArrayList<>();
    private final AtomicBoolean journalDown = new AtomicBoolean(true);
    private final AtomicInteger commits = new AtomicInteger();

    @AfterEach
    void close() throws Exception {
        context.close();
    }

    @Test
    void anOutcomeThatCannotBeRecordedIsReadAgainAtOnceThenAfterADoublingWait() {
        MessageStoreRoutePolicy policy = policy();

        for (int read = 0; read < 9; read++) {
            readFailingToRecord(policy, route(null), 7);
        }

        assertEquals(List.of(1_000L, 2_000L, 4_000L, 8_000L, 16_000L, 30_000L, 30_000L, 30_000L), waits);
        assertEquals(0, commits.get());
    }

    @Test
    void aRecordedOutcomeResetsTheWait() {
        MessageStoreRoutePolicy policy = policy();
        readFailingToRecord(policy, route(null), 7);
        readFailingToRecord(policy, route(null), 7);

        journalDown.set(false);
        policy.onExchangeDone(route(null), begin(policy, 7));
        journalDown.set(true);
        readFailingToRecord(policy, route(null), 8);
        readFailingToRecord(policy, route(null), 8);

        assertEquals(List.of(1_000L, 1_000L), waits, "The next record starts again at once");
        assertEquals(1, commits.get());
    }

    @Test
    void aCommitOfTheSameRecordResetsTheWait() {
        MessageStoreRoutePolicy policy = policy();
        readFailingToRecord(policy, route(null), 7);
        readFailingToRecord(policy, route(null), 7);

        journalDown.set(false);
        policy.onExchangeDone(route(null), begin(policy, 7));
        journalDown.set(true);
        readFailingToRecord(policy, route(null), 7);
        readFailingToRecord(policy, route(null), 7);

        assertEquals(List.of(1_000L, 1_000L), waits, "Held back again from the start");
    }

    @Test
    void aFailedCommitIsReadAgainWithoutAWait() {
        journalDown.set(false);
        MessageStoreRoutePolicy policy = policy();

        for (int read = 0; read < 3; read++) {
            Exchange exchange = begin(policy, 7);
            exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> {
                throw new IllegalStateException("synthetic commit failure");
            });
            exchange.setProperty(PlatformExchangeProperties.KAFKA_MANUAL_COMMIT, exchange.getMessage()
                    .getHeader(KafkaConstants.MANUAL_COMMIT));
            assertThrows(IllegalStateException.class, () -> policy.onExchangeDone(route(null), exchange));
        }

        assertEquals(List.of(), waits, "The outcome is recorded; only the commit is repeated");
    }

    @Test
    void aNewerRecordOnThePartitionStartsAgainAtOnce() {
        MessageStoreRoutePolicy policy = policy();
        readFailingToRecord(policy, route(null), 7);
        readFailingToRecord(policy, route(null), 7);
        readFailingToRecord(policy, route(null), 8);

        assertEquals(List.of(1_000L), waits);
    }

    @Test
    void theWaitStaysWithinHalfTheConsumersPollInterval() {
        MessageStoreRoutePolicy policy = policy();
        KafkaEndpoint endpoint = context.getEndpoint(
                "kafka:input?brokers=localhost:1&breakOnFirstError=true&maxPollIntervalMs=10000", KafkaEndpoint.class);

        for (int read = 0; read < 6; read++) {
            readFailingToRecord(policy, route(endpoint), 7);
        }

        assertEquals(List.of(1_000L, 2_000L, 4_000L), waits.subList(0, 3));
        assertEquals(5, waits.size());
        for (long capped : waits.subList(3, 5)) {
            assertTrue(capped > 4_000 && capped <= 5_000, "Capped at half the interval: " + waits);
        }
    }

    @Test
    void anAttemptThatUsedHalfThePollIntervalWaitsNoLonger() throws Exception {
        MessageStoreRoutePolicy policy = policy();
        // The consumer's own property overrides Camel's named option.
        KafkaEndpoint endpoint = context.getEndpoint("kafka:input?brokers=localhost:1&breakOnFirstError=true"
                + "&additionalProperties.max.poll.interval.ms=2000", KafkaEndpoint.class);
        assertEquals(2_000, MessageStoreRoutePolicy.maxPollIntervalMs(route(endpoint)));
        readFailingToRecord(policy, route(endpoint), 7);

        Exchange slow = begin(policy, 7);
        Thread.sleep(1_100);
        assertThrows(IllegalStateException.class, () -> policy.onExchangeDone(route(endpoint), slow));
        assertEquals(List.of(), waits, "An attempt that took half the poll interval leaves no time to wait");

        readFailingToRecord(policy, route(endpoint), 7);
        assertEquals(1, waits.size());
        assertTrue(waits.getFirst() > 500 && waits.getFirst() <= 1_000, "Capped at half the interval: " + waits);
    }

    @Test
    void theRecordsBeforeItInItsPollCountAgainstTheWait() throws Exception {
        MessageStoreRoutePolicy policy = policy();
        KafkaEndpoint endpoint = context.getEndpoint("kafka:input?brokers=localhost:1&breakOnFirstError=true"
                + "&additionalProperties.max.poll.interval.ms=2000", KafkaEndpoint.class);
        readFailingToRecord(policy, route(endpoint), 7);

        // The next poll: a slow record of another partition first, then the held-back one.
        journalDown.set(false);
        Exchange other = begin(policy, 3, 1);
        Thread.sleep(1_100);
        policy.onExchangeDone(route(endpoint), other);
        journalDown.set(true);
        readFailingToRecord(policy, route(endpoint), 7);

        assertEquals(List.of(), waits, "The poll already used half its interval");
    }

    @Test
    void withoutBreakOnFirstErrorThereIsNoWait() {
        MessageStoreRoutePolicy policy = policy();
        KafkaEndpoint endpoint = context.getEndpoint("kafka:input?brokers=localhost:1&breakOnFirstError=false",
                KafkaEndpoint.class);

        for (int read = 0; read < 3; read++) {
            readFailingToRecord(policy, route(endpoint), 7);
        }

        assertEquals(List.of(), waits);
    }

    @Test
    void theWaitEndsWhenTheContextStopsOrTheThreadIsInterrupted() {
        CamelContext stopping = (CamelContext) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{CamelContext.class}, (proxy, method, args) -> switch (method.getName()) {
                    case "isStopping" -> true;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        Route route = (Route) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Route.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getCamelContext" -> stopping;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        long started = System.nanoTime();
        MessageStoreRoutePolicy.sleepUnlessStopping(route, 30_000);
        assertTrue(System.nanoTime() - started < java.util.concurrent.TimeUnit.SECONDS.toNanos(1));

        Thread.currentThread().interrupt();
        started = System.nanoTime();
        MessageStoreRoutePolicy.sleepUnlessStopping(route(null), 30_000);
        assertTrue(Thread.interrupted(), "The interrupt is kept for the consumer");
        assertTrue(System.nanoTime() - started < java.util.concurrent.TimeUnit.SECONDS.toNanos(1));
    }

    @Test
    void aFailureThatIsNotParkedIsReadAgainWithTheSameWait() {
        journalDown.set(false);
        MessageStoreRoutePolicy policy = policy();

        for (int read = 0; read < 3; read++) {
            Exchange exchange = begin(policy, 7);
            // The DLQ refused the record: the exchange stays failed and is not parked.
            exchange.setException(new IllegalStateException("synthetic DLQ outage"));
            exchange.setProperty(PlatformExchangeProperties.DLQ_PARKED, false);
            assertThrows(IllegalStateException.class, () -> policy.onExchangeDone(route(null), exchange));
        }

        assertEquals(List.of(1_000L, 2_000L), waits);
        assertEquals(0, commits.get());
    }

    @Test
    void aRecordWithoutItsPositionIsReadAgainAtOnce() {
        MessageStoreRoutePolicy policy = policy();

        for (int read = 0; read < 3; read++) {
            Exchange exchange = new DefaultExchange(context);
            exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
            exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
            policy.onExchangeBegin(route(null), exchange);
            assertThrows(IllegalStateException.class, () -> policy.onExchangeDone(route(null), exchange));
        }

        assertEquals(List.of(), waits);
    }

    private MessageStoreRoutePolicy policy() {
        MessageStoreRoutePolicy policy = new MessageStoreRoutePolicy(this::record, "adapter-test");
        policy.setSleeper((route, millis) -> waits.add(millis));
        return policy;
    }

    private void record(MessageAttemptResult result) {
        if (journalDown.get()) {
            throw new IllegalStateException("synthetic journal outage");
        }
    }

    private void readFailingToRecord(MessageStoreRoutePolicy policy, Route route, long offset) {
        Exchange exchange = begin(policy, offset);
        assertThrows(IllegalStateException.class, () -> policy.onExchangeDone(route, exchange));
    }

    private Exchange begin(MessageStoreRoutePolicy policy, long offset) {
        return begin(policy, offset, 0);
    }

    private Exchange begin(MessageStoreRoutePolicy policy, long offset, int partition) {
        Exchange exchange = new DefaultExchange(context);
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-" + offset);
        exchange.getMessage().setHeader(KafkaConstants.TOPIC, "input");
        exchange.getMessage().setHeader(KafkaConstants.PARTITION, partition);
        exchange.getMessage().setHeader(KafkaConstants.OFFSET, offset);
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
        exchange.getMessage().setBody("{}");
        policy.onExchangeBegin(route(null), exchange);
        return exchange;
    }

    private Route route(KafkaEndpoint endpoint) {
        return (Route) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{Route.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getRouteId", "getId" -> "input";
                    case "getEndpoint" -> endpoint;
                    case "getCamelContext" -> context;
                    case "toString" -> "route(input)";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
