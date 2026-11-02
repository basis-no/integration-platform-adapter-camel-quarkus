package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.step.DefaultStepClassifier;
import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import no.basis.integrationplatform.camel.step.NoopStepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepOutcomeStore;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.ProcessorEndpoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.logging.Level;

import static org.apache.camel.builder.Builder.body;
import static org.apache.camel.builder.Builder.constant;
import static org.apache.camel.builder.Builder.simple;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Split parts, loop iterations and multicast and recipient list branches keep the input's
 * message id and so share a step key: a later part is skipped as already delivered
 * without being sent. Each such skip is logged at ERROR; whether a part is sent, the input's
 * outcome, parking and the commit are unchanged. A skip of a step that an earlier attempt
 * sent, or that the same part sent before Camel repeated it, stays INFO.
 */
@Timeout(60)
class RepeatedStepDetectionTest {
    private final Map<String, String> outcomes = new ConcurrentHashMap<>();
    private final List<String> received = Collections.synchronizedList(new ArrayList<>());
    private final List<MessageAttemptResult> rows = Collections.synchronizedList(new ArrayList<>());
    private final List<String> parked = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger commits = new AtomicInteger();
    private final AtomicInteger flakyFailures = new AtomicInteger();
    private volatile CyclicBarrier linesBarrier;
    private DefaultCamelContext context;
    private LogCapture log;

    /** Behaves like the Postgres store: FAILED never replaces DELIVERED. */
    private final StepOutcomeStore store = new StepOutcomeStore() {
        public boolean wasDelivered(String key) { return "DELIVERED".equals(outcomes.get(key)); }
        public void recordDelivered(StepResult step) { outcomes.put(Objects.requireNonNull(step.idempotencyKey()), "DELIVERED"); }
        public void recordFailed(StepResult step) { outcomes.putIfAbsent(Objects.requireNonNull(step.idempotencyKey()), "FAILED"); }
    };

    @AfterEach
    void stop() throws Exception {
        if (log != null) log.close();
        if (context != null) context.close();
    }

    @Test
    void aKafkaSplitCompletesAsBeforeAndEachSkippedPartIsAnError() throws Exception {
        start(store, route -> route.split(body().tokenize(",")).to("vendor:lines").id("send-line").end());

        Exchange result = sendKafka("a,b,c");

        assertNull(result.getException());
        assertEquals(List.of("lines:a"), received);
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
        assertTrue(parked.isEmpty());
        assertEquals(1, commits.get());
        assertEquals(Map.of("message-1:send-line", "DELIVERED"), outcomes);
        assertEquals(List.of("split part", "split part"), errorScopes());
        assertEquals(0, infoSkips());
    }

    @Test
    void aSkipOfWhatAnEarlierAttemptSentStaysInfo() throws Exception {
        outcomes.put("message-1:send-line", "DELIVERED");
        start(store, route -> route.to("vendor:lines").id("send-line"));

        sendKafka("a");

        assertEquals(List.of(), received);
        assertEquals(1, infoSkips());
        assertEquals(List.of(), errorScopes());
    }

    @Test
    void aRedeliveredSplitSkipsItsFirstPartAtInfoAndTheNextAtError() throws Exception {
        // An earlier attempt sent part a. In this attempt the first part's skip is normal; the
        // second part shares the key, so it is not delivered either.
        outcomes.put("message-1:send-line", "DELIVERED");
        start(store, route -> route.split(body().tokenize(",")).to("vendor:lines").id("send-line").end());

        sendKafka("a,b");

        assertEquals(List.of(), received);
        assertEquals(1, infoSkips());
        assertEquals(List.of("split part"), errorScopes());
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
    }

    @Test
    void aSplitThatCamelRepeatsInOneAttemptSkipsThePartsOwnSendAtInfo() throws Exception {
        // The sub-route has no error handler, so the input's retry repeats the whole split:
        // part a meets its own delivered send again, part b is skipped again.
        flakyFailures.set(1);
        start(store, route -> route.to("direct:deliver").id("call-deliver"));
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:deliver").errorHandler(noErrorHandler())
                        .split(body().tokenize(",")).to("vendor:lines").id("send-line").end()
                        .to("vendor:flaky").id("send-flaky");
            }
        });

        sendKafka("a,b");

        assertEquals(List.of("lines:a", "flaky:a,b", "flaky:a,b"), received);
        assertEquals(List.of("split part", "split part"), errorScopes());
        assertEquals(1, infoSkips());
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
        assertEquals(1, commits.get());
    }

    @Test
    void partsThatAllSentInParallelSkipTheirOwnSendsAtInfoWhenCamelRepeatsTheSplit() throws Exception {
        // Both parts pass the delivered check before either delivers, so both are sent. The
        // sub-route has no error handler, so the input's retry repeats the whole split.
        flakyFailures.set(1);
        linesBarrier = new CyclicBarrier(2);
        start(store, route -> route.to("direct:deliver").id("call-deliver"));
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:deliver").errorHandler(noErrorHandler())
                        .split(body().tokenize(",")).parallelProcessing().to("vendor:lines").id("send-line").end()
                        .to("vendor:flaky").id("send-flaky");
            }
        });

        sendKafka("a,b");

        assertEquals(2, received.stream().filter(r -> r.startsWith("lines:")).count(), received.toString());
        assertEquals(List.of("flaky:a,b", "flaky:a,b"), received.stream().filter(r -> r.startsWith("flaky:")).toList());
        assertEquals(List.of(), errorScopes());
        assertEquals(2, infoSkips());
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
    }

    @Test
    void aSplittingSubRouteCalledTwiceTellsTheSecondCallsPartsApart() throws Exception {
        // Each call runs the same split, so the second call's first part has the same
        // position as the first call's; the call it came from tells them apart.
        start(store, route -> route.to("direct:lines").id("first-batch")
                .setBody(constant("c")).to("direct:lines").id("second-batch"));
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:lines").split(body().tokenize(",")).to("vendor:lines").id("send-line").end();
            }
        });

        sendKafka("a,b");

        assertEquals(List.of("lines:a"), received);
        assertEquals(List.of("split part", "split part"), errorScopes());
        assertEquals(0, infoSkips());
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
    }

    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', value = {
            // shape | body | sends that reached the receiver | scope in each ERROR | INFO skips
            "loop             | a     | lines:a                 | loop iteration,loop iteration     | 0",
            "loop-copy        | a     | lines:a                 | loop iteration,loop iteration     | 0",
            "multicast        | a     | lines:a                 | multicast branch                  | 0",
            "recipient-list   | a     | lines:a                 | recipient list branch             | 0",
            "nested-split     | a,b;c | lines:a                 | split part,split part             | 0",
            "split-in-loop    | a,b   | lines:a                 | split part,loop iteration,loop iteration | 0",
            "multicast-in-split | a,b | lines:a                 | multicast branch,split part,split part | 0",
            "per-part-id      | a,b,c | lines:a,lines:b,lines:c |                                   | 0",
            // Not detected: one part reaching the same send twice, and an outer loop's
            // repeat after an inner loop, which leaves its own index on the exchange.
            "same-part-twice  | a     | lines:a                 |                                   | 1",
            "nested-loop      | a     | lines:a                 |                                   | 1",
    })
    void eachScopeIsNamedAndTheSendsAreUnchanged(String shape, String body, String sends, String scopes, int infoSkips)
            throws Exception {
        start(store, route -> {
            switch (shape) {
                case "loop" -> route.loop(3).to("direct:send").end();
                case "loop-copy" -> route.loop(3).copy().to("direct:send").end();
                case "multicast" -> route.multicast().to("direct:send").to("direct:send").end();
                case "recipient-list" -> route.recipientList(constant("direct:send,direct:send"));
                case "nested-split" -> route.split(body().tokenize(";")).split(body().tokenize(",")).to("direct:send").end().end();
                case "split-in-loop" -> route.loop(2).split(body().tokenize(",")).to("direct:send").end().end();
                case "multicast-in-split" -> route.split(body().tokenize(","))
                        .multicast().to("direct:send").to("direct:send").end().end();
                case "per-part-id" -> route.split(body().tokenize(","))
                        .setHeader(PlatformHeaders.MESSAGE_ID,
                                simple("${header." + PlatformHeaders.MESSAGE_ID + "}-${exchangeProperty.CamelSplitIndex}"))
                        .to("direct:send").end();
                case "same-part-twice" -> route.to("direct:send").to("direct:send");
                case "nested-loop" -> route.loop(2).loop(2).log("inner").end().to("direct:send").end();
                default -> throw new IllegalArgumentException(shape);
            }
        });
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:send").to("vendor:lines").id("send-line");
            }
        });

        Exchange result = sendKafka(body);

        assertNull(result.getException());
        assertEquals(List.of(sends.split(",")), received);
        assertEquals(scopes == null ? List.of() : List.of(scopes.split(",")), errorScopes());
        assertEquals(infoSkips, infoSkips());
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
        assertEquals(1, commits.get());
    }

    @Test
    void parallelPartsAreEachSentOrLoggedAtError() throws Exception {
        start(store, route -> route.split(body().tokenize(",")).parallelProcessing().to("vendor:lines").id("send-line").end());

        sendKafka("a,b,c,d");

        assertEquals(4, received.size() + errorScopes().size(), "received=" + received + " errors=" + errorScopes());
        assertEquals(0, infoSkips());
        assertEquals(MessageStatus.COMPLETED, rows.getFirst().status());
    }

    @Test
    void aPolledInputStillRunsItsLaterSteps() throws Exception {
        // A tracked fetch gives the exchange a message id before the split.
        start(store, null);
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:poll").routeId("poll")
                        .to("vendor:fetch").id("fetch-batch")
                        .split(body().tokenize(",")).to("vendor:lines").id("send-line").end()
                        .to("vendor:mark").id("mark-processed");
            }
        });

        Exchange result;
        try (var producer = context.createProducerTemplate()) {
            result = producer.send("direct:poll", e -> e.getMessage().setBody("a,b,c"));
        }

        assertNull(result.getException());
        assertEquals(List.of("fetch:a,b,c", "lines:a", "mark:a,b,c"), received);
        assertEquals(List.of("split part", "split part"), errorScopes());
        assertTrue(LogCapture.text(log.at(Level.SEVERE).getFirst()).contains(" of input route poll,"));
    }

    @Test
    void theNoopStoreSendsEveryPartWithoutAnError() throws Exception {
        start(new NoopStepOutcomeStore(), route -> route.split(body().tokenize(",")).to("vendor:lines").id("send-line").end());

        sendKafka("a,b,c");

        assertEquals(List.of("lines:a", "lines:b", "lines:c"), received);
        assertEquals(List.of(), errorScopes());
        assertEquals(0, infoSkips());
    }

    private List<String> errorScopes() {
        return log.at(Level.SEVERE).stream().map(LogCapture::text).map(text -> {
            String scope = text.substring(text.indexOf(" was skipped in a ") + " was skipped in a ".length());
            return scope.substring(0, scope.indexOf(" of input route "));
        }).toList();
    }

    private long infoSkips() {
        return log.at(Level.INFO).stream().filter(record -> LogCapture.text(record).contains(" SKIPPED for message ")).count();
    }

    private void start(StepOutcomeStore stepStore, Consumer<RouteDefinition> body) throws Exception {
        context = new DefaultCamelContext();
        var properties = new Properties();
        properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
        context.getPropertiesComponent().setOverrideProperties(properties);
        context.addComponent("vendor", new DefaultComponent() {
            @Override protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
                return new ProcessorEndpoint(uri, this, e -> {
                    received.add(remaining + ":" + e.getMessage().getBody(String.class));
                    if (remaining.equals("lines") && linesBarrier != null) {
                        linesBarrier.await(10, TimeUnit.SECONDS);
                    }
                    if (remaining.equals("flaky") && flakyFailures.getAndDecrement() > 0) {
                        throw new IllegalStateException("synthetic transient failure");
                    }
                });
            }
        });
        context.getCamelContextExtension().addInterceptStrategy(new StepTrackingInterceptStrategy(new DefaultStepTracker(stepStore,
                sideEffects("send-line,send-flaky,fetch-batch,mark-processed"))));
        context.addRoutes(new PlatformRouteConfiguration());
        context.addRoutes(new RouteBuilder() {
            public void configure() {
                from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                        .process(e -> parked.add(e.getMessage().getBody(String.class)));
                if (body != null) {
                    body.accept(from("direct:input").routeId("input")
                            .routePolicy(new MessageStoreRoutePolicy(rows::add, "order-pipeline")));
                }
            }
        });
        context.start();
        log = LogCapture.of(DefaultStepTracker.class);
    }

    private Exchange sendKafka(String body) {
        try (var producer = context.createProducerTemplate()) {
            return producer.send("direct:input", e -> {
                e.getMessage().setHeader(KafkaConstants.TOPIC, "bip.order-pipeline.in");
                e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) commits::incrementAndGet);
                e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "message-1");
                e.getMessage().setBody(body);
            });
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    /** On main only sends classified as durable side effects use the delivered-step ledger. */
    private static DefaultStepClassifier sideEffects(String ids) {
        return new DefaultStepClassifier(Optional.empty(), Optional.empty(), Optional.of(ids), Optional.empty());
    }
}
