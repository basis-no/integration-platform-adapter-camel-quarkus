package example;

import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.http.TestHTTPResource;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import java.io.StringReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.store.MessageStoreSink;
import org.apache.camel.CamelContext;
import org.apache.camel.component.kafka.KafkaConsumer;
import org.apache.camel.component.kafka.TaskHealthState;
import org.apache.camel.health.HealthCheck;
import org.apache.camel.health.HealthCheckAware;
import org.apache.camel.health.HealthCheckHelper;
import org.apache.camel.health.HealthCheckRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(AclBroker.class)
class AclRecoveryTest {
    @Inject CamelContext context;
    @Inject CaptureSink sink;
    @Inject Instance<MessageStoreSink> selectedSink;
    @ConfigProperty(name = "fixture.brokers") String brokers;
    @ConfigProperty(name = "fixture.topic") String topic;
    @ConfigProperty(name = "fixture.group") String group;
    @TestHTTPResource("/q/health/ready") URI ready;

    static void await(BooleanSupplier predicate, String failure) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (predicate.getAsBoolean()) return;
            Thread.sleep(50);
        }
        fail(failure);
    }
    List<TaskHealthState> states(String route) {
        return ((KafkaConsumer) context.getRoute(route).getConsumer()).healthStates();
    }
    boolean pollingReady(String route) {
        var state = states(route);
        return !state.isEmpty() && state.stream().allMatch(TaskHealthState::isReady);
    }
    void startObservedRoute(String route) throws Exception {
        // Keep deterministic startup ordering, then make this a Camel-managed
        // route: upstream consumer health exempts autoStartup=false routes.
        if (Boolean.getBoolean("fixture.camel-health-bridge")) context.getRoute(route).setAutoStartup(true);
        context.getRouteController().startRoute(route);
    }
    HealthCheck.State nativeConsumerState(String route) {
        var check = ((HealthCheckAware) context.getRoute(route).getConsumer()).getHealthCheck();
        assertNotNull(check, "Missing native Kafka health check: " + route);
        assertEquals("consumer:kafka-" + route, check.getId());
        assertTrue(check.isEnabled(), "Native Kafka health check is disabled");
        assertTrue(check.isReadiness(), "Native Kafka health check is not readiness");
        return check.call(Map.of(HealthCheck.CHECK_KIND, HealthCheck.Kind.READINESS)).getState();
    }
    HealthCheck.State registeredConsumerState(String route) {
        var results = HealthCheckHelper.invokeReadiness(context, "full");
        for (var result : results) {
            String id = result.getCheck().getId();
            if (id.startsWith("consumer:") && !id.equals("consumer:" + route)) {
                assertEquals(HealthCheck.State.UP, result.getState(), "Unrelated consumer is unhealthy: " + id);
            }
        }
        if (route.equals("invalid-auth-source")) {
            assertTrue(pollingReady("acl-source"), "The valid consumer stopped during dead-poller observation");
            assertEquals(HealthCheck.State.UP, nativeConsumerState("acl-source"));
        }
        return results.stream()
                .filter(result -> result.getCheck().getId().equals("consumer:" + route))
                .findFirst().orElseThrow(() -> new AssertionError("Missing registered consumer wrapper: " + route
                        + "; available=" + results.stream().map(result -> result.getCheck().getId()).toList()))
                .getState();
    }
    void assertHttpFailureBelongsToRoute(String body, String route) {
        try (var reader = Json.createReader(new StringReader(body))) {
            var health = reader.readObject();
            assertEquals("DOWN", health.getString("status"));
            boolean attributed = false;
            for (var check : health.getJsonArray("checks").getValuesAs(JsonObject.class)) {
                if (check.getString("status").equals("DOWN")) {
                    var data = check.getJsonObject("data");
                    assertNotNull(data, "A failing health check has no route attribution: " + check);
                    assertEquals(route, data.getString("route.id", null), "Unrelated HTTP health failure: " + check);
                    attributed = true;
                } else {
                    assertEquals("UP", check.getString("status"), "Unexpected health status: " + check);
                }
            }
            assertTrue(attributed, "No HTTP health failure belongs to the observed route");
        }
    }
    void bridgeReadiness(HttpClient http, String phase, String route, HealthCheck.State state,
                         int expectedHttp) throws Exception {
        if (!Boolean.getBoolean("fixture.camel-health-bridge")) return;
        var registry = HealthCheckRegistry.get(context);
        assertNotNull(registry, "Missing Camel health registry");
        assertEquals("org.apache.camel.microprofile.health.CamelMicroProfileHealthCheckRegistry",
                registry.getClass().getName(), "The upstream MicroProfile bridge is not installed");
        assertEquals(Boolean.TRUE, context.getRoute(route).isAutoStartup(), "Route health is externally managed");
        long phaseStarted = System.nanoTime();
        long deadline = phaseStarted + TimeUnit.SECONDS.toNanos(30);
        boolean stopped = phase.equals("terminated-auth");
        // The pinned SmallRye/Camel bridge evaluates on request, not on a
        // periodic refresh. Three fresh requests one second apart test continued
        // dead-poller projection; one second is our observation cadence.
        int samples = stopped ? 3 : 1;
        for (int sample = 1; sample <= samples; sample++) {
            if (sample > 1) Thread.sleep(1000);
            boolean observed = false;
            while (System.nanoTime() < deadline) {
                var nativeState = nativeConsumerState(route);
                var wrapperState = registeredConsumerState(route);
                long remaining = deadline - System.nanoTime();
                assertTrue(remaining > 0, "Readiness observation deadline expired");
                var response = http.send(HttpRequest.newBuilder(ready)
                                .timeout(Duration.ofNanos(Math.min(TimeUnit.SECONDS.toNanos(5), remaining))).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                System.out.println("fixture-health-observation " + phase + " sample=" + sample + " elapsed-ms="
                        + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - phaseStarted)
                        + " native=" + nativeState + " wrapper=" + wrapperState + " http="
                        + response.statusCode() + " " + response.body());
                assertTrue(response.statusCode() == 200 || response.statusCode() == 503,
                        "Unexpected health HTTP response: " + response.statusCode() + " " + response.body());
                if (response.statusCode() == 503) assertHttpFailureBelongsToRoute(response.body(), route);
                observed = nativeState == state && wrapperState == state && response.statusCode() == expectedHttp;
                if (stopped) {
                    // After causal non-recoverable exit, any contrary sample fails;
                    // do not wait for a later good response to hide a false Ready.
                    assertTrue(observed, "Stopped Kafka consumer did not remain unready: " + response.body());
                }
                if (observed) {
                    System.out.println("fixture-registered " + phase + ": native=consumer:kafka-" + route
                            + " wrapper=consumer:" + route + " state=" + state);
                    System.out.println("fixture-bridge " + phase + ": " + response.statusCode() + " " + response.body());
                    if (stopped) System.out.println("fixture-stable-dead sample=" + sample + " http=503");
                    break;
                }
                Thread.sleep(50);
            }
            assertTrue(observed, "Did not observe native/wrapper/HTTP readiness during " + phase);
        }
    }
    long committed(AdminClient admin) {
        try {
            var offset = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(5, TimeUnit.SECONDS).get(new TopicPartition(topic, 0));
            return offset == null ? -1 : offset.offset();
        } catch (java.util.concurrent.ExecutionException error) {
            if (error.getCause() instanceof org.apache.kafka.common.errors.GroupIdNotFoundException) return -1;
            throw new AssertionError("Broker offset query failed", error);
        } catch (Exception error) { throw new AssertionError("Broker offset query failed", error); }
    }
    @Test
    void realAclGrantRecoversSameConsumerAndReadinessDistinguishesIdleFromTerminated() throws Exception {
        // The adapter producer is @Produces @DefaultBean with the same default
        // qualifier. This ordinary application bean must replace that producer.
        assertTrue(selectedSink.isResolvable(), "MessageStoreSink CDI selection is ambiguous or missing");
        assertInstanceOf(CaptureSink.class, selectedSink.get());
        assertEquals(org.apache.camel.component.kafka.PollOnError.RECONNECT,
                context.getComponent("kafka", org.apache.camel.component.kafka.KafkaComponent.class)
                        .getConfiguration().getPollOnError());
        for (String route : List.of("acl-source", "invalid-auth-source")) {
            var configuration = ((org.apache.camel.component.kafka.KafkaEndpoint)
                    context.getRoute(route).getEndpoint()).getConfiguration();
            assertEquals(brokers, configuration.getBrokers(), "Wrong synthetic broker for " + route);
            assertEquals("SASL_PLAINTEXT", configuration.getSecurityProtocol(), "Wrong security protocol");
            assertEquals("PLAIN", configuration.getSaslMechanism(), "Wrong synthetic mechanism");
            assertEquals(org.apache.camel.component.kafka.PollOnError.RECONNECT, configuration.getPollOnError());
            assertTrue(configuration.getSaslJaasConfig() != null && !configuration.getSaslJaasConfig().isBlank(),
                    "Missing synthetic credential binding");
        }
        String id = UUID.randomUUID().toString();
        AtomicBoolean denied = new AtomicBoolean();
        AtomicBoolean invalidAuthenticationPhase = new AtomicBoolean();
        AtomicLong authenticationThread = new AtomicLong(-1);
        AtomicBoolean samePollThreadTerminating = new AtomicBoolean();
        Logger logger = Logger.getLogger("org.apache.camel.component.kafka.KafkaFetchRecords");
        Handler handler = new Handler() {
            boolean reports(LogRecord record, String exceptionClass) {
                if (record.getMessage() != null && record.getMessage().contains(exceptionClass)) return true;
                if (record.getParameters() != null) for (Object value : record.getParameters()) {
                    if (exceptionClass.equals(value)) return true;
                }
                Throwable error = record.getThrown();
                while (error != null) {
                    if (exceptionClass.equals(error.getClass().getName())) return true;
                    if (error.getCause() == error) break;
                    error = error.getCause();
                }
                return false;
            }
            public void publish(LogRecord record) {
                if (reports(record, "org.apache.kafka.common.errors.TopicAuthorizationException")) denied.set(true);
                if (!invalidAuthenticationPhase.get()) return;
                long thread = record.getLongThreadID();
                if (reports(record, "org.apache.kafka.common.errors.SaslAuthenticationException")) {
                    authenticationThread.compareAndSet(-1, thread);
                }
                if (thread == authenticationThread.get() && thread > 0
                        && record.getMessage() != null
                        && record.getMessage().contains("Terminating KafkaConsumer thread")) {
                    samePollThreadTerminating.set(true);
                }
            }
            public void flush() { }
            public void close() { }
        };
        logger.addHandler(handler);
        try (var admin = AdminClient.create(AclBroker.client(brokers, "admin", AclBroker.ADMIN_PASSWORD));
             var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()) {
            var producerProps = new HashMap<String, Object>(AclBroker.client(brokers, "admin", AclBroker.ADMIN_PASSWORD));
            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            try (var producer = new KafkaProducer<String, String>(producerProps)) {
                var record = new ProducerRecord<String, String>(topic, "key", "synthetic input");
                record.headers().add("x-bip-message-id", id.getBytes(StandardCharsets.UTF_8));
                record.headers().add("x-bip-correlation-id", id.getBytes(StandardCharsets.UTF_8));
                producer.send(record).get(10, TimeUnit.SECONDS);
            }
            startObservedRoute("acl-source");
            Object original = context.getRoute("acl-source").getConsumer();
            await(denied::get, "Did not observe a real Camel poll TopicAuthorizationException");
            assertTrue(sink.snapshot().isEmpty(), "Poll failure created a phantom message outcome");
            assertTrue(committed(admin) <= 0, "Denied READ advanced the committed input offset");
            bridgeReadiness(http, "denied-reconnecting", "acl-source", HealthCheck.State.DOWN, 503);
            admin.createAcls(List.of(AclBroker.allow(ResourceType.TOPIC, topic, AclOperation.READ)))
                    .all().get(10, TimeUnit.SECONDS);
            await(() -> sink.snapshot().stream().anyMatch(row -> row.status() == MessageStatus.COMPLETED),
                    "Consumer did not recover after scoped topic READ grant");
            await(() -> committed(admin) == 1, "Recovered input was not manually committed");
            assertSame(original, context.getRoute("acl-source").getConsumer(), "Route consumer was replaced");
            var rows = sink.snapshot();
            assertEquals(1, rows.size());
            assertEquals(id, rows.getFirst().messageId());
            assertEquals(id, rows.getFirst().correlationId());
            await(() -> pollingReady("acl-source"), "Consumer is not healthy after recovery");
            Thread.sleep(1000); // no further input: healthy idle must not become dead merely for lack of traffic
            assertTrue(pollingReady("acl-source"));
            var idle = http.send(HttpRequest.newBuilder(ready).timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            System.out.println("fixture-readiness healthy-idle: " + idle.statusCode() + " " + idle.body());
            assertEquals(200, idle.statusCode(), idle.body());
            bridgeReadiness(http, "recovered-idle", "acl-source", HealthCheck.State.UP, 200);
            invalidAuthenticationPhase.set(true);
            startObservedRoute("invalid-auth-source");
            // Camel 4.18 only sets isTerminated on exhausted create/subscribe paths,
            // not a natural poll-auth return. Require causal exit evidence AND state.
            await(() -> authenticationThread.get() > 0 && samePollThreadTerminating.get()
                            && !states("invalid-auth-source").isEmpty()
                            && states("invalid-auth-source").stream()
                                    .allMatch(state -> !state.isReady() && !state.isRecoverable()),
                    "Invalid authentication did not stop its polling loop without recovery");
            assertTrue(pollingReady("acl-source"), "The valid consumer stopped while checking invalid authentication");
            System.out.println("fixture-poll-state invalid-auth: authenticationThread=" + authenticationThread.get()
                    + " sameThreadTerminating=" + samePollThreadTerminating.get() + " "
                    + states("invalid-auth-source").stream().map(state -> "ready=" + state.isReady()
                            + ",recoverable=" + state.isRecoverable() + ",terminatedFlag=" + state.isTerminated()).toList());
            var dead = http.send(HttpRequest.newBuilder(ready).timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            System.out.println("fixture-readiness terminated-auth: " + dead.statusCode() + " " + dead.body());
            if (Boolean.getBoolean("fixture.camel-health-bridge")) {
                assertEquals(503, dead.statusCode(), dead.body());
                bridgeReadiness(http, "terminated-auth", "invalid-auth-source", HealthCheck.State.DOWN, 503);
            } else {
                // Characterize the actual SmallRye-only composition; this is an explicit readiness gap.
                assertEquals(200, dead.statusCode(), dead.body());
                assertFalse(dead.body().contains("consumer:kafka-"), dead.body());
            }
            assertEquals(1, sink.snapshot().size(), "Authentication failure created an extra message outcome");
        } finally {
            try { context.getRouteController().stopRoute("invalid-auth-source"); }
            finally {
                try { context.getRouteController().stopRoute("acl-source"); }
                finally { logger.removeHandler(handler); }
            }
        }
    }
}
