package no.basis.integrationplatform.camel.lifecycle;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.traffic.TrafficSignals;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** The opt-in admission counter counts records consumed from a real broker. */
@Testcontainers
class KafkaAdmissionSignalTest {
    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Test
    void countsEveryRecordAdmittedFromARealBroker() throws Exception {
        String input = "input-" + UUID.randomUUID();
        String brokers = KAFKA.getBootstrapServers();
        var registry = new SimpleMeterRegistry();
        var signals = signals(registry);
        var rows = new CopyOnWriteArrayList<MessageAttemptResult>();
        var coordinates = new CopyOnWriteArrayList<String>();
        try (var admin = AdminClient.create(Map.<String, Object>of("bootstrap.servers", brokers));
             var context = new DefaultCamelContext()) {
            admin.createTopics(List.of(new NewTopic(input, 1, (short) 1))).all().get(20, TimeUnit.SECONDS);
            var policy = new MessageStoreRoutePolicy(rows::add, "order-pipeline");
            // As MessageStoreRoutePolicyFactory wires a selected Kafka boundary.
            policy.setTrafficSignals(signals);
            signals.observeKafkaBoundary();
            context.addRoutes(new RouteBuilder() {
                public void configure() {
                    from("kafka:" + input + "?brokers=" + brokers + "&groupId=g-" + UUID.randomUUID()
                            + "&autoOffsetReset=earliest&autoCommitEnable=false&allowManualCommit=true")
                        .routePolicy(policy)
                        .process(e -> coordinates.add(type(e, KafkaConstants.TOPIC) + "/" + type(e, KafkaConstants.PARTITION)
                                + "/" + type(e, KafkaConstants.OFFSET)));
                }
            });
            context.start();
            try (var producer = new KafkaProducer<String, String>(Map.of("bootstrap.servers", brokers),
                    new StringSerializer(), new StringSerializer())) {
                for (int i = 0; i < 3; i++) producer.send(new ProducerRecord<>(input, "k" + i, "v" + i)).get();
            }
            long deadline = System.currentTimeMillis() + 30_000;
            while (rows.size() < 3 && System.currentTimeMillis() < deadline) Thread.sleep(100);
        }
        assertEquals(3, rows.size());
        assertEquals(1.0, registry.get("integration_traffic_signal_supported").tag("signal", "record_admitted").gauge().value(),
                "header types at admission: " + coordinates);
        assertEquals(3.0, registry.get("integration_traffic_events").tag("signal", "record_admitted").functionCounter().count());
    }

    private static TrafficSignals signals(SimpleMeterRegistry registry) throws Exception {
        var constructor = TrafficSignals.class.getDeclaredConstructor(io.micrometer.core.instrument.MeterRegistry.class, String.class, String.class);
        constructor.setAccessible(true);
        return constructor.newInstance(registry, "record_admitted", "");
    }

    private static String type(org.apache.camel.Exchange e, String header) {
        Object value = e.getMessage().getHeader(header);
        return value == null ? "null" : value.getClass().getSimpleName();
    }
}
