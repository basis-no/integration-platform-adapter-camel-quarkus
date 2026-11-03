package example;

import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

public class AclBroker implements QuarkusTestResourceLifecycleManager {
    static final String ADMIN_PASSWORD = "synthetic-admin-only";
    static final String READER_PASSWORD = "synthetic-reader-only";
    private ConfluentKafkaContainer broker;

    static String jaas(String user, String password) {
        return "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
                + user + "\" password=\"" + password + "\";";
    }
    static Map<String, Object> client(String brokers, String user, String password) {
        return Map.of("bootstrap.servers", brokers, "security.protocol", "SASL_PLAINTEXT",
                "sasl.mechanism", "PLAIN", "sasl.jaas.config", jaas(user, password),
                "request.timeout.ms", 5000, "default.api.timeout.ms", 10000);
    }
    static AclBinding allow(ResourceType type, String resource, AclOperation operation) {
        return new AclBinding(new ResourcePattern(type, resource, PatternType.LITERAL),
                new AccessControlEntry("User:reader", "*", operation, AclPermissionType.ALLOW));
    }
    @Override
    public Map<String, String> start() {
        String run = System.getProperty("bip.test.run-id");
        if (run == null || run.isBlank()) throw new IllegalArgumentException("Explicit unique bip.test.run-id required");
        String topic = "acl-input-" + UUID.randomUUID();
        String group = "acl-group-" + UUID.randomUUID();
        broker = new ConfluentKafkaContainer(DockerImageName.parse(
                "confluentinc/cp-kafka@sha256:620734d9fc0bb1f9886932e5baf33806074469f40e3fe246a3fdbb59309535fa"))
                .withImagePullPolicy(image -> false).withLabel("bip.qualification.run", run)
                .withStartupTimeout(Duration.ofSeconds(90))
                .withEnv("KAFKA_HEAP_OPTS", "-Xms128m -Xmx256m")
                .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT")
                .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
                .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"admin\" password=\""
                        + ADMIN_PASSWORD + "\" user_admin=\"" + ADMIN_PASSWORD + "\" user_reader=\"" + READER_PASSWORD + "\";")
                .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
                .withEnv("KAFKA_SUPER_USERS", "User:admin;User:ANONYMOUS")
                .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false")
                .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
                .withCreateContainerCmdModifier(command -> {
                    var ports = command.getExposedPorts();
                    if (ports == null || ports.length == 0) throw new IllegalStateException("No broker port");
                    command.getHostConfig().withMemory(768L * 1024 * 1024).withMemorySwap(768L * 1024 * 1024)
                            .withPidsLimit(256L).withNanoCPUs(1_000_000_000L)
                            .withPortBindings(java.util.Arrays.stream(ports).map(port -> new PortBinding(
                                    Ports.Binding.bindIp("127.0.0.1"), port)).toArray(PortBinding[]::new));
                });
        try {
            broker.start();
            try (var admin = AdminClient.create(client(broker.getBootstrapServers(), "admin", ADMIN_PASSWORD))) {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(15, TimeUnit.SECONDS);
                admin.createAcls(List.of(allow(ResourceType.TOPIC, topic, AclOperation.DESCRIBE),
                        allow(ResourceType.GROUP, group, AclOperation.READ)))
                        .all().get(15, TimeUnit.SECONDS);
            }
            return Map.of("fixture.brokers", broker.getBootstrapServers(), "fixture.topic", topic, "fixture.group", group,
                    "kafka.bootstrap.servers", broker.getBootstrapServers(),
                    "camel.component.kafka.brokers", broker.getBootstrapServers(),
                    "basis.message-store.kafka.security-protocol", "SASL_PLAINTEXT",
                    "basis.message-store.kafka.sasl-mechanism", "PLAIN",
                    "basis.message-store.kafka.sasl-jaas-config", jaas("reader", READER_PASSWORD));
        } catch (Exception failure) {
            stop();
            throw new IllegalStateException("Synthetic broker initialization failed", failure);
        }
    }
    @Override
    public void stop() { if (broker != null) broker.stop(); }
}
