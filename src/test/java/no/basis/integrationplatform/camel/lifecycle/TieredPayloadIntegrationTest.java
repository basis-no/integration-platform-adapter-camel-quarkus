package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.TestJdbc;
import no.basis.integrationplatform.camel.payload.PayloadReference;
import no.basis.integrationplatform.camel.payload.PayloadStorageException;
import no.basis.integrationplatform.camel.payload.S3PayloadObjectStore;
import no.basis.integrationplatform.camel.replay.PostgresKafkaMessageReplayService;
import no.basis.integrationplatform.camel.store.PostgresMessageStoreSink;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TieredPayloadIntegrationTest {
    private static final String BUCKET = "bip-message-payloads";
    private static final String MESSAGE_ID = "large-message-123";
    private static final String TOPIC = "bip.large-payload-validation.in";
    private static final String INLINE_MESSAGE_ID = "inline-message-123";
    private static final String INLINE_TOPIC = "bip.inline-payload-validation.in";
    private static final int S3_MOCK_PORT = 9090;
    private static final String S3_ACCESS_KEY = "test";
    private static final String S3_SECRET_KEY = "test";
    private static final PostgreSQLContainer POSTGRES =
            new PostgreSQLContainer("postgres:16-alpine");
    private static final ConfluentKafkaContainer KAFKA =
            new ConfluentKafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    private static final GenericContainer<?> S3_MOCK =
            new GenericContainer<>(DockerImageName.parse("adobe/s3mock:5.1.0"))
                    .withExposedPorts(S3_MOCK_PORT)
                    .waitingFor(Wait.forHttp("/favicon.ico")
                            .forPort(S3_MOCK_PORT)
                            .forStatusCode(200));

    private static DataSource dataSource;
    private static S3Client s3;
    private static S3PayloadObjectStore objectStore;

    @BeforeAll
    static void startInfrastructure() throws Exception {
        org.testcontainers.lifecycle.Startables.deepStart(POSTGRES, KAFKA, S3_MOCK).join();
        dataSource = dataSource();
        TestJdbc.awaitReachable(dataSource);
        s3 = S3Client.builder()
                .endpointOverride(URI.create(
                        "http://" + S3_MOCK.getHost() + ":" + S3_MOCK.getMappedPort(S3_MOCK_PORT)))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(S3_ACCESS_KEY, S3_SECRET_KEY)))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .build();
        s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        objectStore = new S3PayloadObjectStore(s3, BUCKET, "payloads");
        createSchema();
    }

    @AfterAll
    static void stopInfrastructure() {
        if (s3 != null) {
            s3.close();
        }
        S3_MOCK.stop();
        KAFKA.stop();
        POSTGRES.stop();
    }

    @Test
    void storesStreamsReplaysAndResolvesLargePayloadByClaimCheck() throws Exception {
        byte[] payload = new byte[S3PayloadObjectStore.DEFAULT_PART_SIZE_BYTES + 257];
        for (int index = 0; index < payload.length; index++) {
            payload[index] = (byte) (index % 251);
        }

        PostgresMessageStoreSink sink = new PostgresMessageStoreSink(dataSource);
        MessageStoreRoutePolicy policy = policy(sink, "large-payload-validation");
        Exchange original = exchange(new ByteArrayInputStream(payload));
        original.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, MESSAGE_ID);
        original.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "large-order-123");
        original.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, "application/octet-stream");

        policy.onExchangeBegin(route(), original);
        assertTrue(original.getMessage().getBody() instanceof InputStream);
        assertArrayEquals(payload, original.getMessage().getBody(InputStream.class).readAllBytes());
        policy.onExchangeDone(route(), original);

        assertEquals("s3_object", value("payload_storage"));
        assertEquals(Integer.toString(payload.length), value("payload_size_bytes"));
        assertNull(nullableValue("payload_bytes"));
        assertTrue(value("payload_ref").startsWith("s3://" + BUCKET + "/payloads/"));
        assertEquals(64, value("payload_sha256").length());
        assertEquals("true", value("replayable::text"));
        String originalExpiry = value("payload_expires_at::text");

        PostgresKafkaMessageReplayService replay = new PostgresKafkaMessageReplayService(
                dataSource, KAFKA.getBootstrapServers(), "bip.%s.in", objectStore);
        replay.replay(MESSAGE_ID);

        try (KafkaConsumer<String, byte[]> consumer = consumer(TOPIC)) {
            var records = consumer.poll(Duration.ofSeconds(10));
            assertEquals(1, records.count());
            var record = records.iterator().next();
            assertNotEquals(payload.length, record.value().length);
            assertEquals("s3_object", header(record, PlatformHeaders.PAYLOAD_STORAGE));
            assertEquals(PayloadReference.CONTENT_TYPE,
                    header(record, PlatformHeaders.TRANSPORT_CONTENT_TYPE));
            assertEquals("application/octet-stream", header(record, PlatformHeaders.CONTENT_TYPE));

            PayloadReference reference = PayloadReference.fromJson(record.value());
            assertEquals(payload.length, reference.sizeBytes());
            assertEquals(value("payload_ref"), reference.ref());

            Exchange replayed = exchange(record.value());
            record.headers().forEach(header -> replayed.getMessage().setHeader(
                    header.key(), new String(header.value(), StandardCharsets.UTF_8)));
            replayed.getMessage().removeHeader(PlatformHeaders.PAYLOAD_STORAGE);
            policy.onExchangeBegin(route(), replayed);
            assertArrayEquals(payload, replayed.getMessage().getBody(InputStream.class).readAllBytes());
            policy.onExchangeDone(route(), replayed);
            assertEquals(originalExpiry, attemptValue(2, "payload_expires_at::text"));
        }
    }

    @Test
    void preservesInlinePayloadExpiryAcrossReplay() throws Exception {
        byte[] payload = "inline-payload".getBytes(StandardCharsets.UTF_8);
        PostgresMessageStoreSink sink = new PostgresMessageStoreSink(dataSource);
        MessageStoreRoutePolicy policy = policy(sink, "inline-payload-validation");
        Exchange original = exchange(payload);
        original.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, INLINE_MESSAGE_ID);
        original.getMessage().setHeader(PlatformHeaders.CORRELATION_ID, "inline-order-123");
        original.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, "text/plain");

        policy.onExchangeBegin(route(), original);
        policy.onExchangeDone(route(), original);

        String originalExpiry = value(INLINE_MESSAGE_ID, "payload_expires_at::text");
        String originalSha256 = value(INLINE_MESSAGE_ID, "payload_sha256");

        PostgresKafkaMessageReplayService replay = new PostgresKafkaMessageReplayService(
                dataSource, KAFKA.getBootstrapServers(), "bip.%s.in", objectStore);
        replay.replay(INLINE_MESSAGE_ID);

        try (KafkaConsumer<String, byte[]> consumer = consumer(INLINE_TOPIC)) {
            var records = consumer.poll(Duration.ofSeconds(10));
            assertEquals(1, records.count());
            var record = records.iterator().next();
            assertArrayEquals(payload, record.value());

            Exchange replayed = exchange(record.value());
            record.headers().forEach(header -> replayed.getMessage().setHeader(
                    header.key(), new String(header.value(), StandardCharsets.UTF_8)));
            policy.onExchangeBegin(route(), replayed);
            policy.onExchangeDone(route(), replayed);
        }

        assertEquals(originalExpiry, value(INLINE_MESSAGE_ID, "payload_expires_at::text"));
        assertEquals(originalExpiry, attemptValue(INLINE_MESSAGE_ID, 2, "payload_expires_at::text"));
        assertEquals(originalSha256, attemptValue(INLINE_MESSAGE_ID, 2, "payload_sha256"));
    }

    @Test
    void rejectsPayloadBeyondConfiguredObjectLimitBeforeUploading() {
        byte[] payload = new byte[2048];

        PayloadStorageException failure = assertThrows(PayloadStorageException.class,
                () -> objectStore.store("orders", "too-large", new ByteArrayInputStream(payload),
                        (long) payload.length, "application/octet-stream", expiresAt(), 1024));

        assertEquals("Payload size 2048 exceeds object limit 1024 bytes", failure.getMessage());
    }

    @Test
    void keepsRepeatedCapturesForTheSameMessageImmutable() throws Exception {
        byte[] first = "first-payload".getBytes(StandardCharsets.UTF_8);
        byte[] second = "second-payload".getBytes(StandardCharsets.UTF_8);

        PayloadReference firstReference = objectStore.store(
                "orders", "same-message", new ByteArrayInputStream(first),
                (long) first.length, "text/plain", expiresAt(), 1024 * 1024);
        PayloadReference secondReference = objectStore.store(
                "orders", "same-message", new ByteArrayInputStream(second),
                (long) second.length, "text/plain", expiresAt(), 1024 * 1024);

        assertNotEquals(firstReference.ref(), secondReference.ref());
        assertArrayEquals(first, objectStore.open(firstReference).readAllBytes());
        assertArrayEquals(second, objectStore.open(secondReference).readAllBytes());
    }

    @Test
    void detectsMissingAndCorruptPayloadObjects() throws Exception {
        byte[] original = "expected-payload".getBytes(StandardCharsets.UTF_8);
        PayloadReference reference = objectStore.store(
                "orders", "corrupt-check", new ByteArrayInputStream(original),
                (long) original.length, "text/plain", expiresAt(), 1024 * 1024);
        String key = reference.ref().substring(("s3://" + BUCKET + "/").length());

        s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(),
                RequestBody.fromBytes("different-value!".getBytes(StandardCharsets.UTF_8)));
        assertThrows(Exception.class, () -> objectStore.open(reference).readAllBytes());

        s3.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET).key(key).build());
        PayloadStorageException missing = assertThrows(PayloadStorageException.class,
                () -> objectStore.verify(reference));
        assertTrue(missing.getMessage().contains("unavailable"));
    }

    private static MessageStoreRoutePolicy policy(PostgresMessageStoreSink sink, String integrationName) {
        return new MessageStoreRoutePolicy(
                sink,
                integrationName,
                new PayloadCapturePolicy(PayloadMode.TIERED, 1024, 32L * 1024 * 1024,
                        Duration.ofDays(30), true),
                MessageBoundaryKind.KAFKA,
                true,
                objectStore);
    }

    private static Exchange exchange(Object body) {
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        exchange.getMessage().setBody(body);
        return exchange;
    }

    private static Route route() {
        InvocationHandler handler = (proxy, method, args) -> switch (method.getName()) {
            case "getRouteId", "getId" -> "large-payload-validation";
            case "toString" -> "route(large-payload-validation)";
            default -> throw new UnsupportedOperationException(method.getName());
        };
        return (Route) Proxy.newProxyInstance(
                TieredPayloadIntegrationTest.class.getClassLoader(), new Class<?>[]{Route.class}, handler);
    }

    private static DataSource dataSource() {
        PGSimpleDataSource result = new PGSimpleDataSource();
        result.setURL(POSTGRES.getJdbcUrl());
        result.setUser(POSTGRES.getUsername());
        result.setPassword(POSTGRES.getPassword());
        return result;
    }

    private static void createSchema() throws Exception {
        try (InputStream input = TieredPayloadIntegrationTest.class
                .getResourceAsStream("/db/message-store/bip_message_store.sql");
             Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            String schema = new String(input.readAllBytes(), StandardCharsets.UTF_8);
            for (String sql : schema.split(";")) {
                if (!sql.isBlank()) {
                    statement.execute(sql);
                }
            }
        }
    }

    private static String value(String expression) throws Exception {
        return value(MESSAGE_ID, expression);
    }

    private static String value(String messageId, String expression) throws Exception {
        String result = nullableValue(messageId, expression);
        if (result == null) {
            throw new AssertionError(expression + " was null");
        }
        return result;
    }

    private static String nullableValue(String expression) throws Exception {
        return nullableValue(MESSAGE_ID, expression);
    }

    private static String nullableValue(String messageId, String expression) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "select " + expression + " from bip_message_log where message_id = '" + messageId + "'")) {
            result.next();
            return result.getString(1);
        }
    }

    private static String attemptValue(int attempt, String expression) throws Exception {
        return attemptValue(MESSAGE_ID, attempt, expression);
    }

    private static String attemptValue(String messageId, int attempt, String expression) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "select " + expression + " from bip_message_attempt where message_id = '"
                             + messageId + "' and attempt = " + attempt)) {
            result.next();
            return result.getString(1);
        }
    }

    private static Instant expiresAt() {
        return Instant.now().plus(Duration.ofDays(30));
    }

    private static KafkaConsumer<String, byte[]> consumer(String topic) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "tiered-verifier-" + UUID.randomUUID());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private static String header(org.apache.kafka.clients.consumer.ConsumerRecord<String, byte[]> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
