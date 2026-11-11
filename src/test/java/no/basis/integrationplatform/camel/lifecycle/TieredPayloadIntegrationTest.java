package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.TestJdbc;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
import no.basis.integrationplatform.camel.payload.PayloadReference;
import no.basis.integrationplatform.camel.payload.PayloadStorageException;
import no.basis.integrationplatform.camel.payload.S3PayloadObjectStore;
import no.basis.integrationplatform.camel.payload.UnresolvablePayloadException;
import no.basis.integrationplatform.camel.replay.PostgresKafkaMessageReplayService;
import no.basis.integrationplatform.camel.store.PostgresMessageStoreSink;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
import java.nio.file.Files;
import java.nio.file.Path;
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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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

        try (DefaultCamelContext context = kafkaInput("large-payload-validation", TOPIC)) {
            new PostgresKafkaMessageReplayService(dataSource, KAFKA.getBootstrapServers(), "bip.%s.in",
                    objectStore, context, "large-payload-validation").replay(MESSAGE_ID);
        }

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
    void replayResetsInlinePayloadExpiryWithinTheCap() throws Exception {
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

        try (DefaultCamelContext context = kafkaInput("large-payload-validation", INLINE_TOPIC)) {
            new PostgresKafkaMessageReplayService(dataSource, KAFKA.getBootstrapServers(), "bip.%s.in",
                    objectStore, context, "inline-payload-validation").replay(INLINE_MESSAGE_ID);
        }

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

        // The replay captured the payload again: its expiry is reset, still within first receipt + 2x retention.
        String replayedExpiry = attemptValue(INLINE_MESSAGE_ID, 2, "payload_expires_at::text");
        assertEquals(replayedExpiry, value(INLINE_MESSAGE_ID, "payload_expires_at::text"));
        assertEquals("true", attemptValue(INLINE_MESSAGE_ID, 2, "(payload_expires_at > (select first.payload_expires_at"
                + " from bip_message_attempt first where first.message_id = '" + INLINE_MESSAGE_ID + "' and first.attempt = 1))::text"));
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
        assertInstanceOf(UnresolvablePayloadException.class, missing);
        assertTrue(missing.getMessage().contains("does not exist"), missing.getMessage());
    }

    @Test
    void aChangedObjectIsParkedBeforeTheRouteReadsAnyOfIt() throws Exception {
        byte[] original = new byte[4096];
        for (int index = 0; index < original.length; index++) {
            original[index] = (byte) (index % 199);
        }
        PayloadReference reference = objectStore.store(
                "large-payload-validation", "changed-before-route", new ByteArrayInputStream(original),
                (long) original.length, "application/octet-stream", expiresAt(), 1024 * 1024);
        String key = reference.ref().substring(("s3://" + BUCKET + "/").length());
        byte[] changed = original.clone();
        changed[0] ^= 1;
        // Same size, so the size check passes; only the full SHA-256 check can see it.
        s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(), RequestBody.fromBytes(changed));

        var boundary = new Boundary(objectStore);
        Exchange replayed = boundary.send(claimCheck(reference, "changed-before-route"));

        assertEquals(0, boundary.reads.get(), "the route must not see a byte of an object that does not match");
        assertEquals(List.of("dlq", "record DLQ", "commit"), boundary.events, "committed only after the DLQ accepted it");
        assertArrayEquals(reference.toJsonBytes(), boundary.parked.getFirst(), "parked as its claim check");
        assertFalse(replayed.isFailed());
        MessageAttemptResult row = boundary.rows.getFirst();
        assertTrue(row.error().contains("does not match its reference's SHA-256"), row.error());
        // The outcome keeps the message's claim check, so a resend after the object is restored still has it.
        assertEquals("s3_object", row.payloadStorage());
        assertEquals(reference.ref(), row.payloadRef());
        assertEquals(reference.sha256(), row.payloadSha256());

        // The restored object reaches the route complete.
        s3.putObject(PutObjectRequest.builder().bucket(BUCKET).key(key).build(), RequestBody.fromBytes(original));
        Exchange intact = exchange(reference.toJsonBytes());
        intact.getMessage().setHeader(PlatformHeaders.TRANSPORT_CONTENT_TYPE, PayloadReference.CONTENT_TYPE);
        boundary.policy.onExchangeBegin(route(), intact);
        assertArrayEquals(original, intact.getMessage().getBody(InputStream.class).readAllBytes());
        boundary.policy.onExchangeDone(route(), intact);
    }

    @Test
    void aTruncatedObjectIsParked() throws Exception {
        byte[] original = "an object that is cut short".getBytes(StandardCharsets.UTF_8);
        PayloadReference reference = objectStore.store(
                "large-payload-validation", "truncated-before-route", new ByteArrayInputStream(original),
                (long) original.length, "text/plain", expiresAt(), 1024 * 1024);
        s3.putObject(PutObjectRequest.builder().bucket(BUCKET)
                .key(reference.ref().substring(("s3://" + BUCKET + "/").length())).build(),
                RequestBody.fromBytes(java.util.Arrays.copyOf(original, original.length - 4)));

        var boundary = new Boundary(objectStore);
        Exchange parked = boundary.send(claimCheck(reference, "truncated-before-route"));

        // The store itself reports the other size.
        assertEquals(List.of("dlq", "record DLQ", "commit"), boundary.events);
        assertFalse(parked.isFailed());
        assertTrue(boundary.rows.getFirst().error().contains("Payload object size mismatch"),
                boundary.rows.getFirst().error());
    }

    @Test
    void aDeletedObjectIsParkedAndAMissingBucketIsReadAgain() throws Exception {
        byte[] original = "a deleted object".getBytes(StandardCharsets.UTF_8);
        PayloadReference reference = objectStore.store(
                "large-payload-validation", "deleted-before-route", new ByteArrayInputStream(original),
                (long) original.length, "text/plain", expiresAt(), 1024 * 1024);
        s3.deleteObject(DeleteObjectRequest.builder().bucket(BUCKET)
                .key(reference.ref().substring(("s3://" + BUCKET + "/").length())).build());

        var deleted = new Boundary(objectStore);
        Exchange parked = deleted.send(claimCheck(reference, "deleted-before-route"));

        assertEquals(List.of("dlq", "record DLQ", "commit"), deleted.events);
        assertFalse(parked.isFailed());
        assertTrue(deleted.rows.getFirst().error().contains("Payload object does not exist"), deleted.rows.getFirst().error());

        // A bucket that does not exist, as after a wrong deployment setting, is not the object's fault.
        String missingBucket = "no-such-payload-bucket";
        PayloadReference elsewhere = new PayloadReference(1, PayloadReference.STORAGE,
                reference.ref().replace("s3://" + BUCKET + "/", "s3://" + missingBucket + "/"), reference.sizeBytes(),
                reference.sha256(), reference.contentType(), reference.expiresAt());
        var wrongBucket = new Boundary(new S3PayloadObjectStore(s3, missingBucket, "payloads"));
        Exchange readAgain = wrongBucket.send(claimCheck(elsewhere, "missing-bucket"));

        assertEquals(List.of("record FAILED"), wrongBucket.events, "not parked and not committed");
        assertTrue(readAgain.isFailed());
        assertTrue(wrongBucket.rows.getFirst().error().contains("Payload object is unavailable"),
                wrongBucket.rows.getFirst().error());
    }

    @Test
    void aStreamedInputThatReadsBackChangedIsRefusedBeforeTheRouteReadsAnyOfIt(@TempDir Path spoolDirectory)
            throws Exception {
        byte[] payload = new byte[4096];
        for (int index = 0; index < payload.length; index++) {
            payload[index] = (byte) (index % 197);
        }
        // The object changes after the adapter stored it, before it is read back for the route.
        PayloadObjectStore changedAfterUpload = new PayloadObjectStore() {
            @Override
            public PayloadReference store(String integration, String messageId, InputStream input,
                                          Long declaredSize, String contentType, Instant expiresAt, long maxBytes) {
                PayloadReference stored = objectStore.store(
                        integration, messageId, input, declaredSize, contentType, expiresAt, maxBytes);
                byte[] changed = payload.clone();
                changed[payload.length - 1] ^= 1;
                s3.putObject(PutObjectRequest.builder().bucket(BUCKET)
                        .key(stored.ref().substring(("s3://" + BUCKET + "/").length())).build(),
                        RequestBody.fromBytes(changed));
                return stored;
            }

            @Override
            public InputStream open(PayloadReference reference) {
                return objectStore.open(reference);
            }

            @Override
            public void verify(PayloadReference reference) {
                objectStore.verify(reference);
            }
        };
        var boundary = new Boundary(changedAfterUpload);
        VerifiedPayloadSpool spool = new VerifiedPayloadSpool(spoolDirectory, 1024 * 1024);
        boundary.policy.setPayloadSpool(spool);

        Exchange streamed = boundary.send(message -> {
            message.setBody(new ByteArrayInputStream(payload));
            message.setHeader(PlatformHeaders.MESSAGE_ID, "changed-after-upload");
        });

        assertEquals(0, boundary.reads.get(), "the route must not see a byte of an object that does not match");
        // The record itself is still the source, so it is read again and stored again, not parked.
        assertEquals(List.of("record FAILED"), boundary.events, "not parked and not committed");
        assertTrue(streamed.isFailed());
        assertTrue(streamed.getException().getMessage().contains("does not match its reference's SHA-256"),
                streamed.getException().getMessage());
        assertEquals(0, spool.reservedBytes());
        try (var left = Files.walk(spoolDirectory)) {
            assertEquals(0, left.filter(Files::isRegularFile).count(), "no copy is left");
        }
    }

    private static java.util.function.Consumer<org.apache.camel.Message> claimCheck(PayloadReference reference,
                                                                                  String messageId) {
        return message -> {
            message.setBody(reference.toJsonBytes());
            message.setHeader(PlatformHeaders.MESSAGE_ID, messageId);
            message.setHeader(PlatformHeaders.TRANSPORT_CONTENT_TYPE, PayloadReference.CONTENT_TYPE);
            message.setHeader(PlatformHeaders.PAYLOAD_REF, reference.ref());
            message.setHeader(PlatformHeaders.PAYLOAD_SIZE, Long.toString(reference.sizeBytes()));
            message.setHeader(PlatformHeaders.PAYLOAD_SHA256, reference.sha256());
        };
    }

    /**
     * A managed Kafka boundary route with the adapter's route configuration and dead-letter
     * route, which parks into a recording route. Each record, DLQ publish and commit is an event.
     */
    private static final class Boundary {
        final List<String> events = new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<MessageAttemptResult> rows = new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<byte[]> parked = new java.util.concurrent.CopyOnWriteArrayList<>();
        final java.util.concurrent.atomic.AtomicInteger reads = new java.util.concurrent.atomic.AtomicInteger();
        final MessageStoreRoutePolicy policy;

        Boundary(PayloadObjectStore store) {
            policy = new MessageStoreRoutePolicy(
                    row -> {
                        events.add("record " + row.status());
                        rows.add(row);
                    },
                    "large-payload-validation",
                    new PayloadCapturePolicy(PayloadMode.TIERED, 1024, 32L * 1024 * 1024, Duration.ofDays(30), true),
                    MessageBoundaryKind.KAFKA,
                    true,
                    store);
        }

        Exchange send(java.util.function.Consumer<org.apache.camel.Message> input) throws Exception {
            try (DefaultCamelContext context = new DefaultCamelContext()) {
                var properties = new Properties();
                properties.setProperty("basis.message-store.dlq-uri", "direct:parked");
                context.getPropertiesComponent().setOverrideProperties(properties);
                context.addRoutes(new PlatformRouteConfiguration());
                context.addRoutes(new PlatformDeadLetterRoute());
                context.addRoutes(new RouteBuilder() {
                    @Override
                    public void configure() {
                        from("direct:parked").routeId("parked").errorHandler(noErrorHandler())
                                .process(e -> {
                                    events.add("dlq");
                                    parked.add(e.getMessage().getBody(byte[].class));
                                });
                        from("direct:input").routeId("large-payload-validation").routePolicy(policy)
                                .process(e -> {
                                    // Reading even the first byte would be a failure of the check.
                                    reads.incrementAndGet();
                                    e.getMessage().getBody(InputStream.class).read();
                                });
                    }
                });
                context.start();
                return context.createProducerTemplate().send("direct:input", e -> {
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> events.add("commit"));
                    input.accept(e.getMessage());
                });
            }
        }
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

    /** A context whose route of this id consumes the integration's replay topic, as the recording route did. */
    private static DefaultCamelContext kafkaInput(String routeId, String topic) throws Exception {
        DefaultCamelContext context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("kafka:" + topic + "?brokers=" + KAFKA.getBootstrapServers())
                        .routeId(routeId).autoStartup(false).process(e -> { });
            }
        });
        context.start();
        return context;
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
