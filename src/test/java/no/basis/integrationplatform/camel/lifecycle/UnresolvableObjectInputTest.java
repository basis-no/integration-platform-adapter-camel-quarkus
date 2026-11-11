package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.LogCapture;
import no.basis.integrationplatform.camel.PlatformDeadLetterRoute;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
import no.basis.integrationplatform.camel.payload.PayloadReference;
import no.basis.integrationplatform.camel.payload.PayloadStorageException;
import no.basis.integrationplatform.camel.payload.UnresolvablePayloadException;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Level;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * An object-stored Kafka input whose claim check can never resolve is parked, and committed
 * only once the DLQ accepted it; any failure that may pass keeps the record to be read again.
 */
class UnresolvableObjectInputTest {
    private static final byte[] OBJECT = "<Order id=\"o-1\" total=\"42\"/>".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path spoolDirectory;

    private final List<String> events = new CopyOnWriteArrayList<>();
    private final List<MessageAttemptResult> rows = new CopyOnWriteArrayList<>();
    private final List<byte[]> parked = new CopyOnWriteArrayList<>();
    private final AtomicInteger routeRuns = new AtomicInteger();

    /** Failures that can never pass, each of a claim check that arrives on a replay or a DLQ resend. */
    enum NeverPasses {
        CHANGED("does not match its reference's SHA-256"),
        TRUNCATED("has " + (OBJECT.length - 1) + " bytes; its reference has " + OBJECT.length),
        LONGER("is larger than its reference's " + OBJECT.length + " bytes"),
        EXPIRED("Payload reference has expired"),
        NO_SUCH_KEY("Payload object does not exist"),
        MALFORMED("Invalid payload-reference document"),
        HEADER_MISMATCH("Payload-reference header " + PlatformHeaders.PAYLOAD_SHA256 + " does not match its document");

        final String reason;

        NeverPasses(String reason) {
            this.reason = reason;
        }
    }

    @ParameterizedTest
    @EnumSource(NeverPasses.class)
    void aClaimCheckThatCanNeverPassIsParkedAndThenCommitted(NeverPasses failure) throws Exception {
        PayloadReference reference = reference(failure == NeverPasses.EXPIRED
                ? Instant.now().minus(Duration.ofDays(1)) : Instant.now().plus(Duration.ofDays(1)));
        byte[] body = failure == NeverPasses.MALFORMED
                ? "{\"schemaVersion\": 1, \"ref\":".getBytes(StandardCharsets.UTF_8) : reference.toJsonBytes();
        PayloadObjectStore store = store(ignored -> switch (failure) {
            case CHANGED -> {
                byte[] changed = OBJECT.clone();
                changed[0] ^= 1;
                yield new ByteArrayInputStream(changed);
            }
            case TRUNCATED -> new ByteArrayInputStream(OBJECT, 0, OBJECT.length - 1);
            case LONGER -> new ByteArrayInputStream((new String(OBJECT, StandardCharsets.UTF_8) + " ")
                    .getBytes(StandardCharsets.UTF_8));
            case NO_SUCH_KEY -> throw new UnresolvablePayloadException("Payload object does not exist: " + reference.ref());
            default -> throw new AssertionError("The object is not read for " + failure);
        });

        Exchange exchange;
        try (var log = LogCapture.of(MessageStoreRoutePolicy.class)) {
            exchange = run(store, spool(1024), false, message -> {
                message.setBody(body);
                message.setHeader(PlatformHeaders.TRANSPORT_CONTENT_TYPE, PayloadReference.CONTENT_TYPE);
                if (failure == NeverPasses.HEADER_MISMATCH) {
                    message.setHeader(PlatformHeaders.PAYLOAD_SHA256, "0".repeat(64));
                }
            });
            String logged = LogCapture.text(log.at(Level.SEVERE).getFirst());
            assertTrue(logged.endsWith("; it can never pass its check, so it is parked in the DLQ"), logged);
        }

        assertEquals(0, routeRuns.get(), "the route does not run");
        assertEquals(List.of("dlq", "record DLQ", "commit"), events, "committed only after the DLQ accepted it");
        assertEquals(1, parked.size());
        // Parked as its claim check; one that could not be read is parked as it came.
        boolean readable = failure != NeverPasses.MALFORMED && failure != NeverPasses.HEADER_MISMATCH;
        assertArrayEquals(readable ? reference.toJsonBytes() : body, parked.getFirst());
        assertFalse(exchange.isFailed(), "a parked input is handled");
        assertEquals(MessageStatus.DLQ, rows.getFirst().status());
        assertTrue(rows.getFirst().error().contains(failure.reason), rows.getFirst().error());
        if (readable) {
            assertEquals("s3_object", rows.getFirst().payloadStorage());
            assertEquals(reference.ref(), rows.getFirst().payloadRef());
        }
    }

    /** Failures that may pass, so the record must be read again. */
    enum MayPass {
        STORE_UNAVAILABLE, APPLICATION_STORE_ERROR, COPY_INTERRUPTED, SPOOL_FULL
    }

    @ParameterizedTest
    @EnumSource(MayPass.class)
    void aClaimCheckThatMayStillPassIsNeitherParkedNorCommitted(MayPass failure) throws Exception {
        PayloadReference reference = reference(Instant.now().plus(Duration.ofDays(1)));
        PayloadObjectStore store = store(ignored -> switch (failure) {
            case STORE_UNAVAILABLE -> throw new PayloadStorageException("Payload object is unavailable: " + reference.ref());
            case APPLICATION_STORE_ERROR -> throw new IllegalStateException("store client closed");
            case COPY_INTERRUPTED -> new FilterInputStream(new ByteArrayInputStream(OBJECT)) {
                @Override
                public int read(byte[] bytes, int offset, int length) throws IOException {
                    throw new IOException("Read timed out");
                }
            };
            case SPOOL_FULL -> new ByteArrayInputStream(OBJECT);
        });

        Exchange exchange = run(store, spool(failure == MayPass.SPOOL_FULL ? OBJECT.length - 1 : 1024), false, message -> {
            message.setBody(reference.toJsonBytes());
            message.setHeader(PlatformHeaders.TRANSPORT_CONTENT_TYPE, PayloadReference.CONTENT_TYPE);
        });

        assertReadAgain(exchange);
        assertFalse(exchange.getException() instanceof UnresolvablePayloadException, exchange.getException().toString());
    }

    @Test
    void theFirstAttemptOfAStreamedInputIsNeverParked() throws Exception {
        // The object reads back changed after the adapter stored it; the record itself is still the source.
        PayloadObjectStore store = new PayloadObjectStore() {
            @Override
            public PayloadReference store(String integration, String messageId, InputStream payload,
                                          Long declaredSize, String contentType, Instant expiresAt, long maxBytes) {
                try {
                    payload.readAllBytes();
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
                return reference(expiresAt);
            }

            @Override
            public InputStream open(PayloadReference reference) {
                byte[] changed = OBJECT.clone();
                changed[0] ^= 1;
                return new ByteArrayInputStream(changed);
            }

            @Override
            public void verify(PayloadReference reference) {
            }
        };

        Exchange exchange = run(store, spool(1024), false, message -> message.setBody(new ByteArrayInputStream(OBJECT)));

        assertReadAgain(exchange);
        assertInstanceOf(UnresolvablePayloadException.class, exchange.getException(),
                "the same failure as on a replay, but the first attempt is read again");
    }

    @Test
    void aClaimCheckThatTheDlqRefusesIsNotCommittedAndIsReadAgain() throws Exception {
        PayloadReference reference = reference(Instant.now().minus(Duration.ofDays(1)));

        Exchange exchange;
        try (var log = LogCapture.of(MessageStoreRoutePolicy.class)) {
            exchange = run(store(ignored -> { throw new AssertionError("not read"); }), spool(1024), true,
                    message -> {
                        message.setBody(reference.toJsonBytes());
                        message.setHeader(PlatformHeaders.TRANSPORT_CONTENT_TYPE, PayloadReference.CONTENT_TYPE);
                    });
            assertTrue(log.at(Level.SEVERE).stream().map(LogCapture::text).anyMatch(text -> text.equals(
                    "Message m-1 could not be parked, so it is read again: "
                            + "java.lang.IllegalStateException: synthetic DLQ outage")),
                    log.at(Level.SEVERE).stream().map(LogCapture::text).toList().toString());
        }

        assertEquals(0, routeRuns.get());
        assertEquals(List.of("dlq", "record FAILED"), events, "nothing is committed when the DLQ refuses it");
        assertTrue(parked.isEmpty());
        assertTrue(exchange.isFailed(), "a failed exchange is read again");
        assertInstanceOf(UnresolvablePayloadException.class, exchange.getException());
        assertTrue(rows.getFirst().error().contains("Payload reference has expired"), rows.getFirst().error());
    }

    private void assertReadAgain(Exchange exchange) {
        assertEquals(0, routeRuns.get(), "the route does not run");
        assertEquals(List.of("record FAILED"), events, "not parked and not committed");
        assertTrue(parked.isEmpty());
        assertTrue(exchange.isFailed(), "a failed exchange is read again");
        assertEquals(MessageStatus.FAILED, rows.getFirst().status());
    }

    private Exchange run(PayloadObjectStore store, VerifiedPayloadSpool spool, boolean dlqRefuses,
                         Consumer<Message> input) throws Exception {
        try (var context = new DefaultCamelContext()) {
            var properties = new Properties();
            properties.setProperty("basis.message-store.dlq-uri", "direct:parking");
            context.getPropertiesComponent().setOverrideProperties(properties);
            var policy = new MessageStoreRoutePolicy(row -> {
                events.add("record " + row.status());
                rows.add(row);
            }, "orders", new PayloadCapturePolicy(PayloadMode.TIERED, 8, 1024 * 1024, Duration.ofDays(30), true),
                    MessageBoundaryKind.KAFKA, true, store);
            policy.setPayloadSpool(spool);
            context.addRoutes(new PlatformRouteConfiguration());
            context.addRoutes(new PlatformDeadLetterRoute());
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from("direct:input").routeId("orders").routePolicy(policy)
                            .process(e -> routeRuns.incrementAndGet());
                    from("direct:parking").routeId("parking").errorHandler(noErrorHandler())
                            .process(e -> {
                                events.add("dlq");
                                if (dlqRefuses) {
                                    throw new IllegalStateException("synthetic DLQ outage");
                                }
                                parked.add(e.getMessage().getBody(byte[].class));
                            });
                }
            });
            context.start();
            try (var producer = context.createProducerTemplate()) {
                return producer.send("direct:input", e -> {
                    e.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, (KafkaManualCommit) () -> events.add("commit"));
                    e.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, "m-1");
                    input.accept(e.getMessage());
                });
            }
        }
    }

    private VerifiedPayloadSpool spool(long maxBytes) {
        return new VerifiedPayloadSpool(spoolDirectory, maxBytes);
    }

    private static PayloadObjectStore store(Function<PayloadReference, InputStream> open) {
        return new PayloadObjectStore() {
            @Override
            public PayloadReference store(String integration, String messageId, InputStream payload,
                                          Long declaredSize, String contentType, Instant expiresAt, long maxBytes) {
                throw new UnsupportedOperationException();
            }

            @Override
            public InputStream open(PayloadReference reference) {
                return open.apply(reference);
            }

            @Override
            public void verify(PayloadReference reference) {
            }
        };
    }

    private static PayloadReference reference(Instant expiresAt) {
        try {
            String sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(OBJECT));
            return new PayloadReference(1, PayloadReference.STORAGE, "s3://bucket/payloads/orders/m-1", OBJECT.length,
                    sha256, "application/xml", expiresAt.toString());
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
