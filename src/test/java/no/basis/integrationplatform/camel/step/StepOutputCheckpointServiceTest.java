package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.lifecycle.PayloadCapturePolicy;
import no.basis.integrationplatform.camel.lifecycle.PayloadMode;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
import no.basis.integrationplatform.camel.payload.PayloadReference;
import no.basis.integrationplatform.camel.payload.PayloadStorageException;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StepOutputCheckpointServiceTest {
    @Test
    void resolvesPayloadThresholdWithBackwardCompatiblePrecedence() {
        assertEquals(2048, StepOutputCheckpointService.resolveInlineThreshold(
                Optional.of(2048L), Optional.of(4096L)));
        assertEquals(4096, StepOutputCheckpointService.resolveInlineThreshold(
                Optional.empty(), Optional.of(4096L)));
        assertEquals(PayloadCapturePolicy.DEFAULT_INLINE_THRESHOLD_BYTES,
                StepOutputCheckpointService.resolveInlineThreshold(Optional.empty(), Optional.empty()));
    }

    @Test
    void refusesCredentialLikeCheckpointHeadersAndNamesEach() {
        var refused = assertThrows(IllegalArgumentException.class,
                () -> withHeaders("Partner-Request-Id, Authorization, x-api-key, SET-COOKIE, Session-Token"));

        String message = refused.getMessage();
        assertTrue(message.startsWith("basis.message-store.step.output-checkpoint.header-names names "
                + "credential-like headers Authorization, Session-Token, SET-COOKIE, x-api-key."), message);
        assertTrue(!message.contains("Partner-Request-Id"), message);
    }

    @Test
    void acceptsOrdinaryCheckpointHeadersAndTheAdaptersOwn() {
        withHeaders("Partner-Request-Id, Location, Content-Type, x-bip-correlation-id, Idempotency-Key");
        withHeaders("");
    }

    @Test
    void capturesAndRestoresBodySelectedHeadersAndProperties() {
        StepOutputCheckpointService service = service(1024);
        Exchange original = exchange();
        original.getMessage().setBody("partner-order-42");
        original.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, "text/plain");
        original.getMessage().setHeader("Partner-Request-Id", "request-123");
        original.getMessage().setHeader("Ignored", "not-durable");
        original.setProperty("partnerStatus", 202);

        StepOutputCheckpoint checkpoint = service.capture(original, "message-123", "create-order");

        Exchange replay = exchange();
        replay.getMessage().setBody("original replay input");
        service.restore(replay, "create-order", checkpoint);

        assertEquals(StepOutputCheckpoint.POSTGRES_INLINE, checkpoint.storage());
        assertEquals("partner-order-42", replay.getMessage().getBody());
        assertEquals("text/plain", replay.getMessage().getHeader(PlatformHeaders.CONTENT_TYPE));
        assertEquals("request-123", replay.getMessage().getHeader("Partner-Request-Id"));
        assertNull(replay.getMessage().getHeader("Ignored"));
        assertEquals(202, replay.getProperty("partnerStatus"));
    }

    @Test
    void preservesByteArrayBodyType() {
        StepOutputCheckpointService service = service(1024);
        Exchange original = exchange();
        original.getMessage().setBody(new byte[]{0, 1, -1});

        StepOutputCheckpoint checkpoint = service.capture(original, "message-123", "create-order");
        Exchange replay = exchange();
        service.restore(replay, "create-order", checkpoint);

        assertArrayEquals(new byte[]{0, 1, -1}, replay.getMessage().getBody(byte[].class));
    }

    @Test
    void rejectsCorruptInlineCheckpoint() {
        StepOutputCheckpointService service = service(1024);
        StepOutputCheckpoint corrupt = new StepOutputCheckpoint(
                StepOutputCheckpoint.POSTGRES_INLINE,
                "changed".getBytes(),
                null,
                7,
                "a".repeat(64),
                StepOutputCheckpoint.UTF8_BODY,
                "text/plain",
                Instant.now().plusSeconds(60),
                Map.of(),
                Map.of());

        StepOutputCheckpointException failure = assertThrows(
                StepOutputCheckpointException.class,
                () -> service.restore(exchange(), "create-order", corrupt));

        assertTrue(failure.getMessage().contains("missing or corrupt"));
        assertTrue(failure.getMessage().contains("explicit redo"));
    }

    @Test
    void rejectsExpiredCheckpoint() {
        StepOutputCheckpointService service = service(1024);
        StepOutputCheckpoint expired = new StepOutputCheckpoint(
                StepOutputCheckpoint.POSTGRES_INLINE,
                new byte[0],
                null,
                0,
                "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
                StepOutputCheckpoint.NULL_BODY,
                null,
                Instant.now().minusSeconds(1),
                Map.of(),
                Map.of());

        StepOutputCheckpointException failure = assertThrows(
                StepOutputCheckpointException.class,
                () -> service.restore(exchange(), "create-order", expired));

        assertTrue(failure.getMessage().contains("expired"));
    }

    @Test
    void rejectsOutputAboveInlineThresholdWhenTieredStorageIsDisabled() {
        StepOutputCheckpointService service = service(4);
        Exchange original = exchange();
        original.getMessage().setBody("larger than four bytes");

        StepOutputCheckpointException failure = assertThrows(
                StepOutputCheckpointException.class,
                () -> service.capture(original, "message-123", "create-order"));

        assertTrue(failure.getMessage().contains("tiered S3 storage is disabled"));
    }

    @Test
    void restoresAnObjectBackedTextOutputAsText() {
        Object restored = objectRoundTrip("partner-order-42 with a longer body");

        assertEquals("partner-order-42 with a longer body", restored);
    }

    @Test
    void restoresAnObjectBackedJsonOutputAsItsValue() {
        Object restored = objectRoundTrip(Map.of("orderId", "partner-order-42", "lines", List.of(1, 2, 3)));

        assertEquals(Map.of("orderId", "partner-order-42", "lines", List.of(1, 2, 3)), restored);
    }

    @Test
    void restoresAnObjectBackedByteOutputAsBytes() {
        byte[] output = "binary partner output".getBytes(StandardCharsets.UTF_8);

        Object restored = objectRoundTrip(output);

        assertArrayEquals(output, assertInstanceOf(byte[].class, restored));
    }

    @Test
    void restoresAnObjectBackedStreamedOutputAsVerifiedBytes() {
        byte[] output = "streamed partner output".getBytes(StandardCharsets.UTF_8);

        Object restored = objectRoundTrip(new ByteArrayInputStream(output));

        assertArrayEquals(output, assertInstanceOf(byte[].class, restored));
    }

    @Test
    void refusesAChangedObjectBeforeTheRouteSeesAnyOfIt() {
        InMemoryObjectStore store = new InMemoryObjectStore();
        StepOutputCheckpoint checkpoint = objectCheckpoint(store, "partner-order-42 with a longer body");
        store.replace(checkpoint.bodyRef(), "partner-order-43 with a longer body".getBytes(StandardCharsets.UTF_8));

        assertRefusedBeforeRestore(store, checkpoint);
    }

    @Test
    void refusesATruncatedObjectBeforeTheRouteSeesAnyOfIt() {
        InMemoryObjectStore store = new InMemoryObjectStore();
        StepOutputCheckpoint checkpoint = objectCheckpoint(store, "partner-order-42 with a longer body");
        store.replace(checkpoint.bodyRef(), "partner-order-42".getBytes(StandardCharsets.UTF_8));

        assertRefusedBeforeRestore(store, checkpoint);
    }

    @Test
    void refusesALongerObjectBeforeTheRouteSeesAnyOfIt() {
        InMemoryObjectStore store = new InMemoryObjectStore();
        StepOutputCheckpoint checkpoint = objectCheckpoint(store, "partner-order-42 with a longer body");
        store.replace(checkpoint.bodyRef(),
                "partner-order-42 with a longer body and more".getBytes(StandardCharsets.UTF_8));

        assertRefusedBeforeRestore(store, checkpoint);
    }

    @Test
    void refusesAMissingObject() {
        InMemoryObjectStore store = new InMemoryObjectStore();
        StepOutputCheckpoint checkpoint = objectCheckpoint(store, "partner-order-42 with a longer body");
        store.remove(checkpoint.bodyRef());

        assertRefusedBeforeRestore(store, checkpoint);
    }

    private Object objectRoundTrip(Object output) {
        InMemoryObjectStore store = new InMemoryObjectStore();
        StepOutputCheckpoint checkpoint = objectCheckpoint(store, output);
        Exchange replay = exchange();
        replay.getMessage().setBody("original replay input");

        tieredService(store).restore(replay, "create-order", checkpoint);

        assertEquals("application/vnd.partner+json", replay.getMessage().getHeader(PlatformHeaders.CONTENT_TYPE));
        return replay.getMessage().getBody();
    }

    private StepOutputCheckpoint objectCheckpoint(InMemoryObjectStore store, Object output) {
        Exchange original = exchange();
        original.getMessage().setBody(output);
        original.getMessage().setHeader(PlatformHeaders.CONTENT_TYPE, "application/vnd.partner+json");
        StepOutputCheckpoint checkpoint = tieredService(store).capture(original, "message-123", "create-order");
        assertEquals(StepOutputCheckpoint.S3_OBJECT, checkpoint.storage());
        return checkpoint;
    }

    private void assertRefusedBeforeRestore(InMemoryObjectStore store, StepOutputCheckpoint checkpoint) {
        Exchange replay = exchange();
        replay.getMessage().setBody("original replay input");

        StepOutputCheckpointException failure = assertThrows(
                StepOutputCheckpointException.class,
                () -> tieredService(store).restore(replay, "create-order", checkpoint));

        assertTrue(failure.getMessage().contains("missing or corrupt"), failure.getMessage());
        assertEquals("original replay input", replay.getMessage().getBody(),
                "Nothing of the stored output may reach the exchange");
    }

    private StepOutputCheckpointService tieredService(PayloadObjectStore store) {
        return new StepOutputCheckpointService(
                store,
                new PayloadCapturePolicy(
                        PayloadMode.TIERED,
                        8,
                        10_000,
                        Duration.ofDays(30),
                        true),
                "checkpoint-test",
                "Partner-Request-Id",
                "partnerStatus");
    }

    /**
     * Keeps objects in memory and, unlike the S3 store, checks nothing when it hands one out,
     * so the tests show what the checkpoint service checks itself.
     */
    private static final class InMemoryObjectStore implements PayloadObjectStore {
        private final Map<String, byte[]> objects = new ConcurrentHashMap<>();

        @Override
        public PayloadReference store(String integration, String messageId, InputStream payload,
                                      Long declaredSize, String contentType, Instant expiresAt, long maxBytes) {
            try {
                byte[] bytes = payload.readAllBytes();
                String ref = "s3://checkpoints/" + integration + "/" + objects.size();
                objects.put(ref, bytes);
                return new PayloadReference(1, PayloadReference.STORAGE, ref, bytes.length,
                        HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)),
                        contentType, expiresAt.toString());
            } catch (Exception e) {
                throw new PayloadStorageException("Failed to store test object", e);
            }
        }

        @Override
        public InputStream open(PayloadReference reference) {
            byte[] bytes = objects.get(reference.ref());
            if (bytes == null) {
                throw new PayloadStorageException("Missing test object " + reference.ref());
            }
            return new ByteArrayInputStream(bytes);
        }

        @Override
        public void verify(PayloadReference reference) {
            if (!objects.containsKey(reference.ref())) {
                throw new PayloadStorageException("Missing test object " + reference.ref());
            }
        }

        void replace(String ref, byte[] bytes) {
            objects.put(ref, bytes);
        }

        void remove(String ref) {
            objects.remove(ref);
        }
    }

    private StepOutputCheckpointService service(long inlineThreshold) {
        return new StepOutputCheckpointService(
                PayloadObjectStore.disabled(),
                new PayloadCapturePolicy(
                        PayloadMode.POSTGRES_INLINE,
                        inlineThreshold,
                        10_000,
                        Duration.ofDays(30),
                        true),
                "checkpoint-test",
                "Partner-Request-Id",
                "partnerStatus");
    }

    private Exchange exchange() {
        return new DefaultExchange(new DefaultCamelContext());
    }

    private static StepOutputCheckpointService withHeaders(String headerNames) {
        return new StepOutputCheckpointService(
                PayloadObjectStore.disabled(),
                new PayloadCapturePolicy(PayloadMode.POSTGRES_INLINE, 1024, 10_000, Duration.ofDays(30), true),
                "checkpoint-test",
                headerNames,
                "");
    }
}
