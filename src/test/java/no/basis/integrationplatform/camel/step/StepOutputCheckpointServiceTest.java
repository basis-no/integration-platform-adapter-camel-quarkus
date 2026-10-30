package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.lifecycle.PayloadCapturePolicy;
import no.basis.integrationplatform.camel.lifecycle.PayloadMode;
import no.basis.integrationplatform.camel.payload.PayloadObjectStore;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
