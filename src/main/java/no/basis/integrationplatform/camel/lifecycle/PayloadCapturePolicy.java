package no.basis.integrationplatform.camel.lifecycle;

import java.time.Duration;

public record PayloadCapturePolicy(
        PayloadMode mode,
        long maxBytes,
        Duration retention,
        boolean replayEnabled) {
    static final long DEFAULT_MAX_BYTES = 1_048_576L;
    static final Duration DEFAULT_RETENTION = Duration.ofDays(30);

    public static PayloadCapturePolicy defaultPolicy() {
        return new PayloadCapturePolicy(
                PayloadMode.POSTGRES_INLINE,
                DEFAULT_MAX_BYTES,
                DEFAULT_RETENTION,
                true);
    }

    public static PayloadCapturePolicy fromConfig(String mode, long maxBytes, long retentionDays, boolean replayEnabled) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("basis.message-store.payload.max-bytes must be >= 0");
        }
        if (retentionDays < 0) {
            throw new IllegalArgumentException("basis.message-store.payload.retention-days must be >= 0");
        }
        return new PayloadCapturePolicy(
                PayloadMode.fromConfig(mode),
                maxBytes,
                Duration.ofDays(retentionDays),
                replayEnabled);
    }
}
