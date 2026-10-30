package no.basis.integrationplatform.camel.lifecycle;

import java.time.Duration;

/**
 * Validated payload capture, storage, retention, and replay policy.
 *
 * @param mode payload storage mode
 * @param inlineThresholdBytes maximum inline Postgres payload
 * @param objectMaxBytes maximum object-backed payload
 * @param retention immutable payload retention
 * @param replayEnabled whether payload capture may make a message replayable
 */
public record PayloadCapturePolicy(
        PayloadMode mode,
        long inlineThresholdBytes,
        long objectMaxBytes,
        Duration retention,
        boolean replayEnabled) {
    /** Default 1 MiB inline threshold. */
    public static final long DEFAULT_INLINE_THRESHOLD_BYTES = 1_048_576L;
    /** Enforced 16 MiB inline safety ceiling. */
    public static final long INLINE_THRESHOLD_SAFETY_CEILING_BYTES = 16_777_216L;
    /** Default 10 GiB object payload limit. */
    public static final long DEFAULT_OBJECT_MAX_BYTES = 10_737_418_240L;
    /** Enforced 50 GiB object payload safety ceiling. */
    public static final long OBJECT_MAX_SAFETY_CEILING_BYTES = 53_687_091_200L;
    /** @deprecated use {@link #DEFAULT_INLINE_THRESHOLD_BYTES}. */
    @Deprecated
    static final long DEFAULT_MAX_BYTES = DEFAULT_INLINE_THRESHOLD_BYTES;
    static final Duration DEFAULT_RETENTION = Duration.ofDays(30);

    /**
     * Creates a policy using the default object payload limit.
     *
     * @param mode payload storage mode
     * @param inlineThresholdBytes maximum inline Postgres payload
     * @param retention immutable payload retention
     * @param replayEnabled whether replay capture is enabled
     */
    public PayloadCapturePolicy(PayloadMode mode, long inlineThresholdBytes, Duration retention, boolean replayEnabled) {
        this(mode, inlineThresholdBytes, DEFAULT_OBJECT_MAX_BYTES, retention, replayEnabled);
    }

    /**
     * Returns the compatibility-oriented library default.
     *
     * @return default inline payload policy
     */
    public static PayloadCapturePolicy defaultPolicy() {
        return new PayloadCapturePolicy(
                PayloadMode.POSTGRES_INLINE,
                DEFAULT_INLINE_THRESHOLD_BYTES,
                DEFAULT_OBJECT_MAX_BYTES,
                DEFAULT_RETENTION,
                true);
    }

    /**
     * Validates and creates a complete payload policy.
     *
     * @param mode configured payload mode
     * @param inlineThresholdBytes configured inline threshold
     * @param objectMaxBytes configured object payload limit
     * @param retentionDays configured retention in days
     * @param replayEnabled whether replay capture is enabled
     * @return validated payload policy
     * @throws IllegalArgumentException when a limit or retention is unsafe
     */
    public static PayloadCapturePolicy fromConfig(
            String mode,
            long inlineThresholdBytes,
            long objectMaxBytes,
            long retentionDays,
            boolean replayEnabled) {
        if (inlineThresholdBytes < 0) {
            throw new IllegalArgumentException("basis.message-store.payload.inline-threshold-bytes must be >= 0");
        }
        if (inlineThresholdBytes > INLINE_THRESHOLD_SAFETY_CEILING_BYTES) {
            throw new IllegalArgumentException(
                    "basis.message-store.payload.inline-threshold-bytes exceeds the 16 MiB safety ceiling");
        }
        if (objectMaxBytes < inlineThresholdBytes) {
            throw new IllegalArgumentException(
                    "basis.message-store.payload.object-max-bytes must be >= inline threshold");
        }
        if (objectMaxBytes > OBJECT_MAX_SAFETY_CEILING_BYTES) {
            throw new IllegalArgumentException(
                    "basis.message-store.payload.object-max-bytes exceeds the 50 GiB safety ceiling");
        }
        if (retentionDays < 0) {
            throw new IllegalArgumentException("basis.message-store.payload.retention-days must be >= 0");
        }
        return new PayloadCapturePolicy(
                PayloadMode.fromConfig(mode),
                inlineThresholdBytes,
                objectMaxBytes,
                Duration.ofDays(retentionDays),
                replayEnabled);
    }

    /**
     * Creates a policy using the default object payload limit.
     *
     * @param mode configured payload mode
     * @param maxBytes configured inline threshold
     * @param retentionDays configured retention in days
     * @param replayEnabled whether replay capture is enabled
     * @return validated payload policy
     */
    public static PayloadCapturePolicy fromConfig(String mode, long maxBytes, long retentionDays, boolean replayEnabled) {
        return fromConfig(mode, maxBytes, DEFAULT_OBJECT_MAX_BYTES, retentionDays, replayEnabled);
    }
}
