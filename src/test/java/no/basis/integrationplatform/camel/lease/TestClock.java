package no.basis.integrationplatform.camel.lease;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/** A clock the test advances by hand: both the monotonic nanos and the wall time. */
final class TestClock extends Clock {
    private long nanos = 1_000_000_000L;
    private Instant instant = Instant.parse("2026-10-17T10:00:00Z");

    synchronized long nanoTime() {
        return nanos;
    }

    synchronized void advance(Duration duration) {
        nanos += duration.toNanos();
        instant = instant.plus(duration);
    }

    @Override
    public synchronized Instant instant() {
        return instant;
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }
}
