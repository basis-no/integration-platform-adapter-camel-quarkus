package no.basis.integrationplatform.camel.lease;

import java.time.Duration;

/**
 * The timing of the single-active lease protocol.
 *
 * <p>A 30 second lease is acquired or renewed every 10 seconds, a failed read
 * or renewal is retried every 2 seconds, and a holder stops its work 20
 * seconds after the last write it sent. {@code startupWait} bounds how long a
 * trigger waits for an acquisition in flight. {@code callTimeout} is how long
 * one Key-Value Store call may take: the store bounds each statement to five
 * seconds, so a call blocked behind a lock fails well before the deadline.
 * Tests shorten these through the package-private constructors; nothing else
 * may.</p>
 *
 * @param leaseDuration how long a lease lives from the database's time of the last write
 * @param attemptInterval how often a replica acquires or renews, counted from the last send
 * @param retryInterval how soon a holder retries a read or renewal that failed
 * @param stopDeadline how long after its last successful write a holder stops the work
 * @param startupWait how long a trigger waits for this replica's acquisition attempt in flight
 * @param callTimeout how long one Key-Value Store call of the lease may take
 */
record LeaseTiming(Duration leaseDuration, Duration attemptInterval, Duration retryInterval,
                   Duration stopDeadline, Duration startupWait, Duration callTimeout) {
    /** The adapter's timing, with a 5 second trigger wait and the Key-Value Store's 5 second statement timeout. */
    static final LeaseTiming DEFAULT = new LeaseTiming(Duration.ofSeconds(30), Duration.ofSeconds(10),
            Duration.ofSeconds(2), Duration.ofSeconds(20), Duration.ofSeconds(5), Duration.ofSeconds(5));

    LeaseTiming {
        if (attemptInterval.compareTo(stopDeadline) >= 0 || stopDeadline.compareTo(leaseDuration) >= 0) {
            throw new IllegalArgumentException(
                    "Lease timing needs attemptInterval < stopDeadline < leaseDuration, was " + attemptInterval
                            + " < " + stopDeadline + " < " + leaseDuration);
        }
        if (retryInterval.isNegative() || retryInterval.isZero() || retryInterval.compareTo(attemptInterval) > 0) {
            throw new IllegalArgumentException("Lease retryInterval must be positive and at most the attemptInterval");
        }
        if (startupWait.isNegative()) {
            throw new IllegalArgumentException("Lease startupWait must not be negative");
        }
        if (callTimeout.isNegative() || callTimeout.isZero() || callTimeout.compareTo(stopDeadline) >= 0) {
            throw new IllegalArgumentException("Lease callTimeout must be positive and below the stopDeadline");
        }
    }

    /** How often a holder checks its deadline independently of the store answering. */
    Duration deadlineCheckInterval() {
        Duration check = stopDeadline.dividedBy(20);
        return check.compareTo(Duration.ofMillis(10)) < 0 ? Duration.ofMillis(10) : check;
    }
}
