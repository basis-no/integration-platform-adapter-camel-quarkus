package no.basis.integrationplatform.camel.lease;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStore;
import no.basis.integrationplatform.camel.keyvalue.KeyValueEntry;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStoreException;
import no.basis.integrationplatform.camel.keyvalue.KeyValueLimitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * The lease protocol for one activation of one piece
 * of single-active work, the Key-Value Store key {@code lease/<name>}, and the gate that
 * lets the work run: {@link #isOpen()} is true while this replica holds the
 * lease, or while it runs uncoordinated because the Message Store has no
 * Key-Value Store table, or the application has no Key-Value Store at all.
 *
 * <p>An instance lives for one activation of its route. {@link #detach()} ends
 * it for good: the gate is closed, no result that arrives afterwards, success
 * or failure, changes anything but this dead instance, and its only remaining
 * effect on the store is the conditional release of the lease it held or
 * created. A route that starts again gets a new instance, so nothing an old
 * worker still does can reach the new one.</p>
 *
 * <p>A replica owns the lease while the key holds exactly the value it wrote
 * when it acquired it: its holder name, the acquisition time for people to
 * read, and a random identifier of the acquisition attempt, so no two values
 * are ever equal, not even two acquisitions of one replica within the same
 * millisecond; versions only address the write that follows a read. {@link #attempt()} runs one protocol
 * step and may block on the store; {@link #enforceDeadline()} is pure and runs
 * often, so the gate closes in time even while a store call hangs. The gate is
 * a volatile flag: closing it takes no lock and waits for nothing, and no store
 * call is ever made while the lease's monitor is held. An acquisition is
 * granted only while its write was sent less than the deadline ago, and a
 * pending acquisition counts toward the deadline from its send in every
 * state.</p>
 */
final class Lease {
    /** How the gate changed in a protocol step. */
    enum Change { NONE, OPENED, CLOSED }

    private enum Status { WAITING, HELD, UNCOORDINATED, DETACHED }

    private enum Reported { NOTHING, HELD_ELSEWHERE, UNREACHABLE, LIMIT, UNAVAILABLE }

    static final String KEY_PREFIX = "lease/";

    private static final Logger LOG = LoggerFactory.getLogger(Lease.class);

    private final String name;
    private final String key;
    private final String holder;
    private final Supplier<KeyValueStore> state;
    private final LeaseTiming timing;
    private final LongSupplier nanoTime;
    private final Clock clock;

    private volatile boolean open;
    // Read without the monitor by the deadline check; written under it.
    private volatile Status status = Status.WAITING;
    private volatile long renewalSentNanos;
    private volatile long pendingSentNanos;
    private volatile long attemptSentNanos;

    // Guarded by this. Store calls happen outside the lock; results are applied under it.
    private ObjectNode value;
    private ObjectNode toRelease;
    private long version;
    private boolean retrying;
    private boolean expected;
    private boolean inFlight;
    private boolean releasePending;
    private Reported reported = Reported.NOTHING;

    Lease(String name, String holder, Supplier<KeyValueStore> state, LeaseTiming timing,
          LongSupplier nanoTime, Clock clock) {
        this.name = Objects.requireNonNull(name, "name");
        this.key = KEY_PREFIX + name;
        this.holder = Objects.requireNonNull(holder, "holder");
        this.state = Objects.requireNonNull(state, "state");
        this.timing = Objects.requireNonNull(timing, "timing");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    String name() {
        return name;
    }

    String holder() {
        return holder;
    }

    /** Whether the work may run on this replica right now. */
    boolean isOpen() {
        return open;
    }

    /** Whether this replica holds the lease. */
    boolean isHeld() {
        return status == Status.HELD;
    }

    /** Whether this instance's activation has ended; it never opens again. */
    boolean isDetached() {
        return status == Status.DETACHED;
    }

    /**
     * How long to wait before the next {@link #attempt()}, counted from when the
     * last one was sent rather than from when it returned: the next renewal
     * follows the attempt interval after the last successful one was sent, a
     * failed read or renewal is retried the retry interval after it was sent,
     * and an acquisition follows the attempt interval after the previous one.
     */
    synchronized Duration nextAttemptDelay() {
        boolean retry = status == Status.HELD && retrying;
        Duration interval = retry ? timing.retryInterval() : timing.attemptInterval();
        long since = status == Status.HELD && !retry ? renewalSentNanos : attemptSentNanos;
        if (since == 0) {
            return interval;
        }
        long elapsed = nanoTime.getAsLong() - since;
        return elapsed >= interval.toNanos() ? Duration.ZERO : interval.minusNanos(elapsed);
    }

    /** Marks that an attempt is about to run, so a trigger can wait for its outcome. */
    synchronized void expectAttempt() {
        if (status != Status.DETACHED) {
            expected = true;
        }
    }

    /**
     * Waits while an attempt is expected or in flight, so a trigger that fires
     * during this replica's acquisition can still run its tick if it succeeds.
     *
     * @param timeout how long to wait at most
     * @return whether no attempt is expected or in flight anymore
     */
    synchronized boolean awaitAttempt(Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (expected || inFlight) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0) {
                return false;
            }
            wait(Math.max(1, remaining / 1_000_000), (int) (remaining % 1_000_000));
        }
        return true;
    }

    /**
     * Runs one protocol step: acquire when not holding, renew when holding.
     * Called when the work would start and every attempt interval after. A
     * detached instance does nothing.
     */
    Change attempt() {
        boolean before = open;
        Status current;
        ObjectNode currentValue;
        synchronized (this) {
            expected = false;
            notifyAll();
            if (status == Status.DETACHED) {
                return Change.NONE;
            }
            inFlight = true;
            current = status;
            currentValue = value;
        }
        try {
            KeyValueStore store = state.get();
            if (current == Status.HELD) {
                renew(store, currentValue);
            } else {
                acquire(store);
            }
        } catch (KeyValueLimitException e) {
            limited(e);
        } catch (KeyValueStoreException e) {
            if (KeyValueStores.notProvisioned(e)) {
                unavailable("the Message Store has no " + KeyValueStores.TABLE + " table");
            } else {
                unreachable(e);
            }
        } catch (RuntimeException e) {
            // The Key-Value Store could not be created at all: no Message Store datasource or app.name.
            unavailable(e.toString());
        } finally {
            ObjectNode deferred = null;
            synchronized (this) {
                pendingSentNanos = 0;
                inFlight = false;
                notifyAll();
                if (releasePending) {
                    releasePending = false;
                    deferred = toRelease;
                    toRelease = null;
                }
            }
            // A release that waited for this call to settle runs now, outside the monitor.
            release(deferred);
        }
        return change(before);
    }

    /**
     * Closes the gate when the last successful write, or a pending acquisition,
     * was sent longer ago than the deadline. Pure, and the gate closes with a
     * volatile write before the monitor is taken, so this never waits on an
     * attempt.
     */
    Change enforceDeadline() {
        boolean before = open;
        if (!deadlinePassed()) {
            return Change.NONE;
        }
        open = false;
        synchronized (this) {
            if (deadlinePassed()) {
                LOG.warn("Single-active route {} stops: {} could not be {} by {} within {}", name, key,
                        status == Status.HELD ? "renewed" : "acquired", holder, timing.stopDeadline());
                lose();
            } else {
                // An attempt applied its result between the check and the lock; its state wins.
                open = status == Status.HELD || status == Status.UNCOORDINATED;
            }
        }
        return change(before);
    }

    /**
     * Ends this activation without touching the store: the gate closes for good
     * and the lease this replica held is remembered for
     * {@link #releaseWhenIdle()}.
     */
    synchronized void detach() {
        if (status == Status.HELD) {
            toRelease = value;
        }
        status = Status.DETACHED;
        open = false;
        value = null;
        version = 0;
        expected = false;
        notifyAll();
    }

    /**
     * Releases the lease {@link #detach()} remembered, once no store call of
     * this instance is in flight: now if there is none, otherwise when the one
     * in flight completes, so a release never runs next to an outstanding
     * renewal. The work must have stopped.
     */
    void releaseWhenIdle() {
        ObjectNode detached;
        synchronized (this) {
            if (inFlight) {
                releasePending = true;
                return;
            }
            detached = toRelease;
            toRelease = null;
        }
        release(detached);
    }

    /**
     * Deletes the lease if the key still holds the value this replica wrote: a
     * lease another replica created since is left alone.
     */
    void release(ObjectNode detached) {
        if (detached == null) {
            return;
        }
        try {
            KeyValueStore store = state.get();
            Optional<KeyValueEntry> current = store.get(key);
            if (current.isEmpty() || !detached.equals(current.get().value())) {
                LOG.info("Single-active route {} found {} already gone or another replica's", name, key);
                return;
            }
            long read = current.get().version();
            if (store.delete(key, read)) {
                LOG.info("Single-active route {} released {} (holder {}, version {})", name, key, holder, read);
            } else {
                LOG.info("Single-active route {} found {} changed under it at version {}", name, key, read);
            }
        } catch (RuntimeException e) {
            LOG.warn("Single-active route {} could not release {}; it expires in {}",
                    name, key, timing.leaseDuration(), e);
        }
    }

    private void acquire(KeyValueStore store) {
        long sent = nanoTime.getAsLong();
        ObjectNode candidate = JsonNodeFactory.instance.objectNode()
                .put("holder", holder)
                .put("acquiredAt", clock.instant().truncatedTo(ChronoUnit.MILLIS).toString())
                .put("acquisition", UUID.randomUUID().toString());
        attemptSentNanos = sent;
        pendingSentNanos = sent;
        boolean created = store.putIfAbsent(key, candidate, timing.leaseDuration());
        // The call succeeded, so the table exists: an uncoordinated replica stops before anything else.
        provisioned();
        if (!created) {
            heldElsewhere();
            return;
        }
        // putIfAbsent only says whether the key was created; the version comes from reading it back.
        Optional<KeyValueEntry> entry = store.get(key);
        if (entry.isEmpty() || !candidate.equals(entry.get().value())) {
            LOG.warn("Single-active route {} could not confirm {} after creating it; trying again", name, key);
            return;
        }
        boolean granted;
        synchronized (this) {
            granted = status != Status.DETACHED && nanoTime.getAsLong() - sent <= timing.stopDeadline().toNanos();
            if (granted) {
                status = Status.HELD;
                open = true;
                value = candidate;
                version = entry.get().version();
                renewalSentNanos = sent;
                retrying = false;
                reported = Reported.NOTHING;
                LOG.info("Single-active route {} runs on this replica: {} acquired by {} at version {}",
                        name, key, holder, version);
            } else {
                LOG.warn("Single-active route {} gives {} up: it was {} before the store confirmed it", name, key,
                        status == Status.DETACHED ? "detached" : "past its deadline");
            }
        }
        if (!granted) {
            // Detached or past the deadline while the write was in flight; let the row go rather than block the others.
            release(candidate);
        }
    }

    private void renew(KeyValueStore store, ObjectNode heldValue) {
        attemptSentNanos = nanoTime.getAsLong();
        // The key is this replica's only while it holds the value written at acquisition: a deleted
        // key starts again at version 1 for whoever creates it next, so a version alone proves nothing.
        Optional<KeyValueEntry> current = store.get(key);
        boolean owned = current.isPresent() && heldValue.equals(current.get().value());
        long sent = nanoTime.getAsLong();
        OptionalLong renewed = owned
                ? store.compareAndSet(key, current.get().version(), heldValue, timing.leaseDuration())
                : OptionalLong.empty();
        synchronized (this) {
            if (status != Status.HELD) {
                return;
            }
            if (renewed.isPresent()) {
                version = renewed.getAsLong();
                renewalSentNanos = sent;
                retrying = false;
                reported = Reported.NOTHING;
                return;
            }
            LOG.warn("Single-active route {} stops: {} held by {} was taken over or deleted", name, key, holder);
            lose();
        }
    }

    private synchronized void provisioned() {
        if (status == Status.UNCOORDINATED) {
            LOG.info("Single-active route {} stops running on every replica: the Key-Value Store table exists now, so it"
                    + " waits for {}", name, key);
            status = Status.WAITING;
            open = false;
            value = null;
            version = 0;
            reported = Reported.NOTHING;
        }
    }

    private synchronized void heldElsewhere() {
        if (status != Status.DETACHED && reported != Reported.HELD_ELSEWHERE) {
            LOG.info("Single-active route {} waits: {} is held elsewhere", name, key);
            reported = Reported.HELD_ELSEWHERE;
        }
    }

    private synchronized void unreachable(KeyValueStoreException failure) {
        if (status == Status.HELD) {
            if (deadlinePassed()) {
                LOG.warn("Single-active route {} stops: {} held by {} could not be renewed within {}",
                        name, key, holder, timing.stopDeadline(), failure);
                lose();
                return;
            }
            retrying = true;
            if (reported != Reported.UNREACHABLE) {
                LOG.warn("Single-active route {} could not renew {}; retrying every {} until {} after the last renewal",
                        name, key, timing.retryInterval(), timing.stopDeadline(), failure);
                reported = Reported.UNREACHABLE;
            }
            return;
        }
        // Waiting replicas keep waiting; an uncoordinated one learns nothing new about the table.
        if (status == Status.WAITING && reported != Reported.UNREACHABLE) {
            LOG.warn("Single-active route {} waits: the Key-Value Store is unreachable", name, failure);
            reported = Reported.UNREACHABLE;
        }
    }

    private synchronized void limited(KeyValueLimitException failure) {
        if (status == Status.HELD || status == Status.UNCOORDINATED) {
            lose();
        }
        if (status != Status.DETACHED && reported != Reported.LIMIT) {
            LOG.error("Single-active route {} stays stopped: {} cannot be written because the integration is at"
                    + " a Key-Value Store limit", name, key, failure);
            reported = Reported.LIMIT;
        }
    }

    private synchronized void unavailable(String reason) {
        if (status == Status.UNCOORDINATED || status == Status.DETACHED) {
            return;
        }
        LOG.warn("Single-active route {} runs on every replica: the Key-Value Store is unavailable ({}); it is"
                + " coordinated once the Message Store has " + KeyValueStores.TABLE + " and the application a Key-Value Store", name, reason);
        status = Status.UNCOORDINATED;
        open = true;
        value = null;
        version = 0;
        retrying = false;
        reported = Reported.UNAVAILABLE;
    }

    private void lose() {
        status = Status.WAITING;
        open = false;
        value = null;
        version = 0;
        retrying = false;
        reported = Reported.NOTHING;
    }

    /** Whether the deadline has passed for a held lease or for a pending acquisition of an open gate. */
    private boolean deadlinePassed() {
        long now = nanoTime.getAsLong();
        long deadline = timing.stopDeadline().toNanos();
        Status current = status;
        if (current == Status.HELD) {
            return now - renewalSentNanos > deadline;
        }
        long pending = pendingSentNanos;
        return current == Status.UNCOORDINATED && pending != 0 && now - pending > deadline;
    }

    private Change change(boolean before) {
        boolean after = open;
        return before == after ? Change.NONE : after ? Change.OPENED : Change.CLOSED;
    }
}
