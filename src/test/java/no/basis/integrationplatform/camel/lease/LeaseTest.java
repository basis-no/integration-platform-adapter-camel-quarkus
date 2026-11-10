package no.basis.integrationplatform.camel.lease;

import no.basis.integrationplatform.camel.keyvalue.KeyValueStore;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static no.basis.integrationplatform.camel.lease.Lease.Change.CLOSED;
import static no.basis.integrationplatform.camel.lease.Lease.Change.NONE;
import static no.basis.integrationplatform.camel.lease.Lease.Change.OPENED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LeaseTest {
    private static final String KEY = "lease/poll-orders";
    private static final Duration LEASE = LeaseTiming.DEFAULT.leaseDuration();
    private static final Duration ATTEMPT = LeaseTiming.DEFAULT.attemptInterval();
    private static final Duration RETRY = LeaseTiming.DEFAULT.retryInterval();
    private static final Duration DEADLINE = LeaseTiming.DEFAULT.stopDeadline();

    private final TestClock clock = new TestClock();
    private final FakeKeyValueStore state = new FakeKeyValueStore(clock);
    private final Lease lease = lease("pod-a", () -> state);

    @Test
    void acquiresByCreatingTheLeaseAndRenewsItAtItsVersion() {
        assertFalse(lease.isOpen(), "the gate is closed until the lease is held");
        assertEquals(OPENED, lease.attempt());

        assertTrue(lease.isOpen());
        FakeKeyValueStore.Row row = state.live(KEY);
        assertEquals("pod-a", row.value().get("holder").asText());
        assertEquals(clock.instant().toString(), row.value().get("acquiredAt").asText());
        assertEquals(1, row.version());
        assertEquals(clock.instant().plus(LEASE), row.expiresAt());

        clock.advance(ATTEMPT);
        assertEquals(NONE, lease.attempt());

        row = state.live(KEY);
        assertEquals(2, row.version(), "a renewal is a compare-and-set at the held version");
        assertEquals(clock.instant().plus(LEASE), row.expiresAt(), "a renewal moves the expiry");
        assertEquals("pod-a", row.value().get("holder").asText());
        assertTrue(lease.isOpen());
    }

    @Test
    void closesAtOnceWhenTheLeaseIsAnotherHoldersAndAcquiresAgainLater() {
        lease.attempt();
        // An operator deleted the lease and another replica created a new one at the same version.
        state.delete(KEY);
        state.hold(KEY, "pod-b", LEASE);

        clock.advance(ATTEMPT);
        assertEquals(CLOSED, lease.attempt());
        assertFalse(lease.isOpen());
        FakeKeyValueStore.Row other = state.live(KEY);
        assertEquals("pod-b", other.value().get("holder").asText(), "the other holder's lease is untouched");
        assertEquals(1, other.version(), "the renewal did not compare-and-set a lease with another value");

        clock.advance(ATTEMPT);
        assertEquals(NONE, lease.attempt(), "a live lease held elsewhere keeps this replica waiting");
        clock.advance(LEASE);
        assertEquals(OPENED, lease.attempt(), "an expired lease is acquired, continuing the version");
        assertEquals(2, state.live(KEY).version());
    }

    @Test
    void failedRenewalsChangeNothingUntilTheDeadlineThenCloseTheGate() {
        lease.attempt();
        state.reachable = false;

        clock.advance(ATTEMPT);
        assertEquals(NONE, lease.attempt());
        assertTrue(lease.isOpen(), "a failed renewal before the deadline changes nothing");
        assertEquals(1, state.rows.get(KEY).version());
        assertEquals(NONE, lease.enforceDeadline());
        assertEquals(RETRY, lease.nextAttemptDelay(), "a holder retries a failed renewal sooner");

        clock.advance(DEADLINE.minus(ATTEMPT));
        assertEquals(NONE, lease.enforceDeadline(), "the deadline is exclusive: exactly 20 s is still in time");
        clock.advance(Duration.ofMillis(1));
        assertEquals(CLOSED, lease.enforceDeadline());
        assertFalse(lease.isOpen());
        assertEquals(NONE, lease.enforceDeadline(), "a lease already given up is not lost twice");

        assertEquals(Duration.ZERO, lease.nextAttemptDelay(),
                "the next attempt is already due: 10 s have passed since the failed renewal was sent");
        clock.advance(ATTEMPT);
        assertEquals(NONE, lease.attempt(), "an unreachable store means waiting, not starting");
        state.reachable = true;
        clock.advance(LEASE);
        assertEquals(OPENED, lease.attempt());
    }

    @Test
    void oneFailedRenewalFollowedByASuccessWithinTheDeadlineKeepsTheGateOpenWithoutAGap() {
        lease.attempt();
        clock.advance(ATTEMPT);
        state.reachable = false;
        assertEquals(NONE, lease.attempt());
        assertEquals(RETRY, lease.nextAttemptDelay());

        state.reachable = true;
        clock.advance(RETRY);
        assertEquals(NONE, lease.attempt());

        assertTrue(lease.isOpen(), "the gate stayed open through the failure and the retry");
        assertEquals(2, state.live(KEY).version(), "the retry renewed the lease");
        assertEquals(ATTEMPT, lease.nextAttemptDelay(), "back to the attempt interval");
        clock.advance(DEADLINE);
        assertEquals(NONE, lease.enforceDeadline(), "the deadline counts from the retry that succeeded");
    }

    @Test
    void aRenewalWhoseReplyWasLostKeepsTheLeaseAtTheNextRenewal() {
        lease.attempt();
        // The compare-and-set commits, then the connection drops before the reply arrives.
        state.afterCompareAndSet = () -> {
            throw FakeKeyValueStore.unreachable();
        };
        clock.advance(ATTEMPT);
        assertEquals(NONE, lease.attempt());
        assertTrue(lease.isOpen(), "a lost reply is a failed renewal, not a lost lease");
        assertEquals(2, state.live(KEY).version(), "the store did apply the renewal");

        state.afterCompareAndSet = () -> {
        };
        clock.advance(RETRY);
        assertEquals(NONE, lease.attempt());

        assertTrue(lease.isOpen(), "the key still holds this replica's value, so the lease is kept");
        assertEquals(3, state.live(KEY).version(), "renewed at the version read, one ahead of the last reply");
        clock.advance(DEADLINE);
        assertEquals(NONE, lease.enforceDeadline(), "and the deadline counts from that renewal");
    }

    @Test
    void aFailedRenewalReportedAfterTheDeadlineClosesTheGateWithoutWaitingForTheCheck() {
        lease.attempt();
        state.reachable = false;

        clock.advance(DEADLINE.plusMillis(1));
        assertEquals(CLOSED, lease.attempt());
        assertFalse(lease.isOpen());
    }

    @Test
    void aRenewalThatReturnsAfterTheDeadlineIsIgnored() {
        lease.attempt();
        state.beforeCompareAndSet = () -> {
            // The store answers late: the deadline passed while the write was in flight, the lease did not.
            clock.advance(DEADLINE.minus(ATTEMPT).plusMillis(1));
            assertEquals(CLOSED, lease.enforceDeadline());
        };

        clock.advance(ATTEMPT);
        assertEquals(CLOSED, lease.attempt(), "the gate closed during the attempt and the late result did not reopen it");

        assertFalse(lease.isOpen(), "a late successful renewal does not revive the lease");
        assertEquals(2, state.rows.get(KEY).version(), "the store did apply the late renewal");
    }

    @Test
    void anAcquisitionConfirmedAfterTheDeadlineDoesNotOpenTheGate() {
        // The read-back after putIfAbsent returns only after the deadline measured from the write.
        state.beforeGet = once(() -> clock.advance(DEADLINE.plusMillis(1)));

        assertEquals(NONE, lease.attempt());

        assertFalse(lease.isOpen());
        assertFalse(lease.isHeld());
        assertNull(state.rows.get(KEY), "the row this replica created but cannot use is released");
        state.beforeGet = once(() -> clock.advance(DEADLINE));
        assertEquals(OPENED, lease.attempt(), "exactly the deadline is still in time");
        assertFalse(lease.enforceDeadline() == CLOSED, "granted at the deadline, not past it");
        clock.advance(Duration.ofMillis(1));
        assertEquals(CLOSED, lease.enforceDeadline(), "and the deadline counts from the write, not the confirmation");
    }

    @Test
    void releaseDeletesTheLeaseOnlyWhileItStillHoldsTheValueThisReplicaWrote() {
        lease.attempt();
        clock.advance(ATTEMPT);
        lease.attempt();

        lease.detach();

        assertFalse(lease.isOpen(), "detaching closes the gate before the row is deleted");
        assertTrue(lease.isDetached());
        assertNotNull(state.live(KEY));
        lease.releaseWhenIdle();
        assertNull(state.rows.get(KEY));

        Lease idle = lease("pod-a", () -> state);
        idle.detach();
        idle.releaseWhenIdle();
        assertNull(state.rows.get(KEY), "nothing to delete when nothing was held");
    }

    @Test
    void releaseAfterAnOperatorDeleteLeavesAnotherReplicasNewLeaseAlone() {
        lease.attempt();
        lease.detach();
        // Meanwhile an operator deleted the lease and another replica created a new one at version 1 again.
        state.delete(KEY);
        state.hold(KEY, "pod-b", LEASE);

        lease.releaseWhenIdle();

        FakeKeyValueStore.Row other = state.live(KEY);
        assertNotNull(other, "the new lease is not deleted although it is at the version this replica held");
        assertEquals("pod-b", other.value().get("holder").asText());
        assertEquals(1, other.version());
    }

    @Test
    void aDetachedLeaseIgnoresEveryLaterResultAndOnlyReleasesWhatItHeld() {
        lease.attempt();
        // The activation ends while a renewal is in flight; its success arrives afterwards.
        state.beforeCompareAndSet = once(lease::detach);
        clock.advance(ATTEMPT);
        assertEquals(CLOSED, lease.attempt());
        assertTrue(lease.isDetached());
        assertFalse(lease.isOpen(), "a late success does not reopen a detached instance");
        assertEquals(2, state.live(KEY).version(), "the store did apply the renewal");

        assertEquals(NONE, lease.enforceDeadline());
        lease.releaseWhenIdle();
        assertNull(state.rows.get(KEY), "its only remaining effect is releasing the lease it held");
    }

    @Test
    void aFailureArrivingForADetachedInstancesOwnCallDoesNotReopenIt() {
        lease.attempt();
        // The activation ends while a renewal is in flight; that very call then fails with a missing table.
        state.beforeCompareAndSet = once(() -> {
            lease.detach();
            state.provisioned = false;
        });
        clock.advance(ATTEMPT);
        assertEquals(CLOSED, lease.attempt());
        assertFalse(lease.isOpen(), "the missing-table fallback never opens a detached instance");
        assertTrue(lease.isDetached());
        state.provisioned = true;

        // The same for a call that fails because the store is unreachable.
        Lease other = lease("pod-a", () -> state);
        state.rows.clear();
        other.attempt();
        state.beforeCompareAndSet = once(() -> {
            other.detach();
            state.reachable = false;
        });
        clock.advance(ATTEMPT);
        assertEquals(CLOSED, other.attempt());
        assertFalse(other.isOpen());
        state.reachable = true;
    }

    @Test
    void aReleaseRequestedDuringACallInFlightRunsWhenThatCallCompletes() throws Exception {
        lease.attempt();
        CountDownLatch casStarted = new CountDownLatch(1);
        CountDownLatch casGo = new CountDownLatch(1);
        state.beforeCompareAndSet = once(() -> {
            casStarted.countDown();
            FakeKeyValueStore.blockOn(casGo);
        });
        clock.advance(ATTEMPT);
        Thread renewal = new Thread(lease::attempt, "renewal");
        renewal.start();
        assertTrue(casStarted.await(5, java.util.concurrent.TimeUnit.SECONDS));

        lease.detach();
        lease.releaseWhenIdle();
        assertNotNull(state.live(KEY), "the release waits for the renewal in flight instead of racing it");

        casGo.countDown();
        renewal.join(Duration.ofSeconds(5).toMillis());
        assertFalse(renewal.isAlive());
        assertNull(state.rows.get(KEY), "the release ran when the call completed, at the version it wrote");
    }

    @Test
    void aLiveLeaseWithoutExpiryOrWithAnUnknownHolderBlocksEveryReplica() {
        state.hold(KEY, "someone-else", null);

        for (int attempt = 0; attempt < 12; attempt++) {
            assertEquals(NONE, lease.attempt());
            clock.advance(ATTEMPT);
        }

        assertFalse(lease.isOpen());
        assertEquals(1, state.live(KEY).version());
    }

    @Test
    void theKeyLimitKeepsTheGateClosedUntilAKeyIsFree() {
        state.atKeyLimit = true;

        assertEquals(NONE, lease.attempt());
        assertFalse(lease.isOpen());
        clock.advance(ATTEMPT);
        assertEquals(NONE, lease.attempt());

        state.atKeyLimit = false;
        clock.advance(ATTEMPT);
        assertEquals(OPENED, lease.attempt());
    }

    @Test
    void withoutTheKeyValueTableTheGateIsOpenOnEveryReplicaUntilTheTableExists() {
        state.provisioned = false;

        assertEquals(OPENED, lease.attempt());
        assertTrue(lease.isOpen());
        assertFalse(lease.isHeld());
        assertTrue(state.rows.isEmpty());
        clock.advance(ATTEMPT);
        assertEquals(NONE, lease.attempt(), "checked again at every attempt without a new warning");

        state.provisioned = true;
        clock.advance(ATTEMPT);
        assertEquals(NONE, lease.attempt(), "the table exists and nobody holds the lease: still open, now held");
        assertTrue(lease.isHeld());
        assertEquals("pod-a", state.live(KEY).value().get("holder").asText());
    }

    @Test
    void anUncoordinatedReplicaClosesWhenAnotherReplicaHoldsTheLeaseOnceTheTableExists() {
        state.provisioned = false;
        lease.attempt();
        state.provisioned = true;
        state.hold(KEY, "pod-b", LEASE);

        clock.advance(ATTEMPT);
        assertEquals(CLOSED, lease.attempt());
        assertFalse(lease.isOpen());
    }

    @Test
    void anUncoordinatedReplicaClosesAsSoonAsAStateCallProvesTheTableEvenIfTheReadBackFails() {
        state.provisioned = false;
        lease.attempt();
        assertTrue(lease.isOpen());
        state.provisioned = true;
        // putIfAbsent succeeds, then the database becomes unreachable before the read-back.
        state.afterPutIfAbsent = () -> state.reachable = false;

        clock.advance(ATTEMPT);
        assertEquals(CLOSED, lease.attempt());

        assertFalse(lease.isOpen(), "the table exists, so the work waits for a lease it does not hold");
        assertFalse(lease.isHeld());
        state.afterPutIfAbsent = () -> {
        };
        clock.advance(ATTEMPT);
        assertEquals(NONE, lease.attempt(), "still unreachable: keep waiting, never reopen uncoordinated");
        assertFalse(lease.isOpen());
    }

    @Test
    void aPendingAcquisitionFromTheUncoordinatedFallbackClosesTheGateAtItsDeadline() {
        state.provisioned = false;
        lease.attempt();
        assertTrue(lease.isOpen(), "uncoordinated: open on every replica");
        state.provisioned = true;
        // The table now exists; this replica's putIfAbsent commits but its reply hangs past the deadline.
        AtomicReference<Lease.Change> whileHanging = new AtomicReference<>();
        state.afterPutIfAbsent = once(() -> {
            clock.advance(DEADLINE.plusMillis(1));
            whileHanging.set(lease.enforceDeadline());
        });

        clock.advance(ATTEMPT);
        lease.attempt();

        assertEquals(CLOSED, whileHanging.get(), "the deadline check closed the gate while the reply was outstanding");
        assertFalse(lease.isOpen(), "the late reply did not reopen it");
        assertFalse(lease.isHeld());
        assertNull(state.rows.get(KEY), "the lease this replica created but cannot use is released");
    }

    @Test
    void theDeadlineCheckClosesTheGateWithoutWaitingForTheLeaseMonitor() throws Exception {
        lease.attempt();
        clock.advance(DEADLINE.plusMillis(1));
        AtomicReference<Lease.Change> result = new AtomicReference<>();
        Thread check = new Thread(() -> result.set(lease.enforceDeadline()), "deadline-check");

        synchronized (lease) {
            // Another thread holds the lease's monitor, as an attempt applying its result briefly does.
            check.start();
            check.join(Duration.ofSeconds(2).toMillis());
            assertFalse(lease.isOpen(), "the gate closed although the monitor is held");
        }
        check.join(Duration.ofSeconds(5).toMillis());

        assertFalse(check.isAlive());
        assertEquals(CLOSED, result.get());
        assertFalse(lease.isHeld());
    }

    @Test
    void theNextRenewalFollowsTheIntervalAfterTheSuccessfulSendNotAfterTheReply() {
        lease.attempt();
        assertEquals(ATTEMPT, lease.nextAttemptDelay(), "the first renewal follows the acquisition's send");
        // The renewal's compare-and-set is sent at 10 s and the store answers 4.5 s later.
        state.beforeCompareAndSet = () -> clock.advance(Duration.ofMillis(4_500));

        clock.advance(ATTEMPT);
        lease.attempt();

        assertEquals(Duration.ofMillis(5_500), lease.nextAttemptDelay(),
                "the next renewal is due 10 s after the send at 10 s, so 5.5 s after the reply at 14.5 s");
    }

    @Test
    void theDeadlineAndTheNextRenewalCountFromTheCompareAndSetSendNotFromTheRead() {
        lease.attempt();
        // The renewal's read takes from 10 s to 18 s; the compare-and-set is sent at 18 s and succeeds.
        state.beforeGet = once(() -> clock.advance(Duration.ofSeconds(8)));
        clock.advance(ATTEMPT);
        lease.attempt();
        assertTrue(lease.isHeld());
        assertEquals(ATTEMPT, lease.nextAttemptDelay(), "the next renewal starts at 28 s, 10 s after the send at 18 s");
        state.reachable = false;

        clock.advance(Duration.ofSeconds(12));
        assertEquals(NONE, lease.enforceDeadline(), "30 s: only 12 s since the write that last set the expiry");
        clock.advance(Duration.ofSeconds(8));
        assertEquals(NONE, lease.enforceDeadline(), "38 s: exactly the deadline, still in time");
        clock.advance(Duration.ofMillis(1));
        assertEquals(CLOSED, lease.enforceDeadline(), "the gate closes at 38 s, not 30 s");
    }

    @Test
    void aStoreCallInFlightNeverHoldsTheLeaseMonitor() throws Exception {
        // A late acquisition releases the row it created; that release blocks in the store.
        CountDownLatch blocked = new CountDownLatch(1);
        CountDownLatch released = new CountDownLatch(1);
        AtomicInteger gets = new AtomicInteger();
        state.afterPutIfAbsent = once(() -> clock.advance(DEADLINE.plusMillis(1)));
        state.beforeGet = () -> {
            if (gets.incrementAndGet() == 2) {
                blocked.countDown();
                FakeKeyValueStore.blockOn(released);
            }
        };
        Thread attempt = new Thread(lease::attempt, "attempt");
        attempt.start();
        assertTrue(blocked.await(5, java.util.concurrent.TimeUnit.SECONDS));

        AtomicBoolean detached = new AtomicBoolean();
        Thread detach = new Thread(() -> {
            lease.detach();
            detached.set(true);
        }, "detach");
        detach.start();
        detach.join(Duration.ofSeconds(1).toMillis());

        assertTrue(detached.get(), "detach() does not wait for a store call in flight");
        released.countDown();
        attempt.join(Duration.ofSeconds(5).toMillis());
        assertFalse(attempt.isAlive());
        assertFalse(lease.isOpen());
    }

    @Test
    void anUnreachableDatabaseIsNotTheUncoordinatedCase() {
        state.reachable = false;

        assertEquals(NONE, lease.attempt());
        assertFalse(lease.isOpen());

        state.provisioned = false;
        clock.advance(ATTEMPT);
        assertEquals(NONE, lease.attempt(), "while unreachable, nothing is known about the table");
        state.reachable = true;
        clock.advance(ATTEMPT);
        assertEquals(OPENED, lease.attempt());
    }

    @Test
    void anIntegrationWithoutAMessageStoreRunsUncoordinated() {
        Lease withoutStore = lease("pod-a", () -> {
            throw new IllegalStateException("Key-Value Store requires a JDBC DataSource for the Message Store");
        });

        assertEquals(OPENED, withoutStore.attempt());
        assertTrue(withoutStore.isOpen());
        assertEquals(NONE, withoutStore.attempt());
    }

    private static Runnable once(Runnable action) {
        AtomicBoolean done = new AtomicBoolean();
        return () -> {
            if (done.compareAndSet(false, true)) {
                action.run();
            }
        };
    }

    private Lease lease(String holder, Supplier<KeyValueStore> state) {
        return new Lease("poll-orders", holder, state, LeaseTiming.DEFAULT, clock::nanoTime, clock);
    }
}
