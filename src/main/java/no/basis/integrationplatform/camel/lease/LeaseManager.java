package no.basis.integrationplatform.camel.lease;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import jakarta.interceptor.Interceptor;
import no.basis.integrationplatform.camel.lifecycle.ContractVersionStartupGate;
import no.basis.integrationplatform.camel.keyvalue.KeyValueStore;
import org.apache.camel.support.service.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Keeps the leases of this replica's single-active routes and runs their
 * protocol.
 *
 * <p>It is a Camel context service, so it starts before the routes and stops
 * after them, when it releases the leases that are still held. A lease is
 * active while its route runs: it is taken when the route policy sees the
 * route started, released, after the work in flight has finished, when the
 * route stops or suspends so another replica can take the work over, and
 * taken again when the route starts or resumes. Every activation gets its own
 * {@link Lease} instance with its own worker thread and gate; the instance of
 * an ended activation is detached for good, so whatever a worker still blocked
 * in a store call does afterwards touches only that dead instance, and
 * {@link #current(String)} always names the live one. All leases share one
 * deadline check that closes a gate in time whatever the store does. The
 * replica identifies itself by {@code HOSTNAME}, the pod name, or by an
 * identifier generated once per process.</p>
 *
 * <p>No lease is attempted, and so no gate opens, before the adapter's
 * contract check ({@link ContractVersionStartupGate}) has passed, and never
 * when it failed: Camel Quarkus starts the routes before the startup event
 * that runs the check, so a poll must not act against a Message Store the
 * adapter is about to refuse. The check's own result is reused; the manager
 * runs no check of its own.</p>
 */
@Singleton
public class LeaseManager extends ServiceSupport {
    private static final Logger LOG = LoggerFactory.getLogger(LeaseManager.class);

    private final String holder;
    private final Supplier<KeyValueStore> state;
    private final Supplier<ContractVersionStartupGate.Outcome> contractCheck;
    private final LeaseTiming timing;
    private final LongSupplier nanoTime;
    private final Clock clock;
    private final ConcurrentMap<String, Registration> registrations = new ConcurrentHashMap<>();
    private final Supplier<MeterRegistry> meters;
    private volatile boolean running;
    private volatile boolean contractFailureLogged;
    private ScheduledExecutorService deadlineChecks;

    /**
     * Creates the manager for this replica on the integration's state.
     *
     * @param state the integration's Key-Value Store, created on the first lease attempt
     * @param contractGate the adapter's contract check, whose result gates every lease attempt
     */
    public LeaseManager(Instance<KeyValueStore> state, ContractVersionStartupGate contractGate) {
        this(state, contractGate, null);
    }

    /**
     * Creates the manager for this replica on the integration's state, with a gauge per route
     * when the application has a meter registry.
     *
     * @param state the integration's Key-Value Store, created on the first lease attempt
     * @param contractGate the adapter's contract check, whose result gates every lease attempt
     * @param registries the application's meter registry, if any
     */
    @Inject
    public LeaseManager(Instance<KeyValueStore> state, ContractVersionStartupGate contractGate,
                        Instance<MeterRegistry> registries) {
        this(replicaIdentity(), state::get, contractGate::outcome, LeaseTiming.DEFAULT, System::nanoTime,
                Clock.systemUTC(), () -> registries != null && registries.isResolvable() ? registries.get() : null);
    }

    LeaseManager(String holder, Supplier<KeyValueStore> state, Supplier<ContractVersionStartupGate.Outcome> contractCheck,
                 LeaseTiming timing, LongSupplier nanoTime, Clock clock) {
        this(holder, state, contractCheck, timing, nanoTime, clock, () -> null);
    }

    LeaseManager(String holder, Supplier<KeyValueStore> state, Supplier<ContractVersionStartupGate.Outcome> contractCheck,
                 LeaseTiming timing, LongSupplier nanoTime, Clock clock, Supplier<MeterRegistry> meters) {
        Objects.requireNonNull(state, "state");
        this.meters = Objects.requireNonNull(meters, "meters");
        this.holder = Objects.requireNonNull(holder, "holder");
        this.timing = Objects.requireNonNull(timing, "timing");
        // The Key-Value Store bounds each statement itself, so a lease call fails instead of hanging.
        this.state = state;
        this.contractCheck = Objects.requireNonNull(contractCheck, "contractCheck");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    /**
     * Returns the identifier this replica writes as the lease holder.
     *
     * @return the pod name, or an identifier generated for this process
     */
    public String holder() {
        return holder;
    }

    LeaseTiming timing() {
        return timing;
    }

    /**
     * Registers the named work and returns its current lease, closed. Nothing
     * is acquired until {@link #activate(String)}.
     */
    Lease lease(String name) {
        return registrations.computeIfAbsent(name, Registration::new).current;
    }

    /**
     * Marks the named work as allowed to run only under its lease: without a Key-Value Store its
     * gate stays closed instead of opening on every replica. Used when a deployment declares several
     * replicas and the work's source is safe only because the lease coordinates it.
     *
     * @param name the route id
     */
    public void require(String name) {
        registrations.computeIfAbsent(name, Registration::new).required = true;
    }

    private double gaugeValue(String name) {
        Registration registration = registrations.get(name);
        if (registration == null) {
            return Double.NaN;
        }
        Lease lease = registration.current;
        return lease.isHeld() ? 1 : lease.isUncoordinated() ? 2 : lease.isStoppedWithoutStore() ? 3 : 0;
    }

    /** Whether the named work may run only under its lease. */
    boolean isRequired(String name) {
        Registration registration = registrations.get(name);
        return registration != null && registration.required;
    }

    /**
     * Returns the current lease of the named work, the one its gates consult,
     * or {@code null} when the work is not registered. After a deactivation it
     * is the detached instance, closed for good, until the next activation.
     */
    Lease current(String name) {
        Registration registration = registrations.get(name);
        return registration == null ? null : registration.current;
    }

    /**
     * Notes that Camel is about to start the route's consumer, so a trigger that
     * fires before the policy's {@code onStart} activates the lease waits for
     * that activation (bounded by the startup wait) instead of being dropped.
     * No lease is taken here: if the consumer fails to start, the expectation
     * simply lapses.
     */
    void expectStart(String name) {
        Registration registration = registrations.get(name);
        if (registration != null) {
            registration.expectStart();
        }
    }

    /**
     * Starts acquiring the named lease with a fresh instance, now when the
     * manager runs and otherwise when it starts. Called when Camel has started
     * the route, so only routes Camel actually runs take a lease.
     */
    void activate(String name) {
        Registration registration = registrations.computeIfAbsent(name, Registration::new);
        registration.wanted = true;
        if (running) {
            registration.start();
        } else {
            registration.clearExpectation();
        }
    }

    /**
     * Stops acquiring the named lease and releases it, keeping the registration
     * so the route can activate it again. The work must have stopped.
     */
    void deactivate(String name) {
        Registration registration = registrations.get(name);
        if (registration != null) {
            registration.wanted = false;
            registration.stop();
        }
    }

    /** Forgets the named work: stops acquiring, deletes the lease it held and removes its gauge. The work must have stopped. */
    void release(String name) {
        Registration registration = registrations.remove(name);
        if (registration != null) {
            registration.wanted = false;
            registration.stop();
            registration.removeGauge();
        }
    }

    /**
     * Waits, bounded, while this replica's acquisition of the named lease is
     * still pending: the contract check has not run, the route's start is
     * expected, or an attempt is expected or in flight. A trigger that fires
     * during that time can then run its tick if the attempt succeeded instead
     * of losing it.
     *
     * @return whether the wait ended before the timeout
     */
    boolean awaitAttempt(String name, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        try {
            while (contractCheck.get() == ContractVersionStartupGate.Outcome.PENDING) {
                if (System.nanoTime() >= deadline) {
                    return false;
                }
                Thread.sleep(20);
            }
            Registration registration = registrations.get(name);
            if (registration == null) {
                return true;
            }
            if (!registration.awaitActivation(deadline)) {
                return false;
            }
            return registration.current.awaitAttempt(Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Runs the first attempts of the active leases once the contract check has
     * passed. Observed after {@link ContractVersionStartupGate} on the startup
     * event; when that check throws, the event never reaches this method and
     * the gates stay closed while the application fails to start.
     */
    void onContractVerified(@Observes @Priority(Interceptor.Priority.APPLICATION + 600) StartupEvent event) {
        contractVerified();
    }

    /** Attempts every active lease now if the contract check has passed; a no-op otherwise. */
    void contractVerified() {
        if (contractCheck.get() != ContractVersionStartupGate.Outcome.PASSED) {
            return;
        }
        LOG.info("Message-store contract verified; single-active routes may acquire their leases");
        for (Registration registration : registrations.values()) {
            registration.attemptNow();
        }
    }

    @Override
    protected void doStart() {
        running = true;
        long begun = System.nanoTime();
        List<Future<?>> firstAttempts = new ArrayList<>();
        for (Registration registration : registrations.values()) {
            if (registration.wanted) {
                Future<?> first = registration.start();
                if (first != null) {
                    firstAttempts.add(first);
                }
            }
        }
        long deadline = begun + timing.startupWait().toNanos();
        int pending = 0;
        for (Future<?> first : firstAttempts) {
            try {
                first.get(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS);
            } catch (TimeoutException e) {
                pending++;
            } catch (ExecutionException e) {
                LOG.warn("A first lease attempt failed; it is retried every {}", timing.attemptInterval(), e.getCause());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        if (!firstAttempts.isEmpty()) {
            LOG.info("Single-active routes: {} lease attempt(s) made before the routes start in {} ms, {} still pending",
                    firstAttempts.size(), Duration.ofNanos(System.nanoTime() - begun).toMillis(), pending);
        }
    }

    @Override
    protected void doStop() {
        running = false;
        for (Registration registration : registrations.values()) {
            registration.stop();
        }
        synchronized (this) {
            if (deadlineChecks != null) {
                deadlineChecks.shutdownNow();
                deadlineChecks = null;
            }
        }
    }

    private synchronized ScheduledExecutorService deadlineChecks() {
        if (deadlineChecks == null) {
            deadlineChecks = Executors.newSingleThreadScheduledExecutor(daemon("bip-lease-deadline"));
        }
        return deadlineChecks;
    }

    static String replicaIdentity() {
        String hostname = System.getenv("HOSTNAME");
        return hostname == null || hostname.isBlank() ? "replica-" + UUID.randomUUID() : hostname;
    }

    private static ThreadFactory daemon(String name) {
        return task -> {
            Thread thread = new Thread(task, name);
            thread.setDaemon(true);
            return thread;
        };
    }

    /**
     * The registration of one route: the lease instance its gates consult and,
     * while the route runs, the activation that drives it. Every activation
     * gets a fresh instance with its own executor and deadline check, bound
     * together for their whole life, so a worker still blocked when the route
     * restarts can only finish on its own, detached, instance.
     */
    private final class Registration {
        private final String name;
        private volatile Lease current;
        private volatile boolean wanted;
        private volatile boolean required;
        private Gauge gauge;
        private MeterRegistry gaugeRegistry;
        private Activation activation;
        private long startExpectedAtNanos;

        private Registration(String name) {
            this.name = name;
            this.current = newLease();
            registerGauge();
        }

        private Lease newLease() {
            return new Lease(name, holder, state, timing, nanoTime, clock, () -> required);
        }

        /**
         * 1 while this replica holds the lease, 0 while it waits, 2 while the route runs uncoordinated,
         * 3 while it needs its lease and stays stopped because there is no Key-Value Store. The gauge
         * reads the route's current registration, so a route removed and added again reports its new one.
         */
        private void registerGauge() {
            MeterRegistry registry = meters.get();
            if (registry == null) {
                return;
            }
            gauge = Gauge.builder("integration_single_active_lease", LeaseManager.this,
                            manager -> manager.gaugeValue(name))
                    .description("Single-active lease of a polling route on this replica: 1 held, 0 waiting,"
                            + " 2 running uncoordinated without a Key-Value Store, 3 stopped because it needs"
                            + " its lease and there is no Key-Value Store")
                    .tag("route", name)
                    .strongReference(true)
                    .register(registry);
            gaugeRegistry = registry;
        }

        private void removeGauge() {
            if (gauge != null) {
                gaugeRegistry.remove(gauge);
                gauge = null;
            }
        }

        private synchronized void expectStart() {
            startExpectedAtNanos = System.nanoTime();
        }

        private synchronized void clearExpectation() {
            startExpectedAtNanos = 0;
            notifyAll();
        }

        /** Waits while a start is expected and no activation has happened yet; the expectation lapses after the startup wait. */
        private synchronized boolean awaitActivation(long deadlineNanos) throws InterruptedException {
            while (activation == null && startExpectedAtNanos != 0
                    && System.nanoTime() - startExpectedAtNanos < timing.startupWait().toNanos()) {
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0) {
                    return false;
                }
                wait(Math.max(1, remaining / 1_000_000), (int) (remaining % 1_000_000));
            }
            return true;
        }

        private synchronized Future<?> start() {
            startExpectedAtNanos = 0;
            notifyAll();
            if (activation != null) {
                return activation.firstAttempt;
            }
            Lease lease = current.isDetached() ? newLease() : current;
            current = lease;
            activation = new Activation(lease);
            return activation.firstAttempt;
        }

        private void stop() {
            Activation ended;
            synchronized (this) {
                startExpectedAtNanos = 0;
                notifyAll();
                ended = activation;
                activation = null;
            }
            if (ended != null) {
                ended.end();
            }
        }

        private void attemptNow() {
            Activation running;
            synchronized (this) {
                running = activation;
            }
            if (running != null) {
                running.attemptNow();
            }
        }
    }

    /** One activation: a lease instance, the executor that drives it and its deadline check, bound for life. */
    private final class Activation {
        private final Lease lease;
        private final ScheduledExecutorService executor;
        private final ScheduledFuture<?> deadlineCheck;
        private final Future<?> firstAttempt;

        private Activation(Lease lease) {
            this.lease = lease;
            this.executor = Executors.newSingleThreadScheduledExecutor(daemon("bip-lease-" + lease.name()));
            lease.expectAttempt();
            this.firstAttempt = executor.submit(this::attemptAndReschedule);
            long check = timing.deadlineCheckInterval().toMillis();
            this.deadlineCheck = deadlineChecks().scheduleAtFixedRate(lease::enforceDeadline, check, check, TimeUnit.MILLISECONDS);
        }

        private void end() {
            deadlineCheck.cancel(false);
            // The gate closes here and this instance is dead; a result still in flight changes only it.
            lease.detach();
            executor.shutdownNow();
            try {
                // Give a store call in flight the statement timeout to settle, so the release usually runs now.
                if (!executor.awaitTermination(timing.callTimeout().plusSeconds(1).toMillis(), TimeUnit.MILLISECONDS)) {
                    LOG.warn("Single-active route {} still has a Key-Value Store call in flight; its lease is"
                            + " released when that call completes", lease.name());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // The work has stopped: the lease is deleted now, or when the outstanding call completes.
            lease.releaseWhenIdle();
        }

        private void attemptNow() {
            try {
                lease.expectAttempt();
                executor.execute(this::attempt);
            } catch (RejectedExecutionException e) {
                // Ended meanwhile.
            }
        }

        private void attemptAndReschedule() {
            Duration next = attempt();
            if (next == null) {
                return;
            }
            try {
                executor.schedule(this::attemptAndReschedule, next.toMillis(), TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException e) {
                // This activation ended; a restarted route has its own.
            }
        }

        /** Runs one attempt if the contract check allows it; returns the delay until the next, or null for never. */
        private Duration attempt() {
            switch (contractCheck.get()) {
                case FAILED -> {
                    if (!contractFailureLogged) {
                        contractFailureLogged = true;
                        LOG.warn("Single-active routes stay idle: the message-store contract check failed");
                    }
                    return null;
                }
                case PENDING -> {
                    // Checked again soon; the startup event also triggers an attempt as soon as the check passes.
                    return timing.retryInterval();
                }
                default -> {
                    try {
                        lease.attempt();
                    } catch (RuntimeException e) {
                        LOG.warn("Single-active route {} lease attempt failed; trying again", lease.name(), e);
                    }
                    // A holder whose read or renewal failed retries sooner than the attempt interval.
                    return lease.nextAttemptDelay();
                }
            }
        }
    }
}
