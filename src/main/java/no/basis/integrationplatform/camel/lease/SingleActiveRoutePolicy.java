package no.basis.integrationplatform.camel.lease;

import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.support.RoutePolicySupport;
import org.apache.camel.support.ScheduledPollConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lets a polling route do its work only while this replica holds the route's
 * lease. The route itself starts and stops exactly as Camel would without the
 * adapter; the gate sits inside it.
 *
 * <p>The lease follows the route's life as Camel runs it: it is acquired in
 * {@code onStart} and {@code onResume}, which Camel calls only once the
 * consumer has started, whatever decided that (the route's or the context's
 * {@code autoStartup}, an exclude pattern, an operator), and never for a
 * consumer that failed to start; {@link SingleActiveConsumerStrategy} notes the
 * expected start right before the consumer starts so an early tick waits. It
 * is released, after the work in flight has finished, when the route stops or
 * suspends, so a route an operator stops on one replica is taken over by
 * another. For a {@link ScheduledPollConsumer} the poll
 * is the work and the strategy gates it in the consumer. For a trigger consumer
 * (timer, quartz, cron) the exchange is the work: a trigger that fires while
 * the gate is closed first waits, bounded, for an acquisition attempt of this
 * replica that is in flight, then stops its route before the first processor
 * if the gate is still closed; a trigger has no source to acknowledge, so
 * nothing is lost but the tick.</p>
 */
final class SingleActiveRoutePolicy extends RoutePolicySupport {
    private static final Logger LOG = LoggerFactory.getLogger(SingleActiveRoutePolicy.class);

    private final LeaseManager manager;
    private final AtomicBoolean unregisteredWarned = new AtomicBoolean();
    // The lease instance whose first skipped trigger was logged at INFO; later skips log at DEBUG.
    private volatile Lease skipLogged;

    SingleActiveRoutePolicy(LeaseManager manager) {
        this.manager = manager;
    }

    @Override
    public void onInit(Route route) {
        super.onInit(route);
        // Registered closed; the lease is acquired once Camel starts the consumer, not from the autoStartup flag.
        manager.lease(route.getRouteId());
    }

    @Override
    public void onStart(Route route) {
        // The consumer has started: take the lease with a fresh instance.
        manager.activate(route.getRouteId());
    }

    @Override
    public void onResume(Route route) {
        manager.activate(route.getRouteId());
    }

    @Override
    public void onStop(Route route) {
        // Called after the route's consumer stopped and its exchanges in flight finished.
        manager.deactivate(route.getRouteId());
    }

    @Override
    public void onSuspend(Route route) {
        manager.deactivate(route.getRouteId());
    }

    @Override
    public void onRemove(Route route) {
        manager.release(route.getRouteId());
    }

    /** A coordinated route without a registration is an unexpected state: fail closed, warn once. */
    private void unregistered(String routeId, Exchange exchange) {
        if (unregisteredWarned.compareAndSet(false, true)) {
            LOG.warn("Single-active route {} has no lease registration; its triggers run nothing", routeId);
        }
        exchange.setRouteStop(true);
    }

    @Override
    public void onExchangeBegin(Route route, Exchange exchange) {
        // Decided by the consumer, as the poll gate is: a consumer whose polls are gated needs no
        // exchange gate; any other consumer of a coordinated route is a trigger (others refuse to start).
        if (route.getConsumer() instanceof ScheduledPollConsumer consumer
                && consumer.getPollStrategy() instanceof GatedPollStrategy) {
            return;
        }
        String routeId = route.getRouteId();
        Lease lease = manager.current(routeId);
        if (lease == null) {
            unregistered(routeId, exchange);
            return;
        }
        if (lease.isOpen()) {
            return;
        }
        // An acquisition may be in flight right now (startup, or a route added or started at runtime): a
        // one-shot trigger would lose its only tick, so wait for the attempt's outcome before deciding.
        manager.awaitAttempt(routeId, manager.timing().startupWait());
        // The wait may have spanned an activation, which replaces the route's lease instance.
        lease = manager.current(routeId);
        if (lease == null) {
            unregistered(routeId, exchange);
            return;
        }
        if (!lease.isOpen()) {
            if (skipLogged != lease) {
                skipLogged = lease;
                LOG.info("Single-active route {} skips its triggers while this replica does not hold its lease;"
                        + " the replica that holds it runs them", routeId);
            } else {
                LOG.debug("Single-active route {} skips a trigger: this replica does not hold its lease", routeId);
            }
            exchange.setRouteStop(true);
        }
    }
}
