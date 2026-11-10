package no.basis.integrationplatform.camel.lease;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.camel.CamelContext;
import org.apache.camel.Consumer;
import org.apache.camel.Route;
import org.apache.camel.RouteAware;
import org.apache.camel.Service;
import org.apache.camel.spi.PollingConsumerPollStrategy;
import org.apache.camel.support.DefaultPollingConsumerPollStrategy;
import org.apache.camel.support.LifecycleStrategySupport;
import org.apache.camel.support.ScheduledPollConsumer;

/**
 * Prepares a coordinated route at the moment Camel adds the route's consumer
 * as a service, which is right before the consumer starts and before its
 * first poll or tick: it notes that the route's start is expected, so a
 * trigger that fires before the policy's {@code onStart} waits for the lease
 * instead of losing its tick, and it binds the lease gate to a scheduled poll
 * consumer.
 *
 * <p>No lease is taken here: the consumer may still fail to start, in which
 * case Camel calls no policy callback and the expectation lapses. The lease is
 * taken in the route policy's {@code onStart}, which Camel calls only once the
 * consumer runs. Only the route's own consumer is wrapped. The endpoint it was
 * created from stays untouched, so a {@code pollEnrich} or polling consumer on
 * the same endpoint is never gated by an unrelated route, and a route that is
 * removed and added again gets a fresh consumer with a fresh gate.</p>
 */
@Singleton
public class SingleActiveConsumerStrategy extends LifecycleStrategySupport {
    private final LeaseManager manager;

    /**
     * Creates the strategy on the replica's lease manager.
     *
     * @param manager lease manager of this replica
     */
    @Inject
    public SingleActiveConsumerStrategy(LeaseManager manager) {
        this.manager = manager;
    }

    @Override
    public void onServiceAdd(CamelContext context, Service service, Route route) {
        if (route == null
                || !(service instanceof Consumer)
                || !(service instanceof RouteAware aware) || aware.getRoute() != route
                || manager.current(route.getRouteId()) == null) {
            return;
        }
        manager.expectStart(route.getRouteId());
        if (!(service instanceof ScheduledPollConsumer consumer)) {
            return;
        }
        PollingConsumerPollStrategy configured = consumer.getPollStrategy();
        if (configured instanceof GatedPollStrategy gated && route.getRouteId().equals(gated.routeId())) {
            return;
        }
        consumer.setPollStrategy(new GatedPollStrategy(route.getRouteId(), manager,
                configured != null ? configured : new DefaultPollingConsumerPollStrategy()));
    }
}
