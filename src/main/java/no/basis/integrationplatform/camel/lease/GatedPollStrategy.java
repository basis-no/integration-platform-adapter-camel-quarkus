package no.basis.integrationplatform.camel.lease;

import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.spi.PollingConsumerPollStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lets a scheduled poll of one route's consumer begin only while that route's
 * current lease gate is open.
 *
 * <p>{@code begin()} returning false makes {@code ScheduledPollConsumer} skip
 * the poll: nothing is read, no empty message is sent and nothing is
 * committed. A poll that already began is work in flight and always runs to
 * its end, whatever the gate does meanwhile. Any strategy the consumer was
 * configured with keeps running for the polls that do begin. The gate is bound
 * to the consumer of one route and looks the route's lease up on every poll,
 * so a route that is removed and added again is gated by its new lease. A gate
 * exists only for a coordinated route and the fallback is an open lease, never
 * a missing one, so a route without a registration is an unexpected state and
 * the gate fails closed, warning once.</p>
 */
final class GatedPollStrategy implements PollingConsumerPollStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(GatedPollStrategy.class);

    private final String routeId;
    private final LeaseManager manager;
    private final PollingConsumerPollStrategy delegate;
    private final AtomicBoolean unregisteredWarned = new AtomicBoolean();

    GatedPollStrategy(String routeId, LeaseManager manager, PollingConsumerPollStrategy delegate) {
        this.routeId = routeId;
        this.manager = manager;
        this.delegate = delegate;
    }

    String routeId() {
        return routeId;
    }

    PollingConsumerPollStrategy delegate() {
        return delegate;
    }

    @Override
    public boolean begin(Consumer consumer, Endpoint endpoint) {
        Lease lease = manager.current(routeId);
        if (lease == null) {
            if (unregisteredWarned.compareAndSet(false, true)) {
                LOG.warn("Single-active route {} has no lease registration; its polls are skipped", routeId);
            }
            return false;
        }
        if (!lease.isOpen()) {
            LOG.trace("Single-active route {} skips a poll: this replica does not hold its lease", routeId);
            return false;
        }
        return delegate.begin(consumer, endpoint);
    }

    @Override
    public void commit(Consumer consumer, Endpoint endpoint, int polledMessages) {
        delegate.commit(consumer, endpoint, polledMessages);
    }

    @Override
    public boolean rollback(Consumer consumer, Endpoint endpoint, int retryCounter, Exception cause) throws Exception {
        return delegate.rollback(consumer, endpoint, retryCounter, cause);
    }
}
