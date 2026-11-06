package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.step.DefaultStepTracker;
import org.apache.camel.Exchange;
import org.apache.camel.Route;
import org.apache.camel.support.RoutePolicySupport;

/**
 * Reads a replay's {@code x-bip-redo-steps} when an exchange enters a route that has no
 * Message Store policy, so that no send from the route carries this attempt's list:
 * {@code enrich}, {@code wireTap}, {@code recipientList} and {@code routingSlip} included.
 */
final class RedoStepsRoutePolicy extends RoutePolicySupport {
    @Override
    public void onExchangeBegin(Route route, Exchange exchange) {
        DefaultStepTracker.captureRedoSteps(exchange);
    }
}
