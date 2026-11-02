package no.basis.integrationplatform.camel;

import jakarta.enterprise.context.ApplicationScoped;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.ToDynamicDefinition;

/**
 * Publishes an exhausted exchange to {@code basis.message-store.dlq-uri} and marks it parked
 * only after the endpoint accepted it. A Kafka producer with Camel's default
 * {@code requestRequiredAcks=all} returns once the broker acknowledged the record. A failed
 * publish leaves the exchange failed and the message not parked, so its Kafka offset is not
 * committed.
 */
@ApplicationScoped
public class PlatformDeadLetterRoute extends RouteBuilder {
    /** Route id of the adapter's dead-letter route. Its publish is not a replayable step. */
    public static final String ROUTE_ID = "bip-confirmed-dead-letter";

    /** Creates the dead-letter route. */
    public PlatformDeadLetterRoute() {
    }

    @Override
    public void configure() {
        var route = from(PlatformRouteConfiguration.DLQ_URI).routeId(ROUTE_ID).errorHandler(noErrorHandler());
        // Resolve the producer when parking is needed, so an application with no Kafka
        // input does not need a broker merely to start this route.
        route.addOutput(new ToDynamicDefinition(PlatformRouteConfiguration.DLQ_ENDPOINT).allowOptimisedComponents(false));
        route.process(exchange -> exchange.setProperty(PlatformExchangeProperties.DLQ_PARKED, true));
    }
}
