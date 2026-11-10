package no.basis.integrationplatform.camel.lease;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.NamedNode;
import org.apache.camel.Route;
import org.apache.camel.model.Model;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.spi.RoutePolicy;
import org.apache.camel.spi.RoutePolicyFactory;

import java.util.Optional;
import java.util.Set;

/**
 * Gives every polling route a {@link SingleActiveRoutePolicy}, so that its
 * work runs on the one replica that holds {@code lease/<routeId>}, unless the
 * route is listed in {@value SingleActiveRoutes#EXCLUDED_ROUTES}.
 */
@Singleton
public class SingleActiveRoutePolicyFactory implements RoutePolicyFactory {
    private static final int MAX_KEY_CHARACTERS = 256;

    private static final Logger LOG = LoggerFactory.getLogger(SingleActiveRoutePolicyFactory.class);

    private final LeaseManager manager;
    private final Set<String> excluded;

    /**
     * Creates the factory on the replica's lease manager.
     *
     * @param manager lease manager of this replica
     * @param excludedRoutes ids of polling routes that run on every replica
     */
    @Inject
    public SingleActiveRoutePolicyFactory(
            LeaseManager manager,
            @ConfigProperty(name = SingleActiveRoutes.EXCLUDED_ROUTES) Optional<String> excludedRoutes) {
        this.manager = manager;
        this.excluded = SingleActiveRoutes.excludedRoutes(excludedRoutes);
    }

    SingleActiveRoutePolicyFactory(LeaseManager manager) {
        this(manager, Optional.empty());
    }

    void verifyExclusionsAtStartup(@Observes StartupEvent ignored, CamelContext camelContext) {
        verifyExclusions(camelContext);
    }

    /**
     * Refuses an excluded id that names no polling route of the application, so a
     * typo cannot leave a route coordinated that the application meant to run on
     * every replica, or hide that the id does not exist.
     */
    void verifyExclusions(CamelContext camelContext) {
        Model model = camelContext.getCamelContextExtension().getContextPlugin(Model.class);
        for (String routeId : excluded) {
            Route route = camelContext.getRoute(routeId);
            RouteDefinition definition = model == null ? null : model.getRouteDefinition(routeId);
            if (route == null || definition == null || !SingleActiveRoutes.coordinates(camelContext, definition)) {
                throw new IllegalStateException(SingleActiveRoutes.EXCLUDED_ROUTES + " lists '" + routeId
                        + "', which is not a polling route of this application; list only the ids of timer,"
                        + " cron, quartz, scheduler or polling routes that are to run on every replica");
            }
        }
    }

    @Override
    public RoutePolicy createRoutePolicy(CamelContext camelContext, String routeId, NamedNode route) {
        if (!(route instanceof RouteDefinition definition) || !SingleActiveRoutes.coordinates(camelContext, definition)) {
            return null;
        }
        if (excluded.contains(routeId)) {
            LOG.info("Polling route {} runs on every replica: it is listed in {}", routeId,
                    SingleActiveRoutes.EXCLUDED_ROUTES);
            return null;
        }
        if ((Lease.KEY_PREFIX + routeId).length() > MAX_KEY_CHARACTERS) {
            throw new IllegalArgumentException("Polling route id '" + routeId + "' is too long for its lease key;"
                    + " route ids of single-active routes are at most " + (MAX_KEY_CHARACTERS - Lease.KEY_PREFIX.length())
                    + " characters");
        }
        return new SingleActiveRoutePolicy(manager);
    }
}
