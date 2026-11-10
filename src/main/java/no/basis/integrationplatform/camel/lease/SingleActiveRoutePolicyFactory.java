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
    private final boolean singleReplica;

    /**
     * Creates the factory on the replica's lease manager.
     *
     * @param manager lease manager of this replica
     * @param excludedRoutes ids of polling routes that run on every replica
     * @param deploymentReplicas declared replica count, if the deployment sets it
     */
    @Inject
    public SingleActiveRoutePolicyFactory(
            LeaseManager manager,
            @ConfigProperty(name = SingleActiveRoutes.EXCLUDED_ROUTES) Optional<String> excludedRoutes,
            @ConfigProperty(name = "basis.message-store.deployment-replicas") Optional<Integer> deploymentReplicas) {
        this.manager = manager;
        this.excluded = SingleActiveRoutes.excludedRoutes(excludedRoutes);
        this.singleReplica = deploymentReplicas.equals(Optional.of(1));
    }

    SingleActiveRoutePolicyFactory(LeaseManager manager, Optional<String> excludedRoutes) {
        this(manager, excludedRoutes, Optional.empty());
    }

    SingleActiveRoutePolicyFactory(LeaseManager manager) {
        this(manager, Optional.empty());
    }

    void verifyExclusionsAtStartup(@Observes StartupEvent ignored, CamelContext camelContext) {
        verifyExclusions(camelContext);
    }

    /**
     * Refuses an excluded id that names a route that does not poll, and warns about an id
     * that names no route at startup: a route can still be added later, for example from a
     * route template, but a typo would otherwise leave a route coordinated unnoticed.
     */
    void verifyExclusions(CamelContext camelContext) {
        Model model = camelContext.getCamelContextExtension().getContextPlugin(Model.class);
        for (String routeId : excluded) {
            Route route = camelContext.getRoute(routeId);
            RouteDefinition definition = model == null ? null : model.getRouteDefinition(routeId);
            if (route == null || definition == null) {
                LOG.warn("{} lists '{}', which names no route at startup; it applies if such a polling route"
                        + " is added later", SingleActiveRoutes.EXCLUDED_ROUTES, routeId);
                continue;
            }
            if (SingleActiveRoutes.isFiniteTimer(camelContext, definition)) {
                // Never leased anyway; listing it changes nothing and is harmless.
                continue;
            }
            if (!SingleActiveRoutes.coordinates(camelContext, definition)) {
                throw new IllegalStateException(SingleActiveRoutes.EXCLUDED_ROUTES + " lists '" + routeId
                        + "', which is not a route the adapter runs under a lease; list only the ids of timer,"
                        + " cron, quartz, scheduler or polling routes that are to run on every replica");
            }
        }
    }

    @Override
    public RoutePolicy createRoutePolicy(CamelContext camelContext, String routeId, NamedNode route) {
        if (!(route instanceof RouteDefinition definition)) {
            return null;
        }
        if (SingleActiveRoutes.isFiniteTimer(camelContext, definition)) {
            String message = "Timer route {} has a finite repeatCount, so it runs on every replica, not under a"
                    + " lease: a tick dropped on a replica without the lease, such as the new pod of a rolling"
                    + " update, would never fire again. Make it idempotent across replicas, or use a recurring"
                    + " timer";
            if (singleReplica || excluded.contains(routeId)) {
                LOG.info(message, routeId);
            } else {
                LOG.warn(message, routeId);
            }
            return null;
        }
        if (!SingleActiveRoutes.coordinates(camelContext, definition)) {
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
