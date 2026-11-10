package no.basis.integrationplatform.camel.ingress;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.lease.LeaseManager;
import no.basis.integrationplatform.camel.lease.SingleActiveRoutes;
import org.apache.camel.CamelContext;
import org.apache.camel.model.RouteDefinition;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Validates that ingress normalization sources cannot be duplicated accidentally
 * when an integration deployment is scaled beyond one replica.
 *
 * <p>A polling source the adapter coordinates through a lease
 * ({@link SingleActiveRoutes}) counts as coordinated, as do {@code master:}
 * routes and broker queues with competing consumers. A polling route listed in
 * {@value SingleActiveRoutes#EXCLUDED_ROUTES} does not. When the check accepts a
 * route only because its lease coordinates it (several or no declared replicas),
 * the lease becomes required: without a Key-Value Store the route then does no
 * work, rather than run on every replica.</p>
 */
@ApplicationScoped
public class IngressReplicaSafety {
    /** Deployment-supplied property containing the Kubernetes Deployment replica count. */
    public static final String PROPERTY = "basis.message-store.deployment-replicas";

    private final Optional<Integer> deploymentReplicas;
    private final Set<String> excludedFromLeases;
    private final Consumer<String> leaseRequired;

    /**
     * Creates the startup validator from the deployment-supplied replica count.
     *
     * @param deploymentReplicas declared Kubernetes Deployment replica count
     */
    public IngressReplicaSafety(Optional<Integer> deploymentReplicas) {
        this(deploymentReplicas, Optional.empty(), routeId -> { });
    }

    /**
     * Creates the startup validator from the deployment-supplied replica count and the
     * polling routes excluded from the adapter's leases.
     *
     * @param deploymentReplicas declared Kubernetes Deployment replica count
     * @param excludedRoutes ids of polling routes that run on every replica
     * @param leases the replica's lease manager, told which leases the check relied on
     */
    @Inject
    public IngressReplicaSafety(
            @ConfigProperty(name = PROPERTY) Optional<Integer> deploymentReplicas,
            @ConfigProperty(name = SingleActiveRoutes.EXCLUDED_ROUTES) Optional<String> excludedRoutes,
            LeaseManager leases) {
        this(deploymentReplicas, excludedRoutes, leases::require);
    }

    /**
     * Creates the validator with an explicit callback for the leases the check relies on.
     *
     * @param deploymentReplicas declared Kubernetes Deployment replica count
     * @param excludedRoutes ids of polling routes that run on every replica
     * @param leaseRequired called with the id of a route accepted only because its lease coordinates it
     */
    public IngressReplicaSafety(
            Optional<Integer> deploymentReplicas,
            Optional<String> excludedRoutes,
            Consumer<String> leaseRequired) {
        this.deploymentReplicas = deploymentReplicas;
        this.excludedFromLeases = SingleActiveRoutes.excludedRoutes(excludedRoutes);
        this.leaseRequired = leaseRequired;
        deploymentReplicas.ifPresent(replicas -> {
            if (replicas < 1) {
                throw new IllegalArgumentException(PROPERTY + " must be at least 1");
            }
        });
    }

    /**
     * Creates a validator with an explicit replica count for embedded use and tests.
     *
     * @param replicas declared replica count
     * @return configured validator
     */
    public static IngressReplicaSafety declaredReplicas(int replicas) {
        return new IngressReplicaSafety(Optional.of(replicas));
    }

    /**
     * Rejects normalization routes whose source coordination is unsafe for the
     * declared replica count.
     *
     * @param camelContext context that resolves the route's source endpoint
     * @param routeId stable Camel route id
     * @param route Camel route definition
     */
    public void validate(CamelContext camelContext, String routeId, RouteDefinition route) {
        String sourceUri = route.getInput() == null ? null : route.getInput().getEndpointUri();
        if (isCoordinatedSource(sourceUri)) {
            return;
        }
        if (SingleActiveRoutes.coordinates(camelContext, routeId, route, excludedFromLeases)) {
            if (!deploymentReplicas.equals(Optional.of(1))) {
                // Accepted only because the lease coordinates it: it must not run without the lease.
                leaseRequired.accept(routeId);
            }
            return;
        }

        int replicas = deploymentReplicas.orElseThrow(() -> new IllegalArgumentException(
                "Ingress normalization route '" + routeId + "' uses uncoordinated source '" + sourceUri
                        + "', but " + PROPERTY + " is not set. The deployment must declare its replica "
                        + "count so the adapter can prevent duplicate polling."));
        if (replicas > 1) {
            throw new IllegalArgumentException(
                    "Ingress normalization route '" + routeId + "' uses uncoordinated source '" + sourceUri
                            + "' with " + replicas + " deployment replicas. Use a Camel master: endpoint with "
                            + "cluster leader election, use a supported competing-consumer queue, or deploy one replica.");
        }
    }

    private boolean isCoordinatedSource(String sourceUri) {
        if (sourceUri == null) {
            return false;
        }
        String normalized = sourceUri.toLowerCase(Locale.ROOT);
        return normalized.startsWith("master:")
                || normalized.startsWith("jms:queue:")
                || normalized.startsWith("sjms2:queue:");
    }
}
