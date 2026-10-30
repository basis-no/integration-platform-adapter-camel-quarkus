package no.basis.integrationplatform.camel.ingress;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.camel.model.RouteDefinition;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Locale;
import java.util.Optional;

/**
 * Validates that ingress normalization sources cannot be duplicated accidentally
 * when an integration deployment is scaled beyond one replica.
 */
@ApplicationScoped
public class IngressReplicaSafety {
    /** Platform property containing the Deployment replica count. */
    public static final String PROPERTY = "basis.message-store.deployment-replicas";

    private final Optional<Integer> deploymentReplicas;

    /**
     * Creates the startup validator from the platform-supplied replica count.
     *
     * @param deploymentReplicas declared Kubernetes Deployment replica count
     */
    @Inject
    public IngressReplicaSafety(@ConfigProperty(name = PROPERTY) Optional<Integer> deploymentReplicas) {
        this.deploymentReplicas = deploymentReplicas;
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
     * @param routeId stable Camel route id
     * @param route Camel route definition
     */
    public void validate(String routeId, RouteDefinition route) {
        String sourceUri = route.getInput() == null ? null : route.getInput().getEndpointUri();
        if (isCoordinatedSource(sourceUri)) {
            return;
        }

        int replicas = deploymentReplicas.orElseThrow(() -> new IllegalArgumentException(
                "Ingress normalization route '" + routeId + "' uses uncoordinated source '" + sourceUri
                        + "', but " + PROPERTY + " is not set. The platform must declare the deployment replica "
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
