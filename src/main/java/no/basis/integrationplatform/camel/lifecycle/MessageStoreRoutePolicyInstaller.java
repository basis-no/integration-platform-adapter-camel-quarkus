package no.basis.integrationplatform.camel.lifecycle;

import jakarta.enterprise.context.ApplicationScoped;
import no.basis.integrationplatform.camel.lease.LeaseManager;
import no.basis.integrationplatform.camel.lease.SingleActiveConsumerStrategy;
import no.basis.integrationplatform.camel.lease.SingleActiveRoutePolicyFactory;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.builder.RouteBuilder;

/**
 * Installs the adapter route-policy factories, the lease manager, the
 * consumer gate and the endpoint interception strategy into Camel.
 */
@ApplicationScoped
public class MessageStoreRoutePolicyInstaller extends RouteBuilder {
    private final MessageStoreRoutePolicyFactory policyFactory;
    private final StepTrackingInterceptStrategy stepTrackingInterceptStrategy;
    private final SingleActiveRoutePolicyFactory singleActivePolicyFactory;
    private final LeaseManager leaseManager;
    private final SingleActiveConsumerStrategy singleActiveConsumerStrategy;

    /**
     * Creates the automatic Camel installer.
     *
     * @param policyFactory message-boundary policy factory
     * @param stepTrackingInterceptStrategy replay-aware endpoint interception strategy
     * @param singleActivePolicyFactory policy factory that runs polling routes on one replica
     * @param leaseManager leases of the polling routes, started before and stopped after the routes
     * @param singleActiveConsumerStrategy binds each polling route's consumer to its lease gate
     */
    public MessageStoreRoutePolicyInstaller(
            MessageStoreRoutePolicyFactory policyFactory,
            StepTrackingInterceptStrategy stepTrackingInterceptStrategy,
            SingleActiveRoutePolicyFactory singleActivePolicyFactory,
            LeaseManager leaseManager,
            SingleActiveConsumerStrategy singleActiveConsumerStrategy) {
        this.policyFactory = policyFactory;
        this.stepTrackingInterceptStrategy = stepTrackingInterceptStrategy;
        this.singleActivePolicyFactory = singleActivePolicyFactory;
        this.leaseManager = leaseManager;
        this.singleActiveConsumerStrategy = singleActiveConsumerStrategy;
    }

    @Override
    public void configure() throws Exception {
        // Camel Main registers RoutePolicyFactory beans itself; guard against a second copy.
        if (!getContext().getRoutePolicyFactories().contains(policyFactory)) {
            getContext().addRoutePolicyFactory(policyFactory);
        }
        if (!getContext().getRoutePolicyFactories().contains(singleActivePolicyFactory)) {
            getContext().addRoutePolicyFactory(singleActivePolicyFactory);
        }
        if (!getContext().hasService(leaseManager)) {
            // A context service starts before the routes and stops after them.
            getContext().addService(leaseManager);
        }
        if (!getContext().getLifecycleStrategies().contains(singleActiveConsumerStrategy)) {
            getContext().addLifecycleStrategy(singleActiveConsumerStrategy);
        }
        getContext().getCamelContextExtension().addInterceptStrategy(stepTrackingInterceptStrategy);
    }
}
