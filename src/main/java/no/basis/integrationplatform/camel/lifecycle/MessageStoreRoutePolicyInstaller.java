package no.basis.integrationplatform.camel.lifecycle;

import jakarta.enterprise.context.ApplicationScoped;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.builder.RouteBuilder;

/** Installs the adapter route-policy factory and endpoint interception strategy into Camel. */
@ApplicationScoped
public class MessageStoreRoutePolicyInstaller extends RouteBuilder {
    private final MessageStoreRoutePolicyFactory policyFactory;
    private final StepTrackingInterceptStrategy stepTrackingInterceptStrategy;

    /**
     * Creates the automatic Camel installer.
     *
     * @param policyFactory message-boundary policy factory
     * @param stepTrackingInterceptStrategy replay-aware endpoint interception strategy
     */
    public MessageStoreRoutePolicyInstaller(
            MessageStoreRoutePolicyFactory policyFactory,
            StepTrackingInterceptStrategy stepTrackingInterceptStrategy) {
        this.policyFactory = policyFactory;
        this.stepTrackingInterceptStrategy = stepTrackingInterceptStrategy;
    }

    @Override
    public void configure() {
        if (!getContext().getRoutePolicyFactories().contains(policyFactory)) {
            getContext().addRoutePolicyFactory(policyFactory);
        }
        getContext().getCamelContextExtension().addInterceptStrategy(stepTrackingInterceptStrategy);
    }
}
