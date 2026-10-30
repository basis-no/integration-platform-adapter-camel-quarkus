package no.basis.integrationplatform.camel.lifecycle;

import jakarta.enterprise.context.ApplicationScoped;
import no.basis.integrationplatform.camel.step.StepTrackingInterceptStrategy;
import org.apache.camel.builder.RouteBuilder;

@ApplicationScoped
public class MessageStoreRoutePolicyInstaller extends RouteBuilder {
    private final MessageStoreRoutePolicyFactory policyFactory;
    private final StepTrackingInterceptStrategy stepTrackingInterceptStrategy;

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
