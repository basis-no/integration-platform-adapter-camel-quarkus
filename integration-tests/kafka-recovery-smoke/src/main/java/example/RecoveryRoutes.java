package example;

import jakarta.enterprise.context.ApplicationScoped;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.KafkaComponent;
import org.apache.camel.component.kafka.KafkaEndpoint;

@ApplicationScoped
public class RecoveryRoutes extends RouteBuilder {
    @Override
    public void configure() throws Exception {
        String topic = getContext().resolvePropertyPlaceholders("{{fixture.topic}}");
        String group = getContext().resolvePropertyPlaceholders("{{fixture.group}}");
        from("kafka:" + topic + "?groupId=" + group + "&autoOffsetReset=earliest&pollTimeoutMs=100")
                .routeId("acl-source").autoStartup(false)
                .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                .process(exchange -> { /* only the real source lifecycle records its outcome */ });
        // Configure the synthetic invalid credential on the endpoint object, never in its URI.
        KafkaEndpoint invalid = (KafkaEndpoint) getContext().getComponent("kafka", KafkaComponent.class)
                .createEndpoint("kafka:" + topic + "?groupId=" + group + "-invalid&pollTimeoutMs=100");
        invalid.getConfiguration().setBrokers(getContext().resolvePropertyPlaceholders("{{fixture.brokers}}"));
        invalid.getConfiguration().setSecurityProtocol("SASL_PLAINTEXT");
        invalid.getConfiguration().setSaslMechanism("PLAIN");
        invalid.getConfiguration().setPollOnError(org.apache.camel.component.kafka.PollOnError.RECONNECT);
        invalid.getConfiguration().setSaslJaasConfig(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"reader\" password=\"invalid-fixture-only\";");
        from(invalid).routeId("invalid-auth-source").autoStartup(false)
                .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
                .process(exchange -> { throw new AssertionError("Invalid credential consumed input"); });
    }
}
