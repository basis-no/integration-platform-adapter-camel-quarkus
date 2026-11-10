package example;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.PlatformRouteConfiguration;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class FixtureRoutes extends RouteBuilder {
    @Inject ProducerTemplate producer;
    @ConfigProperty(name = "acceptance.accept-rejected", defaultValue = "false")
    boolean acceptRejected;

    @Override
    public void configure() {
        from("kafka:{{acceptance.input-topic}}?groupId={{acceptance.group}}&maxPollRecords=1&autoOffsetReset=earliest")
            .routeId("source").autoStartup("{{acceptance.enabled}}")
            .process(this::callReceiver);
        from("direct:receiver").routeId("receiver")
            .routeConfigurationId(PlatformRouteConfiguration.NO_RETRY)
            .process(e -> {
                String body = e.getMessage().getBody(String.class);
                if (!body.startsWith("PASS ") && !(acceptRejected && body.startsWith("FAIL "))) {
                    throw new IllegalArgumentException("synthetic receiver rejection");
                }
                e.getMessage().setBody("accepted");
            });
    }

    void callReceiver(Exchange input) {
        String body = input.getMessage().getBody(String.class);
        Exchange reply = producer.request("direct:receiver", child -> child.getMessage().setBody(body));
        if (reply.getException() != null) {
            input.setException(reply.getException());
        }
        // Keep the original body and identity for outcome capture and replay.
    }
}
