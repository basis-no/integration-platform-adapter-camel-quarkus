package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.ForwardedCredentialHeaders;
import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.spi.CamelEvent;
import org.apache.camel.spi.ManagementStrategy;
import org.apache.camel.support.EventNotifierSupport;
import org.apache.camel.support.service.ServiceHelper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Keeps Camel's Kafka consumer headers named {@code kafka.*} off HTTP requests. camel-kafka
 * 4.0.0 to 4.14.7, 4.15.0 to 4.18.2 and 4.19.0 to 4.20.0 use these names, and HTTP producers
 * send them: {@code kafka.HEADERS} lists the bytes of every record header, credentials
 * included. Later versions name them {@code CamelKafka*}, which HTTP producers do not send.
 * <p>
 * The headers are withheld while Camel sends to an endpoint of these components, after it
 * has chosen the endpoint, and restored when the send completes, also when it failed. A
 * {@code toD} URI and a route reached through an internal endpoint still read them, and every
 * EIP that sends through a producer is covered.
 */
final class HttpKafkaConsumerHeaders extends EventNotifierSupport {
    private static final Set<String> HTTP_COMPONENTS = Set.of("http", "https", "netty-http", "vertx-http");
    private static final String PREFIX = "kafka.";
    private static final String WITHHELD = "basis.messageStore.withheldKafkaConsumerHeaders";

    private HttpKafkaConsumerHeaders() {
        setIgnoreCamelContextEvents(true);
        setIgnoreCamelContextInitEvents(true);
        setIgnoreRouteEvents(true);
        setIgnoreServiceEvents(true);
        setIgnoreStepEvents(true);
        setIgnoreExchangeCreatedEvent(true);
        setIgnoreExchangeCompletedEvent(true);
        setIgnoreExchangeFailedEvents(true);
        setIgnoreExchangeRedeliveryEvents(true);
        setIgnoreExchangeAsyncProcessingStartedEvents(true);
    }

    /** Adds the notifier to the context once. */
    static void install(CamelContext context) {
        ManagementStrategy management = context.getManagementStrategy();
        synchronized (management) {
            if (management.getEventNotifiers().stream().noneMatch(HttpKafkaConsumerHeaders.class::isInstance)) {
                HttpKafkaConsumerHeaders notifier = new HttpKafkaConsumerHeaders();
                ServiceHelper.startService(notifier);
                management.addEventNotifier(notifier);
            }
        }
    }

    @Override
    public boolean isEnabled(CamelEvent event) {
        return event instanceof CamelEvent.ExchangeSendingEvent || event instanceof CamelEvent.ExchangeSentEvent;
    }

    @Override
    public void notify(CamelEvent event) {
        if (event instanceof CamelEvent.ExchangeSendingEvent sending && isHttp(sending.getEndpoint())) {
            withhold(sending.getExchange());
        } else if (event instanceof CamelEvent.ExchangeSentEvent sent && isHttp(sent.getEndpoint())) {
            restore(sent.getExchange());
        }
    }

    static boolean isHttp(Endpoint endpoint) {
        String uri = endpoint == null ? null : endpoint.getEndpointUri();
        int colon = uri == null ? -1 : uri.indexOf(':');
        return colon > 0 && HTTP_COMPONENTS.contains(uri.substring(0, colon).toLowerCase(Locale.ROOT));
    }

    @SuppressWarnings("unchecked")
    private static void withhold(Exchange exchange) {
        Map<String, Object> withheld = exchange.getProperty(WITHHELD, Map.class);
        for (String name : List.copyOf(exchange.getMessage().getHeaders().keySet())) {
            if (name.regionMatches(true, 0, PREFIX, 0, PREFIX.length())) {
                if (withheld == null) {
                    withheld = new LinkedHashMap<>();
                    exchange.setProperty(WITHHELD, withheld);
                }
                withheld.put(name, exchange.getMessage().removeHeader(name));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void restore(Exchange exchange) {
        Map<String, Object> withheld = (Map<String, Object>) exchange.removeProperty(WITHHELD);
        if (withheld != null) {
            ForwardedCredentialHeaders.restore(exchange, withheld);
        }
    }
}
