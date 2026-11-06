package no.basis.integrationplatform.camel;

import org.apache.camel.CamelContext;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.spi.CamelEvent;
import org.apache.camel.spi.ManagementStrategy;
import org.apache.camel.support.EventNotifierSupport;
import org.apache.camel.support.service.ServiceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Keeps a consumed Kafka record's credential headers, and Camel's Kafka consumer headers, off
 * HTTP sends. Kafka headers arrive as byte arrays, and an HTTP producer sends a byte array as
 * its list of numbers or as text, so a caller's {@code Authorization} that an intake published
 * with the message reached the next receiver in decodable form. A credential header the route
 * sets itself is text and is sent as before. Camel's Kafka consumer also sets headers named
 * {@code kafka.*} on camel-kafka 4.0.0 to 4.14.7, 4.15.0 to 4.18.2 and 4.19.0 to 4.20.0, and
 * HTTP producers send them: {@code kafka.HEADERS} lists the bytes of every record header,
 * credentials included. Later versions name them {@code CamelKafka*}, which HTTP producers do
 * not send.
 * <p>
 * A tracked {@code to} or {@code toD} through Camel's http component withholds the credential
 * headers around the send. {@link #install} withholds both kinds while Camel sends to an
 * {@code http}, {@code https}, {@code netty-http} or {@code vertx-http} endpoint, after it has
 * chosen the endpoint, through any EIP or a {@code ProducerTemplate}, and restores them when
 * the send completes, also after a failure. That runs on Camel's exchange sending events, so
 * a {@code ProducerTemplate} with its event notifier disabled is not covered, and Camel sends
 * on when a notifier fails. A {@code rest:} send is not covered either.
 */
public final class ForwardedCredentialHeaders {
    private static final Logger LOG = LoggerFactory.getLogger(ForwardedCredentialHeaders.class);
    private static final Set<String> HTTP_SCHEMES = Set.of("http", "https");
    // A rest: producer binds headers into its path and query after the send event, so it is
    // not covered here.
    private static final Set<String> HTTP_COMPONENTS = Set.of("http", "https", "netty-http", "vertx-http");
    private static final String KAFKA_CONSUMER_PREFIX = "kafka.";
    private static final String WITHHELD = "basis.messageStore.withheldRecordHeaders";
    private static final Set<String> WARNED = ConcurrentHashMap.newKeySet();

    private ForwardedCredentialHeaders() {
    }

    /**
     * Whether a send goes through Camel's http component.
     * @param endpointUri the send's endpoint URI
     * @return true for {@code http:} and {@code https:} endpoints
     */
    public static boolean isHttp(String endpointUri) {
        return hasScheme(endpointUri, HTTP_SCHEMES);
    }

    private static boolean hasScheme(String endpointUri, Set<String> schemes) {
        if (endpointUri == null) {
            return false;
        }
        // Camel trims a resolved endpoint URI before it picks the component.
        String uri = endpointUri.trim();
        int colon = uri.indexOf(':');
        return colon > 0 && schemes.contains(uri.substring(0, colon).toLowerCase(Locale.ROOT));
    }

    /**
     * Removes credential-like headers that still hold raw Kafka bytes.
     * @param exchange the exchange about to be sent
     * @param stepId the send's step id, for the warning
     * @return the removed headers, to restore after the send
     */
    public static Map<String, Object> withhold(Exchange exchange, String stepId) {
        Map<String, Object> withheld = new LinkedHashMap<>();
        withhold(exchange, withheld, false, "step " + stepId);
        return withheld.isEmpty() ? Map.of() : withheld;
    }

    /**
     * Puts withheld headers back after the send, unless the response set the same name.
     * @param exchange the exchange after the send
     * @param withheld the headers {@link #withhold} removed
     */
    public static void restore(Exchange exchange, Map<String, Object> withheld) {
        withheld.forEach((name, value) -> {
            if (exchange.getMessage().getHeader(name) == null) {
                exchange.getMessage().setHeader(name, value);
            }
        });
    }

    /**
     * Withholds a consumed record's credential headers and Camel's {@code kafka.*} consumer
     * headers on every send to an HTTP endpoint of the context. Installs once per context.
     * @param context the Camel context
     */
    public static void install(CamelContext context) {
        ManagementStrategy management = context.getManagementStrategy();
        synchronized (management) {
            if (management.getEventNotifiers().stream().noneMatch(OnHttpSend.class::isInstance)) {
                OnHttpSend notifier = new OnHttpSend();
                ServiceHelper.startService(notifier);
                management.addEventNotifier(notifier);
            }
        }
    }

    private static void withhold(Exchange exchange, Map<String, Object> withheld, boolean consumerHeaders,
            String sender) {
        for (String name : List.copyOf(exchange.getMessage().getHeaders().keySet())) {
            Object value = exchange.getMessage().getHeader(name);
            boolean credential = value instanceof byte[] && PlatformRouteConfiguration.isCredentialHeader(name);
            boolean consumer = consumerHeaders
                    && name.regionMatches(true, 0, KAFKA_CONSUMER_PREFIX, 0, KAFKA_CONSUMER_PREFIX.length());
            if (credential || consumer) {
                withheld.put(name, exchange.getMessage().removeHeader(name));
                if (credential && WARNED.add(sender + "\n" + name.toLowerCase(Locale.ROOT))) {
                    LOG.warn("Header {} from the consumed Kafka record was not sent by {}. A credential "
                            + "received with a message must not reach the next receiver; the publishing route "
                            + "should remove caller headers before it publishes", name, sender);
                }
            }
        }
    }

    /** What one send withheld, to restore when that send completes. */
    private record Withheld(Endpoint endpoint, Map<String, Object> headers) {
    }

    /** The sends in progress of one exchange; a copy of the exchange gets its own. */
    private record Sends(Exchange exchange, Deque<Withheld> withheld) {
    }

    /**
     * Withholds the headers while Camel sends to an HTTP endpoint, after it has chosen the
     * endpoint. Sends nest, for example through {@code interceptSendToEndpoint}, so each send
     * keeps its own record and restores only its own: an inner send that completes never puts
     * back what an outer send, still to run, withheld.
     */
    private static final class OnHttpSend extends EventNotifierSupport {
        private OnHttpSend() {
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

        @Override
        public boolean isEnabled(CamelEvent event) {
            return event instanceof CamelEvent.ExchangeSendingEvent || event instanceof CamelEvent.ExchangeSentEvent;
        }

        @Override
        public void notify(CamelEvent event) {
            if (event instanceof CamelEvent.ExchangeSendingEvent sending && isHttpComponent(sending.getEndpoint())) {
                Exchange exchange = sending.getExchange();
                Sends sends = exchange.getProperty(WITHHELD, Sends.class);
                if (sends == null || sends.exchange() != exchange) {
                    sends = new Sends(exchange, new ArrayDeque<>());
                    exchange.setProperty(WITHHELD, sends);
                }
                Map<String, Object> headers = new LinkedHashMap<>();
                withhold(exchange, headers, true, "a send in route " + exchange.getFromRouteId());
                sends.withheld().push(new Withheld(sending.getEndpoint(), headers));
            } else if (event instanceof CamelEvent.ExchangeSentEvent sent && isHttpComponent(sent.getEndpoint())) {
                Exchange exchange = sent.getExchange();
                Sends sends = exchange.getProperty(WITHHELD, Sends.class);
                if (sends == null || sends.exchange() != exchange
                        || sends.withheld().stream().noneMatch(w -> w.endpoint() == sent.getEndpoint())) {
                    return;
                }
                // Restoring late is safe, restoring early is not: also put back what a send
                // that never reported its completion withheld.
                Withheld last;
                do {
                    last = sends.withheld().pop();
                    restore(exchange, last.headers());
                } while (last.endpoint() != sent.getEndpoint());
                if (sends.withheld().isEmpty()) {
                    exchange.removeProperty(WITHHELD);
                }
            }
        }

        private static boolean isHttpComponent(Endpoint endpoint) {
            return endpoint != null && hasScheme(endpoint.getEndpointUri(), HTTP_COMPONENTS);
        }
    }
}
