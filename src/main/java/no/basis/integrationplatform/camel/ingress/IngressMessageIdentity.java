package no.basis.integrationplatform.camel.ingress;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.PlatformHeaders;
import org.apache.camel.Exchange;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/** Derives stable platform message identity before a configured ingress route publishes to Kafka. */
@ApplicationScoped
public class IngressMessageIdentity {
    private static final String JMS_MESSAGE_ID = "JMSMessageID";
    private static final String CAMEL_JMS_MESSAGE_ID = "CamelJmsMessageID";
    private static final String FILE_RELATIVE_PATH = "CamelFileRelativePath";
    private static final String FILE_PATH = "CamelFilePath";
    private static final String FILE_NAME = "CamelFileName";
    private static final String FILE_LENGTH = "CamelFileLength";
    private static final String FILE_LAST_MODIFIED = "CamelFileLastModified";

    private final IngressNormalizationConfig config;
    private final String integrationName;

    /**
     * Creates the identity helper.
     *
     * @param config configured normalization routes
     * @param integrationName stable integration name
     */
    @Inject
    public IngressMessageIdentity(
            IngressNormalizationConfig config,
            @ConfigProperty(name = "app.name", defaultValue = "unknown-integration") String integrationName) {
        this.config = config;
        this.integrationName = integrationName;
    }

    /**
     * Supplies deterministic source and message identity for a normalized Kafka publish.
     *
     * @param exchange intercepted source exchange
     * @param endpointUri intercepted producer endpoint
     * @throws IngressNormalizationException when the source event has no stable identity
     */
    public void prepareKafkaPublish(Exchange exchange, String endpointUri) {
        String routeId = routeId(exchange);
        if (!config.includes(routeId) || !"kafka".equals(scheme(endpointUri))) {
            return;
        }

        String existingMessageId = headerText(exchange, PlatformHeaders.MESSAGE_ID);
        if (hasText(existingMessageId)) {
            return;
        }

        SourceIdentity sourceIdentity = resolveSourceIdentity(exchange);
        if (sourceIdentity == null) {
            throw new IngressNormalizationException(
                    "Ingress normalization route '" + routeId + "' cannot identify the source event. "
                            + "Set header " + PlatformHeaders.SOURCE_EVENT_ID
                            + ", preserve " + PlatformHeaders.MESSAGE_ID
                            + ", or use a file/JMS source with stable source metadata.");
        }

        exchange.getMessage().setHeader(PlatformHeaders.SOURCE_EVENT_ID, sourceIdentity.headerValue());
        exchange.getMessage().setHeader(
                PlatformHeaders.MESSAGE_ID,
                deterministicUuid(integrationName + "\n" + routeId + "\n" + sourceIdentity.canonicalValue()).toString());
        if (!hasText(headerText(exchange, PlatformHeaders.SCHEMA_VERSION))) {
            exchange.getMessage().setHeader(PlatformHeaders.SCHEMA_VERSION, "1");
        }
    }

    private SourceIdentity resolveSourceIdentity(Exchange exchange) {
        String explicit = headerText(exchange, PlatformHeaders.SOURCE_EVENT_ID);
        if (hasText(explicit)) {
            return new SourceIdentity("explicit:" + explicit, explicit);
        }

        String jmsMessageId = firstHeader(exchange, JMS_MESSAGE_ID, CAMEL_JMS_MESSAGE_ID);
        if (hasText(jmsMessageId)) {
            return new SourceIdentity("jms:" + jmsMessageId, jmsMessageId);
        }

        String filePath = firstHeader(exchange, FILE_RELATIVE_PATH, FILE_PATH, FILE_NAME);
        if (hasText(filePath)) {
            String identity = "file:" + filePath
                    + "|length=" + headerText(exchange, FILE_LENGTH)
                    + "|modified=" + headerText(exchange, FILE_LAST_MODIFIED);
            return new SourceIdentity(identity, identity);
        }
        return null;
    }

    private String routeId(Exchange exchange) {
        String routeId = exchange.getFromRouteId();
        if (hasText(routeId)) {
            return routeId;
        }
        return exchange.getProperty(Exchange.INTERCEPTED_ROUTE_ID, String.class);
    }

    private String firstHeader(Exchange exchange, String... names) {
        for (String name : names) {
            String value = headerText(exchange, name);
            if (hasText(value)) {
                return value;
            }
        }
        return null;
    }

    private String headerText(Exchange exchange, String name) {
        Object value = exchange.getMessage().getHeader(name);
        return value == null ? null : value.toString();
    }

    private UUID deterministicUuid(String identity) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(identity.getBytes(StandardCharsets.UTF_8));
            digest[6] = (byte) ((digest[6] & 0x0f) | 0x50);
            digest[8] = (byte) ((digest[8] & 0x3f) | 0x80);
            ByteBuffer bytes = ByteBuffer.wrap(digest);
            return new UUID(bytes.getLong(), bytes.getLong());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private String scheme(String endpointUri) {
        if (endpointUri == null) {
            return "";
        }
        int separator = endpointUri.indexOf(':');
        return (separator < 0 ? endpointUri : endpointUri.substring(0, separator)).toLowerCase();
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }

    private record SourceIdentity(String canonicalValue, String headerValue) {
    }
}
