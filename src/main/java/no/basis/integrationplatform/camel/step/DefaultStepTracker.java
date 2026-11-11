package no.basis.integrationplatform.camel.step;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.LogText;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.Exchange;
import org.apache.camel.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@ApplicationScoped
public class DefaultStepTracker implements StepTracker {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStepTracker.class);

    private static final Set<String> DEFAULT_INTERNAL_SCHEMES = Set.of(
            "bean", "controlbus", "dataformat", "direct", "language", "log", "mock",
            "micrometer", "seda", "stub", "timer", "validator", "vm", "xslt", "xslt-saxon");

    // Text after the array makes the list malformed: it is not the replay service's.
    private static final ObjectMapper REDO_STEPS_READER = new ObjectMapper()
            .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private final StepOutcomeStore stepOutcomeStore;

    @Inject
    public DefaultStepTracker(StepOutcomeStore stepOutcomeStore) {
        this.stepOutcomeStore = stepOutcomeStore;
    }

    @Override
    public void beforeSend(Exchange exchange) {
        String endpointUri = exchange.getProperty(Exchange.INTERCEPTED_ENDPOINT, String.class);
        // Before any send, so that no send carries this attempt's redo list.
        Set<String> redoSteps = captureRedoSteps(exchange);
        clearCurrentStep(exchange);
        // The journal keeps the endpoint without credentials: a resolved password or
        // token in the URI must never reach the Message Store. Step ids still derive
        // from the exact URI, so existing step identities are unchanged.
        exchange.setProperty(PlatformExchangeProperties.CURRENT_ENDPOINT_URI, URISupport.sanitizeUri(endpointUri));

        if (isInternalEndpoint(endpointUri)) {
            exchange.setProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, true);
            exchange.setProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, false);
            return;
        }

        String stepId = resolveStepId(exchange, endpointUri);
        String messageId = ensureMessageId(exchange);
        String idempotencyKey = messageId + ":" + stepId;
        boolean alreadyDelivered = stepOutcomeStore.wasDelivered(idempotencyKey);

        // A step listed in x-bip-redo-steps is sent again although the ledger has it DELIVERED,
        // but only for the adapter's own replay, confirmed by the store once it matters.
        boolean listed = redoSteps.contains(stepId) || redoSteps.contains(idempotencyKey);
        boolean redoRequested = listed && alreadyDelivered && redoAuthorised(exchange);
        boolean skip = alreadyDelivered && !redoRequested;
        if (redoRequested) {
            LOG.info("Step {} was already DELIVERED but is listed in {}; re-executing", LogText.escape(stepId),
                    PlatformHeaders.REDO_STEPS);
        }

        exchange.setProperty(PlatformExchangeProperties.CURRENT_STEP_ID, stepId);
        exchange.setProperty(PlatformExchangeProperties.CURRENT_IDEMPOTENCY_KEY, idempotencyKey);
        exchange.setProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, skip);
        exchange.setProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, skip);
        exchange.getMessage().setHeader(PlatformHeaders.IDEMPOTENCY_KEY, idempotencyKey);
        exchange.getMessage().setHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY, idempotencyKey);
        // Another part of this attempt with the same message id may have sent the key. This
        // only changes how the skip is logged.
        String repeatedIn = skip ? StepVisits.of(exchange).repeatedIn(StepVisits.path(exchange), idempotencyKey) : null;
        if (repeatedIn != null) {
            exchange.setProperty(StepVisits.REPEATED_IN, repeatedIn);
        }
    }

    /**
     * Removes {@code x-bip-redo-steps} from the exchange and returns the attempt's redo list.
     * The list belongs to this integration's replayed attempt: a receiver or a downstream
     * integration that got it would send its own steps with the same ids again for the same
     * message id. With the adapter installed, every route removes it when an exchange enters
     * the route; a tracked send and DLQ parking remove it again in case a route restored it.
     *
     * <p>Only a managed Kafka input's record may carry a list, read by
     * {@link #acceptRedoSteps}. On any other exchange, such as an HTTP request or a timer's,
     * the header is dropped with a warning and the list is empty.</p>
     *
     * @param exchange the exchange
     * @return the step ids to send again in this attempt
     */
    @SuppressWarnings("unchecked")
    public static Set<String> captureRedoSteps(Exchange exchange) {
        Object header = exchange.getMessage().removeHeader(PlatformHeaders.REDO_STEPS);
        Set<String> cached = exchange.getProperty(PlatformExchangeProperties.REDO_STEPS, Set.class);
        if (cached != null) {
            return cached;
        }
        String value = redoStepsHeaderValue(header);
        if (value != null && !value.isBlank()) {
            LOG.warn("Ignoring {} on an exchange that is not a managed Kafka input: only a replay the adapter "
                    + "published may carry it", PlatformHeaders.REDO_STEPS);
        }
        exchange.setProperty(PlatformExchangeProperties.REDO_STEPS, Set.of());
        return Set.of();
    }

    /**
     * Reads a managed Kafka input's {@code x-bip-redo-steps} into the exchange once and removes
     * the header. The list is kept only on a record marked {@code x-bip-replay: true} while
     * replay is enabled; otherwise it is dropped with a warning. A kept list is honoured only
     * when the Message Store confirms that the record is the replay the adapter published for
     * this message, integration, topic and attempt, and that attempt has no outcome yet (see
     * {@link RedoClaim}).
     *
     * @param exchange the input exchange, when its route begins
     * @param messageId the record's message id
     * @param integration this integration's name
     * @param topic the topic the record was consumed from
     * @param attempt the record's attempt
     * @param replayEnabled whether this integration accepts replays
     */
    public static void acceptRedoSteps(Exchange exchange, String messageId, String integration, String topic,
            int attempt, boolean replayEnabled) {
        String value = redoStepsHeaderValue(exchange.getMessage().removeHeader(PlatformHeaders.REDO_STEPS));
        if (exchange.getProperty(PlatformExchangeProperties.REDO_STEPS) != null) {
            // Read already, for example by a second policy on the route.
            return;
        }
        Set<String> redoSteps = parseRedoSteps(value);
        String ignored = null;
        if (redoSteps.isEmpty()) {
            // Nothing to honour or to warn about.
        } else if (!replayEnabled) {
            ignored = "replay is disabled for this integration";
        } else if (!"true".equalsIgnoreCase(String.valueOf(redoStepsHeaderValue(
                exchange.getMessage().getHeader(PlatformHeaders.REPLAY))).trim())) {
            ignored = "the record is not marked as a replay";
        } else if (topic == null || messageId == null) {
            ignored = "the record's topic or message id is unknown";
        }
        if (ignored != null) {
            LOG.warn("Ignoring {} on message {}: {}", PlatformHeaders.REDO_STEPS, LogText.escape(messageId), ignored);
            redoSteps = Set.of();
        }
        exchange.setProperty(PlatformExchangeProperties.REDO_STEPS, redoSteps);
        exchange.removeProperty(PlatformExchangeProperties.REDO_CLAIM);
        if (!redoSteps.isEmpty()) {
            exchange.setProperty(PlatformExchangeProperties.REDO_CLAIM,
                    new RedoClaim(messageId, integration, topic, attempt, value.trim()));
        }
    }

    /**
     * Whether the store confirms the record's redo claim. Asked once per attempt, when a
     * delivered step is listed. A store that cannot be read fails the send as any step outcome
     * check does, so the record is read again; a login that may not read the replay audit gets
     * no redo.
     */
    private boolean redoAuthorised(Exchange exchange) {
        RedoClaim claim = exchange.getProperty(PlatformExchangeProperties.REDO_CLAIM, RedoClaim.class);
        if (claim == null) {
            return false;
        }
        Boolean verdict = claim.verdict();
        if (verdict != null) {
            return verdict;
        }
        boolean authorised;
        try {
            authorised = stepOutcomeStore.isAuthorisedReplay(claim);
        } catch (RuntimeException failure) {
            String sqlState = sqlState(failure);
            if ("42501".equals(sqlState)) {
                LOG.warn("Ignoring {} on message {}: the Message Store login may not read the replay audit, so the "
                        + "replay cannot be confirmed", PlatformHeaders.REDO_STEPS, LogText.escape(claim.messageId()));
            } else if (sqlState != null && sqlState.startsWith("22")) {
                // The record's own values, such as its message id, are not valid for the store: it
                // cannot be the adapter's replay, and reading it again would not change that.
                LOG.warn("Ignoring {} on message {}: the Message Store cannot read the record's values (SQLSTATE {})",
                        PlatformHeaders.REDO_STEPS, LogText.escape(claim.messageId()), sqlState);
            } else {
                throw failure;
            }
            claim.decide(false);
            return false;
        }
        if (!authorised) {
            LOG.warn("Ignoring {} on message {}: it is not a replay the adapter published for this integration, topic "
                            + "and attempt {}, or that attempt already has an outcome", PlatformHeaders.REDO_STEPS,
                    LogText.escape(claim.messageId()), claim.attempt());
        }
        claim.decide(authorised);
        return authorised;
    }

    private static String sqlState(Throwable failure) {
        int depth = 0;
        for (Throwable cause = failure; cause != null && depth++ < 20; cause = cause.getCause() == cause ? null : cause.getCause()) {
            if (cause instanceof SQLException sql && sql.getSQLState() != null) {
                return sql.getSQLState();
            }
        }
        return null;
    }

    private static String redoStepsHeaderValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value.toString();
    }

    /**
     * {@code x-bip-redo-steps} is a JSON array of step
     * ids. A malformed value is logged as a warning and treated as empty; it must
     * never fail message processing. The warning names the header only: the value
     * comes from the message, and the parser's error would repeat it.
     */
    private static Set<String> parseRedoSteps(String headerValue) {
        if (headerValue == null || headerValue.isBlank()) {
            return Set.of();
        }
        try {
            JsonNode root = REDO_STEPS_READER.readTree(headerValue);
            if (root == null || !root.isArray()) {
                LOG.warn("Malformed {} header (expected a JSON array of step ids); treating it as no redo",
                        PlatformHeaders.REDO_STEPS);
                return Set.of();
            }
            Set<String> redoSteps = new LinkedHashSet<>();
            for (JsonNode item : root) {
                if (item.isTextual() && !item.asText().isBlank()) {
                    redoSteps.add(item.asText());
                } else {
                    LOG.warn("Ignoring an entry of the {} header that is not a step id", PlatformHeaders.REDO_STEPS);
                }
            }
            return Set.copyOf(redoSteps);
        } catch (Exception e) {
            LOG.warn("Malformed {} header (expected a JSON array of step ids); treating it as no redo",
                    PlatformHeaders.REDO_STEPS);
            return Set.of();
        }
    }

    @Override
    public void recordDelivered(Exchange exchange) {
        if (shouldSkip(exchange)) {
            return;
        }
        StepResult step = StepResult.delivered(currentStepId(exchange), currentIdempotencyKey(exchange), currentEndpointUri(exchange));
        steps(exchange).add(step);
        StepVisits.of(exchange).sent(StepVisits.path(exchange), step.idempotencyKey());
        stepOutcomeStore.recordDelivered(step);
    }

    @Override
    public void recordSkipped(Exchange exchange) {
        String stepId = currentStepId(exchange);
        if (stepId != null) {
            steps(exchange).add(StepResult.skipped(stepId, currentIdempotencyKey(exchange), currentEndpointUri(exchange)));
            String repeatedIn = exchange.getProperty(StepVisits.REPEATED_IN, String.class);
            if (repeatedIn != null) {
                // Names the route, step and scope only: no message content and no header values.
                LOG.error("Step {} was skipped in a {} of input route {}, so this part was not delivered: another part "
                                + "of the same attempt already sent this step key. In 0.3.x split parts, loop "
                                + "iterations and multicast and recipient list branches keep the input's "
                                + PlatformHeaders.MESSAGE_ID + ", so they share one step key. Give each part its own "
                                + PlatformHeaders.MESSAGE_ID + " before the send, or publish each part with its own id. "
                                + "See docs/split-step-identity.md",
                        LogText.escape(stepId), part(repeatedIn), exchange.getFromRouteId());
                return;
            }
            // A replayed message otherwise looks delivered in the logs although nothing was sent.
            LOG.info("Step {} SKIPPED for message {}: an earlier attempt recorded it as delivered, so the send to {} "
                            + "was not repeated in this attempt; list the step in {} to repeat it",
                    LogText.escape(stepId), LogText.escape(exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID)),
                    LogText.escape(currentEndpointUri(exchange)), PlatformHeaders.REDO_STEPS);
        }
    }

    @Override
    public void recordFailedAndRethrow(Exchange exchange) throws Exception {
        Throwable failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        recordFailed(exchange);

        if (failure instanceof Exception exception) {
            throw exception;
        }
        if (failure != null) {
            throw new RuntimeException(failure);
        }
        throw new IllegalStateException("Endpoint send failed without an exception on the exchange");
    }

    @Override
    public void recordFailed(Exchange exchange) {
        if (shouldSkip(exchange)) {
            return;
        }
        Throwable failure = exchange.getException();
        if (failure == null) {
            failure = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        }
        StepResult step = StepResult.failed(currentStepId(exchange), currentIdempotencyKey(exchange), currentEndpointUri(exchange), failure);
        steps(exchange).add(step);
        stepOutcomeStore.recordFailed(step);
    }

    private static String part(String scope) {
        return switch (scope) {
            case StepVisits.SPLIT -> "split part";
            case StepVisits.LOOP -> "loop iteration";
            case StepVisits.MULTICAST -> "multicast branch";
            case StepVisits.RECIPIENT_LIST -> "recipient list branch";
            default -> scope;
        };
    }

    private void clearCurrentStep(Exchange exchange) {
        exchange.removeProperty(StepVisits.REPEATED_IN);
        exchange.removeProperty(PlatformExchangeProperties.CURRENT_STEP_ID);
        exchange.removeProperty(PlatformExchangeProperties.CURRENT_IDEMPOTENCY_KEY);
        exchange.removeProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP);
        exchange.removeProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED);
    }

    private boolean shouldSkip(Exchange exchange) {
        return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class));
    }

    private String resolveStepId(Exchange exchange, String endpointUri) {
        String nodeId = exchange.getProperty(Exchange.INTERCEPTED_NODE_ID, String.class);
        if (hasText(nodeId)) {
            return nodeId;
        }

        String routeId = exchange.getFromRouteId() == null ? "unknown-route" : exchange.getFromRouteId();
        String fallback = routeId + ":" + scheme(endpointUri) + ":" + shortHash(endpointUri);
        LOG.warn("Endpoint {} has no stable Camel node id; using generated step id {}", URISupport.sanitizeUri(endpointUri), fallback);
        return fallback;
    }

    private String ensureMessageId(Exchange exchange) {
        String messageId = exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID, String.class);
        if (hasText(messageId)) {
            return messageId;
        }

        messageId = UUID.randomUUID().toString();
        exchange.getMessage().setHeader(PlatformHeaders.MESSAGE_ID, messageId);
        LOG.warn("Message missing {}; generated {}", PlatformHeaders.MESSAGE_ID, messageId);
        return messageId;
    }

    @SuppressWarnings("unchecked")
    private List<StepResult> steps(Exchange exchange) {
        List<StepResult> steps = exchange.getProperty(PlatformExchangeProperties.STEPS, List.class);
        if (steps == null) {
            steps = new ArrayList<>();
            exchange.setProperty(PlatformExchangeProperties.STEPS, steps);
        }
        return steps;
    }

    private boolean isInternalEndpoint(String endpointUri) {
        return isInternal(endpointUri);
    }

    /** Whether a send to this endpoint is internal and never tracked as a step. */
    static boolean isInternal(String endpointUri) {
        if (endpointUri == null) {
            return DEFAULT_INTERNAL_SCHEMES.contains("unknown");
        }
        int colon = endpointUri.indexOf(':');
        return DEFAULT_INTERNAL_SCHEMES.contains(colon < 0 ? endpointUri : endpointUri.substring(0, colon));
    }

    private String currentStepId(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.CURRENT_STEP_ID, String.class);
    }

    private String currentIdempotencyKey(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.CURRENT_IDEMPOTENCY_KEY, String.class);
    }

    private String currentEndpointUri(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.CURRENT_ENDPOINT_URI, String.class);
    }

    private String scheme(String endpointUri) {
        if (endpointUri == null) {
            return "unknown";
        }
        int colon = endpointUri.indexOf(':');
        return colon < 0 ? endpointUri : endpointUri.substring(0, colon);
    }

    private String shortHash(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 6);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }

    private boolean hasText(String value) {
        return value != null && !value.isBlank();
    }
}
