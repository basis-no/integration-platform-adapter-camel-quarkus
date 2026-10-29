package no.basis.integrationplatform.camel.step;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.ingress.IngressMessageIdentity;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.Exchange;
import org.apache.camel.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/** Default replay-aware tracker used by the endpoint interception strategy. */
@ApplicationScoped
public class DefaultStepTracker implements StepTracker {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStepTracker.class);

    private static final ObjectMapper REDO_STEPS_READER = new ObjectMapper();

    private final StepOutcomeStore stepOutcomeStore;
    private final StepClassifier stepClassifier;
    private final StepOutputCheckpointService checkpointService;
    private final IngressMessageIdentity ingressMessageIdentity;
    private final Set<String> warnedAmbiguousEndpoints = ConcurrentHashMap.newKeySet();

    /**
     * Creates the CDI tracker with durable storage, classification, and checkpoint services.
     *
     * @param stepOutcomeStore durable step outcome store
     * @param stepClassifier replay-semantic endpoint classifier
     * @param checkpointService output checkpoint service
     * @param ingressMessageIdentity non-Kafka ingress identity helper
     */
    @Inject
    public DefaultStepTracker(
            StepOutcomeStore stepOutcomeStore,
            StepClassifier stepClassifier,
            StepOutputCheckpointService checkpointService,
            IngressMessageIdentity ingressMessageIdentity) {
        this.stepOutcomeStore = stepOutcomeStore;
        this.stepClassifier = stepClassifier;
        this.checkpointService = checkpointService;
        this.ingressMessageIdentity = ingressMessageIdentity;
    }

    /**
     * Creates a tracker without output-checkpoint or ingress-normalization support.
     *
     * @param stepOutcomeStore step outcome store
     * @param stepClassifier replay-semantic endpoint classifier
     */
    public DefaultStepTracker(StepOutcomeStore stepOutcomeStore, StepClassifier stepClassifier) {
        this(stepOutcomeStore, stepClassifier, null, null);
    }

    /**
     * Creates a tracker with output checkpoints but without ingress normalization.
     *
     * @param stepOutcomeStore step outcome store
     * @param stepClassifier replay-semantic endpoint classifier
     * @param checkpointService output checkpoint service
     */
    public DefaultStepTracker(
            StepOutcomeStore stepOutcomeStore,
            StepClassifier stepClassifier,
            StepOutputCheckpointService checkpointService) {
        this(stepOutcomeStore, stepClassifier, checkpointService, null);
    }

    /**
     * Creates a tracker using the default endpoint classifier.
     *
     * @param stepOutcomeStore step outcome store
     */
    public DefaultStepTracker(StepOutcomeStore stepOutcomeStore) {
        this(stepOutcomeStore, DefaultStepClassifier.defaults());
    }

    @Override
    public void beforeSend(Exchange exchange) {
        String endpointUri = exchange.getProperty(Exchange.INTERCEPTED_ENDPOINT, String.class);
        if (ingressMessageIdentity != null) {
            ingressMessageIdentity.prepareKafkaPublish(exchange, endpointUri);
        }
        clearInjectedIdempotencyHeaders(exchange);
        clearCurrentStep(exchange);
        // The journal keeps the endpoint without credentials: a resolved password or token in
        // the URI must never reach the Message Store. Step ids still derive from the exact URI,
        // so existing step identities are unchanged.
        exchange.setProperty(PlatformExchangeProperties.CURRENT_ENDPOINT_URI, URISupport.sanitizeUri(endpointUri));

        if (recordingSuppressed(exchange)) {
            exchange.setProperty(PlatformExchangeProperties.STEP_TRACKING_SUPPRESSED, true);
            return;
        }

        String nodeId = exchange.getProperty(Exchange.INTERCEPTED_NODE_ID, String.class);
        StepClassificationContext context = new StepClassificationContext(endpointUri, nodeId, exchange);
        StepRole role = stepClassifier.classify(context);
        exchange.setProperty(PlatformExchangeProperties.CURRENT_STEP_ROLE, role.name());

        if (role != StepRole.SIDE_EFFECT && role != StepRole.OUTPUT_SIDE_EFFECT) {
            suppressStepTracking(exchange);
            if (role == StepRole.AMBIGUOUS) {
                warnAmbiguous(context);
            }
            return;
        }

        String stepId = resolveStepId(exchange, endpointUri);
        String messageId = ensureMessageId(exchange);
        String idempotencyKey = messageId + ":" + stepId;
        if (role == StepRole.OUTPUT_SIDE_EFFECT) {
            if (!stepOutcomeStore.supportsOutputCheckpoints()) {
                throw new StepOutputCheckpointException(
                        "Output-producing step '" + stepId
                                + "' requires a durable checkpoint-capable step outcome store; "
                                + "configure basis.message-store.step-outcome-store=postgres");
            }
            requiredCheckpointService(stepId).ensureCaptureAvailable(stepId);
        }
        boolean alreadyDelivered = stepOutcomeStore.wasDelivered(idempotencyKey);

        // Contract §9 (DECISION D1): steps listed in x-bip-redo-steps MUST be
        // re-executed even when the durable ledger already has them DELIVERED.
        Set<String> redoSteps = redoSteps(exchange);
        boolean redoRequested = redoSteps.contains(stepId) || redoSteps.contains(idempotencyKey);
        boolean skip = alreadyDelivered && !redoRequested;
        if (alreadyDelivered && redoRequested) {
            LOG.info("Step {} was already DELIVERED but is listed in {}; re-executing", stepId, PlatformHeaders.REDO_STEPS);
        }

        exchange.setProperty(PlatformExchangeProperties.CURRENT_STEP_ID, stepId);
        exchange.setProperty(PlatformExchangeProperties.CURRENT_IDEMPOTENCY_KEY, idempotencyKey);
        exchange.setProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, skip);
        exchange.setProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, skip);
        if (skip && role == StepRole.OUTPUT_SIDE_EFFECT) {
            StepOutputCheckpoint checkpoint = stepOutcomeStore.readCheckpoint(idempotencyKey)
                    .orElseThrow(() -> new StepOutputCheckpointException(
                            "Cannot safely replay output-producing step '" + stepId
                                    + "': its DELIVERED ledger entry has no output checkpoint. "
                                    + "Restore the checkpoint or request explicit redo only when the receiver "
                                    + "honors its stable idempotency key."));
            exchange.setProperty(PlatformExchangeProperties.CURRENT_STEP_CHECKPOINT, checkpoint);
        }
        exchange.getMessage().setHeader(PlatformHeaders.IDEMPOTENCY_KEY, idempotencyKey);
        exchange.getMessage().setHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY, idempotencyKey);
        exchange.setProperty(PlatformExchangeProperties.INJECTED_IDEMPOTENCY_KEY, idempotencyKey);
    }

    @SuppressWarnings("unchecked")
    private Set<String> redoSteps(Exchange exchange) {
        Set<String> cached = exchange.getProperty(PlatformExchangeProperties.REDO_STEPS, Set.class);
        if (cached != null) {
            return cached;
        }
        Set<String> redoSteps = parseRedoSteps(redoStepsHeaderValue(exchange));
        exchange.setProperty(PlatformExchangeProperties.REDO_STEPS, redoSteps);
        return redoSteps;
    }

    private String redoStepsHeaderValue(Exchange exchange) {
        Object value = exchange.getMessage().getHeader(PlatformHeaders.REDO_STEPS);
        if (value == null) {
            return null;
        }
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return value.toString();
    }

    /**
     * Contract §3 (DECISION D1): {@code x-bip-redo-steps} is a JSON array of step
     * ids. A malformed value is logged as a warning and treated as empty; it must
     * never fail message processing.
     */
    private Set<String> parseRedoSteps(String headerValue) {
        if (headerValue == null || headerValue.isBlank()) {
            return Set.of();
        }
        try {
            JsonNode root = REDO_STEPS_READER.readTree(headerValue);
            if (root == null || !root.isArray()) {
                LOG.warn("Malformed {} header (expected JSON array of step ids, got '{}'); treating as no redo",
                        PlatformHeaders.REDO_STEPS, headerValue);
                return Set.of();
            }
            Set<String> redoSteps = new LinkedHashSet<>();
            for (JsonNode item : root) {
                if (item.isTextual() && !item.asText().isBlank()) {
                    redoSteps.add(item.asText());
                } else {
                    LOG.warn("Ignoring non-string entry '{}' in {} header", item, PlatformHeaders.REDO_STEPS);
                }
            }
            return Set.copyOf(redoSteps);
        } catch (Exception e) {
            LOG.warn("Malformed {} header (expected JSON array of step ids, got '{}'); treating as no redo",
                    PlatformHeaders.REDO_STEPS, headerValue, e);
            return Set.of();
        }
    }

    @Override
    public void recordDelivered(Exchange exchange) {
        if (shouldSkip(exchange) || stepTrackingSuppressed(exchange)) {
            return;
        }
        StepResult step = StepResult.delivered(currentStepId(exchange), currentIdempotencyKey(exchange), currentEndpointUri(exchange));
        steps(exchange).add(step);
        if (currentStepRole(exchange) == StepRole.OUTPUT_SIDE_EFFECT) {
            String messageId = ensureMessageId(exchange);
            StepOutputCheckpoint checkpoint = requiredCheckpointService(step.step())
                    .capture(exchange, messageId, step.step());
            stepOutcomeStore.recordDelivered(step, checkpoint);
        } else {
            stepOutcomeStore.recordDelivered(step);
        }
    }

    @Override
    public void restoreSkippedOutput(Exchange exchange) {
        StepOutputCheckpoint checkpoint = exchange.getProperty(
                PlatformExchangeProperties.CURRENT_STEP_CHECKPOINT, StepOutputCheckpoint.class);
        if (checkpoint != null) {
            requiredCheckpointService(currentStepId(exchange)).restore(exchange, currentStepId(exchange), checkpoint);
        }
    }

    @Override
    public void recordSkipped(Exchange exchange) {
        if (stepTrackingSuppressed(exchange)) {
            return;
        }
        String stepId = currentStepId(exchange);
        if (stepId != null) {
            steps(exchange).add(StepResult.skipped(stepId, currentIdempotencyKey(exchange), currentEndpointUri(exchange)));
            // A replayed message otherwise looks delivered in the logs although nothing was sent.
            LOG.info("Step {} SKIPPED for message {}: an earlier attempt recorded it as delivered, so the send to {} "
                            + "was not repeated in this attempt; list the step in {} to repeat it",
                    stepId, exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID),
                    currentEndpointUri(exchange), PlatformHeaders.REDO_STEPS);
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
        if (shouldSkip(exchange) || stepTrackingSuppressed(exchange)) {
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

    private void clearCurrentStep(Exchange exchange) {
        exchange.removeProperty(PlatformExchangeProperties.CURRENT_STEP_ID);
        exchange.removeProperty(PlatformExchangeProperties.CURRENT_IDEMPOTENCY_KEY);
        exchange.removeProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP);
        exchange.removeProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED);
        exchange.removeProperty(PlatformExchangeProperties.STEP_TRACKING_SUPPRESSED);
        exchange.removeProperty(PlatformExchangeProperties.CURRENT_STEP_ROLE);
        exchange.removeProperty(PlatformExchangeProperties.CURRENT_STEP_CHECKPOINT);
    }

    private boolean shouldSkip(Exchange exchange) {
        return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, Boolean.class));
    }

    private boolean recordingSuppressed(Exchange exchange) {
        Boolean recordMessage = exchange.getProperty(PlatformExchangeProperties.RECORD_MESSAGE, Boolean.class);
        return Boolean.FALSE.equals(recordMessage);
    }

    private boolean stepTrackingSuppressed(Exchange exchange) {
        return Boolean.TRUE.equals(exchange.getProperty(PlatformExchangeProperties.STEP_TRACKING_SUPPRESSED, Boolean.class));
    }

    private void clearInjectedIdempotencyHeaders(Exchange exchange) {
        String injectedKey = exchange.getProperty(PlatformExchangeProperties.INJECTED_IDEMPOTENCY_KEY, String.class);
        if (injectedKey == null) {
            return;
        }
        if (injectedKey.equals(exchange.getMessage().getHeader(PlatformHeaders.IDEMPOTENCY_KEY))) {
            exchange.getMessage().removeHeader(PlatformHeaders.IDEMPOTENCY_KEY);
        }
        if (injectedKey.equals(exchange.getMessage().getHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY))) {
            exchange.getMessage().removeHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY);
        }
        exchange.removeProperty(PlatformExchangeProperties.INJECTED_IDEMPOTENCY_KEY);
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

    private void suppressStepTracking(Exchange exchange) {
        exchange.setProperty(PlatformExchangeProperties.STEP_TRACKING_SUPPRESSED, true);
        exchange.setProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, false);
        exchange.setProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, false);
    }

    private void warnAmbiguous(StepClassificationContext context) {
        String nodeId = hasText(context.nodeId()) ? context.nodeId() : "<missing>";
        String warningKey = nodeId + ":" + context.scheme();
        if (warnedAmbiguousEndpoints.add(warningKey)) {
            LOG.warn("Endpoint send node {} with scheme {} has ambiguous replay semantics; "
                            + "it will execute on every replay and is not protected by the step ledger. "
                            + "Classify the node with basis.message-store.step.internal-ids, "
                            + "basis.message-store.step.repeatable-ids, or "
                            + "basis.message-store.step.side-effect-ids/output-side-effect-ids",
                    nodeId, context.scheme());
        }
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

    private StepRole currentStepRole(Exchange exchange) {
        String role = exchange.getProperty(PlatformExchangeProperties.CURRENT_STEP_ROLE, String.class);
        return role == null ? null : StepRole.valueOf(role);
    }

    private StepOutputCheckpointService requiredCheckpointService(String stepId) {
        if (checkpointService == null) {
            throw new StepOutputCheckpointException(
                    "Output-producing step '" + stepId + "' requires output checkpoint support");
        }
        return checkpointService;
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
