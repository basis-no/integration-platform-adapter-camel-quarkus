package no.basis.integrationplatform.camel.step;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.EndpointUriMasking;
import no.basis.integrationplatform.camel.LogText;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.ingress.IngressMessageIdentity;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.Exchange;
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
import java.util.concurrent.ConcurrentHashMap;

/** Default replay-aware tracker used by the endpoint interception strategy. */
@ApplicationScoped
public class DefaultStepTracker implements StepTracker {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStepTracker.class);

    // Text after the array makes the list malformed: it is not the replay service's.
    private static final ObjectMapper REDO_STEPS_READER = new ObjectMapper()
            .enable(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    /**
     * The exchange properties that describe the send in progress: {@link #beforeSend} sets
     * them and the outcome is recorded from them.
     */
    static final List<String> CURRENT_STEP_PROPERTIES = List.of(
            StepVisits.REPEATED_IN,
            PlatformExchangeProperties.CURRENT_STEP_ID,
            PlatformExchangeProperties.CURRENT_IDEMPOTENCY_KEY,
            PlatformExchangeProperties.CURRENT_ENDPOINT_URI,
            PlatformExchangeProperties.STEP_SHOULD_SKIP,
            PlatformExchangeProperties.STEP_ALREADY_DELIVERED,
            PlatformExchangeProperties.STEP_TRACKING_SUPPRESSED,
            PlatformExchangeProperties.CURRENT_STEP_ROLE,
            PlatformExchangeProperties.CURRENT_STEP_CHECKPOINT);

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
        // Before any send, so that no send carries this attempt's redo list.
        Set<String> redoSteps = captureRedoSteps(exchange);
        if (ingressMessageIdentity != null) {
            ingressMessageIdentity.prepareKafkaPublish(exchange, endpointUri);
        }
        clearInjectedIdempotencyHeaders(exchange);
        clearCurrentStep(exchange);
        // The journal keeps the endpoint without credentials: a resolved password or token in
        // the URI must never reach the Message Store. Step ids still derive from the exact URI,
        // so existing step identities are unchanged.
        exchange.setProperty(PlatformExchangeProperties.CURRENT_ENDPOINT_URI, EndpointUriMasking.mask(endpointUri));

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
        if (shouldSkip(exchange) || stepTrackingSuppressed(exchange)) {
            return;
        }
        StepResult step = StepResult.delivered(currentStepId(exchange), currentIdempotencyKey(exchange), currentEndpointUri(exchange));
        steps(exchange).add(step);
        StepVisits.of(exchange).sent(StepVisits.path(exchange), step.idempotencyKey());
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
            String repeatedIn = exchange.getProperty(StepVisits.REPEATED_IN, String.class);
            if (repeatedIn != null) {
                // Names the route, step and scope only: no message content and no header values.
                LOG.error("Step {} was skipped in a {} of input route {}, so this part was not delivered: another part "
                                + "of the same attempt already sent this step key. Split parts, loop "
                                + "iterations and multicast and recipient list branches keep the input's "
                                + PlatformHeaders.MESSAGE_ID + ", so they share one step key. Give each part its own "
                                + PlatformHeaders.MESSAGE_ID + " before the send, or publish each part with its own id. "
                                + "See docs/steps-and-idempotency.md#splitting-before-a-side-effect-send",
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
        CURRENT_STEP_PROPERTIES.forEach(exchange::removeProperty);
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
        LOG.warn("Endpoint {} has no stable Camel node id; using generated step id {}", EndpointUriMasking.mask(endpointUri), fallback);
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
