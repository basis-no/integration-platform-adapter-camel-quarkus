package no.basis.integrationplatform.camel.step;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.EndpointUriMasking;
import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.PlatformHeaders;
import no.basis.integrationplatform.camel.StoreErrorClass;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.Exchange;
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
import java.util.concurrent.TimeUnit;

@ApplicationScoped
public class DefaultStepTracker implements StepTracker {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultStepTracker.class);

    private static final Set<String> DEFAULT_INTERNAL_SCHEMES = Set.of(
            "bean", "controlbus", "dataformat", "direct", "language", "log", "mock",
            "micrometer", "seda", "stub", "timer", "validator", "vm", "xslt", "xslt-saxon");

    private static final ObjectMapper REDO_STEPS_READER = new ObjectMapper();

    // A DELIVERED write that fails is tried again in place: after these waits, but no try starts
    // more than RETRY_IN_PLACE_FOR_MS after the first failure.
    static final long[] RETRY_IN_PLACE_WAITS_MS = {500, 1_000, 2_000};
    static final long RETRY_IN_PLACE_FOR_MS = 10_000;

    /** Waits before a write is tried again; {@code false} when the wait was interrupted. */
    @FunctionalInterface
    interface RetrySleeper {
        boolean sleep(long millis);
    }

    private RetrySleeper retrySleeper = millis -> {
        try {
            Thread.sleep(millis);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    };

    void setRetrySleeper(RetrySleeper retrySleeper) {
        this.retrySleeper = retrySleeper;
    }

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
            PlatformExchangeProperties.STEP_FROM_MARK);

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
        exchange.setProperty(PlatformExchangeProperties.CURRENT_ENDPOINT_URI, EndpointUriMasking.mask(endpointUri));

        if (isInternalEndpoint(endpointUri)) {
            exchange.setProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, true);
            exchange.setProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, false);
            return;
        }

        String stepId = resolveStepId(exchange, endpointUri);
        String messageId = ensureMessageId(exchange);
        String idempotencyKey = messageId + ":" + stepId;
        UnrecordedDeliveries.Mark mark = UnrecordedDeliveries.find(exchange, idempotencyKey);
        if (mark != null) {
            writeFromMark(exchange, mark, idempotencyKey);
            return;
        }
        boolean alreadyDelivered = stepOutcomeStore.wasDelivered(idempotencyKey);

        // Steps listed in x-bip-redo-steps MUST be
        // re-executed even when the durable ledger already has them DELIVERED.
        boolean redoRequested = redoSteps.contains(stepId) || redoSteps.contains(idempotencyKey);
        boolean skip = alreadyDelivered && !redoRequested;
        if (alreadyDelivered && redoRequested) {
            LOG.info("Step {} was already DELIVERED but is listed in {}; re-executing", stepId, PlatformHeaders.REDO_STEPS);
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
     * Reads a replay's {@code x-bip-redo-steps} into the exchange once and removes the header.
     * The list belongs to this integration's replayed attempt: a receiver or a downstream
     * integration that got it would send its own steps with the same ids again for the same
     * message id. With the adapter installed, every route reads it when an exchange enters
     * the route; a tracked send and DLQ parking read it again in case a route restored it.
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
        Set<String> redoSteps = parseRedoSteps(redoStepsHeaderValue(header));
        exchange.setProperty(PlatformExchangeProperties.REDO_STEPS, redoSteps);
        return redoSteps;
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
        writeDelivered(exchange, step);
    }

    /**
     * Writes the outcome of a send that returned. If the write fails, it is tried again in place
     * a few times, unless the record's own data cannot be stored, and then kept as a mark for
     * the record's next read, so that the send is not made again because only its outcome was
     * lost. The failure still fails the attempt.
     */
    private void writeDelivered(Exchange exchange, StepResult step) {
        RuntimeException failure;
        try {
            stepOutcomeStore.recordDelivered(step);
            return;
        } catch (RuntimeException first) {
            failure = first;
        }
        if (StoreErrorClass.of(failure) != StoreErrorClass.RECORD_DATA && retriesInPlace(exchange)) {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RETRY_IN_PLACE_FOR_MS);
            for (long wait : RETRY_IN_PLACE_WAITS_MS) {
                if (System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(wait) > deadline || !retrySleeper.sleep(wait)) {
                    break;
                }
                try {
                    stepOutcomeStore.recordDelivered(step);
                    LOG.info("Step {} of message {}: its DELIVERED outcome was written when tried again in place",
                            step.step(), exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID));
                    return;
                } catch (RuntimeException again) {
                    failure = again;
                    if (StoreErrorClass.of(again) == StoreErrorClass.RECORD_DATA) {
                        break;
                    }
                }
            }
        }
        UnrecordedDeliveries.mark(exchange, step);
        throw failure;
    }

    // Only on the thread that processes the record, so that no I/O thread of an asynchronous
    // client waits, and not in an application's transaction, where every later statement fails.
    private static boolean retriesInPlace(Exchange exchange) {
        Long thread = exchange.getProperty(PlatformExchangeProperties.POLL_THREAD, Long.class);
        return thread != null && thread == Thread.currentThread().getId() && !exchange.isTransacted();
    }

    /**
     * An earlier read of this record made the send, and its outcome could not be written then:
     * write it now and skip the send. A write that fails again fails this attempt before
     * anything is sent.
     */
    private void writeFromMark(Exchange exchange, UnrecordedDeliveries.Mark mark, String idempotencyKey) {
        // The mark stays until its record is superseded: the write may join a transaction that
        // rolls back, and writing it again on a later read is harmless.
        stepOutcomeStore.recordDelivered(mark.step());
        exchange.setProperty(PlatformExchangeProperties.CURRENT_STEP_ID, mark.step().step());
        exchange.setProperty(PlatformExchangeProperties.CURRENT_IDEMPOTENCY_KEY, idempotencyKey);
        exchange.setProperty(PlatformExchangeProperties.STEP_SHOULD_SKIP, true);
        exchange.setProperty(PlatformExchangeProperties.STEP_ALREADY_DELIVERED, true);
        exchange.getMessage().setHeader(PlatformHeaders.IDEMPOTENCY_KEY, idempotencyKey);
        exchange.getMessage().setHeader(PlatformHeaders.HTTP_IDEMPOTENCY_KEY, idempotencyKey);
        String repeatedIn = StepVisits.of(exchange).claim(StepVisits.path(exchange), idempotencyKey);
        if (repeatedIn != null) {
            // Another part of this attempt took the mark's send: this part shares its step key and
            // was not delivered, which the skip reports as a ledger skip does. Parallel parts claim
            // the key in one step, so only one takes it.
            exchange.setProperty(StepVisits.REPEATED_IN, repeatedIn);
            return;
        }
        exchange.setProperty(PlatformExchangeProperties.STEP_FROM_MARK, mark.step());
        LOG.warn("Step {} of message {} was sent in an earlier read of record {}, and its DELIVERED outcome is "
                        + "written now; the send is not repeated", mark.step().step(),
                exchange.getMessage().getHeader(PlatformHeaders.MESSAGE_ID), mark.position());
    }

    @Override
    public void recordSkipped(Exchange exchange) {
        StepResult fromMark = exchange.getProperty(PlatformExchangeProperties.STEP_FROM_MARK, StepResult.class);
        if (fromMark != null) {
            // Delivered by this attempt in an earlier read of the record; written now.
            steps(exchange).add(fromMark);
            StepVisits.of(exchange).sent(StepVisits.path(exchange), fromMark.idempotencyKey());
            return;
        }
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
                        stepId, part(repeatedIn), exchange.getFromRouteId());
                return;
            }
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
        CURRENT_STEP_PROPERTIES.forEach(exchange::removeProperty);
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
