package no.basis.integrationplatform.camel;

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaEndpoint;
import org.apache.camel.processor.errorhandler.RedeliveryPolicy;
import org.apache.camel.spi.HttpResponseAware;

import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;

/**
 * Times the next Camel redelivery after an HTTP 429 or 503 by the receiver's {@code Retry-After},
 * within the Kafka consumer's poll budget.
 *
 * <p>Camel runs this {@code onExceptionOccurred} processor after a failed attempt and before it
 * decides on and times the next redelivery, which takes the per-exchange
 * {@link Exchange#REDELIVERY_DELAY} header in place of the configured delay. {@code Retry-After} is
 * a minimum: the wait is the longer of it and the configured delay, so a {@code Retry-After} of 0
 * or a date in the past (a skewed clock) does not spend the redeliveries at once. A wait that does
 * not fit the budget is not used: the configured delay applies, and the attempt goes on through
 * the normal retries and, when they are exhausted, to the DLQ. Once a wait has been used, every
 * later redelivery of the exchange gets its configured delay explicitly, because Camel's back-off
 * would otherwise grow from the {@code Retry-After} wait.
 *
 * <p>The number of redeliveries is Camel's own for the exchange ({@link
 * Exchange#REDELIVERY_MAX_COUNTER}), so an application's {@code onException} with its own maximum
 * counts; its own delays do not, and the route configuration's apply instead.
 */
final class HttpRetryAfter implements Processor {
    /** The share of the poll interval kept for the rest of the attempt. */
    static final double POLL_MARGIN = 0.2;
    private static final String TIMED = "basis.messageStore.retryAfterTimed";

    private final RedeliveryPolicy policy;
    private final Clock clock;

    HttpRetryAfter(RedeliveryPolicy policy) {
        this(policy, Clock.systemUTC());
    }

    HttpRetryAfter(RedeliveryPolicy policy, Clock clock) {
        this.policy = policy;
        this.clock = clock;
    }

    /**
     * The policy of a route configuration: the same values the configuration gives Camel.
     *
     * @param redeliveries maximum redeliveries
     * @param firstDelayMs first delay
     * @param maximumDelayMs maximum delay, or {@code null} for Camel's default
     * @return the policy
     */
    static RedeliveryPolicy policy(int redeliveries, long firstDelayMs, Long maximumDelayMs) {
        RedeliveryPolicy policy = new RedeliveryPolicy();
        policy.setMaximumRedeliveries(redeliveries);
        policy.setRedeliveryDelay(firstDelayMs);
        policy.setUseExponentialBackOff(true);
        policy.setBackOffMultiplier(2);
        if (maximumDelayMs != null) {
            policy.setMaximumRedeliveryDelay(maximumDelayMs);
        }
        return policy;
    }

    @Override
    public void process(Exchange exchange) {
        Integer next = exchange.getIn().getHeader(Exchange.REDELIVERY_COUNTER, Integer.class);
        if (next == null || next < 1 || next > maximumRedeliveries(exchange)) {
            // No redelivery follows: nothing to time, and no delay left for the failure handler.
            if (Boolean.TRUE.equals(exchange.getProperty(TIMED, Boolean.class))) {
                exchange.getIn().removeHeader(Exchange.REDELIVERY_DELAY);
            }
            return;
        }
        Throwable failure = exchange.getException() != null
                ? exchange.getException()
                : exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        Long retryAfter = retryAfterMillis(failure);
        long configured = configuredDelay(next);
        if (retryAfter != null && retryAfter > configured && retryAfter <= budgetMillis(exchange)) {
            exchange.getIn().setHeader(Exchange.REDELIVERY_DELAY, retryAfter);
            exchange.setProperty(TIMED, true);
        } else if (Boolean.TRUE.equals(exchange.getProperty(TIMED, Boolean.class))) {
            exchange.getIn().setHeader(Exchange.REDELIVERY_DELAY, configured);
        }
    }

    /**
     * Removes the delay header once Camel has used it, before the redelivery runs, so that it
     * cannot time another error handler's redelivery, such as a sub-route's.
     *
     * @param exchange the exchange about to be redelivered
     */
    static void afterTiming(Exchange exchange) {
        if (Boolean.TRUE.equals(exchange.getProperty(TIMED, Boolean.class))) {
            exchange.getIn().removeHeader(Exchange.REDELIVERY_DELAY);
        }
    }

    /**
     * Camel's maximum for this exchange: the header it sets from the effective redelivery policy,
     * which is an application's {@code onException} one where that applies; without it (Camel sets
     * none for 0), the route configuration's. A negative maximum redelivers without end.
     */
    private int maximumRedeliveries(Exchange exchange) {
        Integer max = exchange.getIn().getHeader(Exchange.REDELIVERY_MAX_COUNTER, Integer.class);
        int value = max != null ? max : policy.getMaximumRedeliveries();
        return value < 0 ? Integer.MAX_VALUE : value;
    }

    /**
     * How long the next redelivery may wait: for a Kafka input, the consumer's poll interval less
     * the time the exchange has run and a margin for the rest of the attempt; otherwise the
     * configured maximum delay.
     */
    long budgetMillis(Exchange exchange) {
        if (exchange.getFromEndpoint() instanceof KafkaEndpoint kafka) {
            long interval = KafkaPollInterval.of(kafka.getConfiguration());
            long elapsed = Math.max(0, clock.millis() - exchange.getClock().getCreated());
            return (long) (interval * (1 - POLL_MARGIN)) - elapsed;
        }
        return policy.getMaximumRedeliveryDelay();
    }

    /** The delay Camel's policy gives this redelivery, without a {@code Retry-After}. */
    long configuredDelay(int redelivery) {
        long delay = 0;
        for (int i = 1; i <= redelivery; i++) {
            delay = policy.calculateRedeliveryDelay(delay, i);
        }
        return delay;
    }

    /** The wait a 429 or 503 asks for, or {@code null} when it asks for none or gives no usable one. */
    Long retryAfterMillis(Throwable failure) {
        for (Throwable t = failure, seen = null; t != null && t != seen; seen = t, t = t.getCause()) {
            if (!(t instanceof HttpResponseAware response)) {
                continue;
            }
            int status = response.getHttpResponseCode();
            if (status != 429 && status != 503) {
                return null;
            }
            return parse(header(t, "Retry-After"));
        }
        return null;
    }

    Long parse(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String trimmed = value.trim();
        if (trimmed.chars().allMatch(c -> c >= '0' && c <= '9')) {
            if (trimmed.length() > 9) {
                return Long.MAX_VALUE;
            }
            return Duration.ofSeconds(Long.parseLong(trimmed)).toMillis();
        }
        try {
            ZonedDateTime at = ZonedDateTime.parse(trimmed, DateTimeFormatter.RFC_1123_DATE_TIME);
            return Math.max(0, at.toInstant().toEpochMilli() - clock.millis());
        } catch (DateTimeParseException notADate) {
            return null;
        }
    }

    /**
     * A response header of the HTTP components' failure, read by reflection so that the adapter
     * needs no HTTP component of its own.
     */
    private static String header(Throwable failure, String name) {
        try {
            Method getter = failure.getClass().getMethod("getResponseHeaders");
            if (!(getter.invoke(failure) instanceof Map<?, ?> headers)) {
                return null;
            }
            for (Map.Entry<?, ?> entry : headers.entrySet()) {
                if (entry.getKey() instanceof String key && key.equalsIgnoreCase(name)) {
                    return entry.getValue() == null ? null : entry.getValue().toString();
                }
            }
            return null;
        } catch (ReflectiveOperationException | RuntimeException unavailable) {
            return null;
        }
    }
}
