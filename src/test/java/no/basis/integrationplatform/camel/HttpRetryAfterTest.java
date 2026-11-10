package no.basis.integrationplatform.camel;

import org.apache.camel.Exchange;
import org.apache.camel.http.base.HttpOperationFailedException;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** How the adapter times a redelivery after a 429 or 503. */
class HttpRetryAfterTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-11-10T12:00:00Z"), ZoneOffset.UTC);
    private final HttpRetryAfter defaultProfile = new HttpRetryAfter(HttpRetryAfter.policy(3, 1000, null), clock);

    @Test
    void readsRetryAfterInSecondsOrAsAnHttpDate() {
        assertEquals(0L, defaultProfile.parse("0"));
        assertEquals(120_000L, defaultProfile.parse(" 120 "));
        String inNinetySeconds = DateTimeFormatter.RFC_1123_DATE_TIME.format(
                clock.instant().plusSeconds(90).atZone(ZoneOffset.UTC));
        assertEquals(90_000L, defaultProfile.parse(inNinetySeconds));
        String past = DateTimeFormatter.RFC_1123_DATE_TIME.format(
                clock.instant().minusSeconds(5).atZone(ZoneOffset.UTC));
        assertEquals(0L, defaultProfile.parse(past), "A date in the past asks for no wait");
        assertEquals(Long.MAX_VALUE, defaultProfile.parse("99999999999"), "Too long for any budget");
        assertNull(defaultProfile.parse("-1"));
        assertNull(defaultProfile.parse("soon"));
        assertNull(defaultProfile.parse(""));
        assertNull(defaultProfile.parse(null));
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 503})
    void timesTheNextRedeliveryByRetryAfter(int status) throws Exception {
        try (var context = new DefaultCamelContext()) {
            Exchange exchange = failed(context, status, Map.of("retry-after", "7"), 1);
            defaultProfile.process(exchange);
            assertEquals(7_000L, exchange.getMessage().getHeader(Exchange.REDELIVERY_DELAY, Long.class));
        }
    }

    @Test
    void leavesOtherFailuresAndMissingRetryAfterToTheConfiguredDelay() throws Exception {
        try (var context = new DefaultCamelContext()) {
            Exchange serverError = failed(context, 500, Map.of("Retry-After", "7"), 1);
            defaultProfile.process(serverError);
            assertNull(serverError.getMessage().getHeader(Exchange.REDELIVERY_DELAY));

            Exchange withoutHeader = failed(context, 503, Map.of(), 1);
            defaultProfile.process(withoutHeader);
            assertNull(withoutHeader.getMessage().getHeader(Exchange.REDELIVERY_DELAY));

            Exchange exhausted = failed(context, 503, Map.of("Retry-After", "7"), 4);
            defaultProfile.process(exhausted);
            assertNull(exhausted.getMessage().getHeader(Exchange.REDELIVERY_DELAY),
                    "No redelivery follows the third, so nothing is timed");
        }
    }

    @Test
    void aWaitBeyondTheBudgetKeepsTheConfiguredDelay() throws Exception {
        try (var context = new DefaultCamelContext()) {
            // Without a Kafka input, the budget is the configured maximum delay: Camel's 60 s.
            Exchange exchange = failed(context, 503, Map.of("Retry-After", "61"), 1);
            defaultProfile.process(exchange);
            assertNull(exchange.getMessage().getHeader(Exchange.REDELIVERY_DELAY));
        }
    }

    @Test
    void theKafkaBudgetIsThePollIntervalLessTheRunTimeAndAMargin() throws Exception {
        try (var context = new DefaultCamelContext()) {
            Exchange fresh = failed(context, 503, Map.of(), 1);
            fresh.getExchangeExtension().setFromEndpoint(context.getEndpoint("kafka:orders?maxPollIntervalMs=100000"));
            long created = fresh.getClock().getCreated();
            var atCreation = new HttpRetryAfter(HttpRetryAfter.policy(3, 1000, null),
                    Clock.fixed(Instant.ofEpochMilli(created), ZoneOffset.UTC));
            assertEquals(80_000L, atCreation.budgetMillis(fresh));

            var thirtySecondsLater = new HttpRetryAfter(HttpRetryAfter.policy(3, 1000, null),
                    Clock.fixed(Instant.ofEpochMilli(created + 30_000), ZoneOffset.UTC));
            assertEquals(50_000L, thirtySecondsLater.budgetMillis(fresh));

            Exchange unset = failed(context, 503, Map.of(), 1);
            unset.getExchangeExtension().setFromEndpoint(context.getEndpoint("kafka:orders"));
            var atUnsetCreation = new HttpRetryAfter(HttpRetryAfter.policy(3, 1000, null),
                    Clock.fixed(Instant.ofEpochMilli(unset.getClock().getCreated()), ZoneOffset.UTC));
            assertEquals(240_000L, atUnsetCreation.budgetMillis(unset), "Kafka's default 300 s less 20 %");

            Exchange fits = failed(context, 503, Map.of("Retry-After", "200"), 1);
            fits.getExchangeExtension().setFromEndpoint(context.getEndpoint("kafka:orders"));
            var timing = new HttpRetryAfter(HttpRetryAfter.policy(3, 1000, null),
                    Clock.fixed(Instant.ofEpochMilli(fits.getClock().getCreated()), ZoneOffset.UTC));
            timing.process(fits);
            assertEquals(200_000L, fits.getMessage().getHeader(Exchange.REDELIVERY_DELAY, Long.class),
                    "A Kafka input may wait longer than the configured maximum, within its poll budget");

            Exchange tooLong = failed(context, 503, Map.of("Retry-After", "241"), 1);
            tooLong.getExchangeExtension().setFromEndpoint(context.getEndpoint("kafka:orders"));
            new HttpRetryAfter(HttpRetryAfter.policy(3, 1000, null),
                    Clock.fixed(Instant.ofEpochMilli(tooLong.getClock().getCreated()), ZoneOffset.UTC))
                    .process(tooLong);
            assertNull(tooLong.getMessage().getHeader(Exchange.REDELIVERY_DELAY));
        }
    }

    @Test
    void laterRedeliveriesGetTheirConfiguredDelayOnceARetryAfterWasUsed() throws Exception {
        try (var context = new DefaultCamelContext()) {
            Exchange exchange = failed(context, 503, Map.of("Retry-After", "30"), 1);
            defaultProfile.process(exchange);
            assertEquals(30_000L, exchange.getMessage().getHeader(Exchange.REDELIVERY_DELAY, Long.class));

            HttpRetryAfter.afterTiming(exchange);
            assertNull(exchange.getMessage().getHeader(Exchange.REDELIVERY_DELAY),
                    "Removed before the redelivery runs");

            // The second redelivery after a 500: Camel's back-off would grow from 30 s.
            exchange.setException(new IllegalStateException("receiver failed"));
            exchange.getMessage().setHeader(Exchange.REDELIVERY_COUNTER, 2);
            defaultProfile.process(exchange);
            assertEquals(2_000L, exchange.getMessage().getHeader(Exchange.REDELIVERY_DELAY, Long.class));
        }
    }

    @Test
    void theConfiguredDelaysAreCamelsBackOff() {
        assertEquals(1_000L, defaultProfile.configuredDelay(1));
        assertEquals(2_000L, defaultProfile.configuredDelay(2));
        assertEquals(4_000L, defaultProfile.configuredDelay(3));
        var uncapped = new HttpRetryAfter(HttpRetryAfter.policy(6, 5000, null), clock);
        assertEquals(60_000L, uncapped.configuredDelay(6), "Camel's default 60 s cap");
        var slow = new HttpRetryAfter(HttpRetryAfter.policy(PlatformRouteConfiguration.SLOW_BACKEND_REDELIVERIES,
                PlatformRouteConfiguration.SLOW_BACKEND_FIRST_DELAY_MS,
                PlatformRouteConfiguration.SLOW_BACKEND_MAX_DELAY_MS), clock);
        assertEquals(20_000L, slow.configuredDelay(3), "ip-slow-backend's own cap");
        assertEquals(20_000L, slow.configuredDelay(6));
        assertEquals(20_000L, slow.budgetMillis(new DefaultExchange(new DefaultCamelContext())),
                "Without a Kafka input, ip-slow-backend's budget is its 20 s cap");
    }

    private static Exchange failed(DefaultCamelContext context, int status, Map<String, String> headers, int next) {
        Exchange exchange = new DefaultExchange(context);
        exchange.setException(new HttpOperationFailedException(
                "http://receiver/invoices", status, "status " + status, null, headers, ""));
        exchange.getMessage().setHeader(Exchange.REDELIVERY_COUNTER, next);
        return exchange;
    }
}
