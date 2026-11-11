package no.basis.integrationplatform.camel.step;

import no.basis.integrationplatform.camel.PlatformExchangeProperties;
import no.basis.integrationplatform.camel.model.StepResult;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Sends that returned but whose {@code DELIVERED} outcome could not be written, kept in memory
 * per Camel context: a mark per input route, record position and step key. A later read of the
 * same record writes the outcome from its mark instead of sending again. A mark stays until a
 * record at or after its offset is committed, or a record at another offset begins on its
 * partition; writing it again is harmless. Marks are lost on a restart or a rebalance to another
 * process.
 */
public final class UnrecordedDeliveries {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(UnrecordedDeliveries.class);
    static final int MAX_MARKS = 10_000;
    private static final Map<CamelContext, UnrecordedDeliveries> BY_CONTEXT = new WeakHashMap<>();

    /** The outcome the write would have stored, and the record it belongs to. */
    record Mark(String route, String position, StepResult step) {
    }

    // Oldest first, so the bounds drop the oldest mark.
    private final LinkedHashMap<String, Mark> marks = new LinkedHashMap<>();

    private UnrecordedDeliveries() {
    }

    private static UnrecordedDeliveries of(Exchange exchange) {
        synchronized (BY_CONTEXT) {
            return BY_CONTEXT.computeIfAbsent(exchange.getContext(), context -> new UnrecordedDeliveries());
        }
    }

    private static String position(Exchange exchange) {
        return exchange.getProperty(PlatformExchangeProperties.RECORD_POSITION, String.class);
    }

    // The input route: two routes in one context can consume the same partition in their own groups.
    private static String route(Exchange exchange) {
        String route = exchange.getFromRouteId();
        return route == null ? "" : route;
    }

    /** Keeps a mark for the exchange's record, when its position is known. */
    static void mark(Exchange exchange, StepResult step) {
        String position = position(exchange);
        if (position == null) {
            return;
        }
        of(exchange).put(new Mark(route(exchange), position, step));
    }

    /** The mark of this record and step key, if an earlier read left one. */
    static Mark find(Exchange exchange, String idempotencyKey) {
        String position = position(exchange);
        return position == null ? null : of(exchange).get(key(route(exchange), position, idempotencyKey));
    }

    /**
     * Drops the marks of the record's partition that it supersedes: on a commit, the marks at or
     * before the committed offset; when a record begins, the marks at any other offset.
     *
     * @param exchange a managed Kafka input's exchange
     * @param committed whether its record was committed, or only began
     */
    public static void supersede(Exchange exchange, boolean committed) {
        String position = position(exchange);
        if (position == null) {
            return;
        }
        int at = position.lastIndexOf('@');
        String partition = position.substring(0, at);
        long offset = Long.parseLong(position.substring(at + 1));
        UnrecordedDeliveries deliveries;
        synchronized (BY_CONTEXT) {
            deliveries = BY_CONTEXT.get(exchange.getContext());
        }
        if (deliveries != null) {
            deliveries.dropPartition(route(exchange), partition, offset, committed);
        }
    }

    /**
     * The steps whose sends returned for this record without a written outcome, as the marks of
     * its position name them.
     *
     * @param exchange a managed Kafka input's exchange
     * @return the step ids, in the order they were marked
     */
    public static java.util.List<String> markedSteps(Exchange exchange) {
        String position = position(exchange);
        UnrecordedDeliveries deliveries;
        synchronized (BY_CONTEXT) {
            deliveries = BY_CONTEXT.get(exchange.getContext());
        }
        return position == null || deliveries == null ? java.util.List.of()
                : deliveries.stepsAt(route(exchange), position);
    }

    private synchronized java.util.List<String> stepsAt(String route, String position) {
        return marks.values().stream().filter(mark -> mark.route().equals(route) && mark.position().equals(position))
                .map(mark -> mark.step().step()).toList();
    }

    private static String key(String route, String position, String idempotencyKey) {
        return route + "\n" + position + "\n" + idempotencyKey;
    }

    private static String key(Mark mark) {
        return key(mark.route(), mark.position(), mark.step().idempotencyKey());
    }

    private synchronized void put(Mark mark) {
        marks.remove(key(mark));
        marks.put(key(mark), mark);
        Iterator<Mark> oldest = marks.values().iterator();
        while (marks.size() > MAX_MARKS && oldest.hasNext()) {
            Mark dropped = oldest.next();
            if (dropped == mark) {
                break;
            }
            oldest.remove();
            LOG.warn("Dropped the unwritten DELIVERED outcome of step {} for record {}: more than {} such outcomes "
                            + "are kept; that record's next read sends the step again",
                    dropped.step().step(), dropped.position(), MAX_MARKS);
        }
    }

    private synchronized Mark get(String key) {
        return marks.get(key);
    }

    private synchronized void dropPartition(String route, String partition, long offset, boolean committed) {
        Iterator<Mark> all = marks.values().iterator();
        while (all.hasNext()) {
            Mark mark = all.next();
            int at = mark.position().lastIndexOf('@');
            long markOffset = Long.parseLong(mark.position().substring(at + 1));
            if (mark.route().equals(route) && mark.position().substring(0, at).equals(partition)
                    && (committed ? markOffset <= offset : markOffset != offset)) {
                all.remove();
            }
        }
    }
}
