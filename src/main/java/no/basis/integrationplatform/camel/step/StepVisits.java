package no.basis.integrationplatform.camel.step;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePropertyKey;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * Which parts of one attempt sent each step key, so that a skip which loses a part can be
 * told from a normal skip. Split parts, loop iterations and multicast and recipient list
 * branches keep the input's message id in 0.3.x, so parts that reach the same send share
 * one step key: the first part is sent and the later ones are skipped as delivered. This
 * only tells the skips apart for the log; it never changes whether a step is sent.
 *
 * <p>One instance is held by reference in an exchange property, so the copies Camel makes
 * for parts share it. A part's position names each enclosing split, loop, multicast or
 * recipient list and the part's index in it. A skip is a repeat only when a part at
 * another position sent the key in this attempt, and neither this part nor one of its own
 * parts did. Anything less certain is a normal skip: a key that an earlier attempt
 * delivered, a part that meets its own send again when Camel repeats it, and a part whose
 * position is unknown.</p>
 */
final class StepVisits {
    static final String PROPERTY = "basis.messageStore.stepVisits";
    static final String PATH = "basis.messageStore.stepVisitPath";
    /** The split, loop, multicast or recipient list whose part the exchange is in, until resolved. */
    static final String SCOPE = "basis.messageStore.stepVisitScope";
    /** The loop whose iteration is no longer known, because a nested scope changed Camel's loop index. */
    static final String LOOP_UNKNOWN = "basis.messageStore.stepVisitLoopUnknown";
    /** Set on a skip that repeats another part's step key: the kind of scope the parts differ in. */
    static final String REPEATED_IN = "basis.messageStore.stepRepeatedIn";

    static final String SPLIT = "split";
    static final String LOOP = "loop";
    static final String MULTICAST = "multicast";
    static final String RECIPIENT_LIST = "recipientList";
    private static final String UNKNOWN = "?";
    /** Keys kept per attempt; a forgotten key is not checked. A key every part reaches stays. */
    static final int MAX_KEYS = 10_000;
    /** Positions kept per key; a key that more parts sent is not checked further. */
    static final int MAX_SENDERS = 16;

    private final Map<String, Senders> keys = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Senders> eldest) {
            return size() > MAX_KEYS;
        }
    };

    private static final class Senders {
        private final Set<String> positions = new HashSet<>(4);
        private boolean unchecked;
    }

    static StepVisits of(Exchange exchange) {
        StepVisits visits = exchange.getProperty(PROPERTY, StepVisits.class);
        if (visits == null) {
            visits = new StepVisits();
            exchange.setProperty(PROPERTY, visits);
        }
        return visits;
    }

    /** A scope's marker in a position, such as {@code /split.3#}. */
    static String marker(String kind, long number) {
        return "/" + kind + "." + number + "#";
    }

    static boolean isLoop(String marker) {
        return marker != null && LOOP.equals(kind(marker));
    }

    /**
     * The position of the part this exchange runs in. A split part or branch is its own
     * exchange, so its index is fixed here, before a nested scope can replace Camel's index
     * property. A loop may reuse one exchange for every iteration, so its index is read
     * each time, and is unknown once a nested scope has changed it.
     */
    static String path(Exchange exchange) {
        String path = exchange.getProperty(PATH, String.class);
        if (path == null) {
            path = "";
        }
        String scope = exchange.getProperty(SCOPE, String.class);
        if (scope == null) {
            return path;
        }
        if (isLoop(scope)) {
            Object index = scope.equals(exchange.getProperty(LOOP_UNKNOWN, String.class)) ? UNKNOWN
                    : exchange.getProperty(ExchangePropertyKey.LOOP_INDEX);
            return path + scope + index;
        }
        String resolved = path + scope + exchange.getProperty(SPLIT.equals(kind(scope))
                ? ExchangePropertyKey.SPLIT_INDEX : ExchangePropertyKey.MULTICAST_INDEX);
        exchange.setProperty(PATH, resolved);
        exchange.removeProperty(SCOPE);
        return resolved;
    }

    /** Records that this part sent the key; called before the outcome becomes visible to other parts. */
    synchronized void sent(String path, String idempotencyKey) {
        Senders senders = keys.computeIfAbsent(idempotencyKey, key -> new Senders());
        if (path.contains(UNKNOWN) || senders.positions.size() >= MAX_SENDERS && !senders.positions.contains(path)) {
            senders.unchecked = true;
        } else {
            senders.positions.add(path);
        }
    }

    /**
     * For a skipped send inside a part: the kind of scope in which this part differs from
     * a part that sent the key in this attempt, when neither this part nor one of its own
     * parts sent it. Null for a normal skip, and whenever that is not certain.
     */
    synchronized String repeatedIn(String path, String idempotencyKey) {
        Senders senders = keys.get(idempotencyKey);
        if (senders == null || senders.unchecked || path.isEmpty() || path.contains(UNKNOWN)) {
            return null;
        }
        String other = null;
        for (String sender : senders.positions) {
            if (sender.equals(path) || sender.startsWith(path + "/")) {
                return null;
            }
            other = sender;
        }
        if (other == null) {
            return null;
        }
        String[] mine = path.split("/");
        String[] theirs = other.split("/");
        int i = 1;
        while (i < mine.length && i < theirs.length && mine[i].equals(theirs[i])) {
            i++;
        }
        return i < mine.length ? kind(mine[i]) : null;
    }

    private static String kind(String segment) {
        String kind = segment.startsWith("/") ? segment.substring(1) : segment;
        int dot = kind.indexOf('.');
        return dot < 0 ? kind : kind.substring(0, dot);
    }
}
