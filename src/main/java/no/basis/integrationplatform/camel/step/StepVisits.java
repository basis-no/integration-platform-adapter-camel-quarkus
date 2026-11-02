package no.basis.integrationplatform.camel.step;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangePropertyKey;

import java.util.HashMap;
import java.util.Map;

/**
 * Which part of one attempt claimed each step key first, so that a skip which loses a part
 * can be told from a normal skip. Split parts, loop iterations and multicast and recipient
 * list branches keep the input's message id in 0.3.x, so parts that reach the same send
 * share one step key: the first part is sent and the later ones are skipped as delivered.
 * This only tells the skips apart for the log; it never changes whether a step is sent.
 *
 * <p>One instance is held by reference in an exchange property, so the copies Camel makes
 * for parts share it. A part's path names each enclosing split, loop, multicast or
 * recipient list and the part's index in it. A step that the same part visits again, for
 * example when Camel redelivers a sub-route or a whole split, is a normal skip, and so is
 * the first skip of a key that an earlier attempt delivered.</p>
 */
final class StepVisits {
    static final String PROPERTY = "basis.messageStore.stepVisits";
    static final String PATH = "basis.messageStore.stepVisitPath";
    /** The split, loop, multicast or recipient list whose part the exchange is in, until resolved. */
    static final String SCOPE = "basis.messageStore.stepVisitScope";
    /** Set on a skip that repeats another part's step key: the kind of scope the parts differ in. */
    static final String REPEATED_IN = "basis.messageStore.stepRepeatedIn";

    static final String SPLIT = "split";
    static final String LOOP = "loop";
    static final String MULTICAST = "multicast";
    static final String RECIPIENT_LIST = "recipientList";

    /** Per key, the first part that claimed it and the first other part, if any. */
    private final Map<String, String[]> claimants = new HashMap<>();
    private final Map<String, String> owners = new HashMap<>();

    static StepVisits of(Exchange exchange) {
        StepVisits visits = exchange.getProperty(PROPERTY, StepVisits.class);
        if (visits == null) {
            visits = new StepVisits();
            exchange.setProperty(PROPERTY, visits);
        }
        return visits;
    }

    /** A scope's marker in a path, such as {@code /split.3#}. */
    static String marker(String kind, long number) {
        return "/" + kind + "." + number + "#";
    }

    /**
     * The path of the part this exchange runs in. A split part or branch is its own
     * exchange, so its index is fixed here, before a nested scope can replace Camel's index
     * property. A loop may reuse one exchange for every iteration, so its index is read
     * each time.
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
        String kind = kind(scope);
        String resolved = path + scope + exchange.getProperty(LOOP.equals(kind) ? ExchangePropertyKey.LOOP_INDEX
                : SPLIT.equals(kind) ? ExchangePropertyKey.SPLIT_INDEX : ExchangePropertyKey.MULTICAST_INDEX);
        if (!LOOP.equals(kind)) {
            exchange.setProperty(PATH, resolved);
            exchange.removeProperty(SCOPE);
        }
        return resolved;
    }

    /**
     * Claims the key for this part when its send or skip begins. Claiming before the send
     * means a parallel part that sees another part's delivery also sees its claim.
     * @return for a skip of a key that another part of this attempt sent or skipped first,
     *         the kind of scope the two parts differ in; otherwise null, also when no other
     *         part claimed it, so that the delivery came from an earlier attempt
     */
    synchronized String claim(String path, String idempotencyKey, boolean skip) {
        String[] parts = claimants.computeIfAbsent(idempotencyKey, key -> new String[] {path, null});
        if (parts[1] == null && !parts[0].equals(path)) {
            parts[1] = path;
        }
        if (!skip) {
            return null;
        }
        String owner = owners.get(idempotencyKey);
        String other = !parts[0].equals(path) ? parts[0] : parts[1];
        if (owner == null && other == null) {
            owners.put(idempotencyKey, path);
            return null;
        }
        if (path.equals(owner)) {
            return null;
        }
        return divergence(path, owner != null ? owner : other);
    }

    /** Records that this part sent the key; called before the outcome becomes visible to other parts. */
    synchronized void delivering(String path, String idempotencyKey) {
        owners.put(idempotencyKey, path);
    }

    /** The kind of the first scope in which two different paths name different parts. */
    private static String divergence(String path, String other) {
        String[] mine = path.split("/");
        String[] theirs = other.split("/");
        int i = 1;
        while (i < mine.length && i < theirs.length && mine[i].equals(theirs[i])) {
            i++;
        }
        return kind(i < mine.length ? mine[i] : theirs[i]);
    }

    private static String kind(String segment) {
        String kind = segment.startsWith("/") ? segment.substring(1) : segment;
        int dot = kind.indexOf('.');
        return dot < 0 ? kind : kind.substring(0, dot);
    }
}
