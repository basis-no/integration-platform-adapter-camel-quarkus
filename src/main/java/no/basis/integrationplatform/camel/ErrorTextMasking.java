package no.basis.integrationplatform.camel;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Masks credentials in error texts before the adapter stores, parks or logs them. An exception
 * from a failed send names the receiver's URI, query included, so a key or signature in that
 * URI would otherwise reach the Message Store, the shared DLQ topic and the log.
 *
 * <p>Every URI in the text, a scheme followed by {@code ://} up to the next white space, quote
 * or angle bracket, is masked by {@link EndpointUriMasking#mask(String)}: the rule the adapter
 * and the .NET adapter apply to stored endpoints. At most {@value #MAX_URIS} URIs are masked
 * that way; any further URI in the same text is replaced by {@code xxxxxx}.</p>
 */
public final class ErrorTextMasking {
    static final int MAX_URIS = 20;
    private static final Pattern URI = Pattern.compile("[A-Za-z][A-Za-z0-9+.-]*://[^\\s\"'<>]+");
    private static final int MAX_CAUSES = 20;

    private ErrorTextMasking() {
    }

    /**
     * Masks every URI in a text.
     *
     * @param text an error text; may be {@code null}
     * @return the masked text, or {@code null} for {@code null}
     */
    public static String mask(String text) {
        if (text == null || text.indexOf("://") < 0) {
            return text;
        }
        Matcher uris = URI.matcher(text);
        StringBuilder masked = new StringBuilder(text.length());
        int count = 0;
        while (uris.find()) {
            String replacement = ++count <= MAX_URIS ? EndpointUriMasking.mask(uris.group()) : PlatformRouteConfiguration.MASKED;
            uris.appendReplacement(masked, Matcher.quoteReplacement(replacement));
        }
        uris.appendTail(masked);
        return masked.toString();
    }

    /**
     * The failure's message with its URIs masked, or its class name when it has none.
     *
     * @param failure a failure; may be {@code null}
     * @return the masked message, or {@code null} for {@code null}
     */
    public static String message(Throwable failure) {
        if (failure == null) {
            return null;
        }
        return failure.getMessage() == null ? failure.getClass().getName() : mask(failure.getMessage());
    }

    /**
     * The failure to pass to a logger. When masking changes no message in its cause chain, that
     * is the failure itself. Otherwise it is a copy in which each failure of the chain keeps its
     * class name, in the text, and its stack trace, and has its message masked. The original
     * stays unchanged, since it may be shared.
     *
     * @param failure a failure; may be {@code null}
     * @return the failure or its masked copy, or {@code null} for {@code null}
     */
    public static Throwable forLog(Throwable failure) {
        if (failure == null || !masksAnything(failure)) {
            return failure;
        }
        return copy(failure, new IdentityHashMap<>(), 0);
    }

    private static boolean masksAnything(Throwable failure) {
        Map<Throwable, Boolean> seen = new IdentityHashMap<>();
        Throwable current = failure;
        for (int depth = 0; current != null && depth <= MAX_CAUSES && seen.put(current, true) == null; depth++) {
            String message = current.getMessage();
            if (message != null && !message.equals(mask(message))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static Throwable copy(Throwable failure, Map<Throwable, Throwable> copies, int depth) {
        Throwable known = copies.get(failure);
        if (known != null) {
            return known;
        }
        String message = failure.getMessage();
        MaskedFailure copy = new MaskedFailure(failure.getClass().getName()
                + (message == null ? "" : ": " + mask(message)), failure.getStackTrace());
        copies.put(failure, copy);
        Throwable cause = failure.getCause();
        if (cause != null && cause != failure && depth < MAX_CAUSES) {
            copy.initCause(copy(cause, copies, depth + 1));
        }
        return copy;
    }

    /** A failure as the log shows it: the original class name and stack trace, a masked message. */
    static final class MaskedFailure extends RuntimeException {
        private MaskedFailure(String text, StackTraceElement[] stackTrace) {
            super(text);
            setStackTrace(stackTrace);
        }

        @Override
        public String toString() {
            return getMessage();
        }
    }
}
