package no.basis.integrationplatform.camel;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Masks credentials in error texts before the adapter stores, parks or logs them. An exception
 * from a failed send names the receiver's URI, query included, so a key or signature in that
 * URI would otherwise reach the Message Store, the shared DLQ topic and the log.
 *
 * <p>Every URI in the text is masked by {@link EndpointUriMasking#mask(String)}: the rule the
 * adapter and the .NET adapter apply to stored endpoints. A URI is a scheme (a letter, then
 * letters, digits, {@code +}, {@code -} or {@code .}) followed by {@code ://}, up to the next
 * white space, {@code <} or {@code >}; quotes that end it are not part of it. One deadline of
 * about a second covers the whole text, and at most {@value #MAX_URIS} URIs are masked; any URI
 * past either is replaced by {@code xxxxxx}. The scan is linear in the length of the text.</p>
 *
 * <p>Each text is masked once, as the .NET adapter does: where the adapter takes it from a
 * failure, before it is stored, parked or logged. Masking is not idempotent (a second pass over
 * {@code http://host:8080/a?b=c@d} masks the host as user info), so a text that was masked is
 * never masked again: a {@link MaskedFailure} is the masked copy of a failure, and its text is
 * kept as it is.</p>
 */
public final class ErrorTextMasking {
    static final int MAX_URIS = 20;
    private static final long TEXT_TIMEOUT_NANOS = 1_000_000_000L;
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
        return mask(text, System.nanoTime() + TEXT_TIMEOUT_NANOS);
    }

    // The deadline is shared by every text of one call, such as the messages of a failure's chain.
    private static String mask(String text, long deadline) {
        if (text == null || text.indexOf("://") < 0) {
            return text;
        }
        StringBuilder masked = new StringBuilder(text.length());
        int copied = 0;
        int count = 0;
        for (int separator = text.indexOf("://"); separator >= 0; separator = text.indexOf("://", separator + 3)) {
            if (separator < copied) {
                continue;
            }
            int start = separator;
            while (start > copied && schemeCharacter(text.charAt(start - 1))) {
                start--;
            }
            while (start < separator && !asciiLetter(text.charAt(start))) {
                start++;
            }
            if (start == separator) {
                continue;
            }
            int end = separator + 3;
            while (end < text.length() && !endsUri(text.charAt(end))) {
                end++;
            }
            int uriEnd = end;
            while (uriEnd > separator + 3 && (text.charAt(uriEnd - 1) == '"' || text.charAt(uriEnd - 1) == '\'')) {
                uriEnd--;
            }
            String replacement = ++count <= MAX_URIS && System.nanoTime() - deadline < 0
                    ? EndpointUriMasking.mask(text.substring(start, uriEnd), deadline) : PlatformRouteConfiguration.MASKED;
            // A URI that runs into another, as in JSON without white space between them, keeps the
            // other in a value it does not mask: mask that one too.
            int inner = replacement.indexOf("://") + 3;
            if (inner > 2 && replacement.indexOf("://", inner) >= 0) {
                replacement = replacement.substring(0, inner) + mask(replacement.substring(inner), deadline);
            }
            masked.append(text, copied, start).append(replacement);
            copied = uriEnd;
        }
        masked.append(text, copied, text.length());
        return masked.toString();
    }

    private static boolean asciiLetter(char c) {
        return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z');
    }

    private static boolean schemeCharacter(char c) {
        return asciiLetter(c) || (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.';
    }

    private static boolean endsUri(char c) {
        return Character.isWhitespace(c) || c == '<' || c == '>';
    }

    /**
     * The failure's message with its URIs masked, or its class name when it has none. The text
     * of a masked copy is masked already and is returned as it is.
     *
     * @param failure a failure; may be {@code null}
     * @return the masked message, or {@code null} for {@code null}
     */
    public static String message(Throwable failure) {
        if (failure == null) {
            return null;
        }
        if (failure.getMessage() == null) {
            return failure.getClass().getName();
        }
        return failure instanceof MaskedFailure ? failure.getMessage() : mask(failure.getMessage());
    }

    /**
     * The failure to pass to a logger. When masking changes no message in its cause chain, that
     * is the failure itself. Otherwise it is a copy in which each failure of the chain keeps its
     * class name, in the text, and its stack trace, and has its message masked. The original
     * stays unchanged, since it may be shared. A masked copy in the chain keeps its text.
     *
     * @param failure a failure; may be {@code null}
     * @return the failure or its masked copy, or {@code null} for {@code null}
     */
    public static Throwable forLog(Throwable failure) {
        if (failure == null) {
            return null;
        }
        // One deadline for the check and the copy of the whole chain.
        long deadline = System.nanoTime() + TEXT_TIMEOUT_NANOS;
        if (!masksAnything(failure, new IdentityHashMap<>(), 0, deadline)) {
            return failure;
        }
        return copy(failure, new IdentityHashMap<>(), 0, deadline);
    }

    // A chain deeper than the bound counts as masking something, so that it is copied, not logged
    // as it is. A logger prints toString(), which an exception may override with more text.
    private static boolean masksAnything(Throwable failure, Map<Throwable, Boolean> seen, int depth, long deadline) {
        if (failure == null || seen.put(failure, true) != null) {
            return false;
        }
        if (depth > MAX_CAUSES) {
            return true;
        }
        if (failure instanceof MaskedFailure) {
            return causesMaskAnything(failure, seen, depth, deadline);
        }
        String message = failure.getMessage();
        if (message != null && !message.equals(mask(message, deadline))) {
            return true;
        }
        String text = failure.toString();
        if (text != null && !text.equals(mask(text, deadline))) {
            return true;
        }
        return causesMaskAnything(failure, seen, depth, deadline);
    }

    // Whether masking changes a suppressed failure or the cause of this one.
    private static boolean causesMaskAnything(Throwable failure, Map<Throwable, Boolean> seen, int depth, long deadline) {
        for (Throwable suppressed : failure.getSuppressed()) {
            if (masksAnything(suppressed, seen, depth + 1, deadline)) {
                return true;
            }
        }
        Throwable cause = failure.getCause();
        return cause != failure && masksAnything(cause, seen, depth + 1, deadline);
    }

    private static Throwable copy(Throwable failure, Map<Throwable, Throwable> copies, int depth, long deadline) {
        Throwable known = copies.get(failure);
        if (known != null) {
            return known;
        }
        // toString() names the class, then the message or whatever the exception adds to it.
        String text = failure instanceof MaskedFailure ? failure.getMessage() : mask(String.valueOf(failure), deadline);
        MaskedFailure copy = new MaskedFailure(text, failure.getStackTrace());
        copies.put(failure, copy);
        Throwable cause = failure.getCause();
        if (cause != null && cause != failure && depth < MAX_CAUSES) {
            copy.initCause(copy(cause, copies, depth + 1, deadline));
        }
        if (depth < MAX_CAUSES) {
            for (Throwable suppressed : failure.getSuppressed()) {
                copy.addSuppressed(copy(suppressed, copies, depth + 1, deadline));
            }
        }
        return copy;
    }

    /**
     * A failure as the log shows it: the original class name and stack trace, a masked message.
     * Its text is masked already, so it is never masked again.
     */
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
