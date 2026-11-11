package no.basis.integrationplatform.camel;

import java.nio.charset.StandardCharsets;

/**
 * Makes a value taken from a message safe to write into a log line. A producer sets the
 * envelope's values, such as the message id: a CR or LF in one would end the line and let the
 * rest of the value read as a log entry of its own, and an escape sequence would reach the
 * terminal of whoever reads the log.
 */
public final class LogText {
    private LogText() {
    }

    /**
     * Escapes every character that can end a line, hide text or control a terminal as
     * {@code \}{@code uXXXX}: C0 controls (U+0000 to U+001F), DEL and C1 controls (U+007F to
     * U+009F), the line and paragraph separators U+2028 and U+2029, the bidirectional controls
     * U+061C, U+200E, U+200F, U+202A to U+202E and U+2066 to U+2069, and the invisible U+200B to
     * U+200D, U+2060 to U+2065 and U+FEFF. A backslash becomes {@code \\}, so that the escaped text stands
     * for one value only.
     *
     * @param value a value from a message, as text or UTF-8 bytes; may be {@code null}
     * @return the escaped text, or {@code null} for {@code null}
     */
    public static String escape(Object value) {
        if (value == null) {
            return null;
        }
        String text = value instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : value.toString();
        StringBuilder escaped = null;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            boolean special = c == '\\' || escapes(c);
            if (special && escaped == null) {
                escaped = new StringBuilder(text.length() + 16).append(text, 0, i);
            }
            if (escaped == null) {
                continue;
            }
            if (c == '\\') {
                escaped.append("\\\\");
            } else if (special) {
                escaped.append(String.format("\\u%04x", (int) c));
            } else {
                escaped.append(c);
            }
        }
        return escaped == null ? text : escaped.toString();
    }

    private static boolean escapes(char c) {
        return c < 0x20 || (c >= 0x7f && c <= 0x9f) || c == 0x2028 || c == 0x2029 || c == 0x061c
                || (c >= 0x200b && c <= 0x200f) || (c >= 0x202a && c <= 0x202e) || (c >= 0x2060 && c <= 0x2069)
                || c == 0xfeff;
    }
}
