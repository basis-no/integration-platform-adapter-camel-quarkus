package no.basis.integrationplatform.camel;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class LogTextTest {
    @Test
    void escapesWhatCanEndALineOrControlATerminal() {
        assertEquals("order-1\\u000d\\u000a2026-11-11 INFO forged", LogText.escape("order-1\r\n2026-11-11 INFO forged"));
        assertEquals("a\\u0000b\\u001b[31mc\\u007fd\\u0085e", LogText.escape("a\u0000b\u001b[31mc\u007fd\u0085e"));
        assertEquals("a\\u2028b\\u2029c", LogText.escape("a b c"));
        assertEquals("\\u202eabc\\u2066d\\u200f", LogText.escape("‮abc⁦d‏"));
        assertEquals("tab\\u0009", LogText.escape("tab\t"));
    }

    @Test
    void aBackslashIsDoubledSoTheTextStandsForOneValue() {
        assertEquals("a\\\\u000ab", LogText.escape("a\\u000ab"));
    }

    @Test
    void ordinaryTextIsUnchanged() {
        String id = "ordre-ø-42:send-ünicode 😀";
        assertSame(id, LogText.escape(id));
        assertEquals("message-1", LogText.escape("message-1".getBytes(StandardCharsets.UTF_8)));
        assertEquals("42", LogText.escape(42));
        assertNull(LogText.escape(null));
    }
}
