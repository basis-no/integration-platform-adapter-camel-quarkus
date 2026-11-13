package no.basis.integrationplatform.camel;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ErrorTextMaskingTest {
    @Test
    void masksEveryUriInATextByTheStoredEndpointRule() {
        assertEquals("HTTP operation failed invoking https://receiver.example/hook?code=xxxxxx&sig=xxxxxx&page=2 "
                        + "with statusCode: 500",
                ErrorTextMasking.mask("HTTP operation failed invoking https://receiver.example/hook?code=fn-1&sig=s-1"
                        + "&page=2 with statusCode: 500"));
        assertEquals("Cannot connect to sftp://xxxxxx@files.example:22/in (\"ftp://xxxxxx@backup.example/\")",
                ErrorTextMasking.mask("Cannot connect to sftp://transfer:s3cr3t@files.example:22/in "
                        + "(\"ftp://user:pw@backup.example/\")"));
        assertEquals("Failed: https://receiver.example/x?apiKey=xxxxxx<br>next",
                ErrorTextMasking.mask("Failed: https://receiver.example/x?apiKey=k-1<br>next"));
    }

    @Test
    void leavesTextWithoutAUriAsItIs() {
        String text = "Validation failed: field code=42 is not allowed";
        assertSame(text, ErrorTextMasking.mask(text));
        assertNull(ErrorTextMasking.mask(null));
        assertEquals("java.lang.IllegalStateException", ErrorTextMasking.message(new IllegalStateException()));
        assertNull(ErrorTextMasking.message(null));
    }

    @Test
    void masksAtMostTwentyUrisAndReplacesTheRest() {
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < 22; i++) {
            text.append("https://r.example/").append(i).append("?sig=s ");
        }
        String masked = ErrorTextMasking.mask(text.toString());
        assertEquals(20, masked.split("sig=xxxxxx", -1).length - 1);
        assertEquals("https://r.example/19?sig=xxxxxx xxxxxx xxxxxx ", masked.substring(masked.indexOf("https://r.example/19")));
    }

    @Test
    void aFailureForTheLogKeepsClassesAndStackTracesWithMaskedMessages() {
        var cause = new java.io.IOException("Connection reset by https://receiver.example/hook?code=fn-1");
        var failure = new IllegalStateException("Send to https://receiver.example/hook?sig=s-1 failed", cause);

        Throwable logged = ErrorTextMasking.forLog(failure);

        assertEquals("java.lang.IllegalStateException: Send to https://receiver.example/hook?sig=xxxxxx failed",
                logged.toString());
        assertArrayEquals(failure.getStackTrace(), logged.getStackTrace());
        assertEquals("java.io.IOException: Connection reset by https://receiver.example/hook?code=xxxxxx",
                logged.getCause().toString());
        assertArrayEquals(cause.getStackTrace(), logged.getCause().getStackTrace());
        assertEquals("Send to https://receiver.example/hook?sig=s-1 failed", failure.getMessage(), "The original is unchanged");
        assertNull(ErrorTextMasking.forLog(null));
    }

    @Test
    void takesLinearTimeOnALongRunOfSchemeCharacters() {
        String text = "For input string: \"" + "a1".repeat(150_000) + "\" at http://h/a?sig=s-1";
        long started = System.nanoTime();
        String masked = ErrorTextMasking.mask(text);
        assertTrue(System.nanoTime() - started < java.util.concurrent.TimeUnit.SECONDS.toNanos(2), "Took too long");
        assertTrue(masked.endsWith("http://h/a?sig=xxxxxx"), masked.substring(masked.length() - 40));
    }

    @Test
    void oneDeadlineCoversTheWholeText() {
        String slow = "https://r.example/a?" + "&token".repeat(20_000) + " ";
        long started = System.nanoTime();
        String masked = ErrorTextMasking.mask(slow.repeat(5));
        assertTrue(System.nanoTime() - started < java.util.concurrent.TimeUnit.SECONDS.toNanos(3), "Took too long");
        assertEquals("xxxxxx xxxxxx xxxxxx xxxxxx xxxxxx ", masked);
    }

    @Test
    void aQuoteInsideAUriIsPartOfItAndOneThatEndsItIsNot() {
        assertEquals("Cannot log in to ftp://xxxxxx@files.example/in",
                ErrorTextMasking.mask("Cannot log in to ftp://user:pa'ss@files.example/in"));
        assertEquals("invoking \"https://r.example/a?sig=xxxxxx\" failed",
                ErrorTextMasking.mask("invoking \"https://r.example/a?sig=s-1\" failed"));
        assertEquals("not a uri: ://x and 1https://r.example/?code=xxxxxx",
                ErrorTextMasking.mask("not a uri: ://x and 1https://r.example/?code=c-1"));
    }

    @Test
    void aSuppressedFailureOrADeepCauseIsMaskedToo() {
        var failure = new IllegalStateException("send failed");
        failure.addSuppressed(new RuntimeException("closing https://r.example/a?sig=s-1"));
        Throwable logged = ErrorTextMasking.forLog(failure);
        assertEquals("java.lang.RuntimeException: closing https://r.example/a?sig=xxxxxx",
                logged.getSuppressed()[0].toString());

        Throwable deep = new RuntimeException("https://r.example/a?sig=s-2");
        for (int i = 0; i < 25; i++) {
            deep = new RuntimeException("level " + i, deep);
        }
        Throwable top = deep;
        org.junit.jupiter.api.Assertions.assertNotSame(top, ErrorTextMasking.forLog(top),
                "A chain deeper than the bound is copied, not logged as it is");
    }

    @Test
    void aFailureThatAddsAUriInItsToStringIsMaskedToo() {
        // A logger prints toString(); an exception such as SAXParseException adds its systemId there.
        RuntimeException failure = new RuntimeException("parse failed") {
            @Override
            public String toString() {
                return "ParseFailure: parse failed in https://h/schema.xsd?sig=secret-1";
            }
        };

        Throwable logged = ErrorTextMasking.forLog(failure);

        assertTrue(logged != failure, "Copied, since its text names a URI");
        assertEquals("ParseFailure: parse failed in https://h/schema.xsd?sig=xxxxxx", logged.toString());
    }

    @Test
    void aFailureWithNothingToMaskIsLoggedAsItIs() {
        var failure = new IllegalArgumentException("synthetic receiver rejection", new java.io.IOException("reset"));
        assertSame(failure, ErrorTextMasking.forLog(failure));
    }

    @Test
    void aMaskedCopyIsNeverMaskedAgain() {
        // A second pass would read "host:xxxxxx" as user info.
        String raw = "Refused by http://host:8080/a?b=c@d";
        String once = ErrorTextMasking.mask(raw);
        org.junit.jupiter.api.Assertions.assertNotEquals(once, ErrorTextMasking.mask(once));

        Throwable copy = ErrorTextMasking.forLog(new IllegalStateException(raw));
        assertEquals("java.lang.IllegalStateException: " + once, copy.toString());
        assertSame(copy, ErrorTextMasking.forLog(copy), "Nothing left to mask");
        assertEquals("java.lang.IllegalStateException: " + once, ErrorTextMasking.message(copy));

        var outer = new RuntimeException("Not parked", copy);
        assertSame(outer, ErrorTextMasking.forLog(outer), "A masked cause needs no copy");
        var withUri = new RuntimeException("Not parked: " + raw, copy);
        Throwable logged = ErrorTextMasking.forLog(withUri);
        assertEquals("java.lang.RuntimeException: Not parked: " + once, logged.toString());
        assertEquals(copy.toString(), logged.getCause().toString(), "The masked cause keeps its text");
    }

    @Test
    void aCauseCycleIsCopiedOnce() {
        var first = new RuntimeException("first https://r.example/?sig=s-1");
        var second = new RuntimeException("second", first);
        first.initCause(second);
        Throwable logged = ErrorTextMasking.forLog(first);
        assertSame(logged, logged.getCause().getCause());
    }
}
