package no.basis.integrationplatform.camel;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

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
    void aFailureWithNothingToMaskIsLoggedAsItIs() {
        var failure = new IllegalArgumentException("synthetic receiver rejection", new java.io.IOException("reset"));
        assertSame(failure, ErrorTextMasking.forLog(failure));
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
