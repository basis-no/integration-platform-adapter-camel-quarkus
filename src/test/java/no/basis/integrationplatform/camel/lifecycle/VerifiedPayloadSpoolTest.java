package no.basis.integrationplatform.camel.lifecycle;

import no.basis.integrationplatform.camel.payload.PayloadReference;
import no.basis.integrationplatform.camel.payload.PayloadStorageException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VerifiedPayloadSpoolTest {
    private static final byte[] PAYLOAD = "an object-stored input of some size".getBytes(StandardCharsets.UTF_8);

    @TempDir
    Path directory;

    @Test
    void theRouteGetsTheCheckedCopyAndClosingItDeletesTheFile() throws Exception {
        var spool = new VerifiedPayloadSpool(directory, 1024);

        InputStream copy = spool.checkedCopy(reference(PAYLOAD), () -> new ByteArrayInputStream(PAYLOAD));

        // The open copy holds its share of the spool. Where the platform allows it (Linux), its
        // file is unlinked as soon as it is open, so even a crash leaves nothing behind.
        assertEquals(PAYLOAD.length, spool.reservedBytes());
        assertArrayEquals(PAYLOAD, copy.readAllBytes());
        copy.close();
        copy.close();
        assertEquals(List.of(), files());
        assertEquals(0, spool.reservedBytes(), "closing twice releases the share once");
    }

    @Test
    void aChangedObjectIsRefusedBeforeAnyByteIsHandedOut() {
        var spool = new VerifiedPayloadSpool(directory, 1024);
        byte[] changed = PAYLOAD.clone();
        changed[changed.length - 1] ^= 1;

        PayloadStorageException failure = assertThrows(PayloadStorageException.class,
                () -> spool.checkedCopy(reference(PAYLOAD), () -> new ByteArrayInputStream(changed)));

        assertTrue(failure.getMessage().contains("SHA-256"), failure.getMessage());
        assertNothingLeft(spool);
    }

    @Test
    void aTruncatedOrLongerObjectIsRefused() {
        var spool = new VerifiedPayloadSpool(directory, 1024);
        byte[] shorter = java.util.Arrays.copyOf(PAYLOAD, PAYLOAD.length - 3);
        byte[] longer = java.util.Arrays.copyOf(PAYLOAD, PAYLOAD.length + 3);

        PayloadStorageException truncated = assertThrows(PayloadStorageException.class,
                () -> spool.checkedCopy(reference(PAYLOAD), () -> new ByteArrayInputStream(shorter)));
        PayloadStorageException extended = assertThrows(PayloadStorageException.class,
                () -> spool.checkedCopy(reference(PAYLOAD), () -> new ByteArrayInputStream(longer)));

        assertTrue(truncated.getMessage().contains("bytes; its reference has"), truncated.getMessage());
        assertTrue(extended.getMessage().contains("larger than its reference"), extended.getMessage());
        assertNothingLeft(spool);
    }

    @Test
    void anObjectStoreFailureWhileCopyingLeavesNothingBehind() {
        var spool = new VerifiedPayloadSpool(directory, 1024);
        InputStream failing = new FilterInputStream(new ByteArrayInputStream(PAYLOAD)) {
            private int reads;

            @Override
            public int read(byte[] bytes, int offset, int length) throws IOException {
                if (reads++ > 0) {
                    throw new IOException("connection reset");
                }
                return super.read(bytes, offset, 4);
            }
        };

        PayloadStorageException failure = assertThrows(PayloadStorageException.class,
                () -> spool.checkedCopy(reference(PAYLOAD), () -> failing));

        assertTrue(failure.getMessage().contains("could not be copied for checking"), failure.getMessage());
        assertNothingLeft(spool);
    }

    @Test
    void copiesInUseBoundTheSpoolWithoutReadingTheObject() throws Exception {
        var spool = new VerifiedPayloadSpool(directory, PAYLOAD.length + 10);
        InputStream first = spool.checkedCopy(reference(PAYLOAD), () -> new ByteArrayInputStream(PAYLOAD));
        var opened = new AtomicBoolean();

        PayloadStorageException full = assertThrows(PayloadStorageException.class,
                () -> spool.checkedCopy(reference(PAYLOAD), () -> {
                    opened.set(true);
                    return new ByteArrayInputStream(PAYLOAD);
                }));

        assertTrue(full.getMessage().contains("cannot be checked now"), full.getMessage());
        assertFalse(opened.get(), "a copy that does not fit is refused before the object is read");
        first.close();
        assertDoesNotThrow(() -> spool.checkedCopy(reference(PAYLOAD), () -> new ByteArrayInputStream(PAYLOAD)).close());
        assertEquals(0, spool.reservedBytes());
    }

    @Test
    void startupDeletesCopiesOfProcessesThatAreGoneAndKeepsLiveOnes() throws Exception {
        var spool = new VerifiedPayloadSpool(directory, 1024);
        String current = VerifiedPayloadSpool.ownerTag(ProcessHandle.current());
        Path live = Files.writeString(directory.resolve("spool-" + current + "-live.payload"), "in use");
        Path crashed = Files.writeString(directory.resolve("spool-2147483646-1-dead.payload"), "left by a crash");
        long pid = ProcessHandle.current().pid();
        Path reusedPid = Files.writeString(directory.resolve("spool-" + pid + "-1-reused.payload"), "earlier process");
        Path unrelated = Files.writeString(directory.resolve("notes.txt"), "not a spool file");

        spool.deleteLeftovers();

        assertFalse(Files.exists(crashed));
        assertFalse(Files.exists(reusedPid), "the same process id with another start time is a different process");
        assertTrue(Files.exists(unrelated));
        assertTrue(Files.exists(live), "a copy of this running process stays");
    }

    @Test
    void startupRefusesASpoolSmallerThanTheLargestObjectPayload() {
        var tiered = new PayloadCapturePolicy(PayloadMode.TIERED, 1024, 4096, Duration.ofDays(30), true);
        var inline = new PayloadCapturePolicy(PayloadMode.POSTGRES_INLINE, 1024, 4096, Duration.ofDays(30), true);

        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> new VerifiedPayloadSpool(directory, 4095).validateFor(tiered));

        assertTrue(refused.getMessage().contains(VerifiedPayloadSpool.MAX_BYTES_PROPERTY), refused.getMessage());
        assertDoesNotThrow(() -> new VerifiedPayloadSpool(directory, 4096).validateFor(tiered));
        assertDoesNotThrow(() -> new VerifiedPayloadSpool(directory, 1).validateFor(inline));
        assertThrows(IllegalArgumentException.class, () -> new VerifiedPayloadSpool(directory, 0));
    }

    private void assertNothingLeft(VerifiedPayloadSpool spool) {
        assertEquals(List.of(), files());
        assertEquals(0, spool.reservedBytes());
    }

    private List<Path> files() {
        try (Stream<Path> files = Files.list(directory)) {
            return files.filter(file -> file.getFileName().toString().endsWith(".payload")).toList();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static PayloadReference reference(byte[] payload) throws Exception {
        String sha256 = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
        return new PayloadReference(1, PayloadReference.STORAGE, "s3://bucket/payloads/orders/m-1", payload.length,
                sha256, "application/octet-stream", Instant.now().plus(Duration.ofDays(1)).toString());
    }
}
