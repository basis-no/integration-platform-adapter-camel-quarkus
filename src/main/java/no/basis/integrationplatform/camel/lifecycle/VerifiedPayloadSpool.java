package no.basis.integrationplatform.camel.lifecycle;

import io.quarkus.runtime.StartupEvent;
import jakarta.inject.Singleton;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import no.basis.integrationplatform.camel.payload.PayloadReference;
import no.basis.integrationplatform.camel.payload.PayloadStorageException;
import no.basis.integrationplatform.camel.payload.UnresolvablePayloadException;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Copies an object-stored input to a local file while checking its size and SHA-256 against
 * its reference, so that a route reads only bytes that were checked in full. Memory stays
 * bounded by one copy buffer; the copies in flight are bounded by {@code spool-max-bytes}.
 */
@Singleton
class VerifiedPayloadSpool {
    private static final Logger LOG = LoggerFactory.getLogger(VerifiedPayloadSpool.class);
    static final String DIRECTORY_PROPERTY = "basis.message-store.payload.spool-directory";
    static final String MAX_BYTES_PROPERTY = "basis.message-store.payload.spool-max-bytes";
    private static final String PREFIX = "spool-";
    private static final String SUFFIX = ".payload";
    private static final int BUFFER_BYTES = 64 * 1024;

    private final Path directory;
    private final long maxBytes;
    private final AtomicLong reserved = new AtomicLong();
    private final String owner;

    /**
     * Creates the spool from configuration.
     *
     * @param directory configured spool directory; the JVM's temporary directory by default
     * @param maxBytes bytes of checked copies one process may hold at once
     */
    @Inject
    VerifiedPayloadSpool(
            @ConfigProperty(name = DIRECTORY_PROPERTY) Optional<String> directory,
            @ConfigProperty(name = MAX_BYTES_PROPERTY,
                    defaultValue = "" + PayloadCapturePolicy.DEFAULT_OBJECT_MAX_BYTES) long maxBytes) {
        this(directory.filter(value -> !value.isBlank()).map(Path::of).orElseGet(VerifiedPayloadSpool::defaultDirectory),
                maxBytes);
    }

    VerifiedPayloadSpool(Path directory, long maxBytes) {
        if (maxBytes <= 0) {
            throw new IllegalArgumentException(MAX_BYTES_PROPERTY + " must be > 0");
        }
        // One subdirectory per host, so that replicas sharing a volume never sweep each other's copies.
        this.directory = directory.resolve(hostSegment());
        this.maxBytes = maxBytes;
        this.owner = ownerTag(ProcessHandle.current());
    }

    /** The spool used without CDI: the JVM's temporary directory and the object payload safety ceiling. */
    static VerifiedPayloadSpool defaults() {
        return new VerifiedPayloadSpool(defaultDirectory(), PayloadCapturePolicy.OBJECT_MAX_SAFETY_CEILING_BYTES);
    }

    private static Path defaultDirectory() {
        return Path.of(System.getProperty("java.io.tmpdir"), "basis-payload-spool");
    }

    private static String hostSegment() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (IOException | RuntimeException e) {
            host = "local";
        }
        String safe = host.replaceAll("[^A-Za-z0-9._-]", "_");
        return safe.isEmpty() || safe.startsWith(".") ? "local" : safe;
    }

    Path directory() {
        return directory;
    }

    long maxBytes() {
        return maxBytes;
    }

    long reservedBytes() {
        return reserved.get();
    }

    void sweepAtStartup(@Observes StartupEvent ignored) {
        deleteLeftovers();
    }

    /**
     * Refuses a spool that cannot hold the largest object payload the policy accepts.
     *
     * @param payloadPolicy the effective payload policy
     */
    void validateFor(PayloadCapturePolicy payloadPolicy) {
        if (payloadPolicy.mode() == PayloadMode.TIERED && maxBytes < payloadPolicy.objectMaxBytes()) {
            throw new IllegalStateException(MAX_BYTES_PROPERTY + " (" + maxBytes + ") is smaller than "
                    + "basis.message-store.payload.object-max-bytes (" + payloadPolicy.objectMaxBytes()
                    + "): an object-stored input of the largest accepted size could never be checked before "
                    + "its route reads it. Raise " + MAX_BYTES_PROPERTY + " or lower object-max-bytes");
        }
    }

    /**
     * Deletes checked copies left by a process that is no longer running, such as after a crash.
     * A copy of a running process, including this one, is kept.
     */
    void deleteLeftovers() {
        if (!Files.isDirectory(directory)) {
            return;
        }
        int deleted = 0;
        try (DirectoryStream<Path> files = Files.newDirectoryStream(directory, PREFIX + "*" + SUFFIX)) {
            for (Path file : files) {
                Boolean running = ownerIsRunning(file.getFileName().toString());
                if (running == null || running) {
                    // A name this spool did not write, or a copy still in use, stays.
                    continue;
                }
                try {
                    if (Files.deleteIfExists(file)) {
                        deleted++;
                    }
                } catch (IOException e) {
                    LOG.warn("Could not delete checked payload copy {}: {}", file, e.toString());
                }
            }
        } catch (IOException | java.nio.file.DirectoryIteratorException e) {
            LOG.warn("Could not clean up checked payload copies in {}: {}", directory, e.toString());
        }
        if (deleted > 0) {
            LOG.info("Deleted {} checked payload copies left by a process that is no longer running in {}",
                    deleted, directory);
        }
    }

    /**
     * Copies the object to a local file, checking its size and SHA-256 against the reference,
     * and opens the checked copy. The copy is deleted when the returned stream is closed, and
     * at once if the check fails.
     *
     * @param reference the input's reference
     * @param object opens the object's bytes
     * @return a stream of the checked copy
     * @throws PayloadStorageException when the object does not match, cannot be read, or there is no room
     */
    InputStream checkedCopy(PayloadReference reference, Supplier<InputStream> object) {
        long size = reference.sizeBytes();
        reserve(reference, size);
        Path file = null;
        FileChannel channel = null;
        boolean checked = false;
        try {
            createDirectory();
            file = directory.resolve(PREFIX + owner + "-" + UUID.randomUUID() + SUFFIX);
            // One channel, written, checked and read back without opening the path again. Where the
            // platform supports it (Linux), DELETE_ON_CLOSE removes the name at once, so nothing
            // else can open the copy and even a crash leaves no file behind.
            channel = open(file);
            copyAndCheck(reference, object, channel);
            channel.position(0);
            InputStream copy = new CheckedCopy(
                    new BufferedInputStream(Channels.newInputStream(channel), BUFFER_BYTES), file, size);
            checked = true;
            return copy;
        } catch (PayloadStorageException e) {
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new PayloadStorageException("Object payload " + reference.ref()
                    + " could not be copied for checking to " + directory + ": " + e.getClass().getSimpleName(), e);
        } finally {
            // Also after an Error: the copy and its room go unless the stream was handed out.
            if (!checked) {
                closeQuietly(channel);
                release(size);
                deleteQuietly(file);
            }
        }
    }

    private void createDirectory() throws IOException {
        if (posix()) {
            Files.createDirectories(directory, ownerOnly("rwx------"));
        } else {
            Files.createDirectories(directory);
        }
    }

    private static FileChannel open(Path file) throws IOException {
        Set<OpenOption> options = Set.of(StandardOpenOption.CREATE_NEW, StandardOpenOption.READ,
                StandardOpenOption.WRITE, StandardOpenOption.DELETE_ON_CLOSE);
        return posix()
                ? FileChannel.open(file, options, ownerOnly("rw-------"))
                : FileChannel.open(file, options);
    }

    private static boolean posix() {
        return FileSystems.getDefault().supportedFileAttributeViews().contains("posix");
    }

    private static FileAttribute<?> ownerOnly(String permissions) {
        return PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(permissions));
    }

    private void copyAndCheck(PayloadReference reference, Supplier<InputStream> object, FileChannel output)
            throws IOException {
        MessageDigest digest = sha256();
        long count = 0;
        byte[] buffer = new byte[BUFFER_BYTES];
        try (InputStream input = object.get()) {
            int read;
            while ((read = input.read(buffer)) >= 0) {
                count += read;
                if (count > reference.sizeBytes()) {
                    throw new UnresolvablePayloadException("Object payload " + reference.ref()
                            + " is larger than its reference's " + reference.sizeBytes() + " bytes");
                }
                digest.update(buffer, 0, read);
                ByteBuffer chunk = ByteBuffer.wrap(buffer, 0, read);
                while (chunk.hasRemaining()) {
                    output.write(chunk);
                }
            }
        } catch (PayloadStorageException e) {
            throw e;
        } catch (IOException e) {
            if (e.getCause() instanceof PayloadStorageException storage) {
                throw storage;
            }
            throw e;
        }
        if (count < reference.sizeBytes()) {
            // A stream can end early when its transport is cut, so it says nothing about the object.
            throw new PayloadStorageException("Object payload " + reference.ref() + " ended after " + count
                    + " of its reference's " + reference.sizeBytes() + " bytes");
        }
        String actual = HexFormat.of().formatHex(digest.digest());
        if (!actual.equalsIgnoreCase(reference.sha256())) {
            throw new UnresolvablePayloadException("Object payload " + reference.ref()
                    + " does not match its reference's SHA-256");
        }
    }

    private void reserve(PayloadReference reference, long size) {
        while (true) {
            long current = reserved.get();
            if (size > maxBytes - current) {
                throw new PayloadStorageException("Object payload " + reference.ref() + " (" + size
                        + " bytes) cannot be checked now: checked copies of " + current + " bytes are in use and "
                        + MAX_BYTES_PROPERTY + " is " + maxBytes);
            }
            if (reserved.compareAndSet(current, current + size)) {
                return;
            }
        }
    }

    private void release(long size) {
        reserved.addAndGet(-size);
    }

    private static void closeQuietly(FileChannel channel) {
        if (channel == null) {
            return;
        }
        try {
            channel.close();
        } catch (IOException e) {
            LOG.debug("Could not close a checked payload copy: {}", e.toString());
        }
    }

    private static void deleteQuietly(Path file) {
        if (file == null) {
            return;
        }
        try {
            Files.deleteIfExists(file);
        } catch (IOException e) {
            LOG.warn("Could not delete checked payload copy {}: {}", file, e.toString());
        }
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (Exception e) {
            throw new PayloadStorageException("SHA-256 is unavailable", e);
        }
    }

    /** {@code <pid>-<start millis>}, so a reused process id of a later process does not keep a leftover. */
    static String ownerTag(ProcessHandle process) {
        long started = process.info().startInstant().map(Instant::toEpochMilli).orElse(0L);
        return process.pid() + "-" + started;
    }

    /**
     * Whether the process that wrote a copy still runs: {@code null} for a name this spool does
     * not write, which the sweep leaves alone.
     */
    private static Boolean ownerIsRunning(String fileName) {
        String[] parts = fileName.substring(PREFIX.length()).split("-", 3);
        if (parts.length < 3) {
            return null;
        }
        long pid;
        long started;
        try {
            pid = Long.parseLong(parts[0]);
            started = Long.parseLong(parts[1]);
        } catch (NumberFormatException e) {
            return null;
        }
        return ProcessHandle.of(pid)
                .filter(ProcessHandle::isAlive)
                .map(process -> ownerTag(process).equals(pid + "-" + started))
                .orElse(false);
    }

    /** The checked copy; closing it deletes the file and releases its share of the spool once. */
    private final class CheckedCopy extends FilterInputStream {
        private final Path file;
        private final long size;
        private final AtomicBoolean closed = new AtomicBoolean();

        private CheckedCopy(InputStream input, Path file, long size) {
            super(input);
            this.file = file;
            this.size = size;
        }

        @Override
        public void close() throws IOException {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            try {
                super.close();
            } finally {
                deleteQuietly(file);
                release(size);
            }
        }
    }
}
