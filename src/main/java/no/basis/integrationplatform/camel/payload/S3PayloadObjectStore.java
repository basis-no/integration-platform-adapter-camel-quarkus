package no.basis.integrationplatform.camel.payload;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Streaming S3-compatible immutable payload store with integrity verification. */
public final class S3PayloadObjectStore implements PayloadObjectStore {
    /** Default 8 MiB multipart upload part size. */
    public static final int DEFAULT_PART_SIZE_BYTES = 8 * 1024 * 1024;
    private static final int MIN_PART_SIZE_BYTES = 5 * 1024 * 1024;

    private final S3Client s3;
    private final String bucket;
    private final String prefix;
    private final int partSizeBytes;

    /**
     * Creates an object store using the default multipart part size.
     *
     * @param s3 Quarkus-managed S3 client
     * @param bucket dedicated payload bucket
     * @param prefix owned object-key prefix
     */
    public S3PayloadObjectStore(S3Client s3, String bucket, String prefix) {
        this(s3, bucket, prefix, DEFAULT_PART_SIZE_BYTES);
    }

    /**
     * Creates an object store with an explicit multipart part size.
     *
     * @param s3 Quarkus-managed S3 client
     * @param bucket dedicated payload bucket
     * @param prefix owned object-key prefix
     * @param partSizeBytes multipart part size
     */
    public S3PayloadObjectStore(S3Client s3, String bucket, String prefix, int partSizeBytes) {
        if (s3 == null) {
            throw new IllegalArgumentException("S3 client is required");
        }
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("S3 payload bucket is required");
        }
        if (partSizeBytes < MIN_PART_SIZE_BYTES) {
            throw new IllegalArgumentException("S3 multipart part size must be at least 5 MiB");
        }
        this.s3 = s3;
        this.bucket = bucket;
        this.prefix = normalizePrefix(prefix);
        this.partSizeBytes = partSizeBytes;
    }

    @Override
    public PayloadReference store(
            String integration,
            String messageId,
            InputStream payload,
            Long declaredSize,
            String contentType,
            Instant expiresAt,
            long maxBytes) {
        if (declaredSize != null && declaredSize > maxBytes) {
            throw tooLarge(declaredSize, maxBytes);
        }
        String key = objectKey(integration, messageId);
        String uploadId = null;
        try {
            CreateMultipartUploadRequest createRequest = CreateMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .contentType(contentType)
                    .metadata(Map.of(
                            "bip-message-id", messageId,
                            "bip-expires-at", expiresAt.toString()))
                    .build();
            uploadId = s3.createMultipartUpload(createRequest).uploadId();

            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            List<CompletedPart> parts = new ArrayList<>();
            byte[] buffer = new byte[partSizeBytes];
            long total = 0;
            int partNumber = 1;
            int count;
            while ((count = readPart(payload, buffer)) > 0) {
                total += count;
                if (total > maxBytes) {
                    throw tooLarge(total, maxBytes);
                }
                digest.update(buffer, 0, count);
                byte[] partBytes = count == buffer.length ? buffer.clone() : java.util.Arrays.copyOf(buffer, count);
                var response = s3.uploadPart(
                        UploadPartRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .uploadId(uploadId)
                                .partNumber(partNumber)
                                .contentLength((long) count)
                                .build(),
                        RequestBody.fromBytes(partBytes));
                parts.add(CompletedPart.builder().partNumber(partNumber).eTag(response.eTag()).build());
                partNumber++;
            }

            if (parts.isEmpty()) {
                s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                        .bucket(bucket).key(key).uploadId(uploadId).build());
                uploadId = null;
                s3.putObject(PutObjectRequest.builder()
                                .bucket(bucket)
                                .key(key)
                                .contentType(contentType)
                                .metadata(Map.of(
                                        "bip-message-id", messageId,
                                        "bip-expires-at", expiresAt.toString()))
                                .build(),
                        RequestBody.empty());
            } else {
                s3.completeMultipartUpload(CompleteMultipartUploadRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .uploadId(uploadId)
                        .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                        .build());
            }

            return new PayloadReference(
                    1,
                    PayloadReference.STORAGE,
                    "s3://" + bucket + "/" + key,
                    total,
                    HexFormat.of().formatHex(digest.digest()),
                    contentType,
                    expiresAt.toString());
        } catch (PayloadStorageException e) {
            abortQuietly(key, uploadId, e);
            throw e;
        } catch (Exception e) {
            abortQuietly(key, uploadId, e);
            throw new PayloadStorageException("Failed to store payload in S3-compatible object storage", e);
        }
    }

    @Override
    public InputStream open(PayloadReference reference) {
        ObjectLocation location = validateLocation(reference);
        verify(reference);
        try {
            ResponseInputStream<GetObjectResponse> input = s3.getObject(GetObjectRequest.builder()
                    .bucket(location.bucket())
                    .key(location.key())
                    .build());
            return new VerifyingInputStream(input, reference.sizeBytes(), reference.sha256());
        } catch (Exception e) {
            throw new PayloadStorageException("Failed to open payload object " + reference.ref(), e);
        }
    }

    @Override
    public void verify(PayloadReference reference) {
        ObjectLocation location = validateLocation(reference);
        try {
            HeadObjectResponse head = s3.headObject(HeadObjectRequest.builder()
                    .bucket(location.bucket())
                    .key(location.key())
                    .build());
            if (head.contentLength() != reference.sizeBytes()) {
                throw new PayloadStorageException(
                        "Payload object size mismatch for " + reference.ref()
                                + ": expected " + reference.sizeBytes() + ", got " + head.contentLength());
            }
        } catch (PayloadStorageException e) {
            throw e;
        } catch (Exception e) {
            throw new PayloadStorageException("Payload object is unavailable: " + reference.ref(), e);
        }
    }

    private ObjectLocation validateLocation(PayloadReference reference) {
        try {
            URI uri = URI.create(reference.ref());
            String key = uri.getPath() == null ? "" : uri.getPath().replaceFirst("^/", "");
            if (!"s3".equals(uri.getScheme()) || !bucket.equals(uri.getHost()) || key.isBlank()) {
                throw new PayloadStorageException("Payload reference is outside the configured bucket: " + reference.ref());
            }
            if (!prefix.isEmpty() && !key.startsWith(prefix + "/")) {
                throw new PayloadStorageException("Payload reference is outside the configured prefix: " + reference.ref());
            }
            return new ObjectLocation(uri.getHost(), key);
        } catch (PayloadStorageException e) {
            throw e;
        } catch (Exception e) {
            throw new PayloadStorageException("Invalid S3 payload reference: " + reference.ref(), e);
        }
    }

    private String objectKey(String integration, String messageId) {
        String base = safeSegment(integration) + "/" + safeSegment(messageId) + "/" + UUID.randomUUID();
        return prefix.isEmpty() ? base : prefix + "/" + base;
    }

    private String safeSegment(String value) {
        if (value == null || value.isBlank()) {
            throw new PayloadStorageException("Payload object key requires a non-empty integration and message id");
        }
        return value.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private int readPart(InputStream input, byte[] buffer) throws IOException {
        int offset = 0;
        while (offset < buffer.length) {
            int read = input.read(buffer, offset, buffer.length - offset);
            if (read < 0) {
                break;
            }
            if (read == 0) {
                continue;
            }
            offset += read;
        }
        return offset;
    }

    private PayloadStorageException tooLarge(long actual, long max) {
        return new PayloadStorageException("Payload size " + actual + " exceeds object limit " + max + " bytes");
    }

    private void abortQuietly(String key, String uploadId, Exception failure) {
        if (uploadId == null) {
            return;
        }
        try {
            s3.abortMultipartUpload(AbortMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .uploadId(uploadId)
                    .build());
        } catch (Exception abortFailure) {
            failure.addSuppressed(abortFailure);
        }
    }

    private String normalizePrefix(String value) {
        if (value == null || value.isBlank()) {
            return "";
        }
        return value.replaceAll("^/+|/+$", "");
    }

    private record ObjectLocation(String bucket, String key) {
    }

    private static final class VerifyingInputStream extends FilterInputStream {
        private final MessageDigest digest;
        private final long expectedSize;
        private final String expectedSha256;
        private long count;
        private boolean verified;

        private VerifyingInputStream(InputStream input, long expectedSize, String expectedSha256) throws Exception {
            super(input);
            this.digest = MessageDigest.getInstance("SHA-256");
            this.expectedSize = expectedSize;
            this.expectedSha256 = expectedSha256;
        }

        @Override
        public int read() throws IOException {
            int value = super.read();
            if (value < 0) {
                verifyAtEof();
            } else {
                digest.update((byte) value);
                count++;
            }
            return value;
        }

        @Override
        public int read(byte[] bytes, int offset, int length) throws IOException {
            int read = super.read(bytes, offset, length);
            if (read < 0) {
                verifyAtEof();
            } else {
                digest.update(bytes, offset, read);
                count += read;
            }
            return read;
        }

        private void verifyAtEof() throws IOException {
            if (verified) {
                return;
            }
            verified = true;
            String actual = HexFormat.of().formatHex(digest.digest());
            if (count != expectedSize || !actual.equals(expectedSha256)) {
                throw new IOException("Payload object checksum mismatch: expected " + expectedSha256
                        + "/" + expectedSize + ", got " + actual + "/" + count);
            }
        }
    }
}
