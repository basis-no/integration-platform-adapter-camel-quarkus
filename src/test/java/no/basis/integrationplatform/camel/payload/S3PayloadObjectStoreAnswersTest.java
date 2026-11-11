package no.basis.integrationplatform.camel.payload;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Only the store's own answer that a key does not exist, or an object that does not match its
 * reference, makes a reference unresolvable; every answer that may change is a plain failure.
 */
class S3PayloadObjectStoreAnswersTest {
    private static final String BUCKET = "payload-bucket";
    private static final PayloadReference REFERENCE = new PayloadReference(1, PayloadReference.STORAGE,
            "s3://" + BUCKET + "/payloads/orders/m-1", 5, "0".repeat(64), "application/xml",
            Instant.now().plus(Duration.ofDays(1)).toString());

    @Test
    void aKeyTheStoreSaysDoesNotExistIsUnresolvable() {
        var failure = verify(head -> { throw notFound(); }, get -> { throw noSuchKey(); });

        assertInstanceOf(UnresolvablePayloadException.class, failure);
        assertEquals("Payload object does not exist: " + REFERENCE.ref(), failure.getMessage());
    }

    @Test
    void aHeadNotFoundWithAMissingBucketIsNotUnresolvable() {
        var failure = verify(head -> { throw notFound(); },
                get -> { throw NoSuchBucketException.builder().statusCode(404).message("bucket").build(); });

        assertMayPass(failure);
    }

    @Test
    void aRefusedAccessIsNotUnresolvable() {
        assertMayPass(verify(head -> { throw S3Exception.builder().statusCode(403).message("denied").build(); },
                get -> { throw new AssertionError("only a 404 is checked again"); }));
        assertMayPass(verify(head -> { throw notFound(); },
                get -> { throw S3Exception.builder().statusCode(403).message("denied").build(); }));
    }

    @Test
    void aTimeoutIsNotUnresolvable() {
        assertMayPass(verify(head -> { throw SdkClientException.create("Unable to execute HTTP request: Read timed out"); },
                get -> { throw new AssertionError("only a 404 is checked again"); }));
        assertMayPass(verify(head -> { throw notFound(); },
                get -> { throw SdkClientException.create("Unable to execute HTTP request: Read timed out"); }));
    }

    @Test
    void anObjectThatIsThereAfterAllIsNotUnresolvable() {
        var opened = new AtomicReference<Boolean>(false);
        var failure = verify(head -> { throw notFound(); }, get -> {
            opened.set(true);
            return new ResponseInputStream<>(GetObjectResponse.builder().build(),
                    AbortableInputStream.create(new ByteArrayInputStream(new byte[1])));
        });

        assertMayPass(failure);
        assertEquals(true, opened.get());
    }

    @Test
    void aSizeTheStoreReportsOtherThanTheReferenceIsUnresolvable() {
        var failure = verify(head -> HeadObjectResponse.builder().contentLength(6L).build(),
                get -> { throw new AssertionError("not read"); });

        assertInstanceOf(UnresolvablePayloadException.class, failure);
        assertEquals("Payload object size mismatch for " + REFERENCE.ref() + ": expected 5, got 6", failure.getMessage());
    }

    @Test
    void aReferenceOutsideTheConfiguredBucketIsNotUnresolvable() {
        var elsewhere = new PayloadReference(1, PayloadReference.STORAGE, "s3://another-bucket/payloads/orders/m-1",
                5, "0".repeat(64), "application/xml", Instant.now().plus(Duration.ofDays(1)).toString());
        var store = new S3PayloadObjectStore(s3(head -> { throw new AssertionError(); }, get -> { throw new AssertionError(); }),
                BUCKET, "payloads");

        var failure = assertThrows(PayloadStorageException.class, () -> store.verify(elsewhere));

        // The deployment's configuration may be what is wrong.
        assertFalse(failure instanceof UnresolvablePayloadException, failure.toString());
        assertEquals("Payload reference is outside the configured bucket: " + elsewhere.ref(), failure.getMessage());
    }

    @Test
    void aReferenceThatIsNoUriIsUnresolvable() {
        var malformed = new PayloadReference(1, PayloadReference.STORAGE, "s3://" + BUCKET + "/pay loads/m-1",
                5, "0".repeat(64), "application/xml", Instant.now().plus(Duration.ofDays(1)).toString());
        var store = new S3PayloadObjectStore(s3(head -> { throw new AssertionError(); }, get -> { throw new AssertionError(); }),
                BUCKET, "payloads");

        var failure = assertThrows(PayloadStorageException.class, () -> store.verify(malformed));

        assertInstanceOf(UnresolvablePayloadException.class, failure);
    }

    private static void assertMayPass(PayloadStorageException failure) {
        assertFalse(failure instanceof UnresolvablePayloadException, failure.toString());
        assertEquals("Payload object is unavailable: " + REFERENCE.ref(), failure.getMessage());
    }

    private static PayloadStorageException verify(Function<Object, Object> head, Function<GetObjectRequest, Object> get) {
        var store = new S3PayloadObjectStore(s3(head, get), BUCKET, "payloads");
        return assertThrows(PayloadStorageException.class, () -> store.verify(REFERENCE));
    }

    private static S3Exception notFound() {
        // A HEAD response has no body, so the client cannot tell which resource is missing.
        return (S3Exception) S3Exception.builder().statusCode(404).message("Not Found").build();
    }

    private static NoSuchKeyException noSuchKey() {
        return (NoSuchKeyException) NoSuchKeyException.builder().statusCode(404).message("The specified key does not exist.").build();
    }

    /** An S3 client that answers HEAD and GET object requests as given. */
    private static S3Client s3(Function<Object, Object> head, Function<GetObjectRequest, Object> get) {
        return (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(), new Class<?>[]{S3Client.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "headObject" -> head.apply(args[0]);
                    case "getObject" -> {
                        if (args.length != 1 || !(args[0] instanceof GetObjectRequest request)) {
                            throw new UnsupportedOperationException("getObject" + args.length);
                        }
                        if (!"bytes=0-0".equals(request.range())) {
                            throw new AssertionError("only one byte is asked for: " + request.range());
                        }
                        yield get.apply(request);
                    }
                    case "close" -> null;
                    case "serviceName" -> "s3";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
