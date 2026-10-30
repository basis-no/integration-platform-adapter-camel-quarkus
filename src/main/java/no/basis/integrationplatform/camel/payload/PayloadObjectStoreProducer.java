package no.basis.integrationplatform.camel.payload;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import no.basis.integrationplatform.camel.lifecycle.PayloadMode;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import software.amazon.awssdk.services.s3.S3Client;

import java.util.Optional;

/** CDI producer selecting disabled or S3-compatible object payload storage. */
@ApplicationScoped
public class PayloadObjectStoreProducer {
    /** Creates the stateless CDI producer. */
    public PayloadObjectStoreProducer() {
    }

    @Produces
    @DefaultBean
    @ApplicationScoped
    PayloadObjectStore payloadObjectStore(
            @ConfigProperty(name = "basis.message-store.payload.mode", defaultValue = "postgres-inline") String mode,
            @ConfigProperty(name = "basis.message-store.payload.s3.bucket") Optional<String> bucket,
            @ConfigProperty(name = "basis.message-store.payload.s3.prefix", defaultValue = "payloads") String prefix,
            @ConfigProperty(name = "basis.message-store.payload.s3.part-size-bytes",
                    defaultValue = "" + S3PayloadObjectStore.DEFAULT_PART_SIZE_BYTES) int partSizeBytes,
            Instance<S3Client> s3Client) {
        if (PayloadMode.fromConfig(mode) != PayloadMode.TIERED) {
            return PayloadObjectStore.disabled();
        }
        if (!s3Client.isResolvable()) {
            throw new IllegalStateException(
                    "basis.message-store.payload.mode=tiered requires a configured Quarkus S3 client");
        }
        return new S3PayloadObjectStore(s3Client.get(), bucket.orElse(""), prefix, partSizeBytes);
    }
}
