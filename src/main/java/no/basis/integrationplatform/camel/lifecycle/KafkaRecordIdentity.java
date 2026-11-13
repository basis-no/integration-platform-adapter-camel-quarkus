package no.basis.integrationplatform.camel.lifecycle;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/**
 * The message id derived from a Kafka record's position, shared with the .NET adapter: the UUID
 * made from the first 16 bytes of SHA-256 over {@code kafka-record LF integration LF topic LF
 * partition LF offset}, with the version and variant bits of a name-based UUID.
 */
final class KafkaRecordIdentity {
    private KafkaRecordIdentity() {
    }

    static String derive(String integration, String topic, int partition, long offset) {
        String text = "kafka-record\n" + integration + "\n" + topic + "\n" + partition + "\n" + offset;
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            digest[6] = (byte) ((digest[6] & 0x0f) | 0x50);
            digest[8] = (byte) ((digest[8] & 0x3f) | 0x80);
            ByteBuffer bytes = ByteBuffer.wrap(digest);
            return new UUID(bytes.getLong(), bytes.getLong()).toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is not available", e);
        }
    }
}
