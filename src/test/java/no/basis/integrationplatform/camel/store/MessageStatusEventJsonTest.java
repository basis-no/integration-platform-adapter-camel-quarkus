package no.basis.integrationplatform.camel.store;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.model.MessageStatus;
import no.basis.integrationplatform.camel.model.StepResult;
import no.basis.integrationplatform.camel.model.StepStatus;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MessageStatusEventJsonTest {
    private final MessageStatusEventJson serializer = new MessageStatusEventJson();
    private final ObjectMapper reader = new ObjectMapper().registerModule(new JavaTimeModule());

    @Test
    void serializesTerminalStatusEventContract() throws Exception {
        MessageAttemptResult result = new MessageAttemptResult(
                1,
                "message-123",
                "order-456",
                "order-pipeline",
                "camel-route-id",
                2,
                Instant.parse("2026-06-14T12:00:00Z"),
                MessageStatus.COMPLETED,
                List.of(new StepResult(
                        "sftp",
                        StepStatus.DELIVERED,
                        "message-123:sftp",
                        "sftp:vendor/orders",
                        "vendor/orders/order-456.xml",
                        null,
                        Instant.parse("2026-06-14T12:00:01Z"))),
                null);

        JsonNode json = reader.readTree(serializer.serialize(result));

        assertEquals(1, json.get("schemaVersion").asInt());
        assertEquals("message-123", json.get("messageId").asText());
        assertEquals("order-456", json.get("correlationId").asText());
        assertEquals("order-pipeline", json.get("integration").asText());
        assertEquals(2, json.get("attempt").asInt());
        assertEquals("2026-06-14T12:00:00Z", json.get("occurredAt").asText());
        assertEquals("COMPLETED", json.get("status").asText());
        assertTrue(json.get("error").isNull());

        JsonNode step = json.get("steps").get(0);
        assertEquals("sftp", step.get("step").asText());
        assertEquals("DELIVERED", step.get("status").asText());
        assertEquals("message-123:sftp", step.get("idempotencyKey").asText());
        assertEquals("sftp:vendor/orders", step.get("endpointUri").asText());
        assertEquals("vendor/orders/order-456.xml", step.get("targetRef").asText());
        assertTrue(step.get("error").isNull());
        assertEquals("2026-06-14T12:00:01Z", step.get("occurredAt").asText());

        assertFalse(json.has("routeId"), "Camel route id must not leak into the platform status event");
    }

    @Test
    void stepShapeMatchesStepsJsonSchemaFieldSet() throws Exception {
        // Contract §7 (DECISION D4): steps_json entries carry exactly the fields
        // defined in spec/schemas/steps-json.schema.json (additionalProperties: false).
        MessageAttemptResult result = new MessageAttemptResult(
                1,
                "message-123",
                "order-456",
                "order-pipeline",
                "camel-route-id",
                1,
                Instant.parse("2026-06-14T12:00:00Z"),
                MessageStatus.COMPLETED,
                List.of(new StepResult(
                        "sftp",
                        StepStatus.DELIVERED,
                        "message-123:sftp",
                        "sftp:vendor/orders",
                        null,
                        null,
                        Instant.parse("2026-06-14T12:00:01Z"))),
                null);

        JsonNode steps = reader.readTree(serializer.serializeSteps(result));

        assertTrue(steps.isArray());
        JsonNode step = steps.get(0);
        java.util.Set<String> fields = new java.util.TreeSet<>();
        step.fieldNames().forEachRemaining(fields::add);
        assertEquals(
                new java.util.TreeSet<>(List.of(
                        "step", "status", "idempotencyKey", "endpointUri", "targetRef", "error", "occurredAt")),
                fields);
        assertEquals("2026-06-14T12:00:01Z", step.get("occurredAt").asText());
        assertTrue(step.get("targetRef").isNull());
        assertTrue(step.get("error").isNull());
    }

    @Test
    void serializesFailedStatusWithError() throws Exception {
        MessageAttemptResult result = new MessageAttemptResult(
                1,
                "message-123",
                "order-456",
                "order-pipeline",
                "camel-route-id",
                1,
                Instant.parse("2026-06-14T12:00:00Z"),
                MessageStatus.FAILED,
                List.of(new StepResult(
                        "webhook",
                        StepStatus.FAILED,
                        "message-123:webhook",
                        "https://vendor.example.com/callback",
                        null,
                        "vendor unavailable",
                        Instant.parse("2026-06-14T12:00:02Z"))),
                "vendor unavailable");

        JsonNode json = reader.readTree(serializer.serialize(result));

        assertEquals("FAILED", json.get("status").asText());
        assertEquals("vendor unavailable", json.get("error").asText());
        assertEquals("FAILED", json.get("steps").get(0).get("status").asText());
        assertEquals("vendor unavailable", json.get("steps").get(0).get("error").asText());
    }
}
