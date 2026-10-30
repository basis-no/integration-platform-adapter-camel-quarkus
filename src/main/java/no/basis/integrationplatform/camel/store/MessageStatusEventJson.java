package no.basis.integrationplatform.camel.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;

/** Serializes terminal adapter results into contract-compatible JSON. */
public class MessageStatusEventJson {
    private final ObjectMapper objectMapper;

    /** Creates a serializer with the adapter's default Jackson configuration. */
    public MessageStatusEventJson() {
        this(defaultObjectMapper());
    }

    /**
     * Creates a serializer with a supplied object mapper.
     *
     * @param objectMapper configured JSON mapper
     */
    public MessageStatusEventJson(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Serializes a complete terminal lifecycle event.
     *
     * @param result terminal adapter result
     * @return JSON lifecycle event
     */
    public String serialize(MessageAttemptResult result) {
        try {
            return objectMapper.writeValueAsString(MessageStatusEvent.from(result));
        } catch (JsonProcessingException e) {
            throw new MessageStatusEventJsonException("Failed to serialize message status event", e);
        }
    }

    /**
     * Serializes the contract step array for Postgres JSONB storage.
     *
     * @param result terminal adapter result
     * @return JSON step array
     */
    public String serializeSteps(MessageAttemptResult result) {
        try {
            return objectMapper.writeValueAsString(MessageStatusEvent.from(result).steps());
        } catch (JsonProcessingException e) {
            throw new MessageStatusEventJsonException("Failed to serialize message status event steps", e);
        }
    }

    private static ObjectMapper defaultObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }
}
