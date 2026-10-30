package no.basis.integrationplatform.camel.store;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;

public class MessageStatusEventJson {
    private final ObjectMapper objectMapper;

    public MessageStatusEventJson() {
        this(defaultObjectMapper());
    }

    public MessageStatusEventJson(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public String serialize(MessageAttemptResult result) {
        try {
            return objectMapper.writeValueAsString(MessageStatusEvent.from(result));
        } catch (JsonProcessingException e) {
            throw new MessageStatusEventJsonException("Failed to serialize message status event", e);
        }
    }

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
