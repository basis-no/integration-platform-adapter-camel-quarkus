package no.basis.integrationplatform.camel.replay;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.List;

/** HTTP admin boundary used by the dashboard agent to request audited replay. */
@ApplicationScoped
@Path("/admin/messages")
public class MessageReplayAdminResource {
    private static final String DASHBOARD_USER_HEADER = "x-dashboard-user";
    private static final String DASHBOARD_REASON_HEADER = "x-dashboard-reason";

    private final MessageReplayService replayService;
    private final ObjectMapper objectMapper;

    /**
     * Creates the replay resource.
     *
     * @param replayService audited replay service
     * @param objectMapper JSON request parser
     */
    public MessageReplayAdminResource(MessageReplayService replayService, ObjectMapper objectMapper) {
        this.replayService = replayService;
        this.objectMapper = objectMapper;
    }

    /**
     * Requests replay of one stored message.
     *
     * @param messageId stable platform message id
     * @param requestedBy operator identity supplied by the dashboard
     * @param reason audited replay reason
     * @param body optional JSON request body
     * @return replay result or actionable failure response
     */
    @POST
    @Path("/{messageId}/resend")
    @Produces(MediaType.APPLICATION_JSON)
    public Response resend(
            @PathParam("messageId") String messageId,
            @HeaderParam(DASHBOARD_USER_HEADER) String requestedBy,
            @HeaderParam(DASHBOARD_REASON_HEADER) String reason,
            String body) {
        try {
            ReplayBody replayBody = replayBody(body);
            MessageReplayResult result = replayService.replay(new MessageReplayRequest(
                    messageId,
                    firstNonBlank(replayBody.requestedBy(), requestedBy),
                    firstNonBlank(replayBody.reason(), reason),
                    replayBody.redoSteps()));
            return Response.ok(ReplayResponse.succeeded(result)).build();
        } catch (MessageReplayException e) {
            return Response.status(statusFor(e))
                    .entity(ReplayResponse.failed(messageId, e.getMessage()))
                    .build();
        }
    }

    private ReplayBody replayBody(String body) {
        if (body == null || body.isBlank()) {
            return ReplayBody.empty();
        }
        try {
            JsonNode root = objectMapper.readTree(body);
            if (root == null || root.isNull() || !root.isObject()) {
                return ReplayBody.empty();
            }
            return new ReplayBody(
                    text(root, "requestedBy", "requested_by"),
                    text(root, "reason"),
                    redoSteps(root));
        } catch (Exception e) {
            throw new MessageReplayException("Invalid replay request body", e);
        }
    }

    private List<String> redoSteps(JsonNode root) {
        JsonNode node = first(root, "redoSteps", "redo_steps");
        if (node == null || node.isNull()) {
            return List.of();
        }
        if (!node.isArray()) {
            throw new MessageReplayException("redoSteps must be a JSON array");
        }
        List<String> steps = new ArrayList<>();
        for (JsonNode item : node) {
            if (item.isTextual() && !item.asText().isBlank()) {
                steps.add(item.asText());
            }
        }
        return steps;
    }

    private String text(JsonNode root, String... names) {
        JsonNode node = first(root, names);
        return node != null && node.isTextual() && !node.asText().isBlank() ? node.asText() : null;
    }

    private JsonNode first(JsonNode root, String... names) {
        for (String name : names) {
            JsonNode node = root.get(name);
            if (node != null) {
                return node;
            }
        }
        return null;
    }

    private String firstNonBlank(String first, String second) {
        if (first != null && !first.isBlank()) {
            return first;
        }
        return second != null && !second.isBlank() ? second : null;
    }

    private Response.Status statusFor(MessageReplayException e) {
        String message = e.getMessage();
        if (message != null && message.contains("was not found")) {
            return Response.Status.NOT_FOUND;
        }
        return Response.Status.CONFLICT;
    }

    private record ReplayBody(String requestedBy, String reason, List<String> redoSteps) {
        private ReplayBody {
            redoSteps = redoSteps == null ? List.of() : List.copyOf(redoSteps);
        }

        static ReplayBody empty() {
            return new ReplayBody(null, null, List.of());
        }
    }

    /**
     * JSON response returned by the replay admin endpoint.
     *
     * @param status request result
     * @param messageId stable platform message id
     * @param replayId replay audit id
     * @param integration integration name
     * @param topic replay target topic
     * @param attempt new platform attempt
     * @param error failure description, when present
     */
    public record ReplayResponse(
            String status,
            String messageId,
            String replayId,
            String integration,
            String topic,
            Integer attempt,
            String error) {
        static ReplayResponse succeeded(MessageReplayResult result) {
            return new ReplayResponse(
                    "SUCCEEDED",
                    result.messageId(),
                    result.replayId(),
                    result.integration(),
                    result.topic(),
                    result.attempt(),
                    null);
        }

        static ReplayResponse failed(String messageId, String error) {
            return new ReplayResponse("FAILED", messageId, null, null, null, null, error);
        }
    }
}
