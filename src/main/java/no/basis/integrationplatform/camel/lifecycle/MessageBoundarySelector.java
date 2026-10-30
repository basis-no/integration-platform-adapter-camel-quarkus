package no.basis.integrationplatform.camel.lifecycle;

import org.apache.camel.NamedNode;
import org.apache.camel.model.RouteDefinition;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

final class MessageBoundarySelector {
    static final String DEFAULT_SELECTION = "auto";

    private static final String AUTO = "auto";
    private static final String ALL = "all";

    private final Set<String> selections;

    private MessageBoundarySelector(Set<String> selections) {
        this.selections = selections;
    }

    static MessageBoundarySelector fromConfig(String configuredRoutes) {
        String value = configuredRoutes == null || configuredRoutes.isBlank()
                ? DEFAULT_SELECTION
                : configuredRoutes;
        Set<String> selections = Arrays.stream(value.split(","))
                .map(String::trim)
                .filter(selection -> !selection.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
        return new MessageBoundarySelector(selections.isEmpty() ? Set.of(AUTO) : selections);
    }

    boolean includes(String routeId, NamedNode route) {
        if (selections.contains(ALL) || selections.contains(routeId)) {
            return true;
        }
        return selections.contains(AUTO) && kind(route) == MessageBoundaryKind.KAFKA;
    }

    MessageBoundaryKind kind(NamedNode route) {
        if (!(route instanceof RouteDefinition routeDefinition) || routeDefinition.getInput() == null) {
            return MessageBoundaryKind.OTHER;
        }
        String endpointUri = routeDefinition.getInput().getEndpointUri();
        if (endpointUri == null) {
            return MessageBoundaryKind.OTHER;
        }
        int separator = endpointUri.indexOf(':');
        String scheme = separator < 0 ? endpointUri : endpointUri.substring(0, separator);
        if ("kafka".equalsIgnoreCase(scheme)) {
            return MessageBoundaryKind.KAFKA;
        }
        if ("platform-http".equalsIgnoreCase(scheme)) {
            return MessageBoundaryKind.HTTP;
        }
        return MessageBoundaryKind.OTHER;
    }
}
