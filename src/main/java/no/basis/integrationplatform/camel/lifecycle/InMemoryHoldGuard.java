package no.basis.integrationplatform.camel.lifecycle;

import org.apache.camel.CamelContext;
import org.apache.camel.NamedNode;
import org.apache.camel.model.AggregateDefinition;
import org.apache.camel.model.Model;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.ProcessorDefinitionHelper;
import org.apache.camel.model.ResequenceDefinition;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.ToDefinition;
import org.apache.camel.processor.aggregate.MemoryAggregationRepository;
import org.apache.camel.spi.AggregationRepository;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Refuses a managed Kafka route that holds records in memory after its input completes.
 *
 * <p>Camel's {@code resequence} and an {@code aggregate} on its in-memory repository keep a
 * record after the input exchange has completed, and the adapter commits a completed input. A
 * restart before the held record is released then loses it: it is neither delivered nor read
 * again. The check covers the boundary route and the {@code direct:} routes it sends to with a
 * fixed URI, transitively.
 */
final class InMemoryHoldGuard {
    private InMemoryHoldGuard() {
    }

    static void check(CamelContext camelContext, String routeId, NamedNode route) {
        if (!(route instanceof RouteDefinition definition)) {
            return;
        }
        List<RouteDefinition> all = camelContext == null ? List.of() : routeDefinitions(camelContext);
        List<String> holds = new ArrayList<>();
        Set<String> visited = new HashSet<>();
        Deque<RouteDefinition> pending = new ArrayDeque<>();
        pending.add(definition);
        while (!pending.isEmpty()) {
            RouteDefinition current = pending.poll();
            String currentId = current.getRouteId() != null ? current.getRouteId() : routeId;
            if (!visited.add(currentId)) {
                continue;
            }
            List<ProcessorDefinition<?>> outputs = current.getOutputs();
            for (AggregateDefinition aggregate : ProcessorDefinitionHelper.filterTypeInOutputs(outputs, AggregateDefinition.class)) {
                if (inMemory(camelContext, aggregate)) {
                    holds.add("aggregate " + nodeName(aggregate) + " in route '" + currentId + "' (in-memory repository)");
                }
            }
            for (ResequenceDefinition resequence : ProcessorDefinitionHelper.filterTypeInOutputs(outputs, ResequenceDefinition.class)) {
                holds.add("resequence " + nodeName(resequence) + " in route '" + currentId + "'");
            }
            for (ToDefinition to : ProcessorDefinitionHelper.filterTypeInOutputs(outputs, ToDefinition.class)) {
                String target = directName(to.getUri());
                if (target == null) {
                    continue;
                }
                for (RouteDefinition candidate : all) {
                    if (candidate.getInput() != null && target.equals(directName(candidate.getInput().getEndpointUri()))) {
                        pending.add(candidate);
                    }
                }
            }
        }
        if (!holds.isEmpty()) {
            throw new IllegalArgumentException("Kafka boundary route '" + routeId + "' holds records in memory after "
                    + "its input completes: " + String.join("; ", holds) + ". The adapter commits a completed input, "
                    + "so a restart before such a record is released loses it. Give an aggregate a persistent "
                    + "aggregationRepository; a resequence has no durable form yet.");
        }
    }

    private static List<RouteDefinition> routeDefinitions(CamelContext camelContext) {
        Model model = camelContext.getCamelContextExtension().getContextPlugin(Model.class);
        return model == null ? List.of() : model.getRouteDefinitions();
    }

    private static boolean inMemory(CamelContext camelContext, AggregateDefinition aggregate) {
        AggregationRepository repository = aggregate.getAggregationRepositoryBean();
        String reference = aggregate.getAggregationRepository();
        if (repository == null && reference != null && !reference.isBlank() && camelContext != null) {
            String name = reference.startsWith("#bean:") ? reference.substring(6)
                    : reference.startsWith("#") ? reference.substring(1) : reference;
            Object found = camelContext.getRegistry().lookupByName(name);
            if (found == null) {
                // Not resolvable yet; Camel fails the route itself if it stays missing.
                return false;
            }
            repository = found instanceof AggregationRepository resolved ? resolved : null;
        }
        return repository == null ? reference == null || reference.isBlank() : repository instanceof MemoryAggregationRepository;
    }

    private static String nodeName(ProcessorDefinition<?> node) {
        return node.getId() != null ? "'" + node.getId() + "'" : "(no id)";
    }

    private static String directName(String uri) {
        if (uri == null || !uri.startsWith("direct:")) {
            return null;
        }
        String name = uri.substring("direct:".length());
        if (name.startsWith("//")) {
            name = name.substring(2);
        }
        int query = name.indexOf('?');
        name = query < 0 ? name : name.substring(0, query);
        return name.isEmpty() || name.contains("${") || name.contains("{{") ? null : name;
    }
}
