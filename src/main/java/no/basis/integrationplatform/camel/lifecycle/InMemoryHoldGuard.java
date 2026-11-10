package no.basis.integrationplatform.camel.lifecycle;

import org.apache.camel.CamelContext;
import org.apache.camel.NamedNode;
import org.apache.camel.model.AggregateDefinition;
import org.apache.camel.model.CircuitBreakerDefinition;
import org.apache.camel.model.EnrichDefinition;
import org.apache.camel.model.Model;
import org.apache.camel.model.OnFallbackDefinition;
import org.apache.camel.model.ProcessorDefinition;
import org.apache.camel.model.ProcessorDefinitionHelper;
import org.apache.camel.model.ResequenceDefinition;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.model.ToDefinition;
import org.apache.camel.model.ToDynamicDefinition;
import org.apache.camel.processor.aggregate.MemoryAggregationRepository;
import org.apache.camel.spi.AggregationRepository;
import org.apache.camel.support.PropertyBindingSupport;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Refuses a managed Kafka route that holds records in memory after its input completes.
 *
 * <p>Camel's {@code resequence} and an {@code aggregate} on its in-memory repository keep a
 * record after the input exchange has completed, and the adapter commits a completed input. A
 * restart before the held record is released then loses it: it is neither delivered nor read
 * again. The check covers the boundary route and the {@code direct:} routes it sends to with a
 * fixed URI ({@code to}, a {@code toD} or {@code enrich} without {@code ${...}}, property
 * placeholders resolved), transitively, including circuit-breaker fallbacks.
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
            for (AggregateDefinition aggregate : nodes(outputs, AggregateDefinition.class)) {
                String repository = inMemory(camelContext, aggregate);
                if (repository != null) {
                    holds.add("aggregate " + nodeName(aggregate) + " in route '" + currentId + "' (" + repository + ")");
                }
            }
            for (ResequenceDefinition resequence : nodes(outputs, ResequenceDefinition.class)) {
                holds.add("resequence " + nodeName(resequence) + " in route '" + currentId + "'");
            }
            List<String> sends = new ArrayList<>();
            nodes(outputs, ToDefinition.class).forEach(to -> sends.add(to.getUri()));
            nodes(outputs, ToDynamicDefinition.class).forEach(to -> sends.add(to.getUri()));
            for (EnrichDefinition enrich : nodes(outputs, EnrichDefinition.class)) {
                if (enrich.getExpression() != null) {
                    sends.add(enrich.getExpression().getExpression());
                }
            }
            for (String send : sends) {
                String target = directName(camelContext, send);
                if (target == null) {
                    continue;
                }
                for (RouteDefinition candidate : all) {
                    if (candidate.getInput() != null
                            && target.equals(directName(camelContext, candidate.getInput().getEndpointUri()))) {
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

    /** The nodes of a type in these outputs, including the fallbacks of circuit breakers. */
    private static <T> List<T> nodes(List<ProcessorDefinition<?>> outputs, Class<T> type) {
        Set<T> found = Collections.newSetFromMap(new IdentityHashMap<>());
        found.addAll(ProcessorDefinitionHelper.filterTypeInOutputs(outputs, type));
        for (CircuitBreakerDefinition breaker : ProcessorDefinitionHelper.filterTypeInOutputs(outputs, CircuitBreakerDefinition.class)) {
            OnFallbackDefinition fallback = breaker.getOnFallback();
            if (fallback != null) {
                found.addAll(nodes(fallback.getOutputs(), type));
            }
        }
        return new ArrayList<>(found);
    }

    private static List<RouteDefinition> routeDefinitions(CamelContext camelContext) {
        Model model = camelContext.getCamelContextExtension().getContextPlugin(Model.class);
        return model == null ? List.of() : model.getRouteDefinitions();
    }

    /**
     * Why an aggregate's repository holds records only in memory, or {@code null} when it does not.
     * Only Camel's {@link MemoryAggregationRepository} is recognised as in-memory; whether another
     * repository is durable is the application's responsibility. A reference that cannot be
     * resolved here cannot be shown to be durable, so it counts as in memory.
     */
    private static String inMemory(CamelContext camelContext, AggregateDefinition aggregate) {
        AggregationRepository repository = aggregate.getAggregationRepositoryBean();
        String reference = aggregate.getAggregationRepository();
        if (repository == null && (reference == null || reference.isBlank())) {
            return "in-memory repository";
        }
        if (repository == null) {
            Object found = resolve(camelContext, reference.trim());
            if (!(found instanceof AggregationRepository resolved)) {
                return "repository '" + reference + "' could not be resolved to show that it is not in memory";
            }
            repository = resolved;
        }
        return repository instanceof MemoryAggregationRepository ? "in-memory repository" : null;
    }

    private static Object resolve(CamelContext camelContext, String reference) {
        if (camelContext == null) {
            return null;
        }
        try {
            // #bean:, #class: and #type: as Camel resolves them; a plain name from the registry.
            return reference.startsWith("#") ? PropertyBindingSupport.resolveBean(camelContext, reference)
                    : camelContext.getRegistry().lookupByName(reference);
        } catch (Exception e) {
            return null;
        }
    }

    private static String nodeName(ProcessorDefinition<?> node) {
        return node.getId() != null ? "'" + node.getId() + "'" : "(no id)";
    }

    private static String directName(CamelContext camelContext, String uri) {
        if (uri == null || uri.contains("${")) {
            return null;
        }
        if (camelContext != null && uri.contains("{{")) {
            try {
                uri = camelContext.resolvePropertyPlaceholders(uri);
            } catch (RuntimeException e) {
                return null;
            }
        }
        if (!uri.startsWith("direct:")) {
            return null;
        }
        String name = uri.substring("direct:".length());
        if (name.startsWith("//")) {
            name = name.substring(2);
        }
        int query = name.indexOf('?');
        name = query < 0 ? name : name.substring(0, query);
        return name.isEmpty() || name.contains("{{") ? null : name;
    }
}
