package no.basis.integrationplatform.camel;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.ProcessorEndpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An in-memory external receiver: its scheme is not internal, so the adapter tracks
 * sends to it as business steps. It keeps the exchanges it received.
 */
public final class ReceiverComponent extends DefaultComponent {
    private final List<Exchange> received = new ArrayList<>();
    private final Processor behaviour;

    public ReceiverComponent() {
        this(exchange -> { });
    }

    public ReceiverComponent(Processor behaviour) {
        this.behaviour = behaviour;
    }

    public synchronized List<Exchange> received() {
        return List.copyOf(received);
    }

    /** The correlation header each received exchange carried, in arrival order. */
    public List<Object> correlations() {
        return received().stream().map(exchange -> exchange.getMessage().getHeader(PlatformHeaders.CORRELATION_ID)).toList();
    }

    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
        // Accept any options, such as credentials in masking tests; the receiver ignores them.
        parameters.clear();
        return new ProcessorEndpoint(uri, this, exchange -> {
            synchronized (this) {
                received.add(exchange.copy());
            }
            behaviour.process(exchange);
        });
    }
}
