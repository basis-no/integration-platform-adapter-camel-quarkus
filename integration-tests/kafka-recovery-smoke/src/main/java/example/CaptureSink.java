package example;

import jakarta.enterprise.context.ApplicationScoped;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import no.basis.integrationplatform.camel.model.MessageAttemptResult;
import no.basis.integrationplatform.camel.store.MessageStoreSink;

/** Lifecycle observations only: this fixture does not claim SQL durability. */
@ApplicationScoped
public class CaptureSink implements MessageStoreSink {
    private final List<MessageAttemptResult> observed = new CopyOnWriteArrayList<>();
    @Override
    public void record(MessageAttemptResult result) { observed.add(result); }
    public List<MessageAttemptResult> snapshot() { return List.copyOf(observed); }
}
