package no.basis.integrationplatform.camel.step;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DefaultStepClassifierTest {
    @Test
    void classifiesRoutingTransformationAndObservabilitySchemesAsInternal() {
        DefaultStepClassifier classifier = DefaultStepClassifier.defaults();

        for (String uri : new String[]{"direct:transform", "xslt:order.xsl", "log:audit", "micrometer:counter"}) {
            assertEquals(StepRole.INTERNAL, classify(classifier, uri, "node", null));
        }
    }

    @Test
    void classifiesSafeHttpMethodsAsRepeatable() {
        DefaultStepClassifier classifier = DefaultStepClassifier.defaults();

        for (String method : new String[]{"GET", "HEAD", "OPTIONS", "TRACE", " get "}) {
            assertEquals(StepRole.REPEATABLE,
                    classify(classifier, "https://api.example.test/lookup", "fetch-data", method));
        }
    }

    @Test
    void classifiesKnownProducerSchemesAsSideEffects() {
        DefaultStepClassifier classifier = DefaultStepClassifier.defaults();

        for (String uri : new String[]{"kafka:orders", "sftp:outbox", "file:out", "jms:queue:orders"}) {
            assertEquals(StepRole.SIDE_EFFECT, classify(classifier, uri, "publish", null));
        }
    }

    @Test
    void leavesUnsafeHttpAndUnknownSchemesAmbiguous() {
        DefaultStepClassifier classifier = DefaultStepClassifier.defaults();

        assertEquals(StepRole.AMBIGUOUS,
                classify(classifier, "https://api.example.test/token", "fetch-token", "POST"));
        assertEquals(StepRole.AMBIGUOUS,
                classify(classifier, "https://api.example.test/default-method", "default-method", null));
        assertEquals(StepRole.AMBIGUOUS,
                classify(classifier, "aws2-s3:archive", "s3-operation", null));
        assertEquals(StepRole.AMBIGUOUS,
                classify(classifier, "custom-client:operation", "custom-operation", null));
    }

    @Test
    void classifiesKnownSchemeWhenNodeIdIsMissing() {
        DefaultStepClassifier classifier = new DefaultStepClassifier("", "", "configured-node");

        assertEquals(StepRole.SIDE_EFFECT, classify(classifier, "kafka:orders", null, null));
    }

    @Test
    void explicitNodeRolesOverrideAutomaticSchemeAndMethodClassification() {
        DefaultStepClassifier classifier = new DefaultStepClassifier(
                " ignore-kafka-send ",
                "fetch-token, lookup-account",
                "create-order, publish-event, upload-archive");

        assertEquals(StepRole.INTERNAL,
                classify(classifier, "kafka:orders", "ignore-kafka-send", null));
        assertEquals(StepRole.REPEATABLE,
                classify(classifier, "https://api.example.test/token", "fetch-token", "POST"));
        assertEquals(StepRole.SIDE_EFFECT,
                classify(classifier, "https://api.example.test/orders", "create-order", "POST"));
        assertEquals(StepRole.REPEATABLE,
                classify(classifier, "custom-client:lookup", "lookup-account", null));
        assertEquals(StepRole.SIDE_EFFECT,
                classify(classifier, "https://api.example.test/events", "publish-event", "POST"));
        assertEquals(StepRole.SIDE_EFFECT,
                classify(classifier, "aws2-s3:archive", "upload-archive", null));
    }

    @Test
    void rejectsConflictingNodeClassifications() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new DefaultStepClassifier("shared-node", "shared-node", ""));

        assertEquals("Step node id 'shared-node' is configured as both internal and repeatable", thrown.getMessage());
    }

    @Test
    void explicitlyClassifiesOutputProducingSideEffect() {
        DefaultStepClassifier classifier = new DefaultStepClassifier("", "", "", "create-order");

        assertEquals(StepRole.OUTPUT_SIDE_EFFECT,
                classify(classifier, "https://api.example.test/orders", "create-order", "POST"));
    }

    @Test
    void rejectsOutputSideEffectConfiguredWithAnotherRole() {
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
                () -> new DefaultStepClassifier("", "", "create-order", "create-order"));

        assertEquals(
                "Step node id 'create-order' is configured as both side_effect and output_side_effect",
                thrown.getMessage());
    }

    private StepRole classify(DefaultStepClassifier classifier, String uri, String nodeId, String httpMethod) {
        Exchange exchange = new DefaultExchange(new DefaultCamelContext());
        if (httpMethod != null) {
            exchange.getMessage().setHeader(Exchange.HTTP_METHOD, httpMethod);
        }
        return classifier.classify(new StepClassificationContext(uri, nodeId, exchange));
    }
}
