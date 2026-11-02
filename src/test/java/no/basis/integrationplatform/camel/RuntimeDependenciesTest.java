package no.basis.integrationplatform.camel;

import org.junit.jupiter.api.Test;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilderFactory;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Components the adapter's own routes need at runtime, which the test classpath cannot show. */
class RuntimeDependenciesTest {
    @Test
    void deadLetterRouteComponentIsARuntimeDependency() throws Exception {
        var factory = DocumentBuilderFactory.newInstance();
        factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
        var pom = factory.newDocumentBuilder().parse(Path.of("pom.xml").toFile());
        var project = pom.getDocumentElement();
        List<String> runtime = new ArrayList<>();
        var dependencies = project.getElementsByTagName("dependency");
        for (int i = 0; i < dependencies.getLength(); i++) {
            var dependency = (Element) dependencies.item(i);
            // Managed dependencies only set versions; only the project's own list is packaged.
            if (!"dependencies".equals(dependency.getParentNode().getNodeName())
                    || dependency.getParentNode().getParentNode() != project) {
                continue;
            }
            var scope = dependency.getElementsByTagName("scope");
            if (scope.getLength() == 0 || !"test".equals(scope.item(0).getTextContent().trim())) {
                runtime.add(dependency.getElementsByTagName("artifactId").item(0).getTextContent().trim());
            }
        }
        // PlatformDeadLetterRoute consumes from direct: in every application.
        assertEquals(1, runtime.stream().filter("camel-quarkus-direct"::equals).count(), runtime.toString());
    }
}
