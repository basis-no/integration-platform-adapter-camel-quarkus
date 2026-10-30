package no.basis.integrationplatform.camel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class DocumentationCoverageTest {
    private static final Pattern CONFIG_KEY = Pattern.compile("basis\\.message-store\\.[a-z0-9.-]+");
    private static final Pattern CONFIG_ROW = Pattern.compile("^\\| `(?<key>basis\\.message-store\\.[a-z0-9.-]+)`"
            + " \\| `(?<environment>[A-Z0-9_]+)` \\|");
    private static final Pattern RELATIVE_MARKDOWN_LINK = Pattern.compile("\\[[^]]+]\\((?!https?://|#)([^)#]+\\.md)(?:#[^)]+)?\\)");

    @Test
    void documentsEveryAdapterPropertyAndEnvironmentMapping() throws IOException {
        Path root = Path.of("").toAbsolutePath();
        Set<String> implementedProperties = implementedProperties(root);
        Path reference = root.resolve("docs/configuration.md");
        Set<String> documentedProperties = new TreeSet<>();
        List<String> incorrectMappings = new ArrayList<>();

        for (String line : Files.readAllLines(reference)) {
            Matcher matcher = CONFIG_ROW.matcher(line);
            if (matcher.find()) {
                String key = matcher.group("key");
                documentedProperties.add(key);
                String actualEnvironment = matcher.group("environment");
                String expectedEnvironment = environmentName(key);
                if (!expectedEnvironment.equals(actualEnvironment)) {
                    incorrectMappings.add(key + " expected " + expectedEnvironment + " but documented " + actualEnvironment);
                }
            }
        }

        assertEquals(implementedProperties, documentedProperties,
                "docs/configuration.md must contain exactly the implemented adapter properties");
        assertTrue(incorrectMappings.isEmpty(), "Incorrect environment mappings: " + incorrectMappings);
    }

    @Test
    void localMarkdownLinksResolve() throws IOException {
        Path root = Path.of("").toAbsolutePath();
        List<Path> markdownFiles = new ArrayList<>();
        markdownFiles.add(root.resolve("README.md"));
        try (Stream<Path> paths = Files.walk(root.resolve("docs"))) {
            paths.filter(path -> path.toString().endsWith(".md")).forEach(markdownFiles::add);
        }

        List<String> brokenLinks = new ArrayList<>();
        for (Path markdownFile : markdownFiles) {
            Matcher matcher = RELATIVE_MARKDOWN_LINK.matcher(Files.readString(markdownFile));
            while (matcher.find()) {
                Path target = markdownFile.getParent().resolve(matcher.group(1)).normalize();
                if (!Files.isRegularFile(target)) {
                    brokenLinks.add(root.relativize(markdownFile) + " -> " + matcher.group(1));
                }
            }
        }

        assertTrue(brokenLinks.isEmpty(), "Broken relative Markdown links: " + brokenLinks);
    }

    private static Set<String> implementedProperties(Path root) throws IOException {
        Set<String> properties = new TreeSet<>();
        List<Path> sources = List.of(root.resolve("src/main/java"), root.resolve("src/main/resources"));
        for (Path source : sources) {
            try (Stream<Path> paths = Files.walk(source)) {
                for (Path path : paths.filter(Files::isRegularFile).toList()) {
                    Matcher matcher = CONFIG_KEY.matcher(Files.readString(path));
                    while (matcher.find()) {
                        properties.add(matcher.group());
                    }
                }
            }
        }
        return properties;
    }

    private static String environmentName(String property) {
        return property.toUpperCase(Locale.ROOT).replaceAll("[^A-Z0-9]", "_");
    }
}
