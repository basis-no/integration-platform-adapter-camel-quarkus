package no.basis.integrationplatform.camel;

import org.apache.camel.util.URISupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The stored endpoint is the value the .NET adapter records for the same URI. The cases are the
 * .NET adapter's PlatformStepExecutorTests, ported unchanged.
 */
class EndpointUriMaskingTest {

    /** Mirrors .NET MaskEndpointUriRecordsWhatTheCamelAdapterRecords: Camel's sanitizeUri already gives the value. */
    static Stream<Arguments> camelValues() {
        return Stream.of(
                Arguments.of("https://target.example/api/orders?orderId=42", "https://target.example/api/orders?orderId=42"),
                Arguments.of("//target.example/orders?token=t", "//target.example/orders?token=xxxxxx"),
                Arguments.of("/orders/user:pw@x?page=2", "/orders/user:pw@x?page=2"),
                Arguments.of("https://target.example/a?client%5Fsecret=s&page=2", "https://target.example/a?client%5Fsecret=xxxxxx&page=2"),
                Arguments.of("https://target.example/a?token=t#section", "https://target.example/a?token=xxxxxx"),
                Arguments.of("https://target.example/a?token&page=2", "https://target.example/a?token&page=xxxxxx"),
                Arguments.of("https://target.example/files/user:pw@x?page=2", "https://target.example/files/user:xxxxxx@x?page=2"),
                Arguments.of("orders-service", "orders-service"),
                Arguments.of(
                        "https://target.example/a?user=u&username=n&clientId=c&login=l&realm=r&emailAddress=e&page=2",
                        "https://target.example/a?user=xxxxxx&username=xxxxxx&clientId=xxxxxx&login=xxxxxx&realm=xxxxxx&emailAddress=xxxxxx&page=2"),
                Arguments.of(
                        "https://target.example/a?accessToken=t&refresh_token=r&clientSecret=s&secretKey=k&page=2",
                        "https://target.example/a?accessToken=xxxxxx&refresh_token=xxxxxx&clientSecret=xxxxxx&secretKey=xxxxxx&page=2"),
                Arguments.of("https://target.example/a?password=RAW(p&q)&page=2", "https://target.example/a?password=xxxxxx&page=2"),
                Arguments.of("https://target.example/a?password=RAW{p&q}&page=2", "https://target.example/a?password=xxxxxx&page=2"),
                Arguments.of("https://target.example/a?PASSWORD=p&Token=t", "https://target.example/a?PASSWORD=xxxxxx&Token=xxxxxx"),
                Arguments.of("https://target.example:8443/a?page=2", "https://target.example:8443/a?page=2"),
                Arguments.of("https://target.example/a?access_token=t&page=2", "https://target.example/a?access_token=xxxxxx&page=2"),
                Arguments.of("https://target.example/a?sessionToken=t&page=2", "https://target.example/a?sessionToken=xxxxxx&page=2"),
                Arguments.of("https://target.example/a?userId=42&page=2", "https://target.example/a?userId=xxxxxx&page=2"),
                Arguments.of("https://target.example/a?apikey=k&api_key=k2&page=2", "https://target.example/a?apikey=xxxxxx&api_key=xxxxxx&page=2"),
                Arguments.of("https://target.example/a?page=2&passphrase=p", "https://target.example/a?page=2&passphrase=xxxxxx"),
                Arguments.of("https://target.example/a?token=&page=2", "https://target.example/a?token=xxxxxx&page=2"),
                Arguments.of(
                        "http://target.example/cb?redirect=https://svc:pw@other.example/x",
                        "http://target.example/cb?redirect=https://svc:xxxxxx@other.example/x"),
                // Java's '.' stops at each of these line terminators.
                Arguments.of("https://target.example/a?redirect=https://a:secret@h\rhttps://b:pw@h", "https://target.example/a?redirect=https://a:xxxxxx@h\rhttps://b:pw@h"),
                Arguments.of("https://target.example/a?redirect=https://a:secret@h\nhttps://b:pw@h", "https://target.example/a?redirect=https://a:xxxxxx@h\nhttps://b:pw@h"),
                Arguments.of("https://target.example/a?redirect=https://a:secret@h\u0085https://b:pw@h", "https://target.example/a?redirect=https://a:xxxxxx@h\u0085https://b:pw@h"),
                Arguments.of("https://target.example/a?redirect=https://a:secret@h\u2028https://b:pw@h", "https://target.example/a?redirect=https://a:xxxxxx@h\u2028https://b:pw@h"),
                Arguments.of("https://target.example/a?redirect=https://a:secret@h\u2029https://b:pw@h", "https://target.example/a?redirect=https://a:xxxxxx@h\u2029https://b:pw@h"));
    }

    @ParameterizedTest
    @MethodSource("camelValues")
    void recordsWhatCamelsSanitizerRecordsWhenThatMasksEverything(String endpointUri, String expected) {
        assertEquals(expected, URISupport.sanitizeUri(endpointUri), "the .NET case's Camel value");
        assertEquals(expected, EndpointUriMasking.mask(endpointUri));
    }

    /**
     * Mirrors .NET MaskEndpointUriAlsoMasksUserInfoAndTheAdapterCredentialNames: the whole user
     * info, and the DLQ header rule's names plus key, code, sig and pwd. The third value is what
     * Camel's sanitizeUri alone records, which this adapter stored before.
     */
    static Stream<Arguments> adapterValues() {
        return Stream.of(
                Arguments.of(
                        "https://svc-user:synthetic-password@target.example/a",
                        "https://xxxxxx@target.example/a",
                        "https://svc-user:xxxxxx@target.example/a"),
                Arguments.of("https://synthetic-token@target.example/a", "https://xxxxxx@target.example/a", "https://synthetic-token@target.example/a"),
                Arguments.of(
                        "https://svc-user:synthetic-password@target.example?token=t",
                        "https://xxxxxx@target.example?token=xxxxxx",
                        "https://svc-user:xxxxxx@target.example?token=xxxxxx"),
                Arguments.of(
                        "//svc-user:synthetic-password@target.example/orders",
                        "//xxxxxx@target.example/orders",
                        "//svc-user:synthetic-password@target.example/orders"),
                Arguments.of("https://svc-user@target.example:8443/a", "https://xxxxxx@target.example:8443/a", "https://svc-user@target.example:8443/a"),
                Arguments.of(
                        "https://target.example/a?key=k&code=c&sig=s&Signature=s2&X-Amz-Credential=c2&X-Amz-Security-Token=t&client_secret=cs&password=p&ok=1",
                        "https://target.example/a?key=xxxxxx&code=xxxxxx&sig=xxxxxx&Signature=xxxxxx&X-Amz-Credential=xxxxxx&X-Amz-Security-Token=xxxxxx&client_secret=xxxxxx&password=xxxxxx&ok=1",
                        "https://target.example/a?key=k&code=c&sig=s&Signature=s2&X-Amz-Credential=c2&X-Amz-Security-Token=xxxxxx&client_secret=xxxxxx&password=xxxxxx&ok=1"),
                Arguments.of(
                        "https://target.example/a?subscription-key=k&x-functions-key=k2&apiKey=k3&privatekey=p&passphrase=p2&pwd=p3&Authorization=Bearer%20t",
                        "https://target.example/a?subscription-key=xxxxxx&x-functions-key=xxxxxx&apiKey=xxxxxx&privatekey=xxxxxx&passphrase=xxxxxx&pwd=xxxxxx&Authorization=xxxxxx",
                        "https://target.example/a?subscription-key=k&x-functions-key=k2&apiKey=xxxxxx&privatekey=xxxxxx&passphrase=xxxxxx&pwd=p3&Authorization=Bearer%20t"),
                Arguments.of("https://target.example/a?code=c&page=2", "https://target.example/a?code=xxxxxx&page=2", "https://target.example/a?code=c&page=2"),
                Arguments.of(
                        "https://target.example/a?sig=s&sv=2024&page=2",
                        "https://target.example/a?sig=xxxxxx&sv=2024&page=2",
                        "https://target.example/a?sig=s&sv=2024&page=2"),
                // Camel's user info pattern can reach past '?'; each rule still reads the URI as
                // given, so neither replacement hides a value from the other.
                Arguments.of(
                        "https://a:pw?x=y@h&code=synthetic-secret",
                        "https://a:xxxxxx@h&code=xxxxxx",
                        "https://a:xxxxxx@h&code=synthetic-secret"),
                Arguments.of("https://a:b@h/x?u=c:d@e", "https://xxxxxx@e", "https://a:xxxxxx@e"),
                Arguments.of(
                        "https://u:p@h/cb?r=https://x:y@z",
                        "https://xxxxxx@h/cb?r=https://x:xxxxxx@z",
                        "https://u:p@h/cb?r=https://x:xxxxxx@z"));
    }

    @ParameterizedTest
    @MethodSource("adapterValues")
    void alsoMasksUserInfoAndTheCredentialNamesAsTheDotNetAdapterDoes(String endpointUri, String expected, String camelRecords) {
        assertEquals(camelRecords, URISupport.sanitizeUri(endpointUri), "the .NET case's Camel value");
        assertEquals(expected, EndpointUriMasking.mask(endpointUri));
        assertNotEquals(camelRecords, expected);
    }

    /**
     * Parameter names are decoded as .NET's Uri.UnescapeDataString decodes them: an escape that is
     * not valid UTF-8 stays as written, and only ASCII hex digits make an escape.
     */
    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource(delimiter = '|', value = {
            "%61%75%74%68|auth",
            "%FAuth|%FAuth",
            "%C3%A9%FA|\u00e9%FA",
            "%E2%82%41|%E2%82A",
            "%F0%9F%98%80|\ud83d\ude00",
            "%C0%80|%C0%80",
            "%ED%A0%80|%ED%A0%80",
            "%F4%90%80%80|%F4%90%80%80",
            "a+b%2|a+b%2",
            "%\u0661\u0661|%\u0661\u0661"})
    void decodesNamesAsDotNetDoes(String encoded, String decoded) {
        assertEquals(decoded, EndpointUriMasking.percentDecode(encoded));
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.CsvSource(delimiter = '|', value = {
            // An invalid escape stays as written, so the name still contains "auth".
            "https://target.example/a?%FAuth=s&page=2|https://target.example/a?%FAuth=xxxxxx&page=2",
            // A valid escape is decoded: "%73ig" is "sig".
            "https://target.example/a?%73ig=s&page=2|https://target.example/a?%73ig=xxxxxx&page=2"})
    void masksAParameterByItsDecodedName(String endpointUri, String expected) {
        assertEquals(expected, EndpointUriMasking.mask(endpointUri));
    }

    /** Mirrors .NET MaskEndpointUriRecordsNothingOfAUriThePatternsCannotMaskInTime. */
    @Test
    void recordsNothingOfAUriThePatternsCannotMaskInTime() {
        String pathological = "https://target.example/a?" + "&token".repeat(20_000);

        long start = System.nanoTime();
        assertEquals("xxxxxx", EndpointUriMasking.mask(pathological));
        assertTrue(System.nanoTime() - start < 5_000_000_000L, "masking is bounded");
    }

    @Test
    void readsCamelsAddedKeywordFieldOnThisCamelVersion() {
        // If Camel renames the field, every endpoint would also take Camel's unbounded pass.
        assertTrue(EndpointUriMasking.addedKeywordsReadable(), "URISupport.EXTRA_SECRETS not found");
    }

    @Test
    void masksKeywordsTheApplicationAddedToCamelsSanitizer() throws Exception {
        java.lang.reflect.Field extra = URISupport.class.getDeclaredField("EXTRA_SECRETS");
        extra.setAccessible(true);
        Object before = extra.get(null);
        // Not masked by Camel's or the adapter's built-in names, so only the added keyword masks it.
        assertEquals("https://xxxxxx@target.example/a?zqfixtureword=s&page=2",
                EndpointUriMasking.mask("https://u:p@target.example/a?zqfixtureword=s&page=2"));
        try {
            URISupport.addSanitizeKeywords("zqfixtureword");

            assertEquals("https://xxxxxx@target.example/a?zqfixtureword=xxxxxx&page=2",
                    EndpointUriMasking.mask("https://u:p@target.example/a?zqfixtureword=s&page=2"));
        } finally {
            // Camel keeps added keywords for the JVM; restore them for the other tests.
            extra.set(null, before);
        }
        assertEquals("https://xxxxxx@target.example/a?zqfixtureword=s&page=2",
                EndpointUriMasking.mask("https://u:p@target.example/a?zqfixtureword=s&page=2"));
    }

    @Test
    void keepsNull() {
        assertNull(EndpointUriMasking.mask(null));
    }
}
