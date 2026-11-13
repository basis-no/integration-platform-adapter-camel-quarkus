package no.basis.integrationplatform.camel;

import org.apache.camel.util.URISupport;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Masks the credentials in an endpoint URI before the adapter stores or logs it, by the rule the
 * .NET adapter applies to the step endpoints it records, so that both adapters store the same value
 * for the same URI.
 *
 * <p>The result starts from what Camel 4.18's {@link URISupport#sanitizeUri(String)} masks, with the
 * sensitive option names of Camel 4.18 on every Camel version, and also masks the whole user info
 * of the authority, and the value of each query parameter whose name is credential-like:
 * by the rule for DLQ header names, or {@code key}, {@code code}, {@code sig} or {@code pwd}, or a
 * name that contains {@code passphrase}, {@code passcode}, {@code privatekey}, {@code accesskey},
 * {@code accountkey} or {@code connectionstring}. Both rules read the URI as given, and a run of
 * masked characters becomes one {@code xxxxxx}. Keywords an application adds with
 * {@link URISupport#addSanitizeKeywords(String)} are masked as well, in every parameter whose name
 * contains one; the .NET adapter has no such setting. A URI the adapter's patterns cannot mask
 * within about a second is recorded as {@code xxxxxx}. Only if Camel's added keywords cannot be
 * read does Camel's own sanitizer run too, and that pass is not bounded.
 */
public final class EndpointUriMasking {
    /** The mask written in place of a credential. */
    public static final String MASK = PlatformRouteConfiguration.MASKED;

    private static final Set<String> URI_CREDENTIAL_NAMES = Set.of("key", "code", "sig", "pwd");
    private static final List<String> URI_CREDENTIAL_NAME_PARTS = List.of(
            "passphrase", "passcode", "privatekey", "accesskey", "accountkey", "connectionstring");

    // The option names Camel 4.18's SensitiveUtils treats as sensitive, the list the .NET adapter
    // masks. Later Camel versions changed it (4.22 drops login and passcode and adds others), so
    // the adapter keeps its own copy: the stored value must not change with the Camel version.
    static final List<String> CAMEL_SENSITIVE_NAMES = List.of(
            "accesskey", "accesstoken", "accesstokensecret", "accountkey", "accountsid", "acltoken",
            "api_key", "api_secret", "apikey", "apipassword", "apiuser", "apiusername",
            "authenticationtoken", "authkey", "authorizationtoken", "authtoken", "azureclientid",
            "azureclientsecret", "blobaccesskey", "blobstoragesharedkeycredential", "certresourcepassword",
            "clientid", "clientsecret", "clientsecretcredential", "connectionstring", "consumerkey",
            "consumersecret", "emailaddress", "httpproxypassword", "kerberosauthticket", "keypassword",
            "keystorepassword", "login", "oauthclientid", "oauthclientsecret", "oauthtoken", "oauthtokenurl",
            "p12filename", "passcode", "passphrase", "password", "personalaccesstoken", "privatekey",
            "privatekeyfile", "privatekeyname", "privatekeypassword", "proxyauthpassword",
            "proxyauthusername", "proxypassword", "proxyuser", "proxyusername", "publickeyid", "publishkey",
            "queueownerawsaccountid", "realm", "refreshtoken", "sascredential", "sasljaasconfig",
            "saslpassword", "sassignature", "secret", "secretkey", "securerandom", "serviceinstanceid",
            "sessiontoken", "sharedaccesskey", "sourceblobaccesskey", "sslkeypassword",
            "sslkeystorepassword", "sslpassword", "ssltruststorepassword", "subscribekey", "systemid",
            "token", "tokencredential", "truststorepassword", "user", "userauthenticationcredentials",
            "username", "userpassword", "verificationcode", "webhookverifytoken", "zookeeperpassword");

    // URISupport.sanitizeUri's two patterns, built as Camel builds them, so that the source of
    // every character can be followed.
    private static final Pattern CAMEL_SECRET_PARAMETER = Pattern.compile(
            "([?&][^=]*(?:" + CAMEL_SENSITIVE_NAMES.stream().map(Pattern::quote).collect(Collectors.joining("|"))
                    + ")[^=]*)=(RAW(([{][^}]*[}])|([(][^)]*[)]))|[^&]*)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern CAMEL_USER_INFO_PASSWORD = Pattern.compile("(.*://.*?:)(.*)(@)");

    // One deadline covers both of the adapter's patterns (about a second in all). Camel's own
    // sanitizer, run only when an application added keywords, is not bounded.
    private static final long MASK_TIMEOUT_NANOS = 1_000_000_000L;
    private static final java.lang.reflect.Field EXTRA_SECRETS = extraSecretsField();
    private static final Object UNREADABLE = new Object();

    private EndpointUriMasking() {
    }

    /**
     * Masks the credentials in an endpoint URI.
     *
     * @param endpointUri the URI as the route sends to it, or null
     * @return the URI to store or log, or null for null
     */
    public static String mask(String endpointUri) {
        if (endpointUri == null) {
            return null;
        }
        long deadline = System.nanoTime() + MASK_TIMEOUT_NANOS;
        Sanitized camel;
        try {
            camel = camelSanitized(endpointUri, deadline);
        } catch (MaskTimeout e) {
            return MASK;
        }
        boolean[] adapterMasked = adapterMaskedCharacters(endpointUri);
        StringBuilder masked = new StringBuilder(endpointUri.length());
        int index = 0;
        while (index < camel.length()) {
            if (!camel.isMasked(index, adapterMasked)) {
                masked.append(camel.text.charAt(index++));
                continue;
            }
            // A run of Camel's masks and characters this rule masks becomes one mask; a run of
            // Camel's masks alone stays as Camel wrote it.
            int run = index;
            boolean adds = false;
            for (; index < camel.length() && camel.isMasked(index, adapterMasked); index++) {
                adds |= camel.sources[index] >= 0;
            }
            masked.append(adds ? MASK : camel.text.substring(run, index));
        }
        String result = masked.toString();
        Object added = addedSanitizeKeywords();
        if (added instanceof Pattern keywords) {
            // The application added keywords to Camel's sanitizer, which the patterns above do
            // not know. Apply only that pattern: Camel's own names of the version the
            // application runs are not the list the .NET adapter shares.
            try {
                return keywords.matcher(new DeadlineText(result, deadline)).replaceAll("$1=" + MASK);
            } catch (MaskTimeout e) {
                return MASK;
            }
        }
        if (added == UNREADABLE && !URISupport.sanitizeUri(endpointUri).equals(camel.text.toString())) {
            // Keywords may have been added: mask what Camel masks, so nothing Camel masks is stored.
            return URISupport.sanitizeUri(result);
        }
        return result;
    }

    /**
     * The pattern of the keywords added with {@link URISupport#addSanitizeKeywords(String)}, null
     * when none were added, or {@link #UNREADABLE} when Camel's private field cannot be read as a
     * pattern.
     */
    private static Object addedSanitizeKeywords() {
        if (EXTRA_SECRETS == null) {
            return UNREADABLE;
        }
        try {
            Object added = EXTRA_SECRETS.get(null);
            return added == null || added instanceof Pattern ? added : UNREADABLE;
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            return UNREADABLE;
        }
    }

    /** What Camel 4.18's sanitizeUri records for the URI, without added keywords. */
    static String camelSanitizeUri(String endpointUri) {
        return camelSanitized(endpointUri, Long.MAX_VALUE).text.toString();
    }

    /** Whether Camel's added keywords can be read, so that only they are applied, within the bound. */
    static boolean addedKeywordsReadable() {
        return addedSanitizeKeywords() != UNREADABLE;
    }

    private static java.lang.reflect.Field extraSecretsField() {
        try {
            java.lang.reflect.Field field = URISupport.class.getDeclaredField("EXTRA_SECRETS");
            field.setAccessible(true);
            return field;
        } catch (ReflectiveOperationException | RuntimeException | LinkageError e) {
            // Also a missing reflection registration in a native image: assume keywords were added.
            return null;
        }
    }

    /**
     * Camel's sanitizeUri: the parameter pattern replaced everywhere, then the user info pattern
     * once on that result. Each character keeps the index it had in the URI, or -1 for a mask.
     */
    private static Sanitized camelSanitized(String uri, long deadline) {
        Sanitized parameters = new Sanitized(uri.length() + MASK.length());
        Matcher parameter = CAMEL_SECRET_PARAMETER.matcher(new DeadlineText(uri, deadline));
        int position = 0;
        while (parameter.find()) {
            // "$1=xxxxxx": the name and '=' stay, the value becomes the mask.
            parameters.appendSource(uri, position, parameter.start(2));
            parameters.appendMask();
            position = parameter.end(2);
        }
        parameters.appendSource(uri, position, uri.length());

        Matcher userInfo = CAMEL_USER_INFO_PASSWORD.matcher(new DeadlineText(parameters.text.toString(), deadline));
        if (!userInfo.find()) {
            return parameters;
        }
        // "$1xxxxxx$3": the password becomes the mask.
        Sanitized sanitized = new Sanitized(parameters.length() + MASK.length());
        sanitized.appendFrom(parameters, 0, userInfo.start(2));
        sanitized.appendMask();
        sanitized.appendFrom(parameters, userInfo.end(2), parameters.length());
        return sanitized;
    }

    /** Sanitized text, with the index each character had in the URI, or -1 for a mask. */
    private static final class Sanitized {
        private final StringBuilder text;
        private int[] sources;

        Sanitized(int capacity) {
            text = new StringBuilder(capacity);
            sources = new int[Math.max(capacity, 16)];
        }

        int length() {
            return text.length();
        }

        boolean isMasked(int index, boolean[] adapterMasked) {
            return sources[index] < 0 || adapterMasked[sources[index]];
        }

        void appendSource(String uri, int start, int end) {
            for (int index = start; index < end; index++) {
                append(uri.charAt(index), index);
            }
        }

        void appendMask() {
            for (int index = 0; index < MASK.length(); index++) {
                append(MASK.charAt(index), -1);
            }
        }

        void appendFrom(Sanitized other, int start, int end) {
            for (int index = start; index < end; index++) {
                append(other.text.charAt(index), other.sources[index]);
            }
        }

        private void append(char character, int source) {
            if (text.length() == sources.length) {
                sources = java.util.Arrays.copyOf(sources, sources.length * 2);
            }
            sources[text.length()] = source;
            text.append(character);
        }
    }

    /**
     * The characters of the URI this rule masks: the user info of the authority, and the value of
     * each query parameter with a credential-like name.
     */
    private static boolean[] adapterMaskedCharacters(String uri) {
        boolean[] masked = new boolean[uri.length()];
        int queryOrFragment = firstOf(uri, '?', '#');
        int hierarchyEnd = queryOrFragment < 0 ? uri.length() : queryOrFragment;
        // The authority follows "scheme://" or, in a network-path reference such as
        // "//host/path", the leading "//".
        boolean networkPath = uri.startsWith("//");
        int scheme = networkPath ? -1 : uri.indexOf("://");
        if (scheme >= 0 && scheme + 3 > hierarchyEnd) {
            scheme = -1;
        }
        if (networkPath || scheme >= 0) {
            int authorityStart = scheme < 0 ? 2 : scheme + 3;
            int slash = indexWithin(uri, '/', authorityStart, hierarchyEnd);
            int authorityEnd = slash < 0 ? hierarchyEnd : slash;
            int at = lastIndexWithin(uri, '@', authorityStart, authorityEnd);
            for (int index = authorityStart; index < at; index++) {
                masked[index] = true;
            }
        }

        if (queryOrFragment < 0 || uri.charAt(queryOrFragment) != '?') {
            return masked;
        }
        int fragment = uri.indexOf('#', queryOrFragment);
        int queryEnd = fragment < 0 ? uri.length() : fragment;
        for (int parameterStart = queryOrFragment + 1; parameterStart <= queryEnd; ) {
            int ampersand = indexWithin(uri, '&', parameterStart, queryEnd);
            int parameterEnd = ampersand < 0 ? queryEnd : ampersand;
            int equals = indexWithin(uri, '=', parameterStart, parameterEnd);
            if (equals >= 0 && isCredentialParameter(uri.substring(parameterStart, equals))) {
                for (int index = equals + 1; index < parameterEnd; index++) {
                    masked[index] = true;
                }
            }
            parameterStart = parameterEnd + 1;
        }
        return masked;
    }

    static boolean isCredentialParameter(String encodedName) {
        String name = percentDecode(encodedName);
        String lower = name.toLowerCase(Locale.ROOT);
        return PlatformRouteConfiguration.isCredentialHeader(name)
                || URI_CREDENTIAL_NAMES.contains(lower)
                || URI_CREDENTIAL_NAME_PARTS.stream().anyMatch(lower::contains);
    }

    private static int firstOf(String text, char first, char second) {
        for (int index = 0; index < text.length(); index++) {
            char character = text.charAt(index);
            if (character == first || character == second) {
                return index;
            }
        }
        return -1;
    }

    /** The first index of the character in [start, end), or -1. */
    private static int indexWithin(String text, char character, int start, int end) {
        for (int index = start; index < end; index++) {
            if (text.charAt(index) == character) {
                return index;
            }
        }
        return -1;
    }

    /** The last index of the character in [start, end), or -1. */
    private static int lastIndexWithin(String text, char character, int start, int end) {
        for (int index = end - 1; index >= start; index--) {
            if (text.charAt(index) == character) {
                return index;
            }
        }
        return -1;
    }

    /**
     * Decodes %XX sequences as .NET's Uri.UnescapeDataString does: a run of escaped bytes that
     * forms valid UTF-8 becomes its characters, an escaped byte that does not start a valid UTF-8
     * sequence stays as its %XX text, and '+' and an incomplete escape stay as they are.
     */
    static String percentDecode(String text) {
        if (text.indexOf('%') < 0) {
            return text;
        }
        StringBuilder decoded = new StringBuilder(text.length());
        int index = 0;
        while (index < text.length()) {
            int lead = escapedByte(text, index);
            if (lead < 0) {
                decoded.append(text.charAt(index++));
                continue;
            }
            int length = lead < 0x80 ? 1 : lead >= 0xC2 && lead <= 0xDF ? 2 : lead >= 0xE0 && lead <= 0xEF ? 3
                    : lead >= 0xF0 && lead <= 0xF4 ? 4 : 0;
            String character = length == 0 ? null : utf8(text, index, length);
            if (character == null) {
                // Not the start of a valid sequence: keep this escape as written.
                decoded.append(text, index, index + 3);
                index += 3;
                continue;
            }
            decoded.append(character);
            index += 3 * length;
        }
        return decoded.toString();
    }

    /** The UTF-8 character of the escaped bytes at the index, or null if they are not valid UTF-8. */
    private static String utf8(String text, int index, int length) {
        byte[] bytes = new byte[length];
        for (int position = 0; position < length; position++) {
            int value = escapedByte(text, index + 3 * position);
            if (value < 0) {
                return null;
            }
            bytes[position] = (byte) value;
        }
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            return null;
        }
    }

    /** The byte of a %XX escape at the index, or -1 if there is none. */
    private static int escapedByte(String text, int index) {
        if (index + 2 >= text.length() || text.charAt(index) != '%') {
            return -1;
        }
        int high = hexDigit(text.charAt(index + 1));
        int low = hexDigit(text.charAt(index + 2));
        return high < 0 || low < 0 ? -1 : high * 16 + low;
    }

    private static int hexDigit(char character) {
        if (character >= '0' && character <= '9') {
            return character - '0';
        }
        if (character >= 'a' && character <= 'f') {
            return character - 'a' + 10;
        }
        if (character >= 'A' && character <= 'F') {
            return character - 'A' + 10;
        }
        return -1;
    }

    /** Text that stops a regular expression once its deadline has passed. */
    private static final class DeadlineText implements CharSequence {
        private final String text;
        private final long deadline;
        private int reads;

        DeadlineText(String text, long deadline) {
            this.text = text;
            this.deadline = deadline;
        }

        @Override
        public int length() {
            return text.length();
        }

        @Override
        public char charAt(int index) {
            if ((++reads & 0xFFF) == 0 && System.nanoTime() > deadline) {
                throw new MaskTimeout();
            }
            return text.charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return new DeadlineText(text.substring(start, end), deadline);
        }

        @Override
        public String toString() {
            return text;
        }
    }

    private static final class MaskTimeout extends RuntimeException {
        private static final long serialVersionUID = 1L;

        MaskTimeout() {
            super(null, null, false, false);
        }
    }
}
