package no.basis.integrationplatform.camel;

import org.apache.camel.util.SensitiveUtils;
import org.apache.camel.util.URISupport;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Masks the credentials in an endpoint URI before the adapter stores or logs it, by the rule the
 * .NET adapter applies to the step endpoints it records, so that both adapters store the same value
 * for the same URI.
 *
 * <p>The result starts from Camel's {@link URISupport#sanitizeUri(String)} and also masks the whole
 * user info of the authority, and the value of each query parameter whose name is credential-like:
 * by the rule for DLQ header names, or {@code key}, {@code code}, {@code sig} or {@code pwd}, or a
 * name that contains {@code passphrase}, {@code passcode}, {@code privatekey}, {@code accesskey},
 * {@code accountkey} or {@code connectionstring}. Both rules read the URI as given, and a run of
 * masked characters becomes one {@code xxxxxx}. Keywords an application adds with
 * {@link URISupport#addSanitizeKeywords(String)} are masked as well; the .NET adapter has no such
 * setting. A URI the patterns cannot mask within one second is recorded as {@code xxxxxx}.
 */
public final class EndpointUriMasking {
    /** The mask written in place of a credential. */
    public static final String MASK = PlatformRouteConfiguration.MASKED;

    private static final Set<String> URI_CREDENTIAL_NAMES = Set.of("key", "code", "sig", "pwd");
    private static final List<String> URI_CREDENTIAL_NAME_PARTS = List.of(
            "passphrase", "passcode", "privatekey", "accesskey", "accountkey", "connectionstring");

    // URISupport.sanitizeUri's two patterns, built as Camel builds them, so that the source of
    // every character can be followed.
    private static final Pattern CAMEL_SECRET_PARAMETER = Pattern.compile(
            "([?&][^=]*(?:" + SensitiveUtils.getSensitivePattern() + ")[^=]*)=(RAW(([{][^}]*[}])|([(][^)]*[)]))|[^&]*)",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern CAMEL_USER_INFO_PASSWORD = Pattern.compile("(.*://.*?:)(.*)(@)");

    private static final long MASK_TIMEOUT_NANOS = 1_000_000_000L;

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
        List<Character> characters = new ArrayList<>();
        List<Integer> sources = new ArrayList<>();
        try {
            camelSanitized(endpointUri, characters, sources, System.nanoTime() + MASK_TIMEOUT_NANOS);
        } catch (MaskTimeout e) {
            return MASK;
        }
        boolean[] adapterMasked = adapterMaskedCharacters(endpointUri);
        StringBuilder masked = new StringBuilder(endpointUri.length());
        int index = 0;
        while (index < characters.size()) {
            if (!isMasked(index, sources, adapterMasked)) {
                masked.append(characters.get(index++));
                continue;
            }
            // A run of Camel's masks and characters this rule masks becomes one mask; a run of
            // Camel's masks alone stays as Camel wrote it.
            int run = index;
            boolean adds = false;
            for (; index < characters.size() && isMasked(index, sources, adapterMasked); index++) {
                adds |= sources.get(index) >= 0;
            }
            if (adds) {
                masked.append(MASK);
            } else {
                for (; run < index; run++) {
                    masked.append(characters.get(run));
                }
            }
        }
        String result = masked.toString();
        String camel = URISupport.sanitizeUri(endpointUri);
        if (!camel.equals(sanitizedText(characters))) {
            // Camel masks something the patterns above do not, such as keywords the application
            // added; mask that too, so nothing Camel masks is stored.
            return URISupport.sanitizeUri(result);
        }
        return result;
    }

    private static boolean isMasked(int index, List<Integer> sources, boolean[] adapterMasked) {
        int source = sources.get(index);
        return source < 0 || adapterMasked[source];
    }

    private static String sanitizedText(List<Character> characters) {
        StringBuilder text = new StringBuilder(characters.size());
        characters.forEach(text::append);
        return text.toString();
    }

    /**
     * Camel's sanitizeUri: the parameter pattern replaced everywhere, then the user info pattern
     * once on that result. Each character keeps the index it had in the URI, or -1 for a mask.
     */
    private static void camelSanitized(String uri, List<Character> characters, List<Integer> sources, long deadline) {
        Matcher parameter = CAMEL_SECRET_PARAMETER.matcher(new DeadlineText(uri, deadline));
        int position = 0;
        while (parameter.find()) {
            // "$1=xxxxxx": the name and '=' stay, the value becomes the mask.
            appendSource(uri, position, parameter.start(2), characters, sources);
            appendMask(characters, sources);
            position = parameter.end(2);
        }
        appendSource(uri, position, uri.length(), characters, sources);

        Matcher userInfo = CAMEL_USER_INFO_PASSWORD.matcher(new DeadlineText(sanitizedText(characters), deadline));
        if (!userInfo.find()) {
            return;
        }
        // "$1xxxxxx$3": the password becomes the mask.
        int start = userInfo.start(2);
        int end = userInfo.end(2);
        List<Character> afterCharacters = new ArrayList<>(characters.subList(end, characters.size()));
        List<Integer> afterSources = new ArrayList<>(sources.subList(end, sources.size()));
        characters.subList(start, characters.size()).clear();
        sources.subList(start, sources.size()).clear();
        appendMask(characters, sources);
        characters.addAll(afterCharacters);
        sources.addAll(afterSources);
    }

    private static void appendSource(String uri, int start, int end, List<Character> characters, List<Integer> sources) {
        for (int index = start; index < end; index++) {
            characters.add(uri.charAt(index));
            sources.add(index);
        }
    }

    private static void appendMask(List<Character> characters, List<Integer> sources) {
        for (int index = 0; index < MASK.length(); index++) {
            characters.add(MASK.charAt(index));
            sources.add(-1);
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
     * Decodes %XX sequences as UTF-8, leaving an invalid sequence as it is and '+' as '+', as
     * .NET's Uri.UnescapeDataString does.
     */
    private static String percentDecode(String text) {
        if (text.indexOf('%') < 0) {
            return text;
        }
        StringBuilder decoded = new StringBuilder(text.length());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        int index = 0;
        while (index < text.length()) {
            char character = text.charAt(index);
            if (character == '%' && isHex(text, index + 1) && isHex(text, index + 2)) {
                bytes.write(Integer.parseInt(text.substring(index + 1, index + 3), 16));
                index += 3;
                continue;
            }
            flush(bytes, decoded);
            decoded.append(character);
            index++;
        }
        flush(bytes, decoded);
        return decoded.toString();
    }

    private static boolean isHex(String text, int index) {
        return index < text.length() && Character.digit(text.charAt(index), 16) >= 0;
    }

    private static void flush(ByteArrayOutputStream bytes, StringBuilder decoded) {
        if (bytes.size() > 0) {
            decoded.append(new String(bytes.toByteArray(), StandardCharsets.UTF_8));
            bytes.reset();
        }
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
