package io.relaymesh.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.relaymesh.util.Jsons;

import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public final class SensitiveDataMasker {
    private static final String MASK = "***";
    private static final Set<String> SENSITIVE_HINTS = Set.of(
            "password", "passwd", "secret", "token", "authorization", "apikey", "api_key", "key", "credential"
    );

    private SensitiveDataMasker() {
    }

    public static JsonNode masked(JsonNode input) {
        if (input == null || input.isNull()) {
            return Jsons.mapper().nullNode();
        }
        if (input.isObject()) {
            ObjectNode out = Jsons.mapper().createObjectNode();
            Iterator<Map.Entry<String, JsonNode>> it = input.fields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                String key = entry.getKey();
                JsonNode value = entry.getValue();
                if (isSensitiveKey(key)) {
                    out.put(key, MASK);
                } else {
                    out.set(key, masked(value));
                }
            }
            return out;
        }
        if (input.isArray()) {
            ArrayNode out = Jsons.mapper().createArrayNode();
            for (JsonNode value : input) {
                out.add(masked(value));
            }
            return out;
        }
        if (input.isTextual() && likelySecretValue(input.asText(""))) {
            return Jsons.mapper().valueToTree(MASK);
        }
        return input;
    }

    private static boolean isSensitiveKey(String rawKey) {
        if (rawKey == null || rawKey.isBlank()) {
            return false;
        }
        String key = rawKey.toLowerCase(Locale.ROOT);
        for (String hint : SENSITIVE_HINTS) {
            if (key.contains(hint)) {
                return true;
            }
        }
        return false;
    }

    private static boolean likelySecretValue(String value) {
        if (value == null) {
            return false;
        }
        String v = value.trim();
        if (v.length() < 24) {
            return false;
        }
        // Basic high-entropy token heuristic: long opaque strings should not leak in logs.
        return v.matches("^[A-Za-z0-9+/=_\\-:.]{24,}$");
    }
}
