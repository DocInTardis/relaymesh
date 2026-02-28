package io.relaymesh.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.relaymesh.util.Jsons;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

final class ControlRoomLayoutStore {
    private ControlRoomLayoutStore() {
    }

    static String normalizeProfileName(String raw) {
        if (raw == null || raw.isBlank()) {
            return "";
        }
        String value = raw.trim();
        if (value.length() > 64) {
            return "";
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            boolean ok = (ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '_' || ch == '-' || ch == '.';
            if (!ok) {
                return "";
            }
        }
        return value;
    }

    static LinkedHashMap<String, JsonNode> readProfiles(Path rootBaseDir) throws IOException {
        LinkedHashMap<String, JsonNode> out = new LinkedHashMap<>();
        Path file = layoutsPath(rootBaseDir);
        if (!Files.exists(file)) {
            return out;
        }
        JsonNode root = Jsons.mapper().readTree(file.toFile());
        if (root == null || !root.isObject()) {
            return out;
        }
        root.fields().forEachRemaining(entry -> {
            String key = normalizeProfileName(entry.getKey());
            JsonNode value = entry.getValue();
            if (!key.isEmpty() && value != null && value.isObject()) {
                out.put(key, value);
            }
        });
        return out;
    }

    static void writeProfiles(Path rootBaseDir, Map<String, JsonNode> profiles) throws IOException {
        Path file = layoutsPath(rootBaseDir);
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        ObjectNode root = Jsons.mapper().createObjectNode();
        if (profiles != null) {
            profiles.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> {
                        String key = normalizeProfileName(entry.getKey());
                        JsonNode value = entry.getValue();
                        if (!key.isEmpty() && value != null && value.isObject()) {
                            root.set(key, value);
                        }
                    });
        }
        Jsons.mapper().writerWithDefaultPrettyPrinter().writeValue(file.toFile(), root);
    }

    private static Path layoutsPath(Path rootBaseDir) {
        return rootBaseDir.resolve("control-room-layouts.json");
    }
}

