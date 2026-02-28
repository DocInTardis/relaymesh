package io.relaymesh.cli;

import com.fasterxml.jackson.databind.JsonNode;
import io.relaymesh.config.RelayMeshConfig;
import io.relaymesh.runtime.RelayMeshRuntime;
import io.relaymesh.util.Jsons;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

final class ControlRoomRuntimeSupport {
    private ControlRoomRuntimeSupport() {
    }

    static RelayMeshRuntime runtimeForNamespace(
            String root,
            String namespace,
            ConcurrentMap<String, RelayMeshRuntime> cache
    ) {
        String normalized = normalizeNamespace(namespace, RelayMeshConfig.DEFAULT_NAMESPACE);
        return cache.computeIfAbsent(normalized, ns -> {
            RelayMeshRuntime scoped = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root, ns));
            scoped.init();
            return scoped;
        });
    }

    static List<String> discoverNamespaces(Path rootBaseDir, String activeNamespace) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        String normalizedActive = normalizeNamespace(activeNamespace, RelayMeshConfig.DEFAULT_NAMESPACE);
        if (Files.exists(rootBaseDir.resolve("relaymesh.db"))) {
            out.add(RelayMeshConfig.DEFAULT_NAMESPACE);
        }
        Path namespacesRoot = rootBaseDir.resolve(RelayMeshConfig.DEFAULT_NAMESPACES_DIR);
        if (Files.isDirectory(namespacesRoot)) {
            try (var children = Files.newDirectoryStream(namespacesRoot)) {
                for (Path child : children) {
                    if (!Files.isDirectory(child)) {
                        continue;
                    }
                    if (!Files.exists(child.resolve("relaymesh.db"))) {
                        continue;
                    }
                    out.add(normalizeNamespace(child.getFileName().toString(), normalizedActive));
                }
            } catch (IOException ignored) {
                // Keep best-effort discovery for control-room.
            }
        }
        out.add(normalizedActive);
        List<String> namespaces = new ArrayList<>(out);
        namespaces.sort(String::compareTo);
        if (namespaces.remove(normalizedActive)) {
            namespaces.add(0, normalizedActive);
        }
        return namespaces;
    }

    static List<String> resolveRequestedNamespaces(
            String rawNamespaces,
            String singleNamespace,
            String activeNamespace,
            List<String> discovered
    ) {
        String value = rawNamespaces == null || rawNamespaces.isBlank() ? singleNamespace : rawNamespaces;
        if (value == null || value.isBlank()) {
            return List.of(normalizeNamespace(activeNamespace, RelayMeshConfig.DEFAULT_NAMESPACE));
        }
        LinkedHashSet<String> out = new LinkedHashSet<>();
        for (String token : parseNamespaceList(value)) {
            if ("all".equalsIgnoreCase(token)) {
                if (discovered != null) {
                    for (String ns : discovered) {
                        out.add(normalizeNamespace(ns, activeNamespace));
                    }
                }
                continue;
            }
            out.add(normalizeNamespace(token, activeNamespace));
        }
        if (out.isEmpty()) {
            out.add(normalizeNamespace(activeNamespace, RelayMeshConfig.DEFAULT_NAMESPACE));
        }
        return new ArrayList<>(out);
    }

    static String normalizeNamespace(String raw, String fallback) {
        String candidate = raw == null || raw.isBlank() ? fallback : raw.trim();
        if (candidate == null || candidate.isBlank()) {
            candidate = RelayMeshConfig.DEFAULT_NAMESPACE;
        }
        return RelayMeshConfig.fromRoot("data", candidate).namespace();
    }

    static LinkedHashMap<String, Object> buildSnapshotPayload(
            String root,
            String defaultNamespace,
            ConcurrentMap<String, RelayMeshRuntime> runtimeCache,
            List<String> requestedNamespaces,
            String statusFilter,
            int taskLimit,
            int deadLimit,
            int conflictLimit,
            int sinceHours
    ) {
        LinkedHashMap<String, Object> data = new LinkedHashMap<>();
        for (String ns : requestedNamespaces) {
            RelayMeshRuntime scoped = runtimeForNamespace(root, ns, runtimeCache);
            LinkedHashMap<String, Object> snap = new LinkedHashMap<>();
            snap.put("stats", scoped.stats());
            snap.put("members", scoped.members());
            snap.put("tasks", scoped.tasks(statusFilter, taskLimit, 0));
            snap.put("dead", scoped.tasks("DEAD_LETTER", deadLimit, 0));
            snap.put("conflicts", scoped.leaseConflicts(conflictLimit, sinceHours));
            data.put(ns, snap);
        }
        LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
        payload.put("timestamp", Instant.now().toString());
        payload.put("activeNamespace", defaultNamespace);
        payload.put("requestedNamespaces", requestedNamespaces);
        payload.put("statusFilter", statusFilter == null ? "" : statusFilter);
        payload.put("data", data);
        return payload;
    }

    static List<String> buildWorkflowEdges(Object workflowPayload) {
        List<String> edges = new ArrayList<>();
        if (workflowPayload == null) {
            return edges;
        }
        JsonNode root = Jsons.mapper().valueToTree(workflowPayload);
        JsonNode steps = root.path("steps");
        if (!steps.isArray()) {
            return edges;
        }
        for (JsonNode step : steps) {
            String stepId = step.path("stepId").asText();
            if (stepId == null || stepId.isBlank()) {
                stepId = step.path("id").asText();
            }
            if (stepId == null || stepId.isBlank()) {
                continue;
            }
            JsonNode dependsOn = step.path("dependsOn");
            if (!dependsOn.isArray() || dependsOn.isEmpty()) {
                edges.add("[ROOT] -> " + stepId);
                continue;
            }
            for (JsonNode dep : dependsOn) {
                String depId = dep.asText();
                if (depId == null || depId.isBlank()) {
                    continue;
                }
                edges.add(depId + " -> " + stepId);
            }
        }
        return edges;
    }

    private static List<String> parseNamespaceList(String raw) {
        List<String> out = new ArrayList<>();
        if (raw == null || raw.isBlank()) {
            return out;
        }
        for (String token : raw.split(",")) {
            if (token == null) {
                continue;
            }
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        return out;
    }
}

