package io.relaymesh.cli;

import io.relaymesh.config.RelayMeshConfig;
import io.relaymesh.runtime.RelayMeshRuntime;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlRoomRuntimeSupportTest {
    @Test
    void normalizeNamespaceShouldSanitizeAndFallback() {
        assertEquals("project-a", ControlRoomRuntimeSupport.normalizeNamespace(" Project-A ", "default"));
        assertEquals("default", ControlRoomRuntimeSupport.normalizeNamespace("   ", "default"));
        assertEquals("bad-name", ControlRoomRuntimeSupport.normalizeNamespace("bad/name", "default"));
    }

    @Test
    void resolveRequestedNamespacesShouldSupportAllAndExplicitValues() {
        List<String> discovered = List.of("project-a", "default", "project-b");
        assertEquals(
                List.of("project-a", "default", "project-b"),
                ControlRoomRuntimeSupport.resolveRequestedNamespaces("all", null, "project-a", discovered)
        );
        assertEquals(
                List.of("project-b", "default"),
                ControlRoomRuntimeSupport.resolveRequestedNamespaces(" project-b, default ", null, "project-a", discovered)
        );
        assertEquals(
                List.of("project-a"),
                ControlRoomRuntimeSupport.resolveRequestedNamespaces("", "", "project-a", discovered)
        );
    }

    @Test
    void discoverNamespacesShouldIncludeActiveNamespaceFirst() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-control-room-discover-");
        Files.writeString(root.resolve("relaymesh.db"), "");
        Path nsRoot = root.resolve(RelayMeshConfig.DEFAULT_NAMESPACES_DIR);
        Files.createDirectories(nsRoot.resolve("project-a"));
        Files.createDirectories(nsRoot.resolve("project-b"));
        Files.createDirectories(nsRoot.resolve("project-empty"));
        Files.writeString(nsRoot.resolve("project-a").resolve("relaymesh.db"), "");
        Files.writeString(nsRoot.resolve("project-b").resolve("relaymesh.db"), "");

        List<String> namespaces = ControlRoomRuntimeSupport.discoverNamespaces(root, "project-b");
        assertEquals("project-b", namespaces.get(0));
        assertTrue(namespaces.contains("default"));
        assertTrue(namespaces.contains("project-a"));
        assertTrue(namespaces.contains("project-b"));
    }

    @Test
    void buildWorkflowEdgesShouldProduceRootAndDependencyEdges() {
        Map<String, Object> payload = Map.of(
                "steps", List.of(
                        Map.of("stepId", "ingest", "dependsOn", List.of()),
                        Map.of("stepId", "train", "dependsOn", List.of("ingest")),
                        Map.of("id", "report")
                )
        );
        List<String> edges = ControlRoomRuntimeSupport.buildWorkflowEdges(payload);
        assertTrue(edges.contains("[ROOT] -> ingest"));
        assertTrue(edges.contains("ingest -> train"));
        assertTrue(edges.contains("[ROOT] -> report"));
    }

    @Test
    void buildSnapshotPayloadShouldReturnNamespaceData() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-control-room-snapshot-");
        ConcurrentMap<String, RelayMeshRuntime> cache = new ConcurrentHashMap<>();
        LinkedHashMap<String, Object> payload = ControlRoomRuntimeSupport.buildSnapshotPayload(
                root.toString(),
                "default",
                cache,
                List.of("default"),
                "",
                5,
                5,
                5,
                24
        );

        Object dataObj = payload.get("data");
        assertTrue(dataObj instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> data = (Map<String, Object>) dataObj;
        assertTrue(data.containsKey("default"));
    }
}
