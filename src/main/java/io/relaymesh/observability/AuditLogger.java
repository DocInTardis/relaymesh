package io.relaymesh.observability;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.relaymesh.security.SensitiveDataMasker;
import io.relaymesh.util.Hashing;
import io.relaymesh.util.Jsons;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public final class AuditLogger {
    private static final ObjectMapper COMPACT_MAPPER = new ObjectMapper().findAndRegisterModules();
    private final Path auditFile;
    private final String namespace;
    private final String signingSecret;
    private final OtelTraceExporter otelTraceExporter;
    private String previousHash;

    public AuditLogger(Path auditFile, String namespace, String signingSecret, OtelTraceExporter otelTraceExporter) {
        this.auditFile = auditFile;
        this.namespace = namespace == null || namespace.isBlank() ? "default" : namespace.trim();
        this.signingSecret = signingSecret == null ? "" : signingSecret.trim();
        this.otelTraceExporter = otelTraceExporter;
        this.previousHash = "";
        try {
            Files.createDirectories(auditFile.getParent());
            if (!Files.exists(auditFile)) {
                try {
                    Files.createFile(auditFile);
                } catch (FileAlreadyExistsException ignored) {
                    // Another process created it between exists() and createFile().
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize audit log file: " + auditFile, e);
        }
        this.previousHash = loadLastHash();
    }

    public synchronized void log(AuditEvent event) {
        Instant now = Instant.now();
        Map<String, Object> details = sanitizeDetails(event.details());
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("timestamp", now.toString());
        row.put("namespace", namespace);
        row.put("action", event.action());
        row.put("actor", event.actor());
        row.put("principal_id", event.principalId());
        row.put("resource", event.resource());
        row.put("result", event.result());
        row.put("trace_id", event.traceId());
        row.put("span_id", event.spanId());
        row.put("task_id", event.taskId());
        row.put("step_id", event.stepId());
        row.put("details", details);
        row.put("prev_hash", previousHash);
        String rowHash = Hashing.sha256Hex(toCompactJson(row));
        row.put("hash", rowHash);
        if (!signingSecret.isBlank()) {
            row.put("signature", Hashing.hmacSha256Hex(signingSecret, rowHash));
        }
        String line = toCompactJson(row) + System.lineSeparator();
        try {
            Files.writeString(auditFile, line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
            previousHash = rowHash;
            if (otelTraceExporter != null) {
                otelTraceExporter.export(event, now);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write audit log", e);
        }
    }

    public synchronized String currentHash() {
        return previousHash;
    }

    private String loadLastHash() {
        try {
            if (!Files.exists(auditFile)) {
                return "";
            }
            String last = "";
            for (String line : Files.readAllLines(auditFile, StandardCharsets.UTF_8)) {
                if (line != null && !line.isBlank()) {
                    last = line;
                }
            }
            if (last.isBlank()) {
                return "";
            }
            JsonNode node = Jsons.mapper().readTree(last);
            return node.path("hash").asText("");
        } catch (Exception ignored) {
            return "";
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> sanitizeDetails(Map<String, Object> input) {
        if (input == null || input.isEmpty()) {
            return Map.of();
        }
        JsonNode node = Jsons.mapper().valueToTree(input);
        JsonNode masked = SensitiveDataMasker.masked(node);
        return Jsons.mapper().convertValue(masked, Map.class);
    }

    private String toCompactJson(Map<String, Object> row) {
        try {
            return COMPACT_MAPPER.writeValueAsString(row);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize audit row", e);
        }
    }

    public record AuditEvent(
            String action,
            String actor,
            String principalId,
            String resource,
            String result,
            String traceId,
            String spanId,
            String taskId,
            String stepId,
            Map<String, Object> details
    ) {
        public static AuditEvent of(
                String action,
                String actor,
                String resource,
                String result,
                String traceId,
                String spanId,
                String taskId,
                String stepId,
                Map<String, Object> details
        ) {
            return new AuditEvent(action, actor, null, resource, result, traceId, spanId, taskId, stepId, details == null ? Map.of() : details);
        }

        public static AuditEvent ofPrincipal(
                String action,
                String actor,
                String principalId,
                String resource,
                String result,
                String traceId,
                String spanId,
                String taskId,
                String stepId,
                Map<String, Object> details
        ) {
            return new AuditEvent(
                    action,
                    actor,
                    principalId == null || principalId.isBlank() ? null : principalId.trim(),
                    resource,
                    result,
                    traceId,
                    spanId,
                    taskId,
                    stepId,
                    details == null ? Map.of() : details
            );
        }
    }
}
