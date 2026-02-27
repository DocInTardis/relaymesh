package io.relaymesh.observability;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.relaymesh.util.Jsons;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

public final class OtelTraceExporter {
    private final Path outputFile;
    private final String serviceName;
    private final String namespace;

    public OtelTraceExporter(Path outputFile, String serviceName, String namespace) {
        this.outputFile = outputFile;
        this.serviceName = serviceName == null || serviceName.isBlank() ? "relaymesh" : serviceName.trim();
        this.namespace = namespace == null || namespace.isBlank() ? "default" : namespace.trim();
        try {
            if (outputFile.getParent() != null) {
                Files.createDirectories(outputFile.getParent());
            }
            if (!Files.exists(outputFile)) {
                Files.createFile(outputFile);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize OTel exporter output: " + outputFile, e);
        }
    }

    public synchronized void export(AuditLogger.AuditEvent event, Instant timestamp) {
        if (event == null) {
            return;
        }
        String traceId = normalizeTraceId(event.traceId());
        String spanId = normalizeSpanId(event.spanId(), event.action(), timestamp);
        long tsNs = Math.max(0L, timestamp.toEpochMilli()) * 1_000_000L;
        ObjectNode root = Jsons.mapper().createObjectNode();
        ArrayNode resourceSpans = root.putArray("resourceSpans");
        ObjectNode resourceSpan = resourceSpans.addObject();
        ObjectNode resource = resourceSpan.putObject("resource");
        ArrayNode attrs = resource.putArray("attributes");
        appendStringAttr(attrs, "service.name", serviceName);
        appendStringAttr(attrs, "service.namespace", namespace);
        appendStringAttr(attrs, "relaymesh.namespace", namespace);

        ArrayNode scopeSpans = resourceSpan.putArray("scopeSpans");
        ObjectNode scopeSpan = scopeSpans.addObject();
        ObjectNode scope = scopeSpan.putObject("scope");
        scope.put("name", "relaymesh-audit");
        scope.put("version", "v1");
        ArrayNode spans = scopeSpan.putArray("spans");
        ObjectNode span = spans.addObject();
        span.put("traceId", traceId);
        span.put("spanId", spanId);
        span.put("name", event.action() == null ? "audit.event" : event.action());
        span.put("kind", 1);
        span.put("startTimeUnixNano", Long.toString(tsNs));
        span.put("endTimeUnixNano", Long.toString(tsNs));
        span.put("status", event.result() == null || "ok".equalsIgnoreCase(event.result()) ? 1 : 2);
        ArrayNode spanAttrs = span.putArray("attributes");
        appendStringAttr(spanAttrs, "relaymesh.actor", event.actor());
        appendStringAttr(spanAttrs, "relaymesh.resource", event.resource());
        appendStringAttr(spanAttrs, "relaymesh.result", event.result());
        appendStringAttr(spanAttrs, "relaymesh.task_id", event.taskId());
        appendStringAttr(spanAttrs, "relaymesh.step_id", event.stepId());
        appendStringAttr(spanAttrs, "relaymesh.principal_id", event.principalId());

        String line = Jsons.toJson(root) + System.lineSeparator();
        try {
            Files.writeString(outputFile, line, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write OTel span export", e);
        }
    }

    private void appendStringAttr(ArrayNode attrs, String key, String value) {
        if (key == null || key.isBlank()) {
            return;
        }
        if (value == null || value.isBlank()) {
            return;
        }
        ObjectNode item = attrs.addObject();
        item.put("key", key);
        item.putObject("value").put("stringValue", value);
    }

    private String normalizeTraceId(String raw) {
        if (raw != null) {
            String trimmed = raw.trim().toLowerCase();
            if (trimmed.matches("^[0-9a-f]{32}$")) {
                return trimmed;
            }
        }
        String fallback = Integer.toHexString(Math.abs((raw == null ? "" : raw).hashCode()));
        return "000000000000000000000000" + String.format("%08x", fallback.hashCode());
    }

    private String normalizeSpanId(String rawSpanId, String action, Instant timestamp) {
        if (rawSpanId != null) {
            String trimmed = rawSpanId.trim().toLowerCase();
            if (trimmed.matches("^[0-9a-f]{16}$")) {
                return trimmed;
            }
        }
        String src = (action == null ? "" : action) + "|" + timestamp.toEpochMilli();
        return String.format("%016x", Math.abs(src.hashCode()));
    }
}
