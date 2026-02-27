package io.relaymesh.model;

public record TaskView(
        String taskId,
        String status,
        String idempotencyKey,
        String traceId,
        String resultPayload,
        String lastError,
        long createdAtMs,
        long updatedAtMs
) {
}

