package io.relaymesh.model;

public record MessageEnvelope(
        String msgId,
        String taskId,
        String stepId,
        String fromAgent,
        String toAgent,
        String priority,
        long createdAtMs,
        String payloadPath,
        int attempt
) {
}

