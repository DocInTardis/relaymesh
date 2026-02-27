package io.relaymesh.agent;

public record AgentContext(
        String taskId,
        String stepId,
        String traceId,
        String spanId,
        String traceParent,
        String payload
) {
}
