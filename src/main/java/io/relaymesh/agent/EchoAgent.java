package io.relaymesh.agent;

import java.time.Instant;
import io.relaymesh.util.Jsons;

public final class EchoAgent implements Agent {
    @Override
    public String id() {
        return "echo";
    }

    @Override
    public AgentResult execute(AgentContext context) {
        String escapedPayload = Jsons.toJson(context.payload());
        String output = """
                {
                  "agent": "echo",
                  "timestamp": "%s",
                  "taskId": "%s",
                  "stepId": "%s",
                  "traceId": "%s",
                  "spanId": "%s",
                  "traceParent": "%s",
                  "received": %s
                }
                """.formatted(
                Instant.now(),
                context.taskId(),
                context.stepId(),
                context.traceId(),
                context.spanId(),
                context.traceParent(),
                escapedPayload
        );
        return AgentResult.ok(output);
    }
}
