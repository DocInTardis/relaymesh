package io.relaymesh.agent;

public record AgentResult(
        boolean success,
        String output,
        String error
) {
    public static AgentResult ok(String output) {
        return new AgentResult(true, output, null);
    }

    public static AgentResult fail(String error) {
        return new AgentResult(false, null, error);
    }
}

