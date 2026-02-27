package io.relaymesh.agent;

public final class FailAgent implements Agent {
    @Override
    public String id() {
        return "fail";
    }

    @Override
    public AgentResult execute(AgentContext context) {
        return AgentResult.fail("intentional failure from fail agent");
    }
}

