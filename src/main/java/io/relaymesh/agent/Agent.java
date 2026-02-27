package io.relaymesh.agent;

public interface Agent {
    String id();

    AgentResult execute(AgentContext context) throws Exception;
}

