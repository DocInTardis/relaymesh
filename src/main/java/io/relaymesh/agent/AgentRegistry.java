package io.relaymesh.agent;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public final class AgentRegistry {
    private final Map<String, Agent> agents = new ConcurrentHashMap<>();

    public void register(Agent agent) {
        agents.put(agent.id(), agent);
    }

    public Optional<Agent> findById(String agentId) {
        return Optional.ofNullable(agents.get(agentId));
    }

    public Collection<String> listAgentIds() {
        return agents.keySet();
    }
}

