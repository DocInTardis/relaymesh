package io.relaymesh.agent;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class ScriptAgent implements Agent {
    private static final int MAX_ERROR_CHARS = 512;

    private final String id;
    private final List<String> command;
    private final long timeoutMs;

    public ScriptAgent(String id, List<String> command, long timeoutMs) {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("script agent id cannot be empty");
        }
        if (command == null || command.isEmpty()) {
            throw new IllegalArgumentException("script agent command cannot be empty: " + id);
        }
        this.id = id;
        this.command = List.copyOf(command);
        this.timeoutMs = Math.max(1_000L, timeoutMs);
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public AgentResult execute(AgentContext context) {
        ProcessBuilder pb = new ProcessBuilder(new ArrayList<>(command));
        pb.redirectErrorStream(true);
        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            return AgentResult.fail("script spawn failed: " + e.getMessage());
        }

        try {
            byte[] input = context.payload() == null
                    ? new byte[0]
                    : context.payload().getBytes(StandardCharsets.UTF_8);
            process.getOutputStream().write(input);
            process.getOutputStream().flush();
            process.getOutputStream().close();

            boolean finished = process.waitFor(timeoutMs, TimeUnit.MILLISECONDS);
            if (!finished) {
                process.destroyForcibly();
                process.waitFor(1, TimeUnit.SECONDS);
                return AgentResult.fail("script timeout after " + Duration.ofMillis(timeoutMs));
            }

            String combined = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            if (process.exitValue() == 0) {
                return AgentResult.ok(combined.strip());
            }
            return AgentResult.fail("script exit=" + process.exitValue() + " output=" + truncate(combined));
        } catch (Exception e) {
            process.destroyForcibly();
            return AgentResult.fail("script execution failed: " + e.getMessage());
        }
    }

    private String truncate(String raw) {
        if (raw == null) {
            return "";
        }
        String normalized = raw.replace("\r", " ").replace("\n", " ").trim();
        if (normalized.length() <= MAX_ERROR_CHARS) {
            return normalized;
        }
        return normalized.substring(0, MAX_ERROR_CHARS) + "...";
    }
}
