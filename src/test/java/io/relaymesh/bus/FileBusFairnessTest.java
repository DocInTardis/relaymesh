package io.relaymesh.bus;

import io.relaymesh.config.RelayMeshConfig;
import io.relaymesh.model.MessageEnvelope;
import io.relaymesh.model.Priority;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

final class FileBusFairnessTest {

    @Test
    void forcesLowAfterConfiguredHighStreak() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-filebus-fair-");
        try {
            RelayMeshConfig config = RelayMeshConfig.fromRoot(root.toString());
            createBusDirs(config);
            FileBus bus = new FileBus(config, 2);

            long now = System.currentTimeMillis();
            enqueue(bus, config, "msg-h-1", "task-h-1", "step-h-1", Priority.HIGH, now + 1);
            enqueue(bus, config, "msg-h-2", "task-h-2", "step-h-2", Priority.HIGH, now + 2);
            enqueue(bus, config, "msg-h-3", "task-h-3", "step-h-3", Priority.HIGH, now + 3);
            enqueue(bus, config, "msg-low-1", "task-low-1", "step-low-1", Priority.LOW, now + 4);

            FileBus.ClaimedMessage c1 = bus.claimNext("w1").orElseThrow();
            FileBus.ClaimedMessage c2 = bus.claimNext("w1").orElseThrow();
            FileBus.ClaimedMessage c3 = bus.claimNext("w1").orElseThrow();

            Assertions.assertEquals(Priority.HIGH.name(), c1.envelope().priority());
            Assertions.assertEquals(Priority.HIGH.name(), c2.envelope().priority());
            Assertions.assertEquals(Priority.LOW.name(), c3.envelope().priority());
            Assertions.assertEquals(1L, bus.lowStarvationCount());
        } finally {
            deleteRecursively(root);
        }
    }

    private static void enqueue(
            FileBus bus,
            RelayMeshConfig config,
            String msgId,
            String taskId,
            String stepId,
            Priority priority,
            long createdAtMs
    ) {
        Path payloadPath = config.payloadDir().resolve(msgId + ".payload.json");
        MessageEnvelope envelope = new MessageEnvelope(
                msgId,
                taskId,
                stepId,
                "system",
                "echo",
                priority.name(),
                createdAtMs,
                payloadPath.toString(),
                1
        );
        bus.enqueue(envelope, "payload-" + msgId);
    }

    private static void createBusDirs(RelayMeshConfig config) throws IOException {
        Files.createDirectories(config.rootDir());
        Files.createDirectories(config.metaDir());
        Files.createDirectories(config.payloadDir());
        Files.createDirectories(config.processingDir());
        Files.createDirectories(config.inboxHigh());
        Files.createDirectories(config.inboxNormal());
        Files.createDirectories(config.inboxLow());
        Files.createDirectories(config.doneRoot());
        Files.createDirectories(config.deadRoot());
        Files.createDirectories(config.retryRoot());
        Files.createDirectories(config.auditRoot());
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (Stream<Path> walk = Files.walk(root)) {
            for (Path path : walk.sorted((a, b) -> Integer.compare(b.getNameCount(), a.getNameCount())).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
