package io.relaymesh.runtime;

import io.relaymesh.config.RelayMeshConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

final class RelayMeshRuntimeSecurityTest {

    @Test
    void gossipSigningAcceptsMatchingSecretAndRejectsMismatchedSecret() throws Exception {
        Path rootA = Files.createTempDirectory("relaymesh-test-gossip-sign-a-");
        Path rootB = Files.createTempDirectory("relaymesh-test-gossip-sign-b-");
        Path rootBad = Files.createTempDirectory("relaymesh-test-gossip-sign-bad-");
        try {
            writeSettings(rootA, "mesh-secret");
            writeSettings(rootB, "mesh-secret");
            writeSettings(rootBad, "bad-secret");

            RelayMeshRuntime runtimeA = new RelayMeshRuntime(RelayMeshConfig.fromRoot(rootA.toString()));
            RelayMeshRuntime runtimeB = new RelayMeshRuntime(RelayMeshConfig.fromRoot(rootB.toString()));
            RelayMeshRuntime runtimeBad = new RelayMeshRuntime(RelayMeshConfig.fromRoot(rootBad.toString()));
            runtimeA.init();
            runtimeB.init();
            runtimeBad.init();

            Pair signedPair = runPair(runtimeA, "node-a", 19880, runtimeB, "node-b", 19881);
            Assertions.assertTrue(signedPair.left.accepted() + signedPair.right.accepted() >= 1);

            Pair mismatchedPair = runPair(runtimeA, "node-a", 19882, runtimeBad, "node-bad", 19883);
            Assertions.assertTrue(mismatchedPair.right.invalid() >= 1);

            String metricsSigned = runtimeA.metricsText();
            Assertions.assertTrue(metricsSigned.contains("relaymesh_gossip_signing_enabled 1"));
        } finally {
            deleteRecursively(rootA);
            deleteRecursively(rootB);
            deleteRecursively(rootBad);
        }
    }

    private static Pair runPair(
            RelayMeshRuntime leftRuntime,
            String leftNodeId,
            int leftPort,
            RelayMeshRuntime rightRuntime,
            String rightNodeId,
            int rightPort
    ) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<RelayMeshRuntime.GossipSyncOutcome> rightFuture = pool.submit(() ->
                    rightRuntime.gossipSync(
                            rightNodeId,
                            rightPort,
                            List.of("127.0.0.1:" + leftPort),
                            700,
                            6,
                            100L,
                            1,
                            2
                    ));
            Thread.sleep(200L);
            Future<RelayMeshRuntime.GossipSyncOutcome> leftFuture = pool.submit(() ->
                    leftRuntime.gossipSync(
                            leftNodeId,
                            leftPort,
                            List.of("127.0.0.1:" + rightPort),
                            700,
                            6,
                            100L,
                            1,
                            2
                    ));
            RelayMeshRuntime.GossipSyncOutcome left = leftFuture.get();
            RelayMeshRuntime.GossipSyncOutcome right = rightFuture.get();
            return new Pair(left, right);
        } finally {
            pool.shutdownNow();
        }
    }

    private static void writeSettings(Path root, String secret) throws IOException {
        String body = """
                {
                  "gossipSharedSecret": "%s",
                  "gossipFanout": 1,
                  "gossipPacketTtl": 2,
                  "gossipSyncSampleSize": 16
                }
                """.formatted(secret);
        Files.writeString(root.resolve("relaymesh-settings.json"), body, StandardCharsets.UTF_8);
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

    private record Pair(RelayMeshRuntime.GossipSyncOutcome left, RelayMeshRuntime.GossipSyncOutcome right) {
    }
}
