package io.relaymesh.runtime;

import io.relaymesh.config.RelayMeshConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

final class RelayMeshRuntimeGovernanceTest {

    @Test
    void auditIntegrityVerificationAndSiemExportWork() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-audit-integrity-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();
            runtime.submit("echo", "audit-integrity", "normal", null);

            RelayMeshRuntime.AuditIntegrityOutcome verify = runtime.verifyAuditIntegrity();
            Assertions.assertTrue(verify.ok());
            Assertions.assertTrue(verify.checkedRows() >= 1);

            Path out = root.resolve("siem").resolve("audit.ndjson");
            RelayMeshRuntime.SiemExportOutcome exported = runtime.exportAuditToSiem(out.toString(), 100, null, null);
            Assertions.assertTrue(exported.exported() >= 1);
            Assertions.assertTrue(Files.exists(out));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void payloadKeyRotationKeepsOldPayloadReadable() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-payload-rotate-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();
            RelayMeshRuntime.SubmitOutcome submitted = runtime.submit("echo", "rotate-key-payload", "normal", null);
            Assertions.assertNotNull(submitted.taskId());

            String beforeKid = runtime.payloadKeyStatus().activeKid();
            runtime.rotatePayloadKey();
            String afterKid = runtime.payloadKeyStatus().activeKid();
            Assertions.assertNotEquals(beforeKid, afterKid);

            RelayMeshRuntime.WorkerOutcome worker = runtime.runWorkerOnce("worker-rotate", "node-rotate");
            Assertions.assertTrue(worker.processed());
            Assertions.assertTrue(runtime.getTask(submitted.taskId()).isPresent());
            Assertions.assertEquals("SUCCESS", runtime.getTask(submitted.taskId()).orElseThrow().status());
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void admissionGateRejectsWhenQueueDepthExceeded() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-admission-gate-");
        try {
            Files.writeString(
                    root.resolve("relaymesh-settings.json"),
                    """
                            {
                              "maxIngressQueueDepth": 1,
                              "maxRunningSteps": 100,
                              "submitRateLimitPerMin": 100
                            }
                            """,
                    StandardCharsets.UTF_8
            );
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();
            runtime.submit("echo", "first", "normal", null);
            Assertions.assertThrows(IllegalStateException.class, () -> runtime.submit("echo", "second", "normal", null));

            RelayMeshRuntime.StatsOutcome stats = runtime.stats();
            Assertions.assertTrue(stats.submitRejectedTotal() >= 1L);
        } finally {
            deleteRecursively(root);
        }
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
