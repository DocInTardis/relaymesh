package io.relaymesh.runtime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.relaymesh.config.RelayMeshConfig;
import io.relaymesh.util.Jsons;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

final class RelayMeshRuntimeProductionTest {

    @Test
    void webCompatibilityAndRateLimitCountersAreObservable() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-web-counters-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            long compatTotal = runtime.markWebGetWriteCompat(
                    "/api/replay-batch",
                    Map.of("method", "GET", "timestamp", Instant.now().toString())
            );
            long rateLimitedTotal = runtime.markWebWriteRateLimited(
                    "/api/replay",
                    Map.of("method", "POST", "timestamp", Instant.now().toString())
            );

            Assertions.assertEquals(1L, compatTotal);
            Assertions.assertEquals(1L, rateLimitedTotal);

            RelayMeshRuntime.StatsOutcome stats = runtime.stats();
            Assertions.assertEquals(1L, stats.webWriteGetCompatTotal());
            Assertions.assertEquals(1L, stats.webWriteRateLimitedTotal());

            String metrics = runtime.metricsText();
            Assertions.assertTrue(metrics.contains("relaymesh_web_write_get_compat_total 1"));
            Assertions.assertTrue(metrics.contains("relaymesh_web_write_rate_limited_total 1"));
            Assertions.assertTrue(metrics.contains("relaymesh_low_starvation_count"));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void clusterEpochCanBeBumpedAndObservedInStatsAndMetrics() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-cluster-epoch-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            RelayMeshRuntime.ClusterStateOutcome before = runtime.clusterState();
            Assertions.assertEquals("cluster_epoch", before.key());
            Assertions.assertTrue(before.epoch() >= 1L);

            RelayMeshRuntime.ClusterEpochBumpOutcome bumped = runtime.bumpClusterEpoch("test-bump", "junit");
            Assertions.assertEquals(before.epoch() + 1L, bumped.currentEpoch());
            Assertions.assertEquals(before.epoch(), bumped.previousEpoch());

            RelayMeshRuntime.ClusterStateOutcome after = runtime.clusterState();
            Assertions.assertEquals(bumped.currentEpoch(), after.epoch());

            RelayMeshRuntime.StatsOutcome stats = runtime.stats();
            Assertions.assertEquals(after.epoch(), stats.clusterEpoch());

            String metrics = runtime.metricsText();
            Assertions.assertTrue(metrics.contains("relaymesh_cluster_epoch " + after.epoch()));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void replicationEnvelopeExportIncludesTaskStepAndMessageDeltas() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-replication-export-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            RelayMeshRuntime.SubmitOutcome submit = runtime.submit("echo", "replication-envelope", "high", null);
            Assertions.assertNotNull(submit.taskId());

            Path out = root.resolve("replication").resolve("envelope.json");
            RelayMeshRuntime.ReplicationExportOutcome exported =
                    runtime.exportReplicationEnvelope(0L, 200, out.toString());

            Assertions.assertEquals("relaymesh.replication.v1", exported.schemaVersion());
            Assertions.assertTrue(exported.taskDeltaCount() >= 1);
            Assertions.assertTrue(exported.stepDeltaCount() >= 1);
            Assertions.assertTrue(exported.messageDeltaCount() >= 1);
            Assertions.assertTrue(exported.cursorMaxMs() >= 0L);
            Assertions.assertTrue(Files.exists(out));

            JsonNode envelope = Jsons.mapper().readTree(Files.readString(out, StandardCharsets.UTF_8));
            Assertions.assertEquals("relaymesh.replication.v1", envelope.path("schema_version").asText());
            Assertions.assertEquals(exported.cursorMaxMs(), envelope.path("cursor_max_ms").asLong());
            Assertions.assertEquals(exported.clusterEpoch(), envelope.path("cluster_epoch").asLong());
            Assertions.assertEquals(exported.taskDeltaCount(), envelope.path("task_deltas").size());
            Assertions.assertEquals(exported.stepDeltaCount(), envelope.path("step_deltas").size());
            Assertions.assertEquals(exported.messageDeltaCount(), envelope.path("message_deltas").size());
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void replicationEnvelopeImportAppliesRemoteDeltas() throws Exception {
        Path sourceRoot = Files.createTempDirectory("relaymesh-test-replication-source-");
        Path targetRoot = Files.createTempDirectory("relaymesh-test-replication-target-");
        try {
            RelayMeshRuntime source = new RelayMeshRuntime(RelayMeshConfig.fromRoot(sourceRoot.toString()));
            source.init();
            RelayMeshRuntime.SubmitOutcome submit = source.submit("echo", "replication-import", "normal", null);

            Path envelopePath = sourceRoot.resolve("replication").resolve("envelope.json");
            source.exportReplicationEnvelope(0L, 200, envelopePath.toString());

            RelayMeshRuntime target = new RelayMeshRuntime(RelayMeshConfig.fromRoot(targetRoot.toString()));
            target.init();
            RelayMeshRuntime.ReplicationImportOutcome imported =
                    target.importReplicationEnvelope(envelopePath.toString(), "junit");

            Assertions.assertEquals("relaymesh.replication.v1", imported.schemaVersion());
            Assertions.assertTrue(imported.taskDeltaCount() >= 1);
            Assertions.assertTrue(imported.stepDeltaCount() >= 1);
            Assertions.assertTrue(imported.messageDeltaCount() >= 1);
            Assertions.assertTrue(imported.taskApplied() >= 1);
            Assertions.assertTrue(imported.stepApplied() >= 1);
            Assertions.assertTrue(imported.messageApplied() >= 1);
            Assertions.assertTrue(imported.queueEnqueued() >= 1);

            Assertions.assertTrue(target.getTask(submit.taskId()).isPresent());
            Assertions.assertTrue(target.workflow(submit.taskId()).isPresent());

            RelayMeshRuntime.WorkerOutcome processed = target.runWorkerOnce("worker-target", "node-target");
            Assertions.assertTrue(processed.processed());

            RelayMeshRuntime.ReplicationImportOutcome duplicate =
                    target.importReplicationEnvelope(envelopePath.toString(), "junit");
            Assertions.assertEquals(0, duplicate.taskApplied());
            Assertions.assertEquals(0, duplicate.stepApplied());
            Assertions.assertEquals(0, duplicate.messageApplied());
            Assertions.assertTrue(duplicate.staleSkipped() >= 1);
        } finally {
            deleteRecursively(sourceRoot);
            deleteRecursively(targetRoot);
        }
    }

    @Test
    void replicationImportSkipsOutOfOrderOlderEnvelope() throws Exception {
        Path sourceRoot = Files.createTempDirectory("relaymesh-test-replication-order-source-");
        Path targetRoot = Files.createTempDirectory("relaymesh-test-replication-order-target-");
        try {
            RelayMeshRuntime source = new RelayMeshRuntime(RelayMeshConfig.fromRoot(sourceRoot.toString()));
            source.init();
            RelayMeshRuntime.SubmitOutcome submit = source.submit("echo", "replication-order", "normal", null);

            Path oldEnvelope = sourceRoot.resolve("replication").resolve("old-envelope.json");
            source.exportReplicationEnvelope(0L, 500, oldEnvelope.toString());

            RelayMeshRuntime.WorkerOutcome run = source.runWorkerOnce("worker-src", "node-src");
            Assertions.assertTrue(run.processed());

            Path newEnvelope = sourceRoot.resolve("replication").resolve("new-envelope.json");
            source.exportReplicationEnvelope(0L, 500, newEnvelope.toString());

            RelayMeshRuntime target = new RelayMeshRuntime(RelayMeshConfig.fromRoot(targetRoot.toString()));
            target.init();

            RelayMeshRuntime.ReplicationImportOutcome first =
                    target.importReplicationEnvelope(newEnvelope.toString(), "junit");
            Assertions.assertTrue(first.taskApplied() >= 1);
            Assertions.assertTrue(first.stepApplied() >= 1);
            Assertions.assertTrue(first.messageApplied() >= 1);

            String statusBeforeOlderImport = target.getTask(submit.taskId())
                    .orElseThrow()
                    .status();
            Assertions.assertEquals("SUCCESS", statusBeforeOlderImport);

            RelayMeshRuntime.ReplicationImportOutcome second =
                    target.importReplicationEnvelope(oldEnvelope.toString(), "junit");
            Assertions.assertEquals(0, second.taskApplied());
            Assertions.assertEquals(0, second.stepApplied());
            Assertions.assertEquals(0, second.messageApplied());
            Assertions.assertTrue(second.staleSkipped() >= 1);

            String statusAfterOlderImport = target.getTask(submit.taskId())
                    .orElseThrow()
                    .status();
            Assertions.assertEquals("SUCCESS", statusAfterOlderImport);
        } finally {
            deleteRecursively(sourceRoot);
            deleteRecursively(targetRoot);
        }
    }

    @Test
    void partitionSimulationHarnessConvergesAfterHeal() throws Exception {
        Path nodeARoot = Files.createTempDirectory("relaymesh-test-partition-node-a-");
        Path nodeBRoot = Files.createTempDirectory("relaymesh-test-partition-node-b-");
        try {
            RelayMeshRuntime nodeA = new RelayMeshRuntime(RelayMeshConfig.fromRoot(nodeARoot.toString()));
            RelayMeshRuntime nodeB = new RelayMeshRuntime(RelayMeshConfig.fromRoot(nodeBRoot.toString()));
            nodeA.init();
            nodeB.init();

            RelayMeshRuntime.SubmitOutcome submit = nodeA.submit("echo", "partition-harness", "normal", null);
            String taskId = submit.taskId();
            Assertions.assertNotNull(taskId);

            Path envelopeInit = nodeARoot.resolve("replication").resolve("partition-init.json");
            nodeA.exportReplicationEnvelope(0L, 1000, envelopeInit.toString());
            RelayMeshRuntime.ReplicationImportOutcome boot = nodeB.importReplicationEnvelope(envelopeInit.toString(), "junit");
            Assertions.assertTrue(boot.queueEnqueued() >= 1);

            RelayMeshRuntime.WorkerOutcome aRun = nodeA.runWorkerOnce("worker-a", "node-a");
            RelayMeshRuntime.WorkerOutcome bRun = nodeB.runWorkerOnce("worker-b", "node-b");
            Assertions.assertTrue(aRun.processed());
            Assertions.assertTrue(bRun.processed());

            Path envelopeA = nodeARoot.resolve("replication").resolve("partition-heal-a.json");
            nodeA.exportReplicationEnvelope(0L, 1000, envelopeA.toString());
            nodeB.importReplicationEnvelope(envelopeA.toString(), "junit");

            Path envelopeB = nodeBRoot.resolve("replication").resolve("partition-heal-b.json");
            nodeB.exportReplicationEnvelope(0L, 1000, envelopeB.toString());
            nodeA.importReplicationEnvelope(envelopeB.toString(), "junit");

            String statusA = nodeA.getTask(taskId).orElseThrow().status();
            String statusB = nodeB.getTask(taskId).orElseThrow().status();
            Assertions.assertEquals(statusA, statusB);
            Assertions.assertEquals("SUCCESS", statusA);
        } finally {
            deleteRecursively(nodeARoot);
            deleteRecursively(nodeBRoot);
        }
    }

    @Test
    void replicationTieBreakRespectsEpochVersionAndNodeOrdering() throws Exception {
        Path sourceRoot = Files.createTempDirectory("relaymesh-test-replication-tie-source-");
        Path targetRoot = Files.createTempDirectory("relaymesh-test-replication-tie-target-");
        try {
            RelayMeshRuntime source = new RelayMeshRuntime(RelayMeshConfig.fromRoot(sourceRoot.toString()));
            RelayMeshRuntime target = new RelayMeshRuntime(RelayMeshConfig.fromRoot(targetRoot.toString()));
            source.init();
            target.init();

            RelayMeshRuntime.SubmitOutcome submit = source.submit("echo", "tie-break", "normal", null);
            String taskId = submit.taskId();
            String stepId = source.workflow(taskId).orElseThrow().steps().get(0).stepId();

            Path envelope = sourceRoot.resolve("replication").resolve("tie-envelope.json");
            source.exportReplicationEnvelope(0L, 500, envelope.toString(), "node-a");

            ObjectNode root = (ObjectNode) Jsons.mapper().readTree(Files.readString(envelope, StandardCharsets.UTF_8));
            root.put("cluster_epoch", 1L);
            root.put("source_node_id", "node-a");
            ArrayNode steps = (ArrayNode) root.path("step_deltas");
            Assertions.assertTrue(steps.size() >= 1);
            ObjectNode step = (ObjectNode) steps.get(0);
            step.put("status", "RUNNING");
            step.put("leaseOwner", "node-a");
            step.put("leaseToken", "lease-a");
            step.put("leaseEpoch", 7L);
            step.put("updatedAtMs", 900_000L);
            step.put("createdAtMs", 900_000L);
            Files.writeString(envelope, Jsons.toJson(root), StandardCharsets.UTF_8);

            target.importReplicationEnvelope(envelope.toString(), "junit");
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + targetRoot.resolve("relaymesh.db"))) {
                try (PreparedStatement ps = c.prepareStatement(
                        "UPDATE steps SET status='RUNNING',lease_owner='node-z',lease_token='lease-z',lease_epoch=7,updated_at_ms=900000,created_at_ms=900000 WHERE step_id=?")) {
                    ps.setString(1, stepId);
                    ps.executeUpdate();
                }
            }

            RelayMeshRuntime.ReplicationImportOutcome nodeOrderTie =
                    target.importReplicationEnvelope(envelope.toString(), "junit");
            Assertions.assertTrue(nodeOrderTie.tieResolved() >= 1);
            Assertions.assertEquals("node-a", readStepLeaseOwner(targetRoot, stepId));

            target.bumpClusterEpoch("test", "junit");
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + targetRoot.resolve("relaymesh.db"))) {
                try (PreparedStatement ps = c.prepareStatement(
                        "UPDATE steps SET status='RUNNING',lease_owner='node-z',lease_token='lease-z',lease_epoch=7,updated_at_ms=900000,created_at_ms=900000 WHERE step_id=?")) {
                    ps.setString(1, stepId);
                    ps.executeUpdate();
                }
            }

            RelayMeshRuntime.ReplicationImportOutcome epochTie =
                    target.importReplicationEnvelope(envelope.toString(), "junit");
            Assertions.assertTrue(epochTie.tieKeptLocal() >= 1);
            Assertions.assertEquals("node-z", readStepLeaseOwner(targetRoot, stepId));
        } finally {
            deleteRecursively(sourceRoot);
            deleteRecursively(targetRoot);
        }
    }

    @Test
    void convergenceReportSummarizesReplicationImportsAndWritesOutput() throws Exception {
        Path sourceRoot = Files.createTempDirectory("relaymesh-test-convergence-source-");
        Path targetRoot = Files.createTempDirectory("relaymesh-test-convergence-target-");
        try {
            RelayMeshRuntime source = new RelayMeshRuntime(RelayMeshConfig.fromRoot(sourceRoot.toString()));
            RelayMeshRuntime target = new RelayMeshRuntime(RelayMeshConfig.fromRoot(targetRoot.toString()));
            source.init();
            target.init();

            source.submit("echo", "convergence", "normal", null);
            Path envelope = sourceRoot.resolve("replication").resolve("convergence.json");
            source.exportReplicationEnvelope(0L, 200, envelope.toString(), "node-a");
            target.importReplicationEnvelope(envelope.toString(), "junit");

            Path reportPath = targetRoot.resolve("reports").resolve("convergence.json");
            RelayMeshRuntime.ConvergenceReportOutcome report =
                    target.convergenceReport(24, 200, reportPath.toString());
            Assertions.assertTrue(report.imports() >= 1);
            Assertions.assertTrue(report.appliedRows() >= 1);
            Assertions.assertTrue(report.sourceNodeCounts().containsKey("node-a"));
            Assertions.assertTrue(Files.exists(reportPath));

            String metrics = target.metricsText();
            Assertions.assertTrue(metrics.contains("relaymesh_replication_import_total"));
            Assertions.assertTrue(metrics.contains("relaymesh_replication_last_import_lag_ms"));
        } finally {
            deleteRecursively(sourceRoot);
            deleteRecursively(targetRoot);
        }
    }

    @Test
    void meshSummaryAndMetricsExposeMembershipRiskView() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-mesh-summary-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            RelayMeshRuntime.SubmitOutcome t1 = runtime.submit("echo", "m1", "normal", null);
            RelayMeshRuntime.SubmitOutcome t2 = runtime.submit("echo", "m2", "normal", null);
            RelayMeshRuntime.SubmitOutcome t3 = runtime.submit("echo", "m3", "normal", null);
            RelayMeshRuntime.SubmitOutcome t4 = runtime.submit("echo", "m4", "normal", null);

            String s1 = runtime.workflow(t1.taskId()).orElseThrow().steps().get(0).stepId();
            String s2 = runtime.workflow(t2.taskId()).orElseThrow().steps().get(0).stepId();
            String s3 = runtime.workflow(t3.taskId()).orElseThrow().steps().get(0).stepId();
            String s4 = runtime.workflow(t4.taskId()).orElseThrow().steps().get(0).stepId();

            long now = Instant.now().toEpochMilli();
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + root.resolve("relaymesh.db").toString())) {
                try (PreparedStatement upNode = c.prepareStatement(
                        "INSERT INTO mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) VALUES(?,?,?,?,?) " +
                                "ON CONFLICT(node_id) DO UPDATE SET status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms");
                     PreparedStatement upTask = c.prepareStatement(
                             "UPDATE tasks SET status='RUNNING',updated_at_ms=? WHERE task_id=?");
                     PreparedStatement upStep = c.prepareStatement(
                             "UPDATE steps SET status='RUNNING',lease_owner=?,lease_token=?,updated_at_ms=? WHERE step_id=?")) {
                    upNode.setString(1, "node-a");
                    upNode.setString(2, "ALIVE");
                    upNode.setLong(3, now);
                    upNode.setLong(4, now);
                    upNode.setLong(5, now);
                    upNode.executeUpdate();

                    upNode.setString(1, "node-s");
                    upNode.setString(2, "SUSPECT");
                    upNode.setLong(3, now - 2_000L);
                    upNode.setLong(4, now - 2_000L);
                    upNode.setLong(5, now - 2_000L);
                    upNode.executeUpdate();

                    upNode.setString(1, "node-d");
                    upNode.setString(2, "DEAD");
                    upNode.setLong(3, now - 60_000L);
                    upNode.setLong(4, now - 60_000L);
                    upNode.setLong(5, now - 60_000L);
                    upNode.executeUpdate();

                    upTask.setLong(1, now);
                    upTask.setString(2, t1.taskId());
                    upTask.executeUpdate();
                    upTask.setLong(1, now);
                    upTask.setString(2, t2.taskId());
                    upTask.executeUpdate();
                    upTask.setLong(1, now);
                    upTask.setString(2, t3.taskId());
                    upTask.executeUpdate();
                    upTask.setLong(1, now);
                    upTask.setString(2, t4.taskId());
                    upTask.executeUpdate();

                    upStep.setString(1, "node-a");
                    upStep.setString(2, "lease-a");
                    upStep.setLong(3, now);
                    upStep.setString(4, s1);
                    upStep.executeUpdate();

                    upStep.setString(1, "node-s");
                    upStep.setString(2, "lease-s");
                    upStep.setLong(3, now);
                    upStep.setString(4, s2);
                    upStep.executeUpdate();

                    upStep.setString(1, "node-d");
                    upStep.setString(2, "lease-d");
                    upStep.setLong(3, now);
                    upStep.setString(4, s3);
                    upStep.executeUpdate();

                    upStep.setString(1, "node-unknown");
                    upStep.setString(2, "lease-u");
                    upStep.setLong(3, now);
                    upStep.setString(4, s4);
                    upStep.executeUpdate();
                }
            }

            RelayMeshRuntime.MeshSummaryOutcome mesh = runtime.meshSummary();
            Assertions.assertEquals(3, mesh.totalNodes());
            Assertions.assertEquals(1, mesh.aliveNodes());
            Assertions.assertEquals(1, mesh.suspectNodes());
            Assertions.assertEquals(1, mesh.deadNodes());
            Assertions.assertEquals(1, mesh.runningByAliveOwners());
            Assertions.assertEquals(1, mesh.runningBySuspectOwners());
            Assertions.assertEquals(1, mesh.runningByDeadOwners());
            Assertions.assertEquals(1, mesh.runningByUnknownOwners());
            Assertions.assertTrue(mesh.oldestDeadNodeAgeMs() >= 50_000L);

            RelayMeshRuntime.StatsOutcome stats = runtime.stats();
            Assertions.assertEquals(3, stats.meshTotalNodes());
            Assertions.assertEquals(1, stats.meshRunningByDeadOwners());
            Assertions.assertEquals(1, stats.meshRunningByUnknownOwners());

            String metrics = runtime.metricsText();
            Assertions.assertTrue(metrics.contains("relaymesh_mesh_nodes_total{status=\"alive\"} 1"));
            Assertions.assertTrue(metrics.contains("relaymesh_mesh_nodes_total{status=\"dead\"} 1"));
            Assertions.assertTrue(metrics.contains("relaymesh_running_steps_by_owner_status{owner_status=\"dead\"} 1"));
            Assertions.assertTrue(metrics.contains("relaymesh_running_steps_by_owner_status{owner_status=\"unknown\"} 1"));
            Assertions.assertTrue(metrics.contains("relaymesh_oldest_dead_node_age_ms"));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void workerPausesWhenLocalNodeIsSuspectAndResumesOnAlive() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-worker-pause-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            RelayMeshRuntime.SubmitOutcome submit = runtime.submit("echo", "pause-me", "normal", null);
            String taskId = submit.taskId();
            String stepId = runtime.workflow(taskId)
                    .orElseThrow()
                    .steps()
                    .stream()
                    .findFirst()
                    .orElseThrow()
                    .stepId();

            long now = Instant.now().toEpochMilli();
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + root.resolve("relaymesh.db").toString())) {
                try (PreparedStatement upNode = c.prepareStatement(
                        "INSERT INTO mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) VALUES(?,?,?,?,?) " +
                                "ON CONFLICT(node_id) DO UPDATE SET status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms")) {
                    upNode.setString(1, "node-pause");
                    upNode.setString(2, "SUSPECT");
                    upNode.setLong(3, now);
                    upNode.setLong(4, now);
                    upNode.setLong(5, now);
                    upNode.executeUpdate();
                }
            }

            RelayMeshRuntime.WorkerOutcome paused = runtime.runWorkerOnce("worker-a", "node-pause");
            Assertions.assertFalse(paused.processed());
            Assertions.assertTrue(paused.message().contains("paused"));

            String status1 = runtime.workflow(taskId)
                    .orElseThrow()
                    .steps()
                    .stream()
                    .filter(s -> stepId.equals(s.stepId()))
                    .findFirst()
                    .orElseThrow()
                    .status();
            Assertions.assertEquals("PENDING", status1);

            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + root.resolve("relaymesh.db").toString())) {
                try (PreparedStatement upNode = c.prepareStatement(
                        "UPDATE mesh_nodes SET status='ALIVE', last_heartbeat_ms=?, updated_at_ms=?, status_changed_at_ms=? WHERE node_id=?")) {
                    upNode.setLong(1, now + 1000L);
                    upNode.setLong(2, now + 1000L);
                    upNode.setLong(3, now + 1000L);
                    upNode.setString(4, "node-pause");
                    upNode.executeUpdate();
                }
            }

            RelayMeshRuntime.WorkerOutcome resumed = runtime.runWorkerOnce("worker-a", "node-pause");
            Assertions.assertTrue(resumed.processed());

            String status2 = runtime.workflow(taskId)
                    .orElseThrow()
                    .steps()
                    .stream()
                    .filter(s -> stepId.equals(s.stepId()))
                    .findFirst()
                    .orElseThrow()
                    .status();
            Assertions.assertEquals("SUCCESS", status2);

            RelayMeshRuntime.StatsOutcome stats = runtime.stats();
            Assertions.assertTrue(stats.workerPausedTotal() >= 1L);
            String metrics = runtime.metricsText();
            Assertions.assertTrue(metrics.contains("relaymesh_worker_paused_total"));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void settingsHistoryReturnsRecentRuntimeSettingsLoadSummaries() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-settings-history-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            Path settings = root.resolve("relaymesh-settings.json");
            String body = """
                    {
                      "leaseTimeoutMs": 12000,
                      "suspectAfterMs": 24000,
                      "deadAfterMs": 48000,
                      "maxAttempts": 3,
                      "baseBackoffMs": 1000,
                      "maxBackoffMs": 12000,
                      "retryDispatchLimit": 16,
                      "gossipFanout": 2,
                      "gossipPacketTtl": 2,
                      "gossipSyncSampleSize": 16,
                      "gossipDedupWindowMs": 30000,
                      "gossipDedupMaxEntries": 2048
                    }
                    """;
            Files.writeString(settings, body, StandardCharsets.UTF_8);

            RelayMeshRuntime.SettingsReloadOutcome reload = runtime.reloadSettings();
            Assertions.assertTrue(reload.configExists());

            List<RelayMeshRuntime.SettingsHistoryEntry> history = runtime.settingsHistory(5);
            Assertions.assertFalse(history.isEmpty());

            RelayMeshRuntime.SettingsHistoryEntry latest = history.get(0);
            Assertions.assertEquals("reloaded", latest.result());
            Assertions.assertTrue(latest.changed());
            Assertions.assertTrue(latest.changedCount() > 0);
            Assertions.assertNotNull(latest.configMtimeMs());
            Assertions.assertNotNull(latest.configPath());
            Assertions.assertFalse(latest.changedFields().isEmpty());
            Assertions.assertTrue(latest.changedFields().stream().noneMatch(v -> v.matches(".*\\d+.*")));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void deadOwnerRecoveryReclaimsRunningStepAndEmitsMetrics() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-dead-owner-recovery-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            RelayMeshRuntime.SubmitOutcome submit = runtime.submit("echo", "v2-dead-owner", "normal", null);
            String taskId = submit.taskId();
            String stepId = runtime.workflow(taskId)
                    .orElseThrow()
                    .steps()
                    .stream()
                    .findFirst()
                    .orElseThrow()
                    .stepId();

            long now = Instant.now().toEpochMilli();
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + root.resolve("relaymesh.db").toString())) {
                try (PreparedStatement upTask = c.prepareStatement(
                        "UPDATE tasks SET status='RUNNING',updated_at_ms=? WHERE task_id=?");
                     PreparedStatement upStep = c.prepareStatement(
                             "UPDATE steps SET status='RUNNING',attempt=1,lease_owner='node-dead',lease_token='lease-dead-owner',updated_at_ms=? WHERE step_id=?");
                     PreparedStatement upNode = c.prepareStatement(
                             "INSERT INTO mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) VALUES(?,?,?,?,?) " +
                                     "ON CONFLICT(node_id) DO UPDATE SET status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms")) {
                    upTask.setLong(1, now);
                    upTask.setString(2, taskId);
                    upTask.executeUpdate();

                    upStep.setLong(1, now);
                    upStep.setString(2, stepId);
                    upStep.executeUpdate();

                    upNode.setString(1, "node-dead");
                    upNode.setString(2, "DEAD");
                    upNode.setLong(3, now - 120_000L);
                    upNode.setLong(4, now);
                    upNode.setLong(5, now);
                    upNode.executeUpdate();
                }
            }

            RelayMeshRuntime.DeadOwnerRecoveryOutcome recovered = runtime.recoverDeadOwnerLeases(100);
            Assertions.assertEquals(1, recovered.reclaimed());
            Assertions.assertTrue(recovered.deadNodes().contains("node-dead"));

            String stepStatus = runtime.workflow(taskId)
                    .orElseThrow()
                    .steps()
                    .stream()
                    .filter(s -> stepId.equals(s.stepId()))
                    .findFirst()
                    .orElseThrow()
                    .status();
            Assertions.assertEquals("RETRYING", stepStatus);

            RelayMeshRuntime.StatsOutcome stats = runtime.stats();
            Assertions.assertEquals(1L, stats.deadOwnerReclaimedTotal());
            String metrics = runtime.metricsText();
            Assertions.assertTrue(metrics.contains("relaymesh_dead_owner_reclaimed_total 1"));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void ownershipEventsIncludesRecoverAuditRows() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-ownership-events-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();
            runtime.recoverDeadOwnerLeases(32);

            List<RelayMeshRuntime.OwnershipEvent> events = runtime.ownershipEvents(20, 24);
            Assertions.assertFalse(events.isEmpty());
            Assertions.assertTrue(events.stream().anyMatch(e -> "ownership.recover".equals(e.action())));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void ownershipConflictDecisionIsDeterministicAndExposed() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-ownership-decision-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            RelayMeshRuntime.SubmitOutcome submit = runtime.submit("echo", "ownership-conflict", "normal", null);
            String taskId = submit.taskId();
            String stepId = runtime.workflow(taskId)
                    .orElseThrow()
                    .steps()
                    .stream()
                    .findFirst()
                    .orElseThrow()
                    .stepId();

            long now = Instant.now().toEpochMilli();
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + root.resolve("relaymesh.db").toString())) {
                try (PreparedStatement upTask = c.prepareStatement(
                        "UPDATE tasks SET status='RUNNING',updated_at_ms=? WHERE task_id=?");
                     PreparedStatement upStep = c.prepareStatement(
                             "UPDATE steps SET status='RUNNING',lease_owner='node-z',lease_token='lease-z',lease_epoch=5,updated_at_ms=? WHERE step_id=?")) {
                    upTask.setLong(1, now);
                    upTask.setString(2, taskId);
                    upTask.executeUpdate();

                    upStep.setLong(1, now);
                    upStep.setString(2, stepId);
                    upStep.executeUpdate();
                }
            }

            RelayMeshRuntime.WorkerOutcome out = runtime.runWorkerOnce("worker-a", "node-a");
            Assertions.assertTrue(out.processed());
            Assertions.assertTrue(out.message().contains("already taken"));

            RelayMeshRuntime.OwnershipEvent decision = runtime.ownershipEvents(100, 24)
                    .stream()
                    .filter(e -> "ownership.conflict.decision".equals(e.action()) && stepId.equals(e.stepId()))
                    .findFirst()
                    .orElseThrow();

            Assertions.assertEquals("node-z", String.valueOf(decision.details().get("winner_owner")));
            Assertions.assertEquals("running_owner_wins", String.valueOf(decision.details().get("rule")));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void ownershipEventsExposeLeaseEpochFieldsForConflicts() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-ownership-epoch-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            long now = Instant.now().toEpochMilli();
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + root.resolve("relaymesh.db").toString())) {
                try (PreparedStatement ps = c.prepareStatement(
                        """
                                INSERT INTO lease_conflicts(
                                    event_type,task_id,step_id,worker_id,
                                    expected_status,expected_lease_token,expected_lease_epoch,
                                    actual_status,actual_lease_owner,actual_lease_token,actual_lease_epoch,
                                    occurred_at_ms
                                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                                """)) {
                    ps.setString(1, "failure_commit_conflict");
                    ps.setString(2, "task-epoch");
                    ps.setString(3, "step-epoch");
                    ps.setString(4, "worker-epoch");
                    ps.setString(5, "RUNNING");
                    ps.setString(6, "lease-new");
                    ps.setLong(7, 7L);
                    ps.setString(8, "RUNNING");
                    ps.setString(9, "node-old");
                    ps.setString(10, "lease-old");
                    ps.setLong(11, 6L);
                    ps.setLong(12, now);
                    ps.executeUpdate();
                }
            }

            RelayMeshRuntime.OwnershipEvent row = runtime.ownershipEvents(20, 24)
                    .stream()
                    .filter(e -> "lease_conflicts".equals(e.source()) && "step-epoch".equals(e.stepId()))
                    .findFirst()
                    .orElseThrow();

            Assertions.assertEquals("failure_commit_conflict", row.action());
            Assertions.assertEquals(7L, ((Number) row.details().get("expected_lease_epoch")).longValue());
            Assertions.assertEquals(6L, ((Number) row.details().get("actual_lease_epoch")).longValue());
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void maintenanceAutoPrunesDeadNodesWhenConfigured() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-auto-prune-");
        try {
            String settings = """
                    {
                      "meshPruneOlderThanMs": 5000,
                      "meshPruneLimit": 10,
                      "meshPruneIntervalMs": 1
                    }
                    """;
            Files.writeString(root.resolve("relaymesh-settings.json"), settings, StandardCharsets.UTF_8);

            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            long now = Instant.now().toEpochMilli();
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + root.resolve("relaymesh.db").toString())) {
                try (PreparedStatement upNode = c.prepareStatement(
                        "INSERT INTO mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) VALUES(?,?,?,?,?) " +
                                "ON CONFLICT(node_id) DO UPDATE SET status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms")) {
                    upNode.setString(1, "node-prune-old");
                    upNode.setString(2, "DEAD");
                    upNode.setLong(3, now - 10_000L);
                    upNode.setLong(4, now - 10_000L);
                    upNode.setLong(5, now - 10_000L);
                    upNode.executeUpdate();

                    upNode.setString(1, "node-prune-fresh");
                    upNode.setString(2, "DEAD");
                    upNode.setLong(3, now - 1_000L);
                    upNode.setLong(4, now - 1_000L);
                    upNode.setLong(5, now - 1_000L);
                    upNode.executeUpdate();
                }
            }

            RelayMeshRuntime.MaintenanceOutcome out = runtime.runMaintenance("node-maint");
            Assertions.assertTrue(out.deadNodesPruned() >= 1);

            List<String> memberIds = runtime.members().stream().map(n -> n.nodeId()).toList();
            Assertions.assertFalse(memberIds.contains("node-prune-old"));
            Assertions.assertTrue(memberIds.contains("node-prune-fresh"));

            RelayMeshRuntime.StatsOutcome stats = runtime.stats();
            Assertions.assertTrue(stats.meshNodesPrunedTotal() >= 1L);
            String metrics = runtime.metricsText();
            Assertions.assertTrue(metrics.contains("relaymesh_mesh_nodes_pruned_total"));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void meshPruneRemovesStaleDeadNodesAndExposesMetric() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-mesh-prune-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            long now = Instant.now().toEpochMilli();
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + root.resolve("relaymesh.db").toString())) {
                try (PreparedStatement upNode = c.prepareStatement(
                        "INSERT INTO mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) VALUES(?,?,?,?,?) " +
                                "ON CONFLICT(node_id) DO UPDATE SET status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms")) {
                    upNode.setString(1, "node-old");
                    upNode.setString(2, "DEAD");
                    upNode.setLong(3, now - 7_200_000L);
                    upNode.setLong(4, now - 7_200_000L);
                    upNode.setLong(5, now - 7_200_000L);
                    upNode.executeUpdate();

                    upNode.setString(1, "node-fresh");
                    upNode.setString(2, "DEAD");
                    upNode.setLong(3, now - 1_000L);
                    upNode.setLong(4, now - 1_000L);
                    upNode.setLong(5, now - 1_000L);
                    upNode.executeUpdate();
                }
            }

            RelayMeshRuntime.MeshPruneOutcome prune = runtime.pruneDeadNodesByAge(3_600_000L, 10);
            Assertions.assertEquals(1, prune.pruned());

            List<String> memberIds = runtime.members().stream().map(n -> n.nodeId()).toList();
            Assertions.assertFalse(memberIds.contains("node-old"));
            Assertions.assertTrue(memberIds.contains("node-fresh"));

            RelayMeshRuntime.StatsOutcome stats = runtime.stats();
            Assertions.assertEquals(1L, stats.meshNodesPrunedTotal());
            String metrics = runtime.metricsText();
            Assertions.assertTrue(metrics.contains("relaymesh_mesh_nodes_pruned_total 1"));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void membershipRecoveryRespectsDeadDwellWindowAndRecoversAfterWindow() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-membership-dwell-");
        try {
            RelayMeshRuntime runtime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root.toString()));
            runtime.init();

            long now = Instant.now().toEpochMilli();
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + root.resolve("relaymesh.db").toString())) {
                try (PreparedStatement upNode = c.prepareStatement(
                        "INSERT INTO mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) VALUES(?,?,?,?,?) " +
                                "ON CONFLICT(node_id) DO UPDATE SET status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms")) {
                    upNode.setString(1, "node-flap");
                    upNode.setString(2, "DEAD");
                    upNode.setLong(3, now - 1_000L);
                    upNode.setLong(4, now);
                    upNode.setLong(5, now);
                    upNode.executeUpdate();
                }
            }

            runtime.runMaintenance("node-flap");
            String status1 = runtime.members().stream()
                    .filter(n -> "node-flap".equals(n.nodeId()))
                    .findFirst()
                    .orElseThrow()
                    .status();
            Assertions.assertEquals("DEAD", status1);

            RelayMeshRuntime.StatsOutcome stats = runtime.stats();
            Assertions.assertTrue(stats.membershipRecoverySuppressedTotal() >= 1L);
            String metrics = runtime.metricsText();
            Assertions.assertTrue(metrics.contains("relaymesh_membership_recovery_suppressed_total"));

            long oldChangedAt = now - 31_000L;
            try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + root.resolve("relaymesh.db").toString())) {
                try (PreparedStatement ageNode = c.prepareStatement(
                        "UPDATE mesh_nodes SET status='DEAD', last_heartbeat_ms=?, updated_at_ms=?, status_changed_at_ms=? WHERE node_id=?")) {
                    ageNode.setLong(1, now - 1_000L);
                    ageNode.setLong(2, oldChangedAt);
                    ageNode.setLong(3, oldChangedAt);
                    ageNode.setString(4, "node-flap");
                    ageNode.executeUpdate();
                }
            }

            runtime.runMaintenance("node-flap");
            String status2 = runtime.members().stream()
                    .filter(n -> "node-flap".equals(n.nodeId()))
                    .findFirst()
                    .orElseThrow()
                    .status();
            Assertions.assertEquals("ALIVE", status2);
        } finally {
            deleteRecursively(root);
        }
    }

    private static String readStepLeaseOwner(Path root, String stepId) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:sqlite:" + root.resolve("relaymesh.db"))) {
            try (PreparedStatement ps = c.prepareStatement("SELECT lease_owner FROM steps WHERE step_id=?")) {
                ps.setString(1, stepId);
                try (var rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return null;
                    }
                    return rs.getString("lease_owner");
                }
            }
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
