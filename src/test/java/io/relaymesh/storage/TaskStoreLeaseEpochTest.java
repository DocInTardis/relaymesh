package io.relaymesh.storage;

import io.relaymesh.config.RelayMeshConfig;
import io.relaymesh.model.TaskStatus;
import io.relaymesh.model.TaskView;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

final class TaskStoreLeaseEpochTest {

    @Test
    void leaseEpochFencesStaleFailureCommit() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-lease-epoch-");
        try {
            RelayMeshConfig config = RelayMeshConfig.fromRoot(root.toString());
            Database db = new Database(config);
            db.init();
            TaskStore store = new TaskStore(db);

            long now = Instant.now().toEpochMilli();
            String taskId = "tsk_" + UUID.randomUUID();
            String stepId = "stp_" + UUID.randomUUID();
            String msgId = "msg_" + UUID.randomUUID();
            String idempotencyKey = "idem_" + UUID.randomUUID();

            TaskStore.Submission sub = new TaskStore.Submission(
                    taskId,
                    stepId,
                    msgId,
                    "echo",
                    idempotencyKey,
                    "principal:test",
                    "trace_lease_epoch",
                    "NORMAL",
                    root.resolve("meta").resolve(msgId + ".meta.json").toString(),
                    root.resolve("payload").resolve(msgId + ".payload.json").toString(),
                    now
            );
            TaskStore.SubmitResult submit = store.submitTask(sub);
            Assertions.assertFalse(submit.deduplicated());

            TaskStore.LeaseGrant grant = store.tryMarkTaskRunning(taskId, stepId, "node-a", "lease-a", now + 1L);
            Assertions.assertTrue(grant.started());
            Assertions.assertEquals(1L, grant.leaseEpoch());

            Assertions.assertTrue(store.heartbeatLease(stepId, "lease-a", grant.leaseEpoch(), now + 2L));
            Assertions.assertFalse(store.heartbeatLease(stepId, "lease-a", grant.leaseEpoch() + 1L, now + 2L));

            TaskStore.FailureResolution stale = store.tryCompleteFailureWithLease(
                    taskId,
                    stepId,
                    idempotencyKey,
                    "lease-a",
                    grant.leaseEpoch() + 1L,
                    "boom",
                    now + 3L,
                    3,
                    1_000L,
                    5_000L
            );
            Assertions.assertEquals(TaskStore.FailureOutcome.STALE_LEASE, stale.outcome());

            TaskStore.FailureResolution accepted = store.tryCompleteFailureWithLease(
                    taskId,
                    stepId,
                    idempotencyKey,
                    "lease-a",
                    grant.leaseEpoch(),
                    "boom",
                    now + 4L,
                    3,
                    1_000L,
                    5_000L
            );
            Assertions.assertEquals(TaskStore.FailureOutcome.RETRY_SCHEDULED, accepted.outcome());

            Optional<TaskView> task = store.getTask(taskId);
            Assertions.assertTrue(task.isPresent());
            Assertions.assertEquals(TaskStatus.RETRYING.name(), task.get().status());

            List<TaskStore.LeaseConflict> conflicts = store.listLeaseConflicts(20, now - 1_000L);
            Assertions.assertTrue(conflicts.stream().anyMatch(c ->
                    "failure_commit_conflict".equals(c.eventType())
                            && Long.valueOf(grant.leaseEpoch() + 1L).equals(c.expectedLeaseEpoch())
                            && Long.valueOf(grant.leaseEpoch()).equals(c.actualLeaseEpoch())
            ));
        } finally {
            deleteRecursively(root);
        }
    }

    @Test
    void clusterEpochFencesOwnershipChangingWrites() throws Exception {
        Path root = Files.createTempDirectory("relaymesh-test-cluster-epoch-fence-");
        try {
            RelayMeshConfig config = RelayMeshConfig.fromRoot(root.toString());
            Database db = new Database(config);
            db.init();
            TaskStore store = new TaskStore(db);

            long now = Instant.now().toEpochMilli();
            String taskId = "tsk_" + UUID.randomUUID();
            String stepId = "stp_" + UUID.randomUUID();
            String msgId = "msg_" + UUID.randomUUID();
            String idempotencyKey = "idem_" + UUID.randomUUID();

            TaskStore.Submission sub = new TaskStore.Submission(
                    taskId,
                    stepId,
                    msgId,
                    "echo",
                    idempotencyKey,
                    "principal:test",
                    "trace_cluster_epoch",
                    "NORMAL",
                    root.resolve("meta").resolve(msgId + ".meta.json").toString(),
                    root.resolve("payload").resolve(msgId + ".payload.json").toString(),
                    now
            );
            TaskStore.SubmitResult submit = store.submitTask(sub);
            Assertions.assertFalse(submit.deduplicated());

            long clusterEpoch = Long.parseLong(store.clusterEpoch().value());
            TaskStore.LeaseGrant rejected = store.tryMarkTaskRunning(
                    taskId,
                    stepId,
                    "node-a",
                    "lease-a",
                    now + 1L,
                    clusterEpoch + 1L
            );
            Assertions.assertFalse(rejected.started());

            TaskStore.LeaseGrant grant = store.tryMarkTaskRunning(
                    taskId,
                    stepId,
                    "node-a",
                    "lease-a",
                    now + 2L,
                    clusterEpoch
            );
            Assertions.assertTrue(grant.started());

            TaskStore.ClusterStateValue bumped = store.bumpClusterEpoch(now + 3L);
            long nextClusterEpoch = Long.parseLong(bumped.value());
            Assertions.assertEquals(clusterEpoch + 1L, nextClusterEpoch);

            TaskStore.StepSuccessResolution stale = store.tryMarkStepSuccessWithLease(
                    taskId,
                    stepId,
                    idempotencyKey,
                    "lease-a",
                    grant.leaseEpoch(),
                    clusterEpoch,
                    "ok-stale",
                    now + 4L
            );
            Assertions.assertEquals(TaskStore.StepSuccessOutcome.STALE_LEASE, stale.outcome());

            TaskStore.StepSuccessResolution accepted = store.tryMarkStepSuccessWithLease(
                    taskId,
                    stepId,
                    idempotencyKey,
                    "lease-a",
                    grant.leaseEpoch(),
                    nextClusterEpoch,
                    "ok-final",
                    now + 5L
            );
            Assertions.assertEquals(TaskStore.StepSuccessOutcome.TASK_COMPLETED, accepted.outcome());
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
