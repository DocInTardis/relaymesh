package io.relaymesh.storage;

import io.relaymesh.model.MessageState;
import io.relaymesh.model.TaskStatus;
import io.relaymesh.model.TaskView;
import io.relaymesh.util.Hashing;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.HashMap;
import java.util.Set;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

public final class TaskStore {
    private final Database database;
    private final String namespace;

    public TaskStore(Database database) {
        this.database = database;
        this.namespace = database.namespace();
    }

    public SubmitResult submitTask(Submission s) {
        String exist = findTaskIdByIdempotencyKey(s.idempotencyKey());
        if (exist != null) return new SubmitResult(exist, true);
        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try {
                try (PreparedStatement t = c.prepareStatement(
                        "INSERT INTO tasks(task_id,namespace,status,idempotency_key,principal_id,trace_id,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?,?,?)");
                     PreparedStatement st = c.prepareStatement(
                             "INSERT INTO steps(step_id,namespace,task_id,agent_id,priority,payload_path,status,attempt,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?)");
                     PreparedStatement m = c.prepareStatement(
                             "INSERT INTO messages(msg_id,namespace,task_id,step_id,from_agent,to_agent,priority,state,meta_path,payload_path,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)");
                     PreparedStatement idem = c.prepareStatement(
                             "INSERT INTO idempotency(namespace,idempotency_key,status,result_ref,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?)")) {
                    t.setString(1, s.taskId());
                    t.setString(2, namespace);
                    t.setString(3, TaskStatus.PENDING.name());
                    t.setString(4, s.idempotencyKey());
                    t.setString(5, s.principalId() == null ? "" : s.principalId());
                    t.setString(6, s.traceId());
                    t.setLong(7, s.nowMs());
                    t.setLong(8, s.nowMs());
                    t.executeUpdate();

                    st.setString(1, s.stepId());
                    st.setString(2, namespace);
                    st.setString(3, s.taskId());
                    st.setString(4, s.agentId());
                    st.setString(5, s.priority());
                    st.setString(6, s.payloadPath());
                    st.setString(7, TaskStatus.PENDING.name());
                    st.setInt(8, 0);
                    st.setLong(9, s.nowMs());
                    st.setLong(10, s.nowMs());
                    st.executeUpdate();

                    m.setString(1, s.msgId());
                    m.setString(2, namespace);
                    m.setString(3, s.taskId());
                    m.setString(4, s.stepId());
                    m.setString(5, "system");
                    m.setString(6, s.agentId());
                    m.setString(7, s.priority());
                    m.setString(8, MessageState.QUEUED.name());
                    m.setString(9, s.metaPath());
                    m.setString(10, s.payloadPath());
                    m.setLong(11, s.nowMs());
                    m.setLong(12, s.nowMs());
                    m.executeUpdate();

                    idem.setString(1, namespace);
                    idem.setString(2, s.idempotencyKey());
                    idem.setString(3, TaskStatus.PENDING.name());
                    idem.setString(4, s.taskId());
                    idem.setLong(5, s.nowMs());
                    idem.setLong(6, s.nowMs());
                    idem.executeUpdate();
                }
                c.commit();
                return new SubmitResult(s.taskId(), false);
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            String afterRace = findTaskIdByIdempotencyKey(s.idempotencyKey());
            if (afterRace != null) return new SubmitResult(afterRace, true);
            throw new RuntimeException("Failed to submit task", e);
        }
    }

    public WorkflowSubmitResult submitWorkflow(WorkflowSubmission wf) {
        String exist = findTaskIdByIdempotencyKey(wf.idempotencyKey());
        if (exist != null) return new WorkflowSubmitResult(exist, true, List.of());
        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement t = c.prepareStatement(
                    "INSERT INTO tasks(task_id,namespace,status,idempotency_key,principal_id,trace_id,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?,?,?)");
                 PreparedStatement st = c.prepareStatement(
                         "INSERT INTO steps(step_id,namespace,task_id,agent_id,priority,payload_path,is_enqueued,status,attempt,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?)");
                 PreparedStatement dep = c.prepareStatement(
                         "INSERT INTO step_dependencies(namespace,task_id,step_id,depends_on_step_id,created_at_ms) VALUES(?,?,?,?,?)");
                 PreparedStatement idem = c.prepareStatement(
                         "INSERT INTO idempotency(namespace,idempotency_key,status,result_ref,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?)")) {
                t.setString(1, wf.taskId());
                t.setString(2, namespace);
                t.setString(3, TaskStatus.PENDING.name());
                t.setString(4, wf.idempotencyKey());
                t.setString(5, wf.principalId() == null ? "" : wf.principalId());
                t.setString(6, wf.traceId());
                t.setLong(7, wf.nowMs());
                t.setLong(8, wf.nowMs());
                t.executeUpdate();

                List<ReadyStepDispatch> ready = new ArrayList<>();
                for (WorkflowStep step : wf.steps()) {
                    boolean root = step.dependsOn().isEmpty();
                    st.setString(1, step.stepId());
                    st.setString(2, namespace);
                    st.setString(3, wf.taskId());
                    st.setString(4, step.agentId());
                    st.setString(5, step.priority());
                    st.setString(6, step.payloadPath());
                    st.setInt(7, root ? 1 : 0);
                    st.setString(8, TaskStatus.PENDING.name());
                    st.setInt(9, 0);
                    st.setLong(10, wf.nowMs());
                    st.setLong(11, wf.nowMs());
                    st.executeUpdate();
                    if (root) {
                        ready.add(new ReadyStepDispatch(wf.taskId(), step.stepId(), step.agentId(), step.priority(), step.payloadPath(), 1));
                    }
                    for (String d : step.dependsOn()) {
                        dep.setString(1, namespace);
                        dep.setString(2, wf.taskId());
                        dep.setString(3, step.stepId());
                        dep.setString(4, d);
                        dep.setLong(5, wf.nowMs());
                        dep.executeUpdate();
                    }
                }

                idem.setString(1, namespace);
                idem.setString(2, wf.idempotencyKey());
                idem.setString(3, TaskStatus.PENDING.name());
                idem.setString(4, wf.taskId());
                idem.setLong(5, wf.nowMs());
                idem.setLong(6, wf.nowMs());
                idem.executeUpdate();

                c.commit();
                return new WorkflowSubmitResult(wf.taskId(), false, ready);
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            String after = findTaskIdByIdempotencyKey(wf.idempotencyKey());
            if (after != null) return new WorkflowSubmitResult(after, true, List.of());
            throw new RuntimeException("Failed submit workflow", e);
        }
    }

    public StepSuccessResolution tryMarkStepSuccessWithLease(String taskId, String stepId, String idempotencyKey, String leaseToken, long leaseEpoch,
                                                             String resultPayload, long nowMs) {
        return tryMarkStepSuccessWithLease(taskId, stepId, idempotencyKey, leaseToken, leaseEpoch, currentClusterEpochValue(), resultPayload, nowMs);
    }

    public StepSuccessResolution tryMarkStepSuccessWithLease(String taskId, String stepId, String idempotencyKey, String leaseToken, long leaseEpoch,
                                                             long expectedClusterEpoch, String resultPayload, long nowMs) {
        String update = "UPDATE steps SET status=?,result_payload=?,lease_owner=NULL,lease_token=NULL,next_retry_at_ms=NULL,updated_at_ms=? WHERE step_id=? AND status=? AND lease_token=? AND lease_epoch=? AND ?=(SELECT CAST(state_value AS INTEGER) FROM cluster_state WHERE state_key='cluster_epoch')";
        String readySql = """
                SELECT s.step_id,s.agent_id,s.priority,s.payload_path,s.attempt
                FROM steps s
                WHERE s.task_id=? AND s.status=? AND s.is_enqueued=0
                AND NOT EXISTS (
                    SELECT 1 FROM step_dependencies d
                    LEFT JOIN steps p ON p.task_id=d.task_id AND p.step_id=d.depends_on_step_id
                    WHERE d.task_id=s.task_id AND d.step_id=s.step_id AND (p.status IS NULL OR p.status<>?)
                )
                """;
        String claimReady = "UPDATE steps SET is_enqueued=1,updated_at_ms=? WHERE step_id=? AND status=? AND is_enqueued=0";
        String remainSql = "SELECT COUNT(1) FROM steps WHERE task_id=? AND status<>?";
        String activeSql = "SELECT COUNT(1) FROM steps WHERE task_id=? AND status IN (?, ?, ?, ?, ?)";
        String taskStatusSql = "SELECT status FROM tasks WHERE task_id=?";
        String taskUpdate = "UPDATE tasks SET status=?,result_payload=?,last_error=NULL,updated_at_ms=? WHERE task_id=?";
        String idemUpdate = "UPDATE idempotency SET status=?,result_hash=?,result_hash_algo=?,processed_at_ms=?,updated_at_ms=? WHERE idempotency_key=?";

        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement psUp = c.prepareStatement(update);
                 PreparedStatement psReady = c.prepareStatement(readySql);
                 PreparedStatement psClaim = c.prepareStatement(claimReady);
                 PreparedStatement psRemain = c.prepareStatement(remainSql);
                 PreparedStatement psActive = c.prepareStatement(activeSql);
                 PreparedStatement psTaskStatus = c.prepareStatement(taskStatusSql);
                 PreparedStatement psTask = c.prepareStatement(taskUpdate);
                 PreparedStatement psIdem = c.prepareStatement(idemUpdate)) {
                psUp.setString(1, TaskStatus.SUCCESS.name());
                psUp.setString(2, resultPayload);
                psUp.setLong(3, nowMs);
                psUp.setString(4, stepId);
                psUp.setString(5, TaskStatus.RUNNING.name());
                psUp.setString(6, leaseToken);
                psUp.setLong(7, leaseEpoch);
                psUp.setLong(8, Math.max(1L, expectedClusterEpoch));
                if (psUp.executeUpdate() == 0) {
                    StepLeaseState actual = readStepLeaseState(c, stepId);
                    recordLeaseConflict(
                            c,
                            "success_commit_conflict",
                            taskId,
                            stepId,
                            null,
                            TaskStatus.RUNNING.name(),
                            leaseToken,
                            leaseEpoch,
                            actual
                    );
                    c.commit();
                    return StepSuccessResolution.staleLease();
                }

                List<ReadyStepDispatch> ready = new ArrayList<>();
                psReady.setString(1, taskId);
                psReady.setString(2, TaskStatus.PENDING.name());
                psReady.setString(3, TaskStatus.SUCCESS.name());
                try (ResultSet rs = psReady.executeQuery()) {
                    while (rs.next()) {
                        String sid = rs.getString("step_id");
                        psClaim.setLong(1, nowMs);
                        psClaim.setString(2, sid);
                        psClaim.setString(3, TaskStatus.PENDING.name());
                        if (psClaim.executeUpdate() == 1) {
                            ready.add(new ReadyStepDispatch(taskId, sid, rs.getString("agent_id"),
                                    rs.getString("priority"), rs.getString("payload_path"), rs.getInt("attempt") + 1));
                        }
                    }
                }

                psRemain.setString(1, taskId);
                psRemain.setString(2, TaskStatus.SUCCESS.name());
                int remaining;
                try (ResultSet rs = psRemain.executeQuery()) {
                    rs.next();
                    remaining = rs.getInt(1);
                }

                psActive.setString(1, taskId);
                psActive.setString(2, TaskStatus.PENDING.name());
                psActive.setString(3, TaskStatus.RUNNING.name());
                psActive.setString(4, TaskStatus.RETRYING.name());
                psActive.setString(5, TaskStatus.ABANDONED.name());
                psActive.setString(6, TaskStatus.FAILED.name());
                int activeRemaining;
                try (ResultSet rs = psActive.executeQuery()) {
                    rs.next();
                    activeRemaining = rs.getInt(1);
                }

                String currentTaskStatus = null;
                psTaskStatus.setString(1, taskId);
                try (ResultSet rs = psTaskStatus.executeQuery()) {
                    if (rs.next()) {
                        currentTaskStatus = rs.getString("status");
                    }
                }

                if (remaining == 0) {
                    String finalResult = buildWorkflowResultJson(c, taskId);
                    String hash = Hashing.sha256Hex(finalResult == null ? "" : finalResult);
                    psTask.setString(1, TaskStatus.SUCCESS.name());
                    psTask.setString(2, finalResult);
                    psTask.setLong(3, nowMs);
                    psTask.setString(4, taskId);
                    psTask.executeUpdate();
                    psIdem.setString(1, TaskStatus.SUCCESS.name());
                    psIdem.setString(2, hash);
                    psIdem.setString(3, "sha256");
                    psIdem.setLong(4, nowMs);
                    psIdem.setLong(5, nowMs);
                    psIdem.setString(6, idempotencyKey);
                    psIdem.executeUpdate();
                    c.commit();
                    return StepSuccessResolution.taskCompleted(ready, finalResult);
                }

                if (TaskStatus.CANCELLED.name().equalsIgnoreCase(currentTaskStatus)) {
                    if (activeRemaining == 0) {
                        // Keep task as CANCELLED; do not regress to RUNNING after soft-cancel convergence.
                        psTask.setString(1, TaskStatus.CANCELLED.name());
                        psTask.setString(2, null);
                        psTask.setLong(3, nowMs);
                        psTask.setString(4, taskId);
                        psTask.executeUpdate();
                    }
                    c.commit();
                    return StepSuccessResolution.stepCompleted(ready);
                }

                psTask.setString(1, TaskStatus.RUNNING.name());
                psTask.setString(2, null);
                psTask.setLong(3, nowMs);
                psTask.setString(4, taskId);
                psTask.executeUpdate();
                c.commit();
                return StepSuccessResolution.stepCompleted(ready);
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed step success flow", e);
        }
    }

    public Optional<TaskView> getTask(String taskId) {
        String sql = "SELECT task_id,status,idempotency_key,trace_id,result_payload,last_error,created_at_ms,updated_at_ms FROM tasks WHERE namespace=? AND task_id=?";
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, namespace);
            ps.setString(2, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new TaskView(
                        rs.getString("task_id"), rs.getString("status"), rs.getString("idempotency_key"),
                        rs.getString("trace_id"), rs.getString("result_payload"), rs.getString("last_error"),
                        rs.getLong("created_at_ms"), rs.getLong("updated_at_ms")
                ));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read task", e);
        }
    }

    public Optional<StepDispatchContext> getStepDispatchContext(String stepId) {
        String sql = """
                SELECT s.task_id,s.step_id,s.agent_id,s.priority,s.payload_path,s.attempt,t.idempotency_key,t.trace_id
                FROM steps s JOIN tasks t ON t.namespace=s.namespace AND t.task_id=s.task_id
                WHERE s.namespace=? AND s.step_id=?
                """;
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, namespace);
            ps.setString(2, stepId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new StepDispatchContext(
                        rs.getString("task_id"), rs.getString("step_id"), rs.getString("agent_id"),
                        rs.getString("priority"), rs.getString("payload_path"), rs.getInt("attempt"),
                        rs.getString("idempotency_key"), rs.getString("trace_id")
                ));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read step context", e);
        }
    }

    public void markMessageState(String msgId, MessageState state, long nowMs) {
        exec("UPDATE messages SET state=?,updated_at_ms=? WHERE namespace=? AND msg_id=?", ps -> {
            ps.setString(1, state.name());
            ps.setLong(2, nowMs);
            ps.setString(3, namespace);
            ps.setString(4, msgId);
        });
    }

    public boolean heartbeatLease(String stepId, String leaseToken, long leaseEpoch, long nowMs) {
        String sql = "UPDATE steps SET updated_at_ms=? WHERE namespace=? AND step_id=? AND status=? AND lease_token=? AND lease_epoch=?";
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, nowMs);
            ps.setString(2, namespace);
            ps.setString(3, stepId);
            ps.setString(4, TaskStatus.RUNNING.name());
            ps.setString(5, leaseToken);
            ps.setLong(6, leaseEpoch);
            return ps.executeUpdate() == 1;
        } catch (SQLException e) {
            throw new RuntimeException("Failed heartbeat", e);
        }
    }

    public LeaseGrant tryMarkTaskRunning(String taskId, String stepId, String leaseOwner, String leaseToken, long nowMs) {
        return tryMarkTaskRunning(taskId, stepId, leaseOwner, leaseToken, nowMs, currentClusterEpochValue());
    }

    public LeaseGrant tryMarkTaskRunning(String taskId, String stepId, String leaseOwner, String leaseToken, long nowMs, long expectedClusterEpoch) {
        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps1 = c.prepareStatement(
                    "UPDATE steps SET status=?,lease_owner=?,lease_token=?,lease_epoch=lease_epoch+1,attempt=attempt+1,updated_at_ms=? WHERE namespace=? AND step_id=? AND status=? AND ?=(SELECT CAST(state_value AS INTEGER) FROM cluster_state WHERE state_key='cluster_epoch')");
                 PreparedStatement ps2 = c.prepareStatement("UPDATE tasks SET status=?,updated_at_ms=? WHERE namespace=? AND task_id=?");
                 PreparedStatement psEpoch = c.prepareStatement("SELECT lease_epoch FROM steps WHERE namespace=? AND step_id=?")) {
                ps1.setString(1, TaskStatus.RUNNING.name());
                ps1.setString(2, leaseOwner);
                ps1.setString(3, leaseToken);
                ps1.setLong(4, nowMs);
                ps1.setString(5, namespace);
                ps1.setString(6, stepId);
                ps1.setString(7, TaskStatus.PENDING.name());
                ps1.setLong(8, Math.max(1L, expectedClusterEpoch));
                if (ps1.executeUpdate() == 0) {
                    StepLeaseState actual = readStepLeaseState(c, stepId);
                    recordLeaseConflict(
                            c,
                            "claim_conflict",
                            taskId,
                            stepId,
                            leaseOwner,
                            TaskStatus.PENDING.name(),
                            leaseToken,
                            null,
                            actual
                    );
                    c.commit();
                    return LeaseGrant.conflict();
                }
                ps2.setString(1, TaskStatus.RUNNING.name());
                ps2.setLong(2, nowMs);
                ps2.setString(3, namespace);
                ps2.setString(4, taskId);
                ps2.executeUpdate();
                long epoch = 0L;
                psEpoch.setString(1, namespace);
                psEpoch.setString(2, stepId);
                try (ResultSet rs = psEpoch.executeQuery()) {
                    if (rs.next()) {
                        epoch = rs.getLong("lease_epoch");
                    }
                }
                c.commit();
                return LeaseGrant.granted(epoch);
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed mark running", e);
        }
    }

    public boolean tryMarkTaskSuccessWithLease(String taskId, String stepId, String idempotencyKey, String leaseToken, String resultPayload, long nowMs) {
        return tryMarkTaskSuccessWithLease(taskId, stepId, idempotencyKey, leaseToken, resultPayload, nowMs, currentClusterEpochValue());
    }

    public boolean tryMarkTaskSuccessWithLease(String taskId, String stepId, String idempotencyKey, String leaseToken, String resultPayload, long nowMs,
                                               long expectedClusterEpoch) {
        String hash = Hashing.sha256Hex(resultPayload == null ? "" : resultPayload);
        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement psStep = c.prepareStatement(
                    "UPDATE steps SET status=?,lease_owner=NULL,lease_token=NULL,next_retry_at_ms=NULL,updated_at_ms=? WHERE namespace=? AND step_id=? AND status=? AND lease_token=? AND ?=(SELECT CAST(state_value AS INTEGER) FROM cluster_state WHERE state_key='cluster_epoch')");
                 PreparedStatement psTask = c.prepareStatement(
                         "UPDATE tasks SET status=?,result_payload=?,last_error=NULL,updated_at_ms=? WHERE namespace=? AND task_id=?");
                 PreparedStatement psIdem = c.prepareStatement(
                         "UPDATE idempotency SET status=?,result_hash=?,result_hash_algo=?,processed_at_ms=?,updated_at_ms=? WHERE namespace=? AND idempotency_key=?")) {
                psStep.setString(1, TaskStatus.SUCCESS.name());
                psStep.setLong(2, nowMs);
                psStep.setString(3, namespace);
                psStep.setString(4, stepId);
                psStep.setString(5, TaskStatus.RUNNING.name());
                psStep.setString(6, leaseToken);
                psStep.setLong(7, Math.max(1L, expectedClusterEpoch));
                if (psStep.executeUpdate() == 0) {
                    c.rollback();
                    return false;
                }
                psTask.setString(1, TaskStatus.SUCCESS.name());
                psTask.setString(2, resultPayload);
                psTask.setLong(3, nowMs);
                psTask.setString(4, namespace);
                psTask.setString(5, taskId);
                psTask.executeUpdate();
                psIdem.setString(1, TaskStatus.SUCCESS.name());
                psIdem.setString(2, hash);
                psIdem.setString(3, "sha256");
                psIdem.setLong(4, nowMs);
                psIdem.setLong(5, nowMs);
                psIdem.setString(6, namespace);
                psIdem.setString(7, idempotencyKey);
                psIdem.executeUpdate();
                c.commit();
                return true;
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed mark success", e);
        }
    }

    public FailureResolution tryCompleteFailureWithLease(String taskId, String stepId, String idempotencyKey, String leaseToken, long leaseEpoch, String error,
                                                         long nowMs, int maxAttempts, long baseBackoffMs, long maxBackoffMs) {
        return tryCompleteFailureWithLease(
                taskId,
                stepId,
                idempotencyKey,
                leaseToken,
                leaseEpoch,
                currentClusterEpochValue(),
                error,
                nowMs,
                maxAttempts,
                baseBackoffMs,
                maxBackoffMs
        );
    }

    public FailureResolution tryCompleteFailureWithLease(String taskId, String stepId, String idempotencyKey, String leaseToken, long leaseEpoch,
                                                         long expectedClusterEpoch, String error,
                                                         long nowMs, int maxAttempts, long baseBackoffMs, long maxBackoffMs) {
        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement read = c.prepareStatement("SELECT attempt,status,lease_token,lease_epoch FROM steps WHERE namespace=? AND step_id=?");
                 PreparedStatement setDead = c.prepareStatement(
                         "UPDATE steps SET status=?,lease_owner=NULL,lease_token=NULL,next_retry_at_ms=NULL,updated_at_ms=? WHERE namespace=? AND step_id=? AND ?=(SELECT CAST(state_value AS INTEGER) FROM cluster_state WHERE state_key='cluster_epoch')");
                 PreparedStatement setRetry = c.prepareStatement(
                         "UPDATE steps SET status=?,lease_owner=NULL,lease_token=NULL,next_retry_at_ms=?,updated_at_ms=? WHERE namespace=? AND step_id=? AND ?=(SELECT CAST(state_value AS INTEGER) FROM cluster_state WHERE state_key='cluster_epoch')");
                 PreparedStatement task = c.prepareStatement("UPDATE tasks SET status=?,last_error=?,updated_at_ms=? WHERE namespace=? AND task_id=?");
                 PreparedStatement idem = c.prepareStatement("UPDATE idempotency SET status=?,updated_at_ms=? WHERE namespace=? AND idempotency_key=?")) {
                read.setString(1, namespace);
                read.setString(2, stepId);
                int attempt;
                String status;
                String currentLease;
                long currentEpoch;
                try (ResultSet rs = read.executeQuery()) {
                    if (!rs.next()) {
                        recordLeaseConflict(
                                c,
                                "failure_commit_conflict",
                                taskId,
                                stepId,
                                null,
                                TaskStatus.RUNNING.name(),
                                leaseToken,
                                leaseEpoch,
                                null
                        );
                        c.commit();
                        return FailureResolution.staleLease();
                    }
                    attempt = rs.getInt("attempt");
                    status = rs.getString("status");
                    currentLease = rs.getString("lease_token");
                    currentEpoch = rs.getLong("lease_epoch");
                }
                if (!TaskStatus.RUNNING.name().equalsIgnoreCase(status)
                        || !leaseToken.equals(currentLease)
                        || leaseEpoch != currentEpoch) {
                    recordLeaseConflict(
                            c,
                            "failure_commit_conflict",
                            taskId,
                            stepId,
                            null,
                            TaskStatus.RUNNING.name(),
                            leaseToken,
                            leaseEpoch,
                            new StepLeaseState(status, null, currentLease, currentEpoch)
                    );
                    c.commit();
                    return FailureResolution.staleLease();
                }
                if (attempt >= maxAttempts) {
                    setDead.setString(1, TaskStatus.DEAD_LETTER.name());
                    setDead.setLong(2, nowMs);
                    setDead.setString(3, namespace);
                    setDead.setString(4, stepId);
                    setDead.setLong(5, Math.max(1L, expectedClusterEpoch));
                    if (setDead.executeUpdate() == 0) {
                        recordLeaseConflict(
                                c,
                                "failure_commit_conflict",
                                taskId,
                                stepId,
                                null,
                                TaskStatus.RUNNING.name(),
                                leaseToken,
                                leaseEpoch,
                                readStepLeaseState(c, stepId)
                        );
                        c.commit();
                        return FailureResolution.staleLease();
                    }
                    task.setString(1, TaskStatus.DEAD_LETTER.name());
                    task.setString(2, error);
                    task.setLong(3, nowMs);
                    task.setString(4, namespace);
                    task.setString(5, taskId);
                    task.executeUpdate();
                    idem.setString(1, TaskStatus.FAILED.name());
                    idem.setLong(2, nowMs);
                    idem.setString(3, namespace);
                    idem.setString(4, idempotencyKey);
                    idem.executeUpdate();
                    c.commit();
                    return FailureResolution.deadLetter(attempt);
                }
                long nextRetryAtMs = nowMs + computeBackoffMs(attempt, baseBackoffMs, maxBackoffMs);
                setRetry.setString(1, TaskStatus.RETRYING.name());
                setRetry.setLong(2, nextRetryAtMs);
                setRetry.setLong(3, nowMs);
                setRetry.setString(4, namespace);
                setRetry.setString(5, stepId);
                setRetry.setLong(6, Math.max(1L, expectedClusterEpoch));
                if (setRetry.executeUpdate() == 0) {
                    recordLeaseConflict(
                            c,
                            "failure_commit_conflict",
                            taskId,
                            stepId,
                            null,
                            TaskStatus.RUNNING.name(),
                            leaseToken,
                            leaseEpoch,
                            readStepLeaseState(c, stepId)
                    );
                    c.commit();
                    return FailureResolution.staleLease();
                }
                task.setString(1, TaskStatus.RETRYING.name());
                task.setString(2, error);
                task.setLong(3, nowMs);
                task.setString(4, namespace);
                task.setString(5, taskId);
                task.executeUpdate();
                idem.setString(1, TaskStatus.RETRYING.name());
                idem.setLong(2, nowMs);
                idem.setString(3, namespace);
                idem.setString(4, idempotencyKey);
                idem.executeUpdate();
                c.commit();
                return FailureResolution.retryScheduled(attempt, nextRetryAtMs);
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed complete failure", e);
        }
    }

    public ReclaimSummary reclaimExpiredRunning(long nowMs, long cutoffMs,
                                                int defaultMaxAttempts, long defaultBaseBackoffMs, long defaultMaxBackoffMs,
                                                int limit, Map<String, RetryPolicy> policyOverrides) {
        return reclaimExpiredRunning(
                nowMs,
                cutoffMs,
                defaultMaxAttempts,
                defaultBaseBackoffMs,
                defaultMaxBackoffMs,
                limit,
                policyOverrides,
                currentClusterEpochValue()
        );
    }

    public ReclaimSummary reclaimExpiredRunning(long nowMs, long cutoffMs,
                                                int defaultMaxAttempts, long defaultBaseBackoffMs, long defaultMaxBackoffMs,
                                                int limit, Map<String, RetryPolicy> policyOverrides,
                                                long expectedClusterEpoch) {
        String sel = "SELECT s.step_id,s.task_id,s.agent_id,s.attempt,s.lease_epoch,t.idempotency_key FROM steps s JOIN tasks t ON t.task_id=s.task_id WHERE s.status=? AND s.updated_at_ms<=? ORDER BY s.updated_at_ms ASC LIMIT ?";
        List<ReclaimCandidate> list = new ArrayList<>();
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sel)) {
            ps.setString(1, TaskStatus.RUNNING.name());
            ps.setLong(2, cutoffMs);
            ps.setInt(3, limit);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(new ReclaimCandidate(
                            rs.getString("task_id"),
                            rs.getString("step_id"),
                            rs.getString("agent_id"),
                            rs.getString("idempotency_key"),
                            rs.getInt("attempt"),
                            rs.getLong("lease_epoch"),
                            null
                    ));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed reclaim scan", e);
        }

        int reclaimed = 0;
        int retry = 0;
        int dead = 0;
        for (ReclaimCandidate cnd : list) {
            try (Connection c = database.openConnection()) {
                c.setAutoCommit(false);
                try (PreparedStatement abandon = c.prepareStatement(
                        "UPDATE steps SET status=?,lease_owner=NULL,lease_token=NULL,updated_at_ms=? WHERE step_id=? AND status=? AND updated_at_ms<=? AND lease_epoch=? AND ?=(SELECT CAST(state_value AS INTEGER) FROM cluster_state WHERE state_key='cluster_epoch')");
                     PreparedStatement stepRetry = c.prepareStatement(
                             "UPDATE steps SET status=?,next_retry_at_ms=?,updated_at_ms=? WHERE step_id=?");
                     PreparedStatement stepDead = c.prepareStatement(
                             "UPDATE steps SET status=?,next_retry_at_ms=NULL,updated_at_ms=? WHERE step_id=?");
                     PreparedStatement task = c.prepareStatement(
                             "UPDATE tasks SET status=?,last_error=?,updated_at_ms=? WHERE task_id=?");
                     PreparedStatement idem = c.prepareStatement(
                             "UPDATE idempotency SET status=?,updated_at_ms=? WHERE idempotency_key=?")) {
                    abandon.setString(1, TaskStatus.ABANDONED.name());
                    abandon.setLong(2, nowMs);
                    abandon.setString(3, cnd.stepId());
                    abandon.setString(4, TaskStatus.RUNNING.name());
                    abandon.setLong(5, cutoffMs);
                    abandon.setLong(6, cnd.leaseEpoch());
                    abandon.setLong(7, Math.max(1L, expectedClusterEpoch));
                    if (abandon.executeUpdate() == 0) {
                        StepLeaseState actual = readStepLeaseState(c, cnd.stepId());
                        recordLeaseConflict(
                                c,
                                "reclaim_conflict",
                                cnd.taskId(),
                                cnd.stepId(),
                                null,
                                TaskStatus.RUNNING.name(),
                                null,
                                cnd.leaseEpoch(),
                                actual
                        );
                        c.commit();
                        continue;
                    }
                    reclaimed++;
                    RetryPolicy policy = policyOverrides == null ? null : policyOverrides.get(cnd.agentId());
                    int maxAttempts = policy == null ? defaultMaxAttempts : policy.maxAttempts();
                    long baseBackoffMs = policy == null ? defaultBaseBackoffMs : policy.baseBackoffMs();
                    long maxBackoffMs = policy == null ? defaultMaxBackoffMs : policy.maxBackoffMs();
                    if (cnd.attempt() >= maxAttempts) {
                        stepDead.setString(1, TaskStatus.DEAD_LETTER.name());
                        stepDead.setLong(2, nowMs);
                        stepDead.setString(3, cnd.stepId());
                        stepDead.executeUpdate();

                        task.setString(1, TaskStatus.DEAD_LETTER.name());
                        task.setString(2, "lease timeout reclaim");
                        task.setLong(3, nowMs);
                        task.setString(4, cnd.taskId());
                        task.executeUpdate();

                        idem.setString(1, TaskStatus.FAILED.name());
                        idem.setLong(2, nowMs);
                        idem.setString(3, cnd.idempotencyKey());
                        idem.executeUpdate();
                        dead++;
                    } else {
                        long nextRetryAtMs = nowMs + computeBackoffMs(cnd.attempt(), baseBackoffMs, maxBackoffMs);
                        stepRetry.setString(1, TaskStatus.RETRYING.name());
                        stepRetry.setLong(2, nextRetryAtMs);
                        stepRetry.setLong(3, nowMs);
                        stepRetry.setString(4, cnd.stepId());
                        stepRetry.executeUpdate();

                        task.setString(1, TaskStatus.RETRYING.name());
                        task.setString(2, "lease timeout reclaim");
                        task.setLong(3, nowMs);
                        task.setString(4, cnd.taskId());
                        task.executeUpdate();

                        idem.setString(1, TaskStatus.RETRYING.name());
                        idem.setLong(2, nowMs);
                        idem.setString(3, cnd.idempotencyKey());
                        idem.executeUpdate();
                        retry++;
                    }
                    c.commit();
                } catch (Exception ex) {
                    c.rollback();
                    throw ex;
                } finally {
                    c.setAutoCommit(true);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed reclaim candidate: " + cnd.stepId(), e);
            }
        }
        return new ReclaimSummary(reclaimed, retry, dead);
    }

    public ReclaimSummary reclaimRunningByLeaseOwners(
            Set<String> leaseOwners,
            long nowMs,
            int defaultMaxAttempts,
            long defaultBaseBackoffMs,
            long defaultMaxBackoffMs,
            int limit,
            Map<String, RetryPolicy> policyOverrides
    ) {
        return reclaimRunningByLeaseOwners(
                leaseOwners,
                nowMs,
                defaultMaxAttempts,
                defaultBaseBackoffMs,
                defaultMaxBackoffMs,
                limit,
                policyOverrides,
                currentClusterEpochValue()
        );
    }

    public ReclaimSummary reclaimRunningByLeaseOwners(
            Set<String> leaseOwners,
            long nowMs,
            int defaultMaxAttempts,
            long defaultBaseBackoffMs,
            long defaultMaxBackoffMs,
            int limit,
            Map<String, RetryPolicy> policyOverrides,
            long expectedClusterEpoch
    ) {
        if (leaseOwners == null || leaseOwners.isEmpty()) {
            return new ReclaimSummary(0, 0, 0);
        }
        List<String> owners = leaseOwners.stream()
                .filter(v -> v != null && !v.isBlank())
                .map(String::trim)
                .distinct()
                .toList();
        if (owners.isEmpty()) {
            return new ReclaimSummary(0, 0, 0);
        }
        String sel = "SELECT s.step_id,s.task_id,s.agent_id,s.attempt,s.lease_epoch,t.idempotency_key,s.lease_owner " +
                "FROM steps s JOIN tasks t ON t.task_id=s.task_id " +
                "WHERE s.status=? AND s.lease_owner IN (" + placeholders(owners.size()) + ") " +
                "ORDER BY s.updated_at_ms ASC LIMIT ?";
        List<ReclaimCandidate> list = new ArrayList<>();
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sel)) {
            int idx = 1;
            ps.setString(idx++, TaskStatus.RUNNING.name());
            for (String owner : owners) {
                ps.setString(idx++, owner);
            }
            ps.setInt(idx, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    list.add(new ReclaimCandidate(
                            rs.getString("task_id"),
                            rs.getString("step_id"),
                            rs.getString("agent_id"),
                            rs.getString("idempotency_key"),
                            rs.getInt("attempt"),
                            rs.getLong("lease_epoch"),
                            rs.getString("lease_owner")
                    ));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed dead-owner reclaim scan", e);
        }

        int reclaimed = 0;
        int retry = 0;
        int dead = 0;
        for (ReclaimCandidate cnd : list) {
            try (Connection c = database.openConnection()) {
                c.setAutoCommit(false);
                try (PreparedStatement abandon = c.prepareStatement(
                        "UPDATE steps SET status=?,lease_owner=NULL,lease_token=NULL,updated_at_ms=? WHERE step_id=? AND status=? AND lease_owner=? AND lease_epoch=? AND ?=(SELECT CAST(state_value AS INTEGER) FROM cluster_state WHERE state_key='cluster_epoch')");
                     PreparedStatement stepRetry = c.prepareStatement(
                             "UPDATE steps SET status=?,next_retry_at_ms=?,updated_at_ms=? WHERE step_id=?");
                     PreparedStatement stepDead = c.prepareStatement(
                             "UPDATE steps SET status=?,next_retry_at_ms=NULL,updated_at_ms=? WHERE step_id=?");
                     PreparedStatement task = c.prepareStatement(
                             "UPDATE tasks SET status=?,last_error=?,updated_at_ms=? WHERE task_id=?");
                     PreparedStatement idem = c.prepareStatement(
                             "UPDATE idempotency SET status=?,updated_at_ms=? WHERE idempotency_key=?")) {
                    abandon.setString(1, TaskStatus.ABANDONED.name());
                    abandon.setLong(2, nowMs);
                    abandon.setString(3, cnd.stepId());
                    abandon.setString(4, TaskStatus.RUNNING.name());
                    abandon.setString(5, cnd.leaseOwner() == null ? "" : cnd.leaseOwner());
                    abandon.setLong(6, cnd.leaseEpoch());
                    abandon.setLong(7, Math.max(1L, expectedClusterEpoch));
                    if (abandon.executeUpdate() == 0) {
                        StepLeaseState actual = readStepLeaseState(c, cnd.stepId());
                        recordLeaseConflict(
                                c,
                                "dead_owner_reclaim_conflict",
                                cnd.taskId(),
                                cnd.stepId(),
                                cnd.leaseOwner(),
                                TaskStatus.RUNNING.name(),
                                null,
                                cnd.leaseEpoch(),
                                actual
                        );
                        c.commit();
                        continue;
                    }
                    reclaimed++;
                    RetryPolicy policy = policyOverrides == null ? null : policyOverrides.get(cnd.agentId());
                    int maxAttempts = policy == null ? defaultMaxAttempts : policy.maxAttempts();
                    long baseBackoffMs = policy == null ? defaultBaseBackoffMs : policy.baseBackoffMs();
                    long maxBackoffMs = policy == null ? defaultMaxBackoffMs : policy.maxBackoffMs();
                    if (cnd.attempt() >= maxAttempts) {
                        stepDead.setString(1, TaskStatus.DEAD_LETTER.name());
                        stepDead.setLong(2, nowMs);
                        stepDead.setString(3, cnd.stepId());
                        stepDead.executeUpdate();

                        task.setString(1, TaskStatus.DEAD_LETTER.name());
                        task.setString(2, "dead owner reclaim");
                        task.setLong(3, nowMs);
                        task.setString(4, cnd.taskId());
                        task.executeUpdate();

                        idem.setString(1, TaskStatus.FAILED.name());
                        idem.setLong(2, nowMs);
                        idem.setString(3, cnd.idempotencyKey());
                        idem.executeUpdate();
                        dead++;
                    } else {
                        long nextRetryAtMs = nowMs + computeBackoffMs(cnd.attempt(), baseBackoffMs, maxBackoffMs);
                        stepRetry.setString(1, TaskStatus.RETRYING.name());
                        stepRetry.setLong(2, nextRetryAtMs);
                        stepRetry.setLong(3, nowMs);
                        stepRetry.setString(4, cnd.stepId());
                        stepRetry.executeUpdate();

                        task.setString(1, TaskStatus.RETRYING.name());
                        task.setString(2, "dead owner reclaim");
                        task.setLong(3, nowMs);
                        task.setString(4, cnd.taskId());
                        task.executeUpdate();

                        idem.setString(1, TaskStatus.RETRYING.name());
                        idem.setLong(2, nowMs);
                        idem.setString(3, cnd.idempotencyKey());
                        idem.executeUpdate();
                        retry++;
                    }
                    c.commit();
                } catch (Exception ex) {
                    c.rollback();
                    throw ex;
                } finally {
                    c.setAutoCommit(true);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed dead-owner reclaim candidate: " + cnd.stepId(), e);
            }
        }
        return new ReclaimSummary(reclaimed, retry, dead);
    }

    public Optional<DeadLetterTask> getDeadLetterTask(String taskId) {
        String sql = """
                SELECT s.task_id,s.step_id,s.agent_id,s.priority,s.payload_path,t.idempotency_key,s.status
                FROM steps s JOIN tasks t ON t.task_id=s.task_id WHERE s.task_id=?
                """;
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(new DeadLetterTask(
                        rs.getString("task_id"), rs.getString("step_id"), rs.getString("agent_id"),
                        rs.getString("priority"), rs.getString("payload_path"), rs.getString("idempotency_key"), rs.getString("status")));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed get dead letter task", e);
        }
    }

    public boolean resetDeadLetterTask(DeadLetterTask t, long nowMs) {
        return resetDeadLetterTask(t, nowMs, currentClusterEpochValue());
    }

    public boolean resetDeadLetterTask(DeadLetterTask t, long nowMs, long expectedClusterEpoch) {
        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement psStep = c.prepareStatement(
                    "UPDATE steps SET status=?,attempt=0,next_retry_at_ms=NULL,lease_owner=NULL,lease_token=NULL,updated_at_ms=? WHERE step_id=? AND status=? AND ?=(SELECT CAST(state_value AS INTEGER) FROM cluster_state WHERE state_key='cluster_epoch')");
                 PreparedStatement psTask = c.prepareStatement(
                         "UPDATE tasks SET status=?,result_payload=NULL,last_error=NULL,updated_at_ms=? WHERE task_id=?");
                 PreparedStatement psIdem = c.prepareStatement(
                         "UPDATE idempotency SET status=?,processed_at_ms=NULL,result_hash=NULL,result_hash_algo=NULL,updated_at_ms=? WHERE idempotency_key=?")) {
                psStep.setString(1, TaskStatus.PENDING.name());
                psStep.setLong(2, nowMs);
                psStep.setString(3, t.stepId());
                psStep.setString(4, TaskStatus.DEAD_LETTER.name());
                psStep.setLong(5, Math.max(1L, expectedClusterEpoch));
                if (psStep.executeUpdate() == 0) {
                    c.rollback();
                    return false;
                }
                psTask.setString(1, TaskStatus.PENDING.name());
                psTask.setLong(2, nowMs);
                psTask.setString(3, t.taskId());
                psTask.executeUpdate();
                psIdem.setString(1, TaskStatus.PENDING.name());
                psIdem.setLong(2, nowMs);
                psIdem.setString(3, t.idempotencyKey());
                psIdem.executeUpdate();
                c.commit();
                return true;
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed reset dead letter", e);
        }
    }

    public List<RetryDispatch> claimDueRetries(long nowMs, int limit) {
        String select = "SELECT task_id,step_id,agent_id,priority,payload_path,attempt FROM steps WHERE status=? AND next_retry_at_ms IS NOT NULL AND next_retry_at_ms<=? ORDER BY next_retry_at_ms ASC LIMIT ?";
        String claim = "UPDATE steps SET status=?,next_retry_at_ms=NULL,updated_at_ms=? WHERE step_id=? AND status=? AND next_retry_at_ms IS NOT NULL AND next_retry_at_ms<=?";
        List<RetryDispatch> out = new ArrayList<>();
        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement s = c.prepareStatement(select); PreparedStatement up = c.prepareStatement(claim)) {
                s.setString(1, TaskStatus.RETRYING.name());
                s.setLong(2, nowMs);
                s.setInt(3, limit);
                List<RetryDispatch> cand = new ArrayList<>();
                try (ResultSet rs = s.executeQuery()) {
                    while (rs.next()) {
                        cand.add(new RetryDispatch(rs.getString("task_id"), rs.getString("step_id"), rs.getString("agent_id"),
                                rs.getString("priority"), rs.getString("payload_path"), rs.getInt("attempt") + 1));
                    }
                }
                for (RetryDispatch d : cand) {
                    up.setString(1, TaskStatus.PENDING.name());
                    up.setLong(2, nowMs);
                    up.setString(3, d.stepId());
                    up.setString(4, TaskStatus.RETRYING.name());
                    up.setLong(5, nowMs);
                    if (up.executeUpdate() == 1) out.add(d);
                }
                c.commit();
                return out;
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed claim retries", e);
        }
    }

    public void insertRetryMessage(RetryMessage m) {
        String sql = "INSERT INTO messages(msg_id,task_id,step_id,from_agent,to_agent,priority,state,meta_path,payload_path,created_at_ms,updated_at_ms) VALUES(?,?,?,?,?,?,?,?,?,?,?)";
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, m.msgId());
            ps.setString(2, m.taskId());
            ps.setString(3, m.stepId());
            ps.setString(4, "system");
            ps.setString(5, m.agentId());
            ps.setString(6, m.priority());
            ps.setString(7, MessageState.QUEUED.name());
            ps.setString(8, m.metaPath());
            ps.setString(9, m.payloadPath());
            ps.setLong(10, m.nowMs());
            ps.setLong(11, m.nowMs());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed insert retry message", e);
        }
    }

    public int purgeIdempotencyOlderThan(long cutoffMs) {
        String sql = "DELETE FROM idempotency WHERE processed_at_ms IS NOT NULL AND processed_at_ms < ?";
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, cutoffMs);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed purge idempotency", e);
        }
    }

    public HeartbeatOutcome heartbeatNode(String nodeId, long nowMs) {
        return heartbeatNodeIfNewer(nodeId, nowMs, nowMs, 0L, 0L);
    }

    public HeartbeatOutcome heartbeatNode(String nodeId, long nowMs, long suspectRecoverMinMs, long deadRecoverMinMs) {
        return heartbeatNodeIfNewer(nodeId, nowMs, nowMs, suspectRecoverMinMs, deadRecoverMinMs);
    }

    public HeartbeatOutcome heartbeatNodeIfNewer(String nodeId, long heartbeatMs, long nowMs) {
        return heartbeatNodeIfNewer(nodeId, heartbeatMs, nowMs, 0L, 0L);
    }

    public HeartbeatOutcome heartbeatNodeIfNewer(
            String nodeId,
            long heartbeatMs,
            long nowMs,
            long suspectRecoverMinMs,
            long deadRecoverMinMs
    ) {
        String select = "SELECT status,last_heartbeat_ms,status_changed_at_ms FROM mesh_nodes WHERE node_id=?";
        String insert = "INSERT INTO mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) VALUES(?,?,?,?,?)";
        String update = "UPDATE mesh_nodes SET status=?,last_heartbeat_ms=?,updated_at_ms=?,status_changed_at_ms=? WHERE node_id=?";
        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement psSel = c.prepareStatement(select);
                 PreparedStatement psIns = c.prepareStatement(insert);
                 PreparedStatement psUp = c.prepareStatement(update)) {
                psSel.setString(1, nodeId);
                try (ResultSet rs = psSel.executeQuery()) {
                    if (!rs.next()) {
                        psIns.setString(1, nodeId);
                        psIns.setString(2, "ALIVE");
                        psIns.setLong(3, heartbeatMs);
                        psIns.setLong(4, nowMs);
                        psIns.setLong(5, nowMs);
                        psIns.executeUpdate();
                        c.commit();
                        return new HeartbeatOutcome(true, "", "ALIVE", false);
                    }
                    String previousStatus = rs.getString("status");
                    long lastHeartbeat = rs.getLong("last_heartbeat_ms");
                    long statusChangedAt = rs.getLong("status_changed_at_ms");

                    if (heartbeatMs < lastHeartbeat) {
                        c.commit();
                        return new HeartbeatOutcome(false, previousStatus, previousStatus, false);
                    }

                    String nextStatus = previousStatus;
                    boolean recoverySuppressed = false;
                    long statusAgeMs = Math.max(0L, nowMs - statusChangedAt);
                    long suspectGuard = Math.max(0L, suspectRecoverMinMs);
                    long deadGuard = Math.max(0L, deadRecoverMinMs);

                    if ("SUSPECT".equalsIgnoreCase(previousStatus)) {
                        if (statusAgeMs >= suspectGuard) {
                            nextStatus = "ALIVE";
                        } else {
                            recoverySuppressed = true;
                            nextStatus = "SUSPECT";
                        }
                    } else if ("DEAD".equalsIgnoreCase(previousStatus)) {
                        if (statusAgeMs >= deadGuard) {
                            nextStatus = "ALIVE";
                        } else {
                            recoverySuppressed = true;
                            nextStatus = "DEAD";
                        }
                    } else {
                        nextStatus = "ALIVE";
                    }

                    long nextStatusChangedAt = previousStatus.equalsIgnoreCase(nextStatus)
                            ? statusChangedAt
                            : nowMs;
                    psUp.setString(1, nextStatus);
                    psUp.setLong(2, heartbeatMs);
                    psUp.setLong(3, nowMs);
                    psUp.setLong(4, nextStatusChangedAt);
                    psUp.setString(5, nodeId);
                    psUp.executeUpdate();
                    c.commit();
                    return new HeartbeatOutcome(true, previousStatus, nextStatus, recoverySuppressed);
                }
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed node heartbeat: " + nodeId, e);
        }
    }

    public int markDeadNodes(long deadBeforeMs) {
        return reconcileNodeStates(System.currentTimeMillis(), deadBeforeMs, deadBeforeMs).dead();
    }

    public NodeStateTransition reconcileNodeStates(long nowMs, long suspectBeforeMs, long deadBeforeMs) {
        String markSuspect = """
                UPDATE mesh_nodes
                SET status='SUSPECT', updated_at_ms=?, status_changed_at_ms=?
                WHERE last_heartbeat_ms<? AND last_heartbeat_ms>=? AND status='ALIVE'
                """;
        String markDead = """
                UPDATE mesh_nodes
                SET status='DEAD', updated_at_ms=?, status_changed_at_ms=?
                WHERE last_heartbeat_ms<? AND status<>'DEAD'
                """;
        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement suspect = c.prepareStatement(markSuspect);
                 PreparedStatement dead = c.prepareStatement(markDead)) {
                suspect.setLong(1, nowMs);
                suspect.setLong(2, nowMs);
                suspect.setLong(3, suspectBeforeMs);
                suspect.setLong(4, deadBeforeMs);
                int suspectedRows = suspect.executeUpdate();

                dead.setLong(1, nowMs);
                dead.setLong(2, nowMs);
                dead.setLong(3, deadBeforeMs);
                int deadRows = dead.executeUpdate();

                c.commit();
                return new NodeStateTransition(suspectedRows, deadRows);
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to reconcile mesh node states", e);
        }
    }

    public List<MeshNode> listNodes() {
        String sql = "SELECT node_id,status,last_heartbeat_ms,updated_at_ms FROM mesh_nodes ORDER BY node_id";
        List<MeshNode> out = new ArrayList<>();
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql); ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                out.add(new MeshNode(
                        rs.getString("node_id"),
                        rs.getString("status"),
                        rs.getLong("last_heartbeat_ms"),
                        rs.getLong("updated_at_ms")
                ));
            }
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("Failed list nodes", e);
        }
    }

    public List<String> listNodeIdsByStatus(String status, int limit) {
        String target = status == null ? "" : status.trim().toUpperCase(Locale.ROOT);
        if (target.isBlank()) {
            return List.of();
        }
        String sql = "SELECT node_id FROM mesh_nodes WHERE status=? ORDER BY node_id LIMIT ?";
        List<String> out = new ArrayList<>();
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, target);
            ps.setInt(2, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String nodeId = rs.getString("node_id");
                    if (nodeId != null && !nodeId.isBlank()) {
                        out.add(nodeId);
                    }
                }
            }
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("Failed list nodes by status: " + target, e);
        }
    }

    public Optional<String> nodeStatus(String nodeId) {
        String target = nodeId == null ? "" : nodeId.trim();
        if (target.isBlank()) {
            return Optional.empty();
        }
        String sql = "SELECT status FROM mesh_nodes WHERE node_id=?";
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, target);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                String status = rs.getString("status");
                if (status == null || status.isBlank()) {
                    return Optional.empty();
                }
                return Optional.of(status.trim().toUpperCase(Locale.ROOT));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query node status: " + target, e);
        }
    }

    public Optional<LeaseSnapshot> stepLeaseSnapshot(String stepId) {
        String sid = stepId == null ? "" : stepId.trim();
        if (sid.isBlank()) {
            return Optional.empty();
        }
        String sql = "SELECT step_id,status,lease_owner,lease_token,lease_epoch,updated_at_ms FROM steps WHERE step_id=?";
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, sid);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(new LeaseSnapshot(
                        rs.getString("step_id"),
                        rs.getString("status"),
                        rs.getString("lease_owner"),
                        rs.getString("lease_token"),
                        rs.getLong("lease_epoch"),
                        rs.getLong("updated_at_ms")
                ));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query lease snapshot: " + sid, e);
        }
    }

    public Optional<ClusterStateValue> clusterState(String key) {
        String target = key == null ? "" : key.trim();
        if (target.isBlank()) {
            return Optional.empty();
        }
        String sql = "SELECT state_key,state_value,version,updated_at_ms FROM cluster_state WHERE state_key=?";
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, target);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Optional.empty();
                }
                return Optional.of(new ClusterStateValue(
                        rs.getString("state_key"),
                        rs.getString("state_value"),
                        rs.getLong("version"),
                        rs.getLong("updated_at_ms")
                ));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read cluster state: " + target, e);
        }
    }

    public ClusterStateValue clusterEpoch() {
        return clusterState("cluster_epoch")
                .orElse(new ClusterStateValue("cluster_epoch", "1", 1L, System.currentTimeMillis()));
    }

    public ClusterStateValue bumpClusterEpoch(long nowMs) {
        final String stateKey = "cluster_epoch";
        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement read = c.prepareStatement(
                    "SELECT state_value,version FROM cluster_state WHERE state_key=?");
                 PreparedStatement insert = c.prepareStatement(
                         "INSERT OR IGNORE INTO cluster_state(state_key,state_value,version,updated_at_ms) VALUES(?,?,?,?)");
                 PreparedStatement update = c.prepareStatement(
                         "UPDATE cluster_state SET state_value=?,version=?,updated_at_ms=? WHERE state_key=? AND version=?")) {
                read.setString(1, stateKey);
                String currentValue = null;
                long currentVersion = 0L;
                try (ResultSet rs = read.executeQuery()) {
                    if (rs.next()) {
                        currentValue = rs.getString("state_value");
                        currentVersion = rs.getLong("version");
                    }
                }

                if (currentValue == null) {
                    insert.setString(1, stateKey);
                    insert.setString(2, "1");
                    insert.setLong(3, 1L);
                    insert.setLong(4, nowMs);
                    insert.executeUpdate();

                    read.clearParameters();
                    read.setString(1, stateKey);
                    try (ResultSet rs = read.executeQuery()) {
                        if (rs.next()) {
                            currentValue = rs.getString("state_value");
                            currentVersion = rs.getLong("version");
                        }
                    }
                }

                long currentEpoch = parseLongOrDefault(currentValue, Math.max(1L, currentVersion));
                long nextEpoch = Math.max(1L, currentEpoch + 1L);
                long nextVersion = Math.max(1L, currentVersion + 1L);

                update.setString(1, Long.toString(nextEpoch));
                update.setLong(2, nextVersion);
                update.setLong(3, nowMs);
                update.setString(4, stateKey);
                update.setLong(5, currentVersion);
                int rows = update.executeUpdate();
                if (rows != 1) {
                    c.rollback();
                    throw new RuntimeException("Failed to bump cluster_epoch due to concurrent update");
                }
                c.commit();
                return new ClusterStateValue(stateKey, Long.toString(nextEpoch), nextVersion, nowMs);
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to bump cluster_epoch", e);
        }
    }

    public int pruneDeadNodes(long updatedBeforeMs, int limit) {
        if (limit <= 0) {
            return 0;
        }
        String sql = """
                DELETE FROM mesh_nodes
                WHERE rowid IN (
                    SELECT rowid FROM mesh_nodes
                    WHERE status='DEAD' AND updated_at_ms<?
                    ORDER BY updated_at_ms ASC
                    LIMIT ?
                )
                """;
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, updatedBeforeMs);
            ps.setInt(2, limit);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed prune dead mesh nodes", e);
        }
    }

    public List<LeaseConflict> listLeaseConflicts(int limit, long sinceMs) {
        String sql = """
                SELECT id,event_type,task_id,step_id,worker_id,expected_status,expected_lease_token,expected_lease_epoch,
                       actual_status,actual_lease_owner,actual_lease_token,actual_lease_epoch,occurred_at_ms
                FROM lease_conflicts
                WHERE occurred_at_ms>=?
                ORDER BY occurred_at_ms DESC, id DESC
                LIMIT ?
                """;
        List<LeaseConflict> out = new ArrayList<>();
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, Math.max(0L, sinceMs));
            ps.setInt(2, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new LeaseConflict(
                            rs.getLong("id"),
                            rs.getString("event_type"),
                            rs.getString("task_id"),
                            rs.getString("step_id"),
                            rs.getString("worker_id"),
                            rs.getString("expected_status"),
                            rs.getString("expected_lease_token"),
                            rs.getObject("expected_lease_epoch") == null ? null : rs.getLong("expected_lease_epoch"),
                            rs.getString("actual_status"),
                            rs.getString("actual_lease_owner"),
                            rs.getString("actual_lease_token"),
                            rs.getObject("actual_lease_epoch") == null ? null : rs.getLong("actual_lease_epoch"),
                            rs.getLong("occurred_at_ms")
                    ));
                }
            }
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list lease conflicts", e);
        }
    }

    public LeaseConflictSummary loadLeaseConflictSummary(long nowMs) {
        int total;
        int last1h;
        String totalSql = "SELECT COUNT(1) FROM lease_conflicts";
        String last1hSql = "SELECT COUNT(1) FROM lease_conflicts WHERE occurred_at_ms>=?";
        String byTypeSql = "SELECT event_type,COUNT(1) AS c FROM lease_conflicts GROUP BY event_type ORDER BY c DESC";
        Map<String, Integer> byType = new LinkedHashMap<>();
        try (Connection c = database.openConnection()) {
            try (PreparedStatement ps = c.prepareStatement(totalSql); ResultSet rs = ps.executeQuery()) {
                rs.next();
                total = rs.getInt(1);
            }
            try (PreparedStatement ps = c.prepareStatement(last1hSql)) {
                ps.setLong(1, nowMs - 3_600_000L);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    last1h = rs.getInt(1);
                }
            }
            try (PreparedStatement ps = c.prepareStatement(byTypeSql); ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    byType.put(rs.getString("event_type"), rs.getInt("c"));
                }
            }
            return new LeaseConflictSummary(total, last1h, byType);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load lease conflict summary", e);
        }
    }

    public RuntimeStats loadRuntimeStats() {
        Map<String, Integer> taskStatus = groupCount("tasks", "status");
        Map<String, Integer> stepStatus = groupCount("steps", "status");
        Map<String, Integer> messageStatus = groupCount("messages", "state");
        return new RuntimeStats(taskStatus, stepStatus, messageStatus);
    }

    public int countStepsByStatus(String status) {
        String sql = "SELECT COUNT(1) FROM steps WHERE namespace=? AND status=?";
        try (Connection c = database.openConnection();
             PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, namespace);
            ps.setString(2, status);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return 0;
                }
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to count steps by status", e);
        }
    }

    public List<TaskDelta> listTaskDeltas(long sinceMs, int limit) {
        String sql = """
                SELECT task_id,status,idempotency_key,trace_id,result_payload,last_error,created_at_ms,updated_at_ms
                FROM tasks
                WHERE namespace=? AND updated_at_ms>=?
                ORDER BY updated_at_ms ASC, task_id ASC
                LIMIT ?
                """;
        List<TaskDelta> out = new ArrayList<>();
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, namespace);
            ps.setLong(2, Math.max(0L, sinceMs));
            ps.setInt(3, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new TaskDelta(
                            rs.getString("task_id"),
                            rs.getString("status"),
                            rs.getString("idempotency_key"),
                            rs.getString("trace_id"),
                            rs.getString("result_payload"),
                            rs.getString("last_error"),
                            rs.getLong("created_at_ms"),
                            rs.getLong("updated_at_ms")
                    ));
                }
            }
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list task deltas", e);
        }
    }

    public List<StepDelta> listStepDeltas(long sinceMs, int limit) {
        String sql = """
                SELECT
                    s.step_id,s.task_id,s.agent_id,s.priority,s.payload_path,s.is_enqueued,s.result_payload,
                    s.status,s.attempt,s.next_retry_at_ms,s.lease_owner,s.lease_token,s.lease_epoch,
                    s.created_at_ms,s.updated_at_ms,
                    (
                      SELECT GROUP_CONCAT(d.depends_on_step_id, ',')
                      FROM step_dependencies d
                      WHERE d.namespace=s.namespace AND d.task_id=s.task_id AND d.step_id=s.step_id
                    ) AS depends_on_csv
                FROM steps s
                WHERE s.namespace=? AND s.updated_at_ms>=?
                ORDER BY s.updated_at_ms ASC, s.step_id ASC
                LIMIT ?
                """;
        List<StepDelta> out = new ArrayList<>();
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, namespace);
            ps.setLong(2, Math.max(0L, sinceMs));
            ps.setInt(3, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new StepDelta(
                            rs.getString("step_id"),
                            rs.getString("task_id"),
                            rs.getString("agent_id"),
                            rs.getString("priority"),
                            rs.getString("payload_path"),
                            safeReadPayloadText(rs.getString("payload_path")),
                            rs.getInt("is_enqueued") == 1,
                            rs.getString("result_payload"),
                            rs.getString("status"),
                            rs.getInt("attempt"),
                            rs.getObject("next_retry_at_ms") == null ? null : rs.getLong("next_retry_at_ms"),
                            rs.getString("lease_owner"),
                            rs.getString("lease_token"),
                            rs.getLong("lease_epoch"),
                            parseDependsOnCsv(rs.getString("depends_on_csv")),
                            rs.getLong("created_at_ms"),
                            rs.getLong("updated_at_ms")
                    ));
                }
            }
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list step deltas", e);
        }
    }

    public List<MessageDelta> listMessageDeltas(long sinceMs, int limit) {
        String sql = """
                SELECT msg_id,task_id,step_id,from_agent,to_agent,priority,state,meta_path,payload_path,created_at_ms,updated_at_ms
                FROM messages
                WHERE namespace=? AND updated_at_ms>=?
                ORDER BY updated_at_ms ASC, msg_id ASC
                LIMIT ?
                """;
        List<MessageDelta> out = new ArrayList<>();
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, namespace);
            ps.setLong(2, Math.max(0L, sinceMs));
            ps.setInt(3, Math.max(1, limit));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new MessageDelta(
                            rs.getString("msg_id"),
                            rs.getString("task_id"),
                            rs.getString("step_id"),
                            rs.getString("from_agent"),
                            rs.getString("to_agent"),
                            rs.getString("priority"),
                            rs.getString("state"),
                            rs.getString("meta_path"),
                            rs.getString("payload_path"),
                            rs.getLong("created_at_ms"),
                            rs.getLong("updated_at_ms")
                    ));
                }
            }
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list message deltas", e);
        }
    }

    public ReplicationApplySummary applyReplicationDeltas(
            List<TaskDelta> taskDeltas,
            List<StepDelta> stepDeltas,
            List<MessageDelta> messageDeltas
    ) {
        return applyReplicationDeltas(taskDeltas, stepDeltas, messageDeltas, 1L, 1L, null);
    }

    public ReplicationApplySummary applyReplicationDeltas(
            List<TaskDelta> taskDeltas,
            List<StepDelta> stepDeltas,
            List<MessageDelta> messageDeltas,
            long sourceClusterEpoch,
            long localClusterEpoch,
            String sourceNodeId
    ) {
        List<TaskDelta> safeTasks = taskDeltas == null ? List.of() : taskDeltas;
        List<StepDelta> safeSteps = stepDeltas == null ? List.of() : stepDeltas;
        List<MessageDelta> safeMessages = messageDeltas == null ? List.of() : messageDeltas;

        String updateTaskSql = """
                UPDATE tasks
                SET namespace=?,status=?,idempotency_key=?,trace_id=?,result_payload=?,last_error=?,created_at_ms=?,updated_at_ms=?
                WHERE namespace=? AND task_id=? AND updated_at_ms<?
                """;
        String insertTaskSql = """
                INSERT OR IGNORE INTO tasks(task_id,namespace,status,idempotency_key,principal_id,trace_id,result_payload,last_error,created_at_ms,updated_at_ms)
                VALUES(?,?,?,?,?,?,?,?,?,?)
                """;
        String insertIdempotencySql = """
                INSERT OR IGNORE INTO idempotency(namespace,idempotency_key,status,result_ref,result_hash,result_hash_algo,processed_at_ms,created_at_ms,updated_at_ms)
                VALUES(?,?,?,?,?,?,?,?,?)
                """;
        String updateIdempotencySql = """
                UPDATE idempotency
                SET status=?,result_ref=?,result_hash=?,result_hash_algo=?,processed_at_ms=?,updated_at_ms=?
                WHERE namespace=? AND idempotency_key=? AND updated_at_ms<?
                """;

        String updateStepSql = """
                UPDATE steps
                SET namespace=?,task_id=?,agent_id=?,priority=?,payload_path=?,is_enqueued=?,result_payload=?,status=?,attempt=?,
                    next_retry_at_ms=?,lease_owner=?,lease_token=?,lease_epoch=?,created_at_ms=?,updated_at_ms=?
                WHERE namespace=? AND step_id=? AND updated_at_ms<?
                """;
        String updateStepTieSql = """
                UPDATE steps
                SET namespace=?,task_id=?,agent_id=?,priority=?,payload_path=?,is_enqueued=?,result_payload=?,status=?,attempt=?,
                    next_retry_at_ms=?,lease_owner=?,lease_token=?,lease_epoch=?,created_at_ms=?,updated_at_ms=?
                WHERE namespace=? AND step_id=? AND updated_at_ms=?
                """;
        String insertStepSql = """
                INSERT OR IGNORE INTO steps(
                    step_id,namespace,task_id,agent_id,priority,payload_path,is_enqueued,result_payload,status,attempt,
                    next_retry_at_ms,lease_owner,lease_token,lease_epoch,created_at_ms,updated_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """;
        String insertDependencySql = """
                INSERT OR IGNORE INTO step_dependencies(namespace,task_id,step_id,depends_on_step_id,created_at_ms)
                VALUES(?,?,?,?,?)
                """;

        String updateMessageSql = """
                UPDATE messages
                SET namespace=?,task_id=?,step_id=?,from_agent=?,to_agent=?,priority=?,state=?,meta_path=?,payload_path=?,created_at_ms=?,updated_at_ms=?
                WHERE namespace=? AND msg_id=? AND updated_at_ms<?
                """;
        String insertMessageSql = """
                INSERT OR IGNORE INTO messages(
                    msg_id,namespace,task_id,step_id,from_agent,to_agent,priority,state,meta_path,payload_path,created_at_ms,updated_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """;

        int taskApplied = 0;
        int stepApplied = 0;
        int messageApplied = 0;
        int staleSkipped = 0;
        int tieResolved = 0;
        int tieKeptLocal = 0;

        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement updateTask = c.prepareStatement(updateTaskSql);
                 PreparedStatement insertTask = c.prepareStatement(insertTaskSql);
                 PreparedStatement insertIdem = c.prepareStatement(insertIdempotencySql);
                 PreparedStatement updateIdem = c.prepareStatement(updateIdempotencySql);
                 PreparedStatement updateStep = c.prepareStatement(updateStepSql);
                 PreparedStatement updateStepTie = c.prepareStatement(updateStepTieSql);
                 PreparedStatement insertStep = c.prepareStatement(insertStepSql);
                 PreparedStatement insertDep = c.prepareStatement(insertDependencySql);
                 PreparedStatement updateMessage = c.prepareStatement(updateMessageSql);
                 PreparedStatement insertMessage = c.prepareStatement(insertMessageSql)) {
                for (TaskDelta delta : safeTasks) {
                    if (delta == null || delta.taskId() == null || delta.taskId().isBlank()) {
                        staleSkipped++;
                        continue;
                    }
                    long createdAtMs = Math.max(0L, delta.createdAtMs());
                    long updatedAtMs = Math.max(createdAtMs, delta.updatedAtMs());
                    String idempotencyKey = (delta.idempotencyKey() == null || delta.idempotencyKey().isBlank())
                            ? delta.taskId()
                            : delta.idempotencyKey();
                    int updateRows = bindTaskDeltaUpdate(updateTask, delta, idempotencyKey, createdAtMs, updatedAtMs).executeUpdate();
                    boolean applied = updateRows == 1;
                    if (!applied) {
                        int insertRows = bindTaskDeltaInsert(insertTask, delta, idempotencyKey, createdAtMs, updatedAtMs).executeUpdate();
                        applied = insertRows == 1;
                    }
                    if (applied) {
                        taskApplied++;
                        bindIdempotencyInsert(insertIdem, delta, idempotencyKey, createdAtMs, updatedAtMs).executeUpdate();
                        bindIdempotencyUpdate(updateIdem, delta, idempotencyKey, updatedAtMs).executeUpdate();
                    } else {
                        staleSkipped++;
                    }
                }

                for (StepDelta delta : safeSteps) {
                    if (delta == null || delta.stepId() == null || delta.stepId().isBlank()
                            || delta.taskId() == null || delta.taskId().isBlank()) {
                        staleSkipped++;
                        continue;
                    }
                    long createdAtMs = Math.max(0L, delta.createdAtMs());
                    long updatedAtMs = Math.max(createdAtMs, delta.updatedAtMs());
                    int updateRows = bindStepDeltaUpdate(updateStep, delta, createdAtMs, updatedAtMs).executeUpdate();
                    boolean applied = updateRows == 1;
                    if (!applied) {
                        int insertRows = bindStepDeltaInsert(insertStep, delta, createdAtMs, updatedAtMs).executeUpdate();
                        applied = insertRows == 1;
                    }
                    if (!applied) {
                        StepReplicationRow local = readStepReplicationRow(c, delta.stepId());
                        if (local != null
                                && local.updatedAtMs() == updatedAtMs
                                && isRunningStatus(local.status())
                                && isRunningStatus(delta.status())) {
                            StepTieDecision decision = decideStepTie(
                                    delta,
                                    local,
                                    sourceClusterEpoch,
                                    localClusterEpoch,
                                    sourceNodeId
                            );
                            if (decision == StepTieDecision.INCOMING_WINS) {
                                int tieRows = bindStepDeltaTieUpdate(updateStepTie, delta, createdAtMs, updatedAtMs).executeUpdate();
                                if (tieRows == 1) {
                                    applied = true;
                                    tieResolved++;
                                } else {
                                    tieKeptLocal++;
                                }
                            } else {
                                tieKeptLocal++;
                            }
                        }
                    }
                    if (applied) {
                        stepApplied++;
                    } else {
                        staleSkipped++;
                    }
                    for (String dependsOn : delta.dependsOn() == null ? List.<String>of() : delta.dependsOn()) {
                        if (dependsOn == null || dependsOn.isBlank()) {
                            continue;
                        }
                        insertDep.setString(1, namespace);
                        insertDep.setString(2, delta.taskId());
                        insertDep.setString(3, delta.stepId());
                        insertDep.setString(4, dependsOn);
                        insertDep.setLong(5, createdAtMs);
                        insertDep.executeUpdate();
                    }
                }

                for (MessageDelta delta : safeMessages) {
                    if (delta == null || delta.msgId() == null || delta.msgId().isBlank()
                            || delta.taskId() == null || delta.taskId().isBlank()
                            || delta.stepId() == null || delta.stepId().isBlank()) {
                        staleSkipped++;
                        continue;
                    }
                    long createdAtMs = Math.max(0L, delta.createdAtMs());
                    long updatedAtMs = Math.max(createdAtMs, delta.updatedAtMs());
                    int updateRows = bindMessageDeltaUpdate(updateMessage, delta, createdAtMs, updatedAtMs).executeUpdate();
                    boolean applied = updateRows == 1;
                    if (!applied) {
                        int insertRows = bindMessageDeltaInsert(insertMessage, delta, createdAtMs, updatedAtMs).executeUpdate();
                        applied = insertRows == 1;
                    }
                    if (applied) {
                        messageApplied++;
                    } else {
                        staleSkipped++;
                    }
                }
                c.commit();
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
            return new ReplicationApplySummary(taskApplied, stepApplied, messageApplied, staleSkipped, tieResolved, tieKeptLocal);
        } catch (Exception e) {
            throw new RuntimeException("Failed to apply replication deltas", e);
        }
    }

    public MeshSnapshot loadMeshSnapshot(long nowMs) {
        String nodeSql = """
                SELECT
                    COUNT(1) AS total_nodes,
                    SUM(CASE WHEN status='ALIVE' THEN 1 ELSE 0 END) AS alive_nodes,
                    SUM(CASE WHEN status='SUSPECT' THEN 1 ELSE 0 END) AS suspect_nodes,
                    SUM(CASE WHEN status='DEAD' THEN 1 ELSE 0 END) AS dead_nodes
                FROM mesh_nodes
                """;
        String runningSql = """
                SELECT
                    SUM(CASE WHEN COALESCE(m.status,'UNKNOWN')='ALIVE' THEN 1 ELSE 0 END) AS running_alive,
                    SUM(CASE WHEN COALESCE(m.status,'UNKNOWN')='SUSPECT' THEN 1 ELSE 0 END) AS running_suspect,
                    SUM(CASE WHEN COALESCE(m.status,'UNKNOWN')='DEAD' THEN 1 ELSE 0 END) AS running_dead,
                    SUM(CASE WHEN COALESCE(m.status,'UNKNOWN') NOT IN ('ALIVE','SUSPECT','DEAD') THEN 1 ELSE 0 END) AS running_unknown
                FROM steps s
                LEFT JOIN mesh_nodes m ON s.lease_owner = m.node_id
                WHERE s.status='RUNNING' AND s.lease_owner IS NOT NULL AND s.lease_owner<>''
                """;
        String oldestDeadSql = "SELECT MIN(updated_at_ms) FROM mesh_nodes WHERE status='DEAD'";

        int totalNodes = 0;
        int aliveNodes = 0;
        int suspectNodes = 0;
        int deadNodes = 0;
        int runningByAliveOwners = 0;
        int runningBySuspectOwners = 0;
        int runningByDeadOwners = 0;
        int runningByUnknownOwners = 0;
        long oldestDeadNodeAgeMs = 0L;

        try (Connection c = database.openConnection()) {
            try (PreparedStatement ps = c.prepareStatement(nodeSql); ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    totalNodes = rs.getInt("total_nodes");
                    aliveNodes = rs.getInt("alive_nodes");
                    suspectNodes = rs.getInt("suspect_nodes");
                    deadNodes = rs.getInt("dead_nodes");
                }
            }
            try (PreparedStatement ps = c.prepareStatement(runningSql); ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    runningByAliveOwners = rs.getInt("running_alive");
                    runningBySuspectOwners = rs.getInt("running_suspect");
                    runningByDeadOwners = rs.getInt("running_dead");
                    runningByUnknownOwners = rs.getInt("running_unknown");
                }
            }
            try (PreparedStatement ps = c.prepareStatement(oldestDeadSql); ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long oldestDeadUpdatedAtMs = rs.getLong(1);
                    if (!rs.wasNull() && oldestDeadUpdatedAtMs > 0L) {
                        oldestDeadNodeAgeMs = Math.max(0L, nowMs - oldestDeadUpdatedAtMs);
                    }
                }
            }
            return new MeshSnapshot(
                    totalNodes,
                    aliveNodes,
                    suspectNodes,
                    deadNodes,
                    runningByAliveOwners,
                    runningBySuspectOwners,
                    runningByDeadOwners,
                    runningByUnknownOwners,
                    oldestDeadNodeAgeMs
            );
        } catch (SQLException e) {
            throw new RuntimeException("Failed to load mesh snapshot", e);
        }
    }

    public SlaSnapshot loadSlaSnapshot(long nowMs) {
        long queueLagMs = 0L;
        String queueLagSql = "SELECT MIN(created_at_ms) FROM steps WHERE status IN (?,?)";
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(queueLagSql)) {
            ps.setString(1, TaskStatus.PENDING.name());
            ps.setString(2, TaskStatus.RETRYING.name());
            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    long oldest = rs.getLong(1);
                    if (!rs.wasNull()) {
                        queueLagMs = Math.max(0L, nowMs - oldest);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to compute queue lag", e);
        }

        List<Long> latencies = new ArrayList<>();
        String latencySql = """
                SELECT (updated_at_ms - created_at_ms) AS latency_ms
                FROM steps
                WHERE status IN (?, ?, ?)
                ORDER BY updated_at_ms DESC
                LIMIT 5000
                """;
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(latencySql)) {
            ps.setString(1, TaskStatus.SUCCESS.name());
            ps.setString(2, TaskStatus.DEAD_LETTER.name());
            ps.setString(3, TaskStatus.CANCELLED.name());
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    latencies.add(Math.max(0L, rs.getLong("latency_ms")));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to compute step latency percentiles", e);
        }
        Collections.sort(latencies);
        long p95 = percentile(latencies, 95.0);
        long p99 = percentile(latencies, 99.0);

        int deadGrowth1h;
        String deadSql = "SELECT COUNT(1) FROM tasks WHERE status=? AND updated_at_ms>=?";
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(deadSql)) {
            ps.setString(1, TaskStatus.DEAD_LETTER.name());
            ps.setLong(2, nowMs - 3_600_000L);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                deadGrowth1h = rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to compute dead letter growth", e);
        }

        return new SlaSnapshot(queueLagMs, p95, p99, deadGrowth1h);
    }

    public List<TaskSummary> listTasks(String status, int limit, int offset) {
        String base = "SELECT task_id,status,trace_id,last_error,created_at_ms,updated_at_ms FROM tasks";
        String sql;
        boolean withStatus = status != null && !status.isBlank();
        if (withStatus) {
            sql = base + " WHERE status=? ORDER BY updated_at_ms DESC LIMIT ? OFFSET ?";
        } else {
            sql = base + " ORDER BY updated_at_ms DESC LIMIT ? OFFSET ?";
        }
        List<TaskSummary> out = new ArrayList<>();
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            int i = 1;
            if (withStatus) {
                ps.setString(i++, status);
            }
            ps.setInt(i++, Math.max(1, limit));
            ps.setInt(i, Math.max(0, offset));
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new TaskSummary(
                            rs.getString("task_id"),
                            rs.getString("status"),
                            rs.getString("trace_id"),
                            rs.getString("last_error"),
                            rs.getLong("created_at_ms"),
                            rs.getLong("updated_at_ms")
                    ));
                }
            }
            return out;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to list tasks", e);
        }
    }

    public Optional<WorkflowDetail> getWorkflowDetail(String taskId) {
        String taskSql = "SELECT task_id,status,trace_id,created_at_ms,updated_at_ms FROM tasks WHERE task_id=?";
        String stepSql = "SELECT step_id,agent_id,status,attempt,priority,is_enqueued,created_at_ms,updated_at_ms FROM steps WHERE task_id=? ORDER BY step_id";
        String depSql = "SELECT step_id,depends_on_step_id FROM step_dependencies WHERE task_id=? ORDER BY step_id,depends_on_step_id";
        try (Connection c = database.openConnection();
             PreparedStatement taskPs = c.prepareStatement(taskSql);
             PreparedStatement stepPs = c.prepareStatement(stepSql);
             PreparedStatement depPs = c.prepareStatement(depSql)) {
            taskPs.setString(1, taskId);
            TaskHeader header;
            try (ResultSet rs = taskPs.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                header = new TaskHeader(
                        rs.getString("task_id"),
                        rs.getString("status"),
                        rs.getString("trace_id"),
                        rs.getLong("created_at_ms"),
                        rs.getLong("updated_at_ms")
                );
            }

            Map<String, List<String>> depMap = new HashMap<>();
            depPs.setString(1, taskId);
            try (ResultSet rs = depPs.executeQuery()) {
                while (rs.next()) {
                    depMap.computeIfAbsent(rs.getString("step_id"), k -> new ArrayList<>())
                            .add(rs.getString("depends_on_step_id"));
                }
            }

            List<StepDetail> steps = new ArrayList<>();
            stepPs.setString(1, taskId);
            try (ResultSet rs = stepPs.executeQuery()) {
                while (rs.next()) {
                    String sid = rs.getString("step_id");
                    steps.add(new StepDetail(
                            sid,
                            rs.getString("agent_id"),
                            rs.getString("status"),
                            rs.getInt("attempt"),
                            rs.getString("priority"),
                            rs.getInt("is_enqueued") == 1,
                            depMap.getOrDefault(sid, List.of()),
                            rs.getLong("created_at_ms"),
                            rs.getLong("updated_at_ms")
                    ));
                }
            }
            return Optional.of(new WorkflowDetail(header, steps));
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query workflow detail: " + taskId, e);
        }
    }

    public CancelResult cancelTask(String taskId, String reason, CancelMode mode, long nowMs) {
        return cancelTask(taskId, reason, mode, nowMs, currentClusterEpochValue());
    }

    public CancelResult cancelTask(String taskId, String reason, CancelMode mode, long nowMs, long expectedClusterEpoch) {
        String check = "SELECT status,idempotency_key FROM tasks WHERE task_id=?";
        String updateTask = "UPDATE tasks SET status=?, last_error=?, updated_at_ms=? WHERE task_id=?";
        String updateIdem = "UPDATE idempotency SET status=?, updated_at_ms=? WHERE idempotency_key=?";
        List<String> stepTargets = mode == CancelMode.HARD
                ? List.of(
                TaskStatus.PENDING.name(),
                TaskStatus.RUNNING.name(),
                TaskStatus.RETRYING.name(),
                TaskStatus.ABANDONED.name(),
                TaskStatus.FAILED.name()
        )
                : List.of(
                TaskStatus.PENDING.name(),
                TaskStatus.RETRYING.name(),
                TaskStatus.ABANDONED.name(),
                TaskStatus.FAILED.name()
        );
        List<String> messageTargets = mode == CancelMode.HARD
                ? List.of(MessageState.QUEUED.name(), MessageState.PROCESSING.name(), MessageState.FAILED.name())
                : List.of(MessageState.QUEUED.name(), MessageState.FAILED.name());
        String updateSteps = "UPDATE steps SET status=?, lease_owner=NULL, lease_token=NULL, next_retry_at_ms=NULL, updated_at_ms=? " +
                "WHERE task_id=? AND status IN (" + placeholders(stepTargets.size()) + ") " +
                "AND ?=(SELECT CAST(state_value AS INTEGER) FROM cluster_state WHERE state_key='cluster_epoch')";
        String updateMsg = "UPDATE messages SET state=?, updated_at_ms=? " +
                "WHERE task_id=? AND state IN (" + placeholders(messageTargets.size()) + ")";

        try (Connection c = database.openConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement psCheck = c.prepareStatement(check);
                 PreparedStatement psTask = c.prepareStatement(updateTask);
                 PreparedStatement psSteps = c.prepareStatement(updateSteps);
                 PreparedStatement psMsg = c.prepareStatement(updateMsg);
                 PreparedStatement psIdem = c.prepareStatement(updateIdem)) {
                psCheck.setString(1, taskId);
                String status;
                String idem;
                try (ResultSet rs = psCheck.executeQuery()) {
                    if (!rs.next()) {
                        c.rollback();
                        return new CancelResult(taskId, false, mode.name().toLowerCase(Locale.ROOT), "not_found", 0, 0);
                    }
                    status = rs.getString("status");
                    idem = rs.getString("idempotency_key");
                }
                if (TaskStatus.SUCCESS.name().equalsIgnoreCase(status)
                        || TaskStatus.DEAD_LETTER.name().equalsIgnoreCase(status)
                        || TaskStatus.CANCELLED.name().equalsIgnoreCase(status)) {
                    c.rollback();
                    return new CancelResult(taskId, false, mode.name().toLowerCase(Locale.ROOT), "terminal_state:" + status, 0, 0);
                }

                psTask.setString(1, TaskStatus.CANCELLED.name());
                psTask.setString(2, reason == null || reason.isBlank() ? "cancelled by user" : reason);
                psTask.setLong(3, nowMs);
                psTask.setString(4, taskId);
                psTask.executeUpdate();

                psSteps.setString(1, TaskStatus.CANCELLED.name());
                psSteps.setLong(2, nowMs);
                psSteps.setString(3, taskId);
                int stepBindIdx = 4;
                for (String state : stepTargets) {
                    psSteps.setString(stepBindIdx++, state);
                }
                psSteps.setLong(stepBindIdx, Math.max(1L, expectedClusterEpoch));
                int stepRows = psSteps.executeUpdate();

                psMsg.setString(1, MessageState.DEAD.name());
                psMsg.setLong(2, nowMs);
                psMsg.setString(3, taskId);
                int msgBindIdx = 4;
                for (String state : messageTargets) {
                    psMsg.setString(msgBindIdx++, state);
                }
                int msgRows = psMsg.executeUpdate();

                psIdem.setString(1, TaskStatus.FAILED.name());
                psIdem.setLong(2, nowMs);
                psIdem.setString(3, idem);
                psIdem.executeUpdate();

                c.commit();
                return new CancelResult(taskId, true, mode.name().toLowerCase(Locale.ROOT), "cancelled", stepRows, msgRows);
            } catch (Exception e) {
                c.rollback();
                throw e;
            } finally {
                c.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to cancel task: " + taskId, e);
        }
    }

    private StepLeaseState readStepLeaseState(Connection c, String stepId) {
        String sql = "SELECT status,lease_owner,lease_token,lease_epoch FROM steps WHERE step_id=?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, stepId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new StepLeaseState(
                        rs.getString("status"),
                        rs.getString("lease_owner"),
                        rs.getString("lease_token"),
                        rs.getLong("lease_epoch")
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read step lease state: " + stepId, e);
        }
    }

    private void recordLeaseConflict(Connection c,
                                     String eventType,
                                     String taskId,
                                     String stepId,
                                     String workerId,
                                     String expectedStatus,
                                     String expectedLeaseToken,
                                     Long expectedLeaseEpoch,
                                     StepLeaseState actual) {
        String sql = """
                INSERT INTO lease_conflicts(
                    event_type,task_id,step_id,worker_id,
                    expected_status,expected_lease_token,expected_lease_epoch,
                    actual_status,actual_lease_owner,actual_lease_token,actual_lease_epoch,
                    occurred_at_ms
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """;
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, eventType);
            ps.setString(2, taskId);
            ps.setString(3, stepId);
            ps.setString(4, workerId);
            ps.setString(5, expectedStatus);
            ps.setString(6, expectedLeaseToken);
            if (expectedLeaseEpoch == null) {
                ps.setObject(7, null);
            } else {
                ps.setLong(7, expectedLeaseEpoch);
            }
            ps.setString(8, actual == null ? null : actual.status());
            ps.setString(9, actual == null ? null : actual.leaseOwner());
            ps.setString(10, actual == null ? null : actual.leaseToken());
            if (actual == null) {
                ps.setObject(11, null);
            } else {
                ps.setLong(11, actual.leaseEpoch());
            }
            ps.setLong(12, System.currentTimeMillis());
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to record lease conflict", e);
        }
    }

    private String findTaskIdByIdempotencyKey(String key) {
        String sql = "SELECT result_ref FROM idempotency WHERE namespace=? AND idempotency_key=?";
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, namespace);
            ps.setString(2, key);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString("result_ref") : null;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed idempotency lookup", e);
        }
    }

    private Map<String, Integer> groupCount(String table, String column) {
        boolean scoped = "tasks".equals(table) || "steps".equals(table) || "messages".equals(table);
        String sql = "SELECT " + column + ", COUNT(1) AS c FROM " + table
                + (scoped ? " WHERE namespace=?" : "")
                + " GROUP BY " + column;
        Map<String, Integer> out = new LinkedHashMap<>();
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            if (scoped) {
                ps.setString(1, namespace);
            }
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.put(rs.getString(1), rs.getInt(2));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to collect stats: " + table + "." + column, e);
        }
    }

    private String buildWorkflowResultJson(Connection c, String taskId) throws SQLException {
        String sql = "SELECT step_id,result_payload FROM steps WHERE task_id=? ORDER BY step_id";
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        boolean first = true;
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, taskId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (!first) sb.append(",");
                    first = false;
                    String key = rs.getString("step_id");
                    String val = rs.getString("result_payload");
                    sb.append("\"").append(escapeJson(key)).append("\":");
                    if (val == null) {
                        sb.append("null");
                    } else {
                        sb.append("\"").append(escapeJson(val)).append("\"");
                    }
                }
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private void exec(String sql, Binder binder) {
        try (Connection c = database.openConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            binder.bind(ps);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("DB exec failed", e);
        }
    }

    private long computeBackoffMs(int attempt, long baseBackoffMs, long maxBackoffMs) {
        long backoff = baseBackoffMs;
        for (int i = 1; i < attempt; i++) {
            if (backoff >= maxBackoffMs / 2L) {
                backoff = maxBackoffMs;
                break;
            }
            backoff *= 2L;
        }
        backoff = Math.min(backoff, maxBackoffMs);
        long jitter = ThreadLocalRandom.current().nextLong(0L, 251L);
        return Math.min(maxBackoffMs, backoff + jitter);
    }

    private long parseLongOrDefault(String raw, long fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (Exception ignored) {
            return fallback;
        }
    }

    private long currentClusterEpochValue() {
        ClusterStateValue value = clusterEpoch();
        return Math.max(1L, parseLongOrDefault(value.value(), 1L));
    }

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }

    private List<String> parseDependsOnCsv(String csv) {
        if (csv == null || csv.isBlank()) {
            return List.of();
        }
        String[] parts = csv.split(",");
        List<String> out = new ArrayList<>(parts.length);
        for (String part : parts) {
            if (part == null) {
                continue;
            }
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        return out;
    }

    private PreparedStatement bindTaskDeltaUpdate(
            PreparedStatement ps,
            TaskDelta delta,
            String idempotencyKey,
            long createdAtMs,
            long updatedAtMs
    ) throws SQLException {
        ps.setString(1, namespace);
        ps.setString(2, delta.status());
        ps.setString(3, idempotencyKey);
        ps.setString(4, delta.traceId());
        ps.setString(5, delta.resultPayload());
        ps.setString(6, delta.lastError());
        ps.setLong(7, createdAtMs);
        ps.setLong(8, updatedAtMs);
        ps.setString(9, namespace);
        ps.setString(10, delta.taskId());
        ps.setLong(11, updatedAtMs);
        return ps;
    }

    private PreparedStatement bindTaskDeltaInsert(
            PreparedStatement ps,
            TaskDelta delta,
            String idempotencyKey,
            long createdAtMs,
            long updatedAtMs
    ) throws SQLException {
        ps.setString(1, delta.taskId());
        ps.setString(2, namespace);
        ps.setString(3, delta.status());
        ps.setString(4, idempotencyKey);
        ps.setString(5, "");
        ps.setString(6, delta.traceId());
        ps.setString(7, delta.resultPayload());
        ps.setString(8, delta.lastError());
        ps.setLong(9, createdAtMs);
        ps.setLong(10, updatedAtMs);
        return ps;
    }

    private PreparedStatement bindIdempotencyInsert(
            PreparedStatement ps,
            TaskDelta delta,
            String idempotencyKey,
            long createdAtMs,
            long updatedAtMs
    ) throws SQLException {
        String resultPayload = delta.resultPayload();
        String resultHash = resultPayload == null ? null : Hashing.sha256Hex(resultPayload);
        String status = delta.status() == null ? TaskStatus.PENDING.name() : delta.status();
        Long processedAtMs = terminalTaskStatus(status) ? updatedAtMs : null;
        ps.setString(1, namespace);
        ps.setString(2, idempotencyKey);
        ps.setString(3, status);
        ps.setString(4, delta.taskId());
        ps.setString(5, resultHash);
        ps.setString(6, resultHash == null ? null : "sha256");
        ps.setObject(7, processedAtMs);
        ps.setLong(8, createdAtMs);
        ps.setLong(9, updatedAtMs);
        return ps;
    }

    private PreparedStatement bindIdempotencyUpdate(
            PreparedStatement ps,
            TaskDelta delta,
            String idempotencyKey,
            long updatedAtMs
    ) throws SQLException {
        String resultPayload = delta.resultPayload();
        String resultHash = resultPayload == null ? null : Hashing.sha256Hex(resultPayload);
        String status = delta.status() == null ? TaskStatus.PENDING.name() : delta.status();
        Long processedAtMs = terminalTaskStatus(status) ? updatedAtMs : null;
        ps.setString(1, status);
        ps.setString(2, delta.taskId());
        ps.setString(3, resultHash);
        ps.setString(4, resultHash == null ? null : "sha256");
        ps.setObject(5, processedAtMs);
        ps.setLong(6, updatedAtMs);
        ps.setString(7, namespace);
        ps.setString(8, idempotencyKey);
        ps.setLong(9, updatedAtMs);
        return ps;
    }

    private PreparedStatement bindStepDeltaUpdate(
            PreparedStatement ps,
            StepDelta delta,
            long createdAtMs,
            long updatedAtMs
    ) throws SQLException {
        ps.setString(1, namespace);
        ps.setString(2, delta.taskId());
        ps.setString(3, delta.agentId());
        ps.setString(4, delta.priority());
        ps.setString(5, delta.payloadPath());
        ps.setInt(6, delta.enqueued() ? 1 : 0);
        ps.setString(7, delta.resultPayload());
        ps.setString(8, delta.status());
        ps.setInt(9, Math.max(0, delta.attempt()));
        ps.setObject(10, delta.nextRetryAtMs());
        ps.setString(11, delta.leaseOwner());
        ps.setString(12, delta.leaseToken());
        ps.setLong(13, Math.max(0L, delta.leaseEpoch()));
        ps.setLong(14, createdAtMs);
        ps.setLong(15, updatedAtMs);
        ps.setString(16, namespace);
        ps.setString(17, delta.stepId());
        ps.setLong(18, updatedAtMs);
        return ps;
    }

    private PreparedStatement bindStepDeltaTieUpdate(
            PreparedStatement ps,
            StepDelta delta,
            long createdAtMs,
            long updatedAtMs
    ) throws SQLException {
        return bindStepDeltaUpdate(ps, delta, createdAtMs, updatedAtMs);
    }

    private PreparedStatement bindStepDeltaInsert(
            PreparedStatement ps,
            StepDelta delta,
            long createdAtMs,
            long updatedAtMs
    ) throws SQLException {
        ps.setString(1, delta.stepId());
        ps.setString(2, namespace);
        ps.setString(3, delta.taskId());
        ps.setString(4, delta.agentId());
        ps.setString(5, delta.priority());
        ps.setString(6, delta.payloadPath());
        ps.setInt(7, delta.enqueued() ? 1 : 0);
        ps.setString(8, delta.resultPayload());
        ps.setString(9, delta.status());
        ps.setInt(10, Math.max(0, delta.attempt()));
        ps.setObject(11, delta.nextRetryAtMs());
        ps.setString(12, delta.leaseOwner());
        ps.setString(13, delta.leaseToken());
        ps.setLong(14, Math.max(0L, delta.leaseEpoch()));
        ps.setLong(15, createdAtMs);
        ps.setLong(16, updatedAtMs);
        return ps;
    }

    private PreparedStatement bindMessageDeltaUpdate(
            PreparedStatement ps,
            MessageDelta delta,
            long createdAtMs,
            long updatedAtMs
    ) throws SQLException {
        ps.setString(1, namespace);
        ps.setString(2, delta.taskId());
        ps.setString(3, delta.stepId());
        ps.setString(4, delta.fromAgent());
        ps.setString(5, delta.toAgent());
        ps.setString(6, delta.priority());
        ps.setString(7, delta.state());
        ps.setString(8, delta.metaPath());
        ps.setString(9, delta.payloadPath());
        ps.setLong(10, createdAtMs);
        ps.setLong(11, updatedAtMs);
        ps.setString(12, namespace);
        ps.setString(13, delta.msgId());
        ps.setLong(14, updatedAtMs);
        return ps;
    }

    private PreparedStatement bindMessageDeltaInsert(
            PreparedStatement ps,
            MessageDelta delta,
            long createdAtMs,
            long updatedAtMs
    ) throws SQLException {
        ps.setString(1, delta.msgId());
        ps.setString(2, namespace);
        ps.setString(3, delta.taskId());
        ps.setString(4, delta.stepId());
        ps.setString(5, delta.fromAgent());
        ps.setString(6, delta.toAgent());
        ps.setString(7, delta.priority());
        ps.setString(8, delta.state());
        ps.setString(9, delta.metaPath());
        ps.setString(10, delta.payloadPath());
        ps.setLong(11, createdAtMs);
        ps.setLong(12, updatedAtMs);
        return ps;
    }

    private boolean terminalTaskStatus(String status) {
        if (status == null) {
            return false;
        }
        return TaskStatus.SUCCESS.name().equals(status)
                || TaskStatus.DEAD_LETTER.name().equals(status)
                || TaskStatus.CANCELLED.name().equals(status);
    }

    private StepReplicationRow readStepReplicationRow(Connection c, String stepId) {
        String sql = "SELECT status,lease_owner,lease_epoch,updated_at_ms FROM steps WHERE namespace=? AND step_id=?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, namespace);
            ps.setString(2, stepId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return new StepReplicationRow(
                        rs.getString("status"),
                        rs.getString("lease_owner"),
                        rs.getLong("lease_epoch"),
                        rs.getLong("updated_at_ms")
                );
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to read step replication row", e);
        }
    }

    private StepTieDecision decideStepTie(
            StepDelta incoming,
            StepReplicationRow local,
            long sourceClusterEpoch,
            long localClusterEpoch,
            String sourceNodeId
    ) {
        long incomingEpoch = Math.max(1L, sourceClusterEpoch);
        long localEpoch = Math.max(1L, localClusterEpoch);
        if (incomingEpoch > localEpoch) {
            return StepTieDecision.INCOMING_WINS;
        }
        if (incomingEpoch < localEpoch) {
            return StepTieDecision.LOCAL_WINS;
        }

        long incomingVersion = Math.max(0L, incoming.leaseEpoch());
        long localVersion = Math.max(0L, local.leaseEpoch());
        if (incomingVersion > localVersion) {
            return StepTieDecision.INCOMING_WINS;
        }
        if (incomingVersion < localVersion) {
            return StepTieDecision.LOCAL_WINS;
        }

        String incomingNode = canonicalNodeId(preferredNodeId(incoming.leaseOwner(), sourceNodeId));
        String localNode = canonicalNodeId(local.leaseOwner());
        if (incomingNode.compareTo(localNode) < 0) {
            return StepTieDecision.INCOMING_WINS;
        }
        return StepTieDecision.LOCAL_WINS;
    }

    private boolean isRunningStatus(String status) {
        return TaskStatus.RUNNING.name().equals(status);
    }

    private String preferredNodeId(String leaseOwner, String sourceNodeId) {
        if (leaseOwner != null && !leaseOwner.isBlank()) {
            return leaseOwner;
        }
        if (sourceNodeId != null && !sourceNodeId.isBlank()) {
            return sourceNodeId;
        }
        return "";
    }

    private String canonicalNodeId(String raw) {
        if (raw == null || raw.isBlank()) {
            return "~";
        }
        return raw.trim();
    }

    private String safeReadPayloadText(String payloadPath) {
        if (payloadPath == null || payloadPath.isBlank()) {
            return null;
        }
        try {
            Path path = Path.of(payloadPath);
            if (!Files.exists(path) || !Files.isRegularFile(path)) {
                return null;
            }
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (IOException | RuntimeException ignored) {
            return null;
        }
    }

    private long percentile(List<Long> sorted, double p) {
        if (sorted == null || sorted.isEmpty()) {
            return 0L;
        }
        int idx = (int) Math.ceil((p / 100.0) * sorted.size()) - 1;
        idx = Math.max(0, Math.min(sorted.size() - 1, idx));
        return sorted.get(idx);
    }

    private String placeholders(int count) {
        if (count <= 0) {
            throw new IllegalArgumentException("count must be > 0");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append('?');
        }
        return sb.toString();
    }

    private interface Binder { void bind(PreparedStatement ps) throws SQLException; }

    public record Submission(String taskId, String stepId, String msgId, String agentId, String idempotencyKey,
                             String principalId, String traceId, String priority, String metaPath, String payloadPath, long nowMs) {}
    public record WorkflowSubmission(String taskId, String traceId, String idempotencyKey, String principalId, List<WorkflowStep> steps, long nowMs) {}
    public record WorkflowStep(String stepId, String agentId, String priority, String payloadPath, List<String> dependsOn) {}
    public record SubmitResult(String taskId, boolean deduplicated) {}
    public record WorkflowSubmitResult(String taskId, boolean deduplicated, List<ReadyStepDispatch> readySteps) {}
    public record StepDispatchContext(String taskId, String stepId, String agentId, String priority, String payloadPath,
                                      int attempt, String idempotencyKey, String traceId) {}
    public enum CancelMode {
        HARD,
        SOFT;

        public static CancelMode fromString(String raw) {
            if (raw == null || raw.isBlank()) {
                return HARD;
            }
            String v = raw.trim().toUpperCase(Locale.ROOT);
            if ("HARD".equals(v)) {
                return HARD;
            }
            if ("SOFT".equals(v)) {
                return SOFT;
            }
            throw new IllegalArgumentException("Unsupported cancel mode: " + raw);
        }
    }
    public enum StepSuccessOutcome { STALE_LEASE, STEP_COMPLETED, TASK_COMPLETED }
    public record StepSuccessResolution(StepSuccessOutcome outcome, List<ReadyStepDispatch> readySteps, String finalResultPayload) {
        public static StepSuccessResolution staleLease() { return new StepSuccessResolution(StepSuccessOutcome.STALE_LEASE, List.of(), null); }
        public static StepSuccessResolution stepCompleted(List<ReadyStepDispatch> readySteps) { return new StepSuccessResolution(StepSuccessOutcome.STEP_COMPLETED, readySteps, null); }
        public static StepSuccessResolution taskCompleted(List<ReadyStepDispatch> readySteps, String payload) { return new StepSuccessResolution(StepSuccessOutcome.TASK_COMPLETED, readySteps, payload); }
    }
    public enum FailureOutcome { RETRY_SCHEDULED, DEAD_LETTERED, STALE_LEASE }
    public record FailureResolution(FailureOutcome outcome, int attempt, Long nextRetryAtMs) {
        public static FailureResolution retryScheduled(int attempt, long nextRetryAtMs) { return new FailureResolution(FailureOutcome.RETRY_SCHEDULED, attempt, nextRetryAtMs); }
        public static FailureResolution deadLetter(int attempt) { return new FailureResolution(FailureOutcome.DEAD_LETTERED, attempt, null); }
        public static FailureResolution staleLease() { return new FailureResolution(FailureOutcome.STALE_LEASE, 0, null); }
    }
    public record RetryDispatch(String taskId, String stepId, String agentId, String priority, String payloadPath, int nextAttempt) {}
    public record ReadyStepDispatch(String taskId, String stepId, String agentId, String priority, String payloadPath, int nextAttempt) {}
    public record RetryMessage(String msgId, String taskId, String stepId, String agentId, String priority, String metaPath, String payloadPath, long nowMs) {}
    public record RetryPolicy(int maxAttempts, long baseBackoffMs, long maxBackoffMs) {}
    public record LeaseGrant(boolean started, long leaseEpoch) {
        public static LeaseGrant granted(long leaseEpoch) {
            return new LeaseGrant(true, leaseEpoch);
        }

        public static LeaseGrant conflict() {
            return new LeaseGrant(false, 0L);
        }
    }
    public record ReclaimSummary(int reclaimed, int retryScheduled, int deadLettered) {}
    public record NodeStateTransition(int suspected, int dead) {}
    public record HeartbeatOutcome(boolean updated, String previousStatus, String currentStatus, boolean recoverySuppressed) {}
    public record MeshSnapshot(
            int totalNodes,
            int aliveNodes,
            int suspectNodes,
            int deadNodes,
            int runningByAliveOwners,
            int runningBySuspectOwners,
            int runningByDeadOwners,
            int runningByUnknownOwners,
            long oldestDeadNodeAgeMs
    ) {}
    public record TaskDelta(
            String taskId,
            String status,
            String idempotencyKey,
            String traceId,
            String resultPayload,
            String lastError,
            long createdAtMs,
            long updatedAtMs
    ) {}
    public record StepDelta(
            String stepId,
            String taskId,
            String agentId,
            String priority,
            String payloadPath,
            String payloadText,
            boolean enqueued,
            String resultPayload,
            String status,
            int attempt,
            Long nextRetryAtMs,
            String leaseOwner,
            String leaseToken,
            long leaseEpoch,
            List<String> dependsOn,
            long createdAtMs,
            long updatedAtMs
    ) {}
    public record MessageDelta(
            String msgId,
            String taskId,
            String stepId,
            String fromAgent,
            String toAgent,
            String priority,
            String state,
            String metaPath,
            String payloadPath,
            long createdAtMs,
            long updatedAtMs
    ) {}
    public record ReplicationApplySummary(
            int taskApplied,
            int stepApplied,
            int messageApplied,
            int staleSkipped,
            int tieResolved,
            int tieKeptLocal
    ) {}
    public record RuntimeStats(Map<String, Integer> taskStatus, Map<String, Integer> stepStatus, Map<String, Integer> messageState) {}
    public record SlaSnapshot(long queueLagMs, long stepLatencyP95Ms, long stepLatencyP99Ms, int deadLetterGrowth1h) {}
    public record LeaseConflictSummary(int total, int last1h, Map<String, Integer> byEventType) {}
    public record LeaseConflict(
            long id,
            String eventType,
            String taskId,
            String stepId,
            String workerId,
            String expectedStatus,
            String expectedLeaseToken,
            Long expectedLeaseEpoch,
            String actualStatus,
            String actualLeaseOwner,
            String actualLeaseToken,
            Long actualLeaseEpoch,
            long occurredAtMs
    ) {}
    public record MeshNode(String nodeId, String status, long lastHeartbeatMs, long updatedAtMs) {}
    public record LeaseSnapshot(String stepId, String status, String leaseOwner, String leaseToken, long leaseEpoch, long updatedAtMs) {}
    public record ClusterStateValue(String key, String value, long version, long updatedAtMs) {}
    public record DeadLetterTask(String taskId, String stepId, String agentId, String priority, String payloadPath, String idempotencyKey, String status) {}
    public record CancelResult(String taskId, boolean cancelled, String mode, String message, int stepRows, int messageRows) {}
    public record TaskSummary(String taskId, String status, String traceId, String lastError, long createdAtMs, long updatedAtMs) {}
    public record TaskHeader(String taskId, String status, String traceId, long createdAtMs, long updatedAtMs) {}
    public record StepDetail(String stepId, String agentId, String status, int attempt, String priority, boolean enqueued,
                             List<String> dependsOn, long createdAtMs, long updatedAtMs) {}
    public record WorkflowDetail(TaskHeader task, List<StepDetail> steps) {}
    private record StepLeaseState(String status, String leaseOwner, String leaseToken, long leaseEpoch) {}
    private record ReclaimCandidate(String taskId, String stepId, String agentId, String idempotencyKey, int attempt,
                                    long leaseEpoch, String leaseOwner) {}
    private record StepReplicationRow(String status, String leaseOwner, long leaseEpoch, long updatedAtMs) {}
    private enum StepTieDecision {
        INCOMING_WINS,
        LOCAL_WINS
    }
}
