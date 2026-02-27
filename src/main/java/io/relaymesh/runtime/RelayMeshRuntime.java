package io.relaymesh.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.relaymesh.agent.Agent;
import io.relaymesh.agent.AgentContext;
import io.relaymesh.agent.AgentRegistry;
import io.relaymesh.agent.AgentResult;
import io.relaymesh.agent.EchoAgent;
import io.relaymesh.agent.FailAgent;
import io.relaymesh.agent.ScriptAgent;
import io.relaymesh.bus.FileBus;
import io.relaymesh.config.RelayMeshConfig;
import io.relaymesh.model.MessageEnvelope;
import io.relaymesh.model.MessageState;
import io.relaymesh.model.Priority;
import io.relaymesh.model.TaskStatus;
import io.relaymesh.model.TaskView;
import io.relaymesh.observability.AuditLogger;
import io.relaymesh.observability.OtelTraceExporter;
import io.relaymesh.observability.PrometheusFormatter;
import io.relaymesh.observability.TraceContextUtil;
import io.relaymesh.security.PayloadCrypto;
import io.relaymesh.storage.Database;
import io.relaymesh.storage.TaskStore;
import io.relaymesh.util.Hashing;
import io.relaymesh.util.Jsons;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Collections;
import java.util.Locale;
import java.util.stream.Stream;
import java.sql.Connection;
import java.security.SecureRandom;

public final class RelayMeshRuntime {
    private static final Duration IDEMPOTENCY_TTL = Duration.ofDays(7);
    private static final String REPLICATION_SCHEMA_VERSION = "relaymesh.replication.v1";
    private static final ObjectMapper COMPACT_JSON = new ObjectMapper().findAndRegisterModules();

    private final RelayMeshConfig config;
    private final Database database;
    private final TaskStore taskStore;
    private final FileBus fileBus;
    private final AgentRegistry agentRegistry;
    private final ConcurrentMap<String, LeaseWatch> activeLeases;
    private final ConcurrentMap<String, TaskStore.RetryPolicy> retryPolicyByAgent;
    private final ConcurrentMap<String, Long> gossipDedupCache;
    private final AtomicLong antiEntropyMergeTotal;
    private final AtomicLong webWriteGetCompatTotal;
    private final AtomicLong webWriteRateLimitedTotal;
    private final AtomicLong deadOwnerReclaimedTotal;
    private final AtomicLong membershipRecoverySuppressedTotal;
    private final AtomicLong meshNodesPrunedTotal;
    private final AtomicLong workerPausedTotal;
    private final AtomicLong replicationImportTotal;
    private final AtomicLong replicationTieResolvedTotal;
    private final AtomicLong replicationTieKeptLocalTotal;
    private final AtomicLong replicationLastImportLagMs;
    private final AtomicLong replicationSyncTotal;
    private final AtomicLong sloAlertFireTotal;
    private final AtomicLong submitRejectedTotal;
    private final AtomicLong submitWindowCount;
    private final AuditLogger auditLogger;
    private final String auditSigningSecret;
    private final Object leaseReportLock;
    private final Object gossipDedupLock;
    private final Object submitRateLock;
    private volatile long lastLeaseReportBucket;
    private volatile long lastGossipDedupCleanupMs;
    private volatile long runtimeSettingsFileMtimeMs;
    private volatile long lastRuntimeSettingsCheckMs;
    private volatile long lastMeshPruneAtMs;
    private volatile long lastReplicationSyncAtMs;
    private volatile long lastDrSnapshotAtMs;
    private volatile long lastDrDrillAtMs;
    private volatile long submitWindowStartMs;
    private volatile RuntimeSettings runtimeSettings;

    public RelayMeshRuntime(RelayMeshConfig config) {
        this.config = config;
        this.database = new Database(config);
        this.taskStore = new TaskStore(database);
        this.fileBus = new FileBus(config);
        this.agentRegistry = new AgentRegistry();
        this.activeLeases = new ConcurrentHashMap<>();
        this.retryPolicyByAgent = new ConcurrentHashMap<>();
        this.gossipDedupCache = new ConcurrentHashMap<>();
        this.antiEntropyMergeTotal = new AtomicLong(0L);
        this.webWriteGetCompatTotal = new AtomicLong(0L);
        this.webWriteRateLimitedTotal = new AtomicLong(0L);
        this.deadOwnerReclaimedTotal = new AtomicLong(0L);
        this.membershipRecoverySuppressedTotal = new AtomicLong(0L);
        this.meshNodesPrunedTotal = new AtomicLong(0L);
        this.workerPausedTotal = new AtomicLong(0L);
        this.replicationImportTotal = new AtomicLong(0L);
        this.replicationTieResolvedTotal = new AtomicLong(0L);
        this.replicationTieKeptLocalTotal = new AtomicLong(0L);
        this.replicationLastImportLagMs = new AtomicLong(0L);
        this.replicationSyncTotal = new AtomicLong(0L);
        this.sloAlertFireTotal = new AtomicLong(0L);
        this.submitRejectedTotal = new AtomicLong(0L);
        this.submitWindowCount = new AtomicLong(0L);
        this.auditSigningSecret = loadOrCreateAuditSigningSecret(config.securityRoot().resolve("audit-signing.key"));
        this.auditLogger = new AuditLogger(
                config.auditRoot().resolve("audit.log"),
                config.namespace(),
                auditSigningSecret,
                new OtelTraceExporter(config.traceRoot().resolve("otel-spans.jsonl"), "relaymesh", config.namespace())
        );
        this.leaseReportLock = new Object();
        this.gossipDedupLock = new Object();
        this.submitRateLock = new Object();
        this.lastLeaseReportBucket = -1L;
        this.lastGossipDedupCleanupMs = 0L;
        this.runtimeSettingsFileMtimeMs = Long.MIN_VALUE;
        this.lastRuntimeSettingsCheckMs = 0L;
        this.lastMeshPruneAtMs = 0L;
        this.lastReplicationSyncAtMs = 0L;
        this.lastDrSnapshotAtMs = 0L;
        this.lastDrDrillAtMs = 0L;
        this.submitWindowStartMs = 0L;
        this.runtimeSettings = RuntimeSettings.defaults();
        registerDefaultAgents();
    }

    public void init() {
        database.init();
        loadRuntimeSettings(true);
        loadRetryPolicies();
        registerConfiguredScriptAgents();
        // Run startup TTL cleanup to avoid long-offline accumulation.
        long cutoff = Instant.now().minus(IDEMPOTENCY_TTL).toEpochMilli();
        taskStore.purgeIdempotencyOlderThan(cutoff);
    }

    public SettingsReloadOutcome reloadSettings() {
        SettingsReloadOutcome out = loadRuntimeSettings(true);
        loadRetryPolicies();
        return out;
    }

    public RuntimeSettingsView currentSettings() {
        return runtimeSettings.toView();
    }

    public SettingsReloadOutcome maybeReloadSettings(long minIntervalMs) {
        long nowMs = Instant.now().toEpochMilli();
        long interval = Math.max(1_000L, minIntervalMs);
        if ((nowMs - lastRuntimeSettingsCheckMs) < interval) {
            RuntimeSettings settings = runtimeSettings;
            return new SettingsReloadOutcome(
                    false,
                    runtimeSettingsFileMtimeMs >= 0L,
                    config.rootDir().resolve("relaymesh-settings.json").toString(),
                    settings.toView(),
                    "skip_interval",
                    nowMs,
                    List.of()
            );
        }
        lastRuntimeSettingsCheckMs = nowMs;
        return loadRuntimeSettings(false);
    }

    public SubmitOutcome submit(String agentId, String input, String priorityRaw, String idempotencyKey) {
        Agent agent = agentRegistry.findById(agentId)
                .orElseThrow(() -> new IllegalArgumentException("Unknown agent: " + agentId));

        long payloadBytes = input.getBytes(StandardCharsets.UTF_8).length;
        if (payloadBytes > RelayMeshConfig.DEFAULT_PAYLOAD_MAX_BYTES) {
            throw new IllegalArgumentException(
                    "Payload too large: " + payloadBytes + " bytes, max=" + RelayMeshConfig.DEFAULT_PAYLOAD_MAX_BYTES
            );
        }
        enforceAdmissionGate("submit", priorityRaw, 1);

        String canonical = agent.id() + "\n" + input;
        String resolvedIdempotencyKey = (idempotencyKey == null || idempotencyKey.isBlank())
                ? Hashing.sha256Hex(canonical)
                : idempotencyKey;

        String taskId = "tsk_" + UUID.randomUUID();
        String stepId = "stp_" + UUID.randomUUID();
        String msgId = "msg_" + UUID.randomUUID();
        String traceId = TraceContextUtil.newTraceId();
        long now = Instant.now().toEpochMilli();
        Priority priority = Priority.fromString(priorityRaw);

        Path payloadPath = config.payloadDir().resolve(msgId + ".payload.json");
        Path metaPath = config.metaDir().resolve(msgId + ".meta.json");

        TaskStore.Submission submission = new TaskStore.Submission(
                taskId,
                stepId,
                msgId,
                agent.id(),
                resolvedIdempotencyKey,
                "principal:cli",
                traceId,
                priority.name(),
                metaPath.toString(),
                payloadPath.toString(),
                now
        );

        TaskStore.SubmitResult submitResult = taskStore.submitTask(submission);
        if (submitResult.deduplicated()) {
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "task.submit",
                    "cli",
                    "task/" + submitResult.taskId(),
                    "deduplicated",
                    traceId,
                    null,
                    submitResult.taskId(),
                    stepId,
                    Map.of("agent", agent.id(), "priority", priority.name())
            ));
            return new SubmitOutcome(submitResult.taskId(), true, "Task deduplicated by idempotency key");
        }

        MessageEnvelope envelope = new MessageEnvelope(
                msgId,
                taskId,
                stepId,
                "system",
                agent.id(),
                priority.name(),
                now,
                payloadPath.toString(),
                1
        );
        fileBus.enqueue(envelope, input);
        auditLogger.log(AuditLogger.AuditEvent.of(
                "task.submit",
                "cli",
                "task/" + taskId,
                "submitted",
                traceId,
                null,
                taskId,
                stepId,
                Map.of("agent", agent.id(), "priority", priority.name(), "msg_id", msgId)
        ));
        return new SubmitOutcome(taskId, false, "Task submitted");
    }

    public SubmitOutcome submitWorkflow(List<WorkflowStepSpec> stepSpecs, String idempotencyKey) {
        if (stepSpecs == null || stepSpecs.isEmpty()) {
            throw new IllegalArgumentException("Workflow must contain at least one step");
        }
        validateWorkflow(stepSpecs);
        enforceAdmissionGate("submit_workflow", null, stepSpecs.size());

        String canonical = Jsons.toJson(stepSpecs);
        String resolvedIdempotencyKey = (idempotencyKey == null || idempotencyKey.isBlank())
                ? Hashing.sha256Hex(canonical)
                : idempotencyKey;

        String taskId = "tsk_" + UUID.randomUUID();
        String traceId = TraceContextUtil.newTraceId();
        long now = Instant.now().toEpochMilli();

        Map<String, String> rawToInternalId = new HashMap<>();
        for (WorkflowStepSpec step : stepSpecs) {
            rawToInternalId.put(step.id(), "stp_" + taskId + "_" + step.id());
        }

        List<TaskStore.WorkflowStep> wfSteps = new ArrayList<>();
        for (WorkflowStepSpec step : stepSpecs) {
            Agent agent = agentRegistry.findById(step.agent())
                    .orElseThrow(() -> new IllegalArgumentException("Unknown agent: " + step.agent()));
            long payloadBytes = step.input().getBytes(StandardCharsets.UTF_8).length;
            if (payloadBytes > RelayMeshConfig.DEFAULT_PAYLOAD_MAX_BYTES) {
                throw new IllegalArgumentException("Payload too large in step " + step.id() + ": " + payloadBytes);
            }
            String internalStepId = rawToInternalId.get(step.id());
            Path payloadPath = config.payloadDir().resolve(internalStepId + ".payload.json");
            try {
                Files.writeString(payloadPath, step.input(), StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new RuntimeException("Failed to write workflow payload for step: " + step.id(), e);
            }
            List<String> dependsOnInternal = step.dependsOn().stream().map(rawToInternalId::get).toList();
            wfSteps.add(new TaskStore.WorkflowStep(
                    internalStepId,
                    agent.id(),
                    Priority.fromString(step.priority()).name(),
                    payloadPath.toString(),
                    dependsOnInternal
            ));
        }

        TaskStore.WorkflowSubmission wfSubmission = new TaskStore.WorkflowSubmission(
                taskId,
                traceId,
                resolvedIdempotencyKey,
                "principal:cli",
                wfSteps,
                now
        );
        TaskStore.WorkflowSubmitResult submitResult = taskStore.submitWorkflow(wfSubmission);
        if (submitResult.deduplicated()) {
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "workflow.submit",
                    "cli",
                    "task/" + submitResult.taskId(),
                    "deduplicated",
                    traceId,
                    null,
                    submitResult.taskId(),
                    null,
                    Map.of("step_count", stepSpecs.size())
            ));
            return new SubmitOutcome(submitResult.taskId(), true, "Workflow deduplicated by idempotency key");
        }
        dispatchReadySteps(submitResult.readySteps(), now);
        auditLogger.log(AuditLogger.AuditEvent.of(
                "workflow.submit",
                "cli",
                "task/" + taskId,
                "submitted",
                traceId,
                null,
                taskId,
                null,
                Map.of("step_count", stepSpecs.size(), "ready_steps", submitResult.readySteps().size())
        ));
        return new SubmitOutcome(taskId, false, "Workflow submitted");
    }

    public MaintenanceOutcome runMaintenance() {
        return runMaintenance("node-local");
    }

    public MaintenanceOutcome runMaintenance(String nodeId) {
        RuntimeSettings settings = runtimeSettings;
        long nowMs = Instant.now().toEpochMilli();
        long clusterEpoch = parseLongOrDefault(taskStore.clusterEpoch().value(), 1L);
        TaskStore.HeartbeatOutcome localHeartbeat = taskStore.heartbeatNode(
                nodeId,
                nowMs,
                settings.suspectRecoverMinMs(),
                settings.deadRecoverMinMs()
        );
        if (localHeartbeat.recoverySuppressed()) {
            membershipRecoverySuppressedTotal.incrementAndGet();
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "membership.recovery.suppressed",
                    "maintenance",
                    "node/" + nodeId,
                    "suppressed",
                    null,
                    null,
                    null,
                    null,
                    Map.of(
                            "previous_status", localHeartbeat.previousStatus(),
                            "current_status", localHeartbeat.currentStatus()
                    )
            ));
        }
        long suspectBeforeMs = nowMs - settings.suspectAfterMs();
        long deadBeforeMs = nowMs - settings.deadAfterMs();
        TaskStore.NodeStateTransition nodeTransition = taskStore.reconcileNodeStates(nowMs, suspectBeforeMs, deadBeforeMs);
        int suspectedNodesMarked = nodeTransition.suspected();
        int deadNodesMarked = nodeTransition.dead();
        long nowMonoNs = System.nanoTime();
        int heartbeats = 0;
        int monotonicReclaimed = 0;
        int retryScheduledFromMonotonic = 0;
        int deadFromMonotonic = 0;

        for (LeaseWatch lease : activeLeases.values()) {
            boolean heartbeated = taskStore.heartbeatLease(lease.stepId(), lease.leaseToken(), lease.leaseEpoch(), nowMs);
            if (heartbeated) {
                heartbeats++;
            }
            long elapsedNs = nowMonoNs - lease.lastHeartbeatMonoNs();
            if (elapsedNs <= Duration.ofMillis(settings.leaseTimeoutMs()).toNanos()) {
                continue;
            }
            Optional<TaskStore.StepDispatchContext> maybeContext = taskStore.getStepDispatchContext(lease.stepId());
            if (maybeContext.isEmpty()) {
                activeLeases.remove(lease.stepId(), lease);
                continue;
            }
            TaskStore.StepDispatchContext context = maybeContext.get();
            TaskStore.RetryPolicy policy = retryPolicyForAgent(context.agentId());
            TaskStore.FailureResolution resolution = taskStore.tryCompleteFailureWithLease(
                    context.taskId(),
                    context.stepId(),
                    context.idempotencyKey(),
                    lease.leaseToken(),
                    lease.leaseEpoch(),
                    clusterEpoch,
                    "lease timeout (monotonic reclaim)",
                    nowMs,
                    policy.maxAttempts(),
                    policy.baseBackoffMs(),
                    policy.maxBackoffMs()
            );
            if (resolution.outcome() == TaskStore.FailureOutcome.STALE_LEASE) {
                auditLogger.log(AuditLogger.AuditEvent.of(
                        "lease.conflict",
                        "maintenance",
                        "step/" + context.stepId(),
                        "reclaim_conflict",
                        context.traceId(),
                        null,
                        context.taskId(),
                        context.stepId(),
                        Map.of("reason", "monotonic_reclaim")
                ));
            }
            if (resolution.outcome() != TaskStore.FailureOutcome.STALE_LEASE) {
                monotonicReclaimed++;
                if (resolution.outcome() == TaskStore.FailureOutcome.RETRY_SCHEDULED) {
                    retryScheduledFromMonotonic++;
                } else if (resolution.outcome() == TaskStore.FailureOutcome.DEAD_LETTERED) {
                    deadFromMonotonic++;
                }
            }
            activeLeases.remove(lease.stepId(), lease);
        }

        DeadOwnerRecoveryOutcome deadOwnerRecovery = recoverDeadOwnerLeasesInternal(settings.retryDispatchLimit(), nowMs, clusterEpoch);
        int deadNodesPruned = maybeAutoPruneDeadNodes(settings, nowMs);
        TaskStore.ReclaimSummary wallReclaim = taskStore.reclaimExpiredRunning(
                nowMs,
                nowMs - settings.leaseTimeoutMs(),
                settings.maxAttempts(),
                settings.baseBackoffMs(),
                settings.maxBackoffMs(),
                settings.retryDispatchLimit(),
                Map.copyOf(retryPolicyByAgent),
                clusterEpoch
        );

        int dispatchedRetryMessages = dispatchDueRetries(settings.retryDispatchLimit());
        LeaseReportOutcome leaseReport = maybeWriteLeaseConflictReport(nowMs);
        MaintenanceOutcome out = new MaintenanceOutcome(
                nodeId,
                suspectedNodesMarked,
                deadNodesMarked,
                heartbeats,
                monotonicReclaimed,
                retryScheduledFromMonotonic,
                deadFromMonotonic,
                deadOwnerRecovery.reclaimed(),
                deadOwnerRecovery.retryScheduled(),
                deadOwnerRecovery.deadLettered(),
                wallReclaim.reclaimed(),
                wallReclaim.retryScheduled(),
                wallReclaim.deadLettered(),
                dispatchedRetryMessages,
                deadNodesPruned,
                leaseReport.generated()
        );
        if (out.wallReclaimed() > 0
                || out.monotonicReclaimed() > 0
                || out.deadOwnerReclaimed() > 0
                || out.retryMessagesDispatched() > 0
                || out.suspectedNodesMarked() > 0
                || out.deadNodesMarked() > 0) {
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "maintenance.tick",
                    "system",
                    "runtime/maintenance",
                    "ok",
                    null,
                    null,
                    null,
                    null,
                    Map.ofEntries(
                            Map.entry("lease_heartbeats", out.leaseHeartbeats()),
                            Map.entry("monotonic_reclaimed", out.monotonicReclaimed()),
                            Map.entry("dead_owner_reclaimed", out.deadOwnerReclaimed()),
                            Map.entry("dead_owner_retry_scheduled", out.deadOwnerRetryScheduled()),
                            Map.entry("dead_owner_dead_lettered", out.deadOwnerDeadLettered()),
                            Map.entry("wall_reclaimed", out.wallReclaimed()),
                            Map.entry("retry_dispatched", out.retryMessagesDispatched()),
                            Map.entry("mesh_nodes_pruned", out.deadNodesPruned()),
                            Map.entry("node_id", out.nodeId()),
                            Map.entry("suspected_nodes_marked", out.suspectedNodesMarked()),
                            Map.entry("dead_nodes_marked", out.deadNodesMarked()),
                            Map.entry("lease_report_generated", out.leaseReportGenerated())
                    )
            ));
        }
        return out;
    }

    private int maybeAutoPruneDeadNodes(RuntimeSettings settings, long nowMs) {
        long intervalMs = settings.meshPruneIntervalMs();
        if (intervalMs <= 0L) {
            return 0;
        }
        long last = lastMeshPruneAtMs;
        if (last > 0L && (nowMs - last) < intervalMs) {
            return 0;
        }
        long olderThanMs = settings.meshPruneOlderThanMs();
        int limit = settings.meshPruneLimit();
        MeshPruneOutcome out = pruneDeadNodesByAgeInternal(olderThanMs, limit, nowMs, "maintenance", false);
        lastMeshPruneAtMs = nowMs;
        return out.pruned();
    }

    public DeadOwnerRecoveryOutcome recoverDeadOwnerLeases(int limit) {
        DeadOwnerRecoveryOutcome out = recoverDeadOwnerLeasesInternal(
                limit,
                Instant.now().toEpochMilli(),
                parseLongOrDefault(taskStore.clusterEpoch().value(), 1L)
        );
        auditLogger.log(AuditLogger.AuditEvent.of(
                "ownership.recover",
                "cli",
                "runtime/ownership",
                out.reclaimed() > 0 ? "recovered" : "noop",
                null,
                null,
                null,
                null,
                Map.of(
                        "limit", Math.max(1, limit),
                        "dead_nodes", out.deadNodes(),
                        "reclaimed", out.reclaimed(),
                        "retry_scheduled", out.retryScheduled(),
                        "dead_lettered", out.deadLettered()
                )
        ));
        return out;
    }

    public MeshPruneOutcome pruneDeadNodesByAge(long olderThanMs, int limit) {
        long nowMs = Instant.now().toEpochMilli();
        return pruneDeadNodesByAgeInternal(olderThanMs, limit, nowMs, "cli", true);
    }

    private MeshPruneOutcome pruneDeadNodesByAgeInternal(
            long olderThanMs,
            int limit,
            long nowMs,
            String actor,
            boolean logNoop
    ) {
        long safeAgeMs = Math.max(0L, olderThanMs);
        int safeLimit = Math.max(1, limit);
        long cutoffMs = nowMs - safeAgeMs;
        int pruned = taskStore.pruneDeadNodes(cutoffMs, safeLimit);
        long total = meshNodesPrunedTotal.addAndGet(pruned);
        MeshPruneOutcome out = new MeshPruneOutcome(
                safeAgeMs,
                cutoffMs,
                safeLimit,
                pruned,
                total
        );
        if (pruned > 0 || logNoop) {
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "membership.prune",
                    actor,
                    "runtime/membership",
                    pruned > 0 ? "pruned" : "noop",
                    null,
                    null,
                    null,
                    null,
                    Map.of(
                            "older_than_ms", safeAgeMs,
                            "cutoff_ms", cutoffMs,
                            "limit", safeLimit,
                            "pruned", pruned,
                            "total", total
                    )
            ));
        }
        return out;
    }

    private DeadOwnerRecoveryOutcome recoverDeadOwnerLeasesInternal(int limit, long nowMs, long clusterEpoch) {
        int safeLimit = Math.max(1, limit);
        List<String> deadNodes = taskStore.listNodeIdsByStatus("DEAD", safeLimit);
        if (deadNodes.isEmpty()) {
            return new DeadOwnerRecoveryOutcome(0, List.of(), 0, 0, 0);
        }
        TaskStore.ReclaimSummary summary = taskStore.reclaimRunningByLeaseOwners(
                Set.copyOf(deadNodes),
                nowMs,
                runtimeSettings.maxAttempts(),
                runtimeSettings.baseBackoffMs(),
                runtimeSettings.maxBackoffMs(),
                safeLimit,
                Map.copyOf(retryPolicyByAgent),
                clusterEpoch
        );
        if (summary.reclaimed() > 0) {
            deadOwnerReclaimedTotal.addAndGet(summary.reclaimed());
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "ownership.recover.dead_node",
                    "maintenance",
                    "runtime/ownership",
                    "recovered",
                    null,
                    null,
                    null,
                    null,
                    Map.of(
                            "dead_nodes", deadNodes,
                            "reclaimed", summary.reclaimed(),
                            "retry_scheduled", summary.retryScheduled(),
                            "dead_lettered", summary.deadLettered()
                    )
            ));
        }
        return new DeadOwnerRecoveryOutcome(
                deadNodes.size(),
                deadNodes,
                summary.reclaimed(),
                summary.retryScheduled(),
                summary.deadLettered()
        );
    }

    public WorkerOutcome runWorkerOnce(String workerId) {
        return runWorkerOnce(workerId, workerId);
    }

    public WorkerOutcome runWorkerOnce(String workerId, String nodeId) {
        String leaseOwner = (nodeId == null || nodeId.isBlank()) ? workerId : nodeId.trim();
        RuntimeSettings settings = runtimeSettings;
        if (settings.pauseWorkerWhenLocalNotAlive()) {
            Optional<String> localStatus = taskStore.nodeStatus(leaseOwner);
            if (localStatus.isPresent()) {
                String status = localStatus.get();
                if ("SUSPECT".equals(status) || "DEAD".equals(status)) {
                    long total = workerPausedTotal.incrementAndGet();
                    auditLogger.log(AuditLogger.AuditEvent.of(
                            "worker.pause.local_status",
                            workerId,
                            "node/" + leaseOwner,
                            "paused",
                            null,
                            null,
                            null,
                            null,
                            Map.of(
                                    "node_id", leaseOwner,
                                    "status", status,
                                    "total", total
                            )
                    ));
                    return new WorkerOutcome(false, null, null, "Worker paused: local node status is " + status);
                }
            }
        }
        Optional<FileBus.ClaimedMessage> maybe = fileBus.claimNext(workerId);
        if (maybe.isEmpty()) {
            return new WorkerOutcome(false, null, null, "No queued messages");
        }

        FileBus.ClaimedMessage claimed = maybe.get();
        MessageEnvelope envelope = claimed.envelope();
        long now = Instant.now().toEpochMilli();
        long clusterEpoch = parseLongOrDefault(taskStore.clusterEpoch().value(), 1L);
        String leaseToken = "lease_" + UUID.randomUUID();
        String taskId = envelope.taskId();
        String stepId = envelope.stepId();
        String msgId = envelope.msgId();

        Optional<TaskStore.StepDispatchContext> maybeContext = taskStore.getStepDispatchContext(stepId);
        if (maybeContext.isEmpty()) {
            fileBus.completeFailure(claimed);
            taskStore.markMessageState(msgId, MessageState.DEAD, now);
            return new WorkerOutcome(true, taskId, msgId, "Step not found");
        }
        TaskStore.StepDispatchContext context = maybeContext.get();
        TaskStore.RetryPolicy retryPolicy = retryPolicyForAgent(context.agentId());
        String spanId = TraceContextUtil.newSpanId();
        String traceParent = TraceContextUtil.toTraceParent(context.traceId(), spanId);

        taskStore.markMessageState(msgId, MessageState.PROCESSING, now);
        TaskStore.LeaseGrant leaseGrant = taskStore.tryMarkTaskRunning(taskId, stepId, leaseOwner, leaseToken, now, clusterEpoch);
        if (!leaseGrant.started()) {
            fileBus.completeFailure(claimed);
            taskStore.markMessageState(msgId, MessageState.DEAD, Instant.now().toEpochMilli());
            Optional<TaskStore.LeaseSnapshot> snapshot = taskStore.stepLeaseSnapshot(stepId);
            String currentOwner = snapshot.map(TaskStore.LeaseSnapshot::leaseOwner).orElse("");
            String winnerOwner = decideOwnershipWinner(leaseOwner, currentOwner);
            String rule = (currentOwner == null || currentOwner.isBlank()) ? "lexical_owner" : "running_owner_wins";
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "lease.conflict",
                    workerId,
                    "step/" + stepId,
                    "claim_conflict",
                    context.traceId(),
                    spanId,
                    taskId,
                    stepId,
                    Map.of("msg_id", msgId, "cluster_epoch", clusterEpoch)
            ));
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "ownership.conflict.decision",
                    workerId,
                    "step/" + stepId,
                    "decided",
                    context.traceId(),
                    spanId,
                    taskId,
                    stepId,
                    Map.of(
                            "msg_id", msgId,
                            "cluster_epoch", clusterEpoch,
                            "candidate_owner", leaseOwner,
                            "current_owner", currentOwner == null ? "" : currentOwner,
                            "winner_owner", winnerOwner,
                            "rule", rule,
                            "actual_status", snapshot.map(TaskStore.LeaseSnapshot::status).orElse(""),
                            "actual_lease_epoch", snapshot.map(TaskStore.LeaseSnapshot::leaseEpoch).orElse(-1L)
                    )
            ));
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "worker.claim",
                    workerId,
                    "step/" + stepId,
                    "stale",
                    context.traceId(),
                    spanId,
                    taskId,
                    stepId,
                    Map.of("msg_id", msgId, "cluster_epoch", clusterEpoch)
            ));
            return new WorkerOutcome(true, taskId, msgId, "Step already taken by another worker");
        }

        long leaseEpoch = leaseGrant.leaseEpoch();
        LeaseWatch leaseWatch = new LeaseWatch(taskId, stepId, leaseToken, leaseEpoch, System.nanoTime());
        activeLeases.put(stepId, leaseWatch);
        Agent agent = agentRegistry.findById(envelope.toAgent()).orElse(null);
        if (agent == null) {
            activeLeases.remove(stepId, leaseWatch);
            TaskStore.FailureResolution resolution = taskStore.tryCompleteFailureWithLease(
                    taskId,
                    stepId,
                    context.idempotencyKey(),
                    leaseToken,
                    leaseEpoch,
                    clusterEpoch,
                    "Target agent not found: " + envelope.toAgent(),
                    Instant.now().toEpochMilli(),
                    retryPolicy.maxAttempts(),
                    retryPolicy.baseBackoffMs(),
                    retryPolicy.maxBackoffMs()
            );
            applyFailureDisposition(claimed, msgId, resolution);
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "step.execute",
                    workerId,
                    "step/" + stepId,
                    "agent_not_found",
                    context.traceId(),
                    spanId,
                    taskId,
                    stepId,
                    Map.of("msg_id", msgId, "lease_epoch", leaseEpoch, "cluster_epoch", clusterEpoch, "agent", envelope.toAgent())
            ));
            return new WorkerOutcome(true, taskId, msgId, "Target agent not found");
        }

        try {
            String payload = fileBus.readPayload(context.payloadPath());
            AgentResult result = agent.execute(new AgentContext(taskId, stepId, context.traceId(), spanId, traceParent, payload));
            if (result.success()) {
                TaskStore.StepSuccessResolution success = taskStore.tryMarkStepSuccessWithLease(
                        taskId,
                        stepId,
                        context.idempotencyKey(),
                        leaseToken,
                        leaseEpoch,
                        clusterEpoch,
                        result.output(),
                        Instant.now().toEpochMilli()
                );
                if (success.outcome() != TaskStore.StepSuccessOutcome.STALE_LEASE) {
                    taskStore.markMessageState(msgId, MessageState.DONE, Instant.now().toEpochMilli());
                    fileBus.completeSuccess(claimed);
                    dispatchReadySteps(success.readySteps(), Instant.now().toEpochMilli());
                    auditLogger.log(AuditLogger.AuditEvent.of(
                            "step.execute",
                            workerId,
                            "step/" + stepId,
                            success.outcome() == TaskStore.StepSuccessOutcome.TASK_COMPLETED ? "task_completed" : "step_completed",
                            context.traceId(),
                            spanId,
                            taskId,
                            stepId,
                            Map.of("msg_id", msgId, "lease_epoch", leaseEpoch, "cluster_epoch", clusterEpoch, "ready_steps", success.readySteps().size())
                    ));
                    if (success.outcome() == TaskStore.StepSuccessOutcome.TASK_COMPLETED) {
                        return new WorkerOutcome(true, taskId, msgId, "Task executed successfully");
                    }
                    return new WorkerOutcome(true, taskId, msgId, "Step executed successfully");
                }
                taskStore.markMessageState(msgId, MessageState.DEAD, Instant.now().toEpochMilli());
                fileBus.completeFailure(claimed);
                auditLogger.log(AuditLogger.AuditEvent.of(
                        "lease.conflict",
                        workerId,
                        "step/" + stepId,
                        "success_commit_conflict",
                        context.traceId(),
                        spanId,
                        taskId,
                        stepId,
                        Map.of("msg_id", msgId, "lease_epoch", leaseEpoch, "cluster_epoch", clusterEpoch)
                ));
                auditLogger.log(AuditLogger.AuditEvent.of(
                        "step.execute",
                        workerId,
                        "step/" + stepId,
                        "stale_lease_on_success",
                        context.traceId(),
                        spanId,
                        taskId,
                        stepId,
                        Map.of("msg_id", msgId, "lease_epoch", leaseEpoch, "cluster_epoch", clusterEpoch)
                ));
                return new WorkerOutcome(true, taskId, msgId, "Lease lost before success commit");
            }

            TaskStore.FailureResolution resolution = taskStore.tryCompleteFailureWithLease(
                    taskId,
                    stepId,
                    context.idempotencyKey(),
                    leaseToken,
                    leaseEpoch,
                    clusterEpoch,
                    result.error(),
                    Instant.now().toEpochMilli(),
                    retryPolicy.maxAttempts(),
                    retryPolicy.baseBackoffMs(),
                    retryPolicy.maxBackoffMs()
            );
            if (resolution.outcome() == TaskStore.FailureOutcome.STALE_LEASE) {
                auditLogger.log(AuditLogger.AuditEvent.of(
                        "lease.conflict",
                        workerId,
                        "step/" + stepId,
                        "failure_commit_conflict",
                        context.traceId(),
                        spanId,
                        taskId,
                        stepId,
                        Map.of("msg_id", msgId, "lease_epoch", leaseEpoch, "cluster_epoch", clusterEpoch)
                ));
            }
            applyFailureDisposition(claimed, msgId, resolution);
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "step.execute",
                    workerId,
                    "step/" + stepId,
                    resolution.outcome().name().toLowerCase(),
                    context.traceId(),
                    spanId,
                    taskId,
                    stepId,
                    Map.of("msg_id", msgId, "lease_epoch", leaseEpoch, "cluster_epoch", clusterEpoch, "attempt", resolution.attempt())
            ));
            return new WorkerOutcome(true, taskId, msgId, workerMessageForFailure(resolution));
        } catch (Exception e) {
            TaskStore.FailureResolution resolution = taskStore.tryCompleteFailureWithLease(
                    taskId,
                    stepId,
                    context.idempotencyKey(),
                    leaseToken,
                    leaseEpoch,
                    clusterEpoch,
                    e.getMessage(),
                    Instant.now().toEpochMilli(),
                    retryPolicy.maxAttempts(),
                    retryPolicy.baseBackoffMs(),
                    retryPolicy.maxBackoffMs()
            );
            if (resolution.outcome() == TaskStore.FailureOutcome.STALE_LEASE) {
                auditLogger.log(AuditLogger.AuditEvent.of(
                        "lease.conflict",
                        workerId,
                        "step/" + stepId,
                        "failure_commit_conflict",
                        context.traceId(),
                        spanId,
                        taskId,
                        stepId,
                        Map.of("msg_id", msgId, "lease_epoch", leaseEpoch, "cluster_epoch", clusterEpoch)
                ));
            }
            applyFailureDisposition(claimed, msgId, resolution);
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "step.execute",
                    workerId,
                    "step/" + stepId,
                    "exception",
                    context.traceId(),
                    spanId,
                    taskId,
                    stepId,
                    Map.of("msg_id", msgId, "lease_epoch", leaseEpoch, "cluster_epoch", clusterEpoch, "error", e.getMessage())
            ));
            return new WorkerOutcome(true, taskId, msgId, "Exception while executing task: " + e.getMessage());
        } finally {
            activeLeases.remove(stepId, leaseWatch);
        }
    }

    public Optional<TaskView> getTask(String taskId) {
        return taskStore.getTask(taskId);
    }

    public List<TaskStore.TaskSummary> tasks(String status, int limit, int offset) {
        List<TaskStore.TaskSummary> rows = taskStore.listTasks(status, limit, offset);
        auditLogger.log(AuditLogger.AuditEvent.of(
                "tasks.list",
                "cli",
                "runtime/tasks",
                "ok",
                null,
                null,
                null,
                null,
                Map.of("status", status == null ? "" : status, "limit", limit, "offset", offset, "count", rows.size())
        ));
        return rows;
    }

    public Optional<TaskStore.WorkflowDetail> workflow(String taskId) {
        Optional<TaskStore.WorkflowDetail> wf = taskStore.getWorkflowDetail(taskId);
        auditLogger.log(AuditLogger.AuditEvent.of(
                "workflow.get",
                "cli",
                "task/" + taskId,
                wf.isPresent() ? "ok" : "not_found",
                null,
                null,
                taskId,
                null,
                Map.of("found", wf.isPresent())
        ));
        return wf;
    }

    public TaskStore.CancelResult cancelTask(String taskId, String reason) {
        return cancelTask(taskId, reason, TaskStore.CancelMode.HARD.name());
    }

    public TaskStore.CancelResult cancelTask(String taskId, String reason, String modeRaw) {
        TaskStore.CancelMode mode = TaskStore.CancelMode.fromString(modeRaw);
        long clusterEpoch = parseLongOrDefault(taskStore.clusterEpoch().value(), 1L);
        TaskStore.CancelResult out = taskStore.cancelTask(taskId, reason, mode, Instant.now().toEpochMilli(), clusterEpoch);
        auditLogger.log(AuditLogger.AuditEvent.of(
                "task.cancel",
                "cli",
                "task/" + taskId,
                out.cancelled() ? "cancelled" : out.message(),
                null,
                null,
                taskId,
                null,
                Map.of("mode", out.mode(), "step_rows", out.stepRows(), "message_rows", out.messageRows(), "cluster_epoch", clusterEpoch)
        ));
        return out;
    }

    public TaskStore.CancelResult cancelTaskViaWeb(
            String taskId,
            String reason,
            String modeRaw,
            Map<String, Object> requestMeta
    ) {
        TaskStore.CancelResult out = cancelTask(taskId, reason, modeRaw);
        String principalId = principalFromRequestMeta(requestMeta);
        Map<String, Object> details = mergeAuditDetails(
                requestMeta,
                Map.of(
                        "mode", out.mode(),
                        "cancelled", out.cancelled(),
                        "step_rows", out.stepRows(),
                        "message_rows", out.messageRows()
                )
        );
        auditLogger.log(AuditLogger.AuditEvent.ofPrincipal(
                "web.write.cancel",
                principalId == null ? "web" : "web:" + principalId,
                principalId,
                "task/" + taskId,
                out.cancelled() ? "ok" : out.message(),
                null,
                null,
                taskId,
                null,
                details
        ));
        return out;
    }

    public ReplayOutcome replayDeadLetter(String taskId) {
        Optional<TaskStore.DeadLetterTask> maybe = taskStore.getDeadLetterTask(taskId);
        if (maybe.isEmpty()) {
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "dead.replay",
                    "cli",
                    "task/" + taskId,
                    "not_found",
                    null,
                    null,
                    taskId,
                    null,
                    Map.of()
            ));
            return new ReplayOutcome(taskId, false, "Task not found");
        }
        TaskStore.DeadLetterTask task = maybe.get();
        if (!TaskStatus.DEAD_LETTER.name().equalsIgnoreCase(task.status())) {
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "dead.replay",
                    "cli",
                    "task/" + taskId,
                    "invalid_state",
                    null,
                    null,
                    taskId,
                    task.stepId(),
                    Map.of("status", task.status())
            ));
            return new ReplayOutcome(taskId, false, "Task is not in DEAD_LETTER state");
        }

        long now = Instant.now().toEpochMilli();
        boolean reset = taskStore.resetDeadLetterTask(task, now);
        if (!reset) {
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "dead.replay",
                    "cli",
                    "task/" + taskId,
                    "stale",
                    null,
                    null,
                    taskId,
                    task.stepId(),
                    Map.of()
            ));
            return new ReplayOutcome(taskId, false, "Replay lost due to concurrent update");
        }

        String msgId = "msg_" + UUID.randomUUID();
        Path metaPath = config.metaDir().resolve(msgId + ".meta.json");
        MessageEnvelope envelope = new MessageEnvelope(
                msgId,
                task.taskId(),
                task.stepId(),
                "system",
                task.agentId(),
                task.priority(),
                now,
                task.payloadPath(),
                1
        );
        TaskStore.RetryMessage replayMessage = new TaskStore.RetryMessage(
                msgId,
                task.taskId(),
                task.stepId(),
                task.agentId(),
                task.priority(),
                metaPath.toString(),
                task.payloadPath(),
                now
        );
        taskStore.insertRetryMessage(replayMessage);
        fileBus.enqueueMetaOnly(envelope);
        auditLogger.log(AuditLogger.AuditEvent.of(
                "dead.replay",
                "cli",
                "task/" + taskId,
                "replayed",
                null,
                null,
                taskId,
                task.stepId(),
                Map.of("msg_id", msgId)
        ));
        return new ReplayOutcome(taskId, true, "Replayed to queue");
    }

    public ReplayOutcome replayDeadLetterViaWeb(String taskId, Map<String, Object> requestMeta) {
        ReplayOutcome out = replayDeadLetter(taskId);
        String principalId = principalFromRequestMeta(requestMeta);
        Map<String, Object> details = mergeAuditDetails(
                requestMeta,
                Map.of("task_id", taskId, "replayed", out.replayed(), "message", out.message())
        );
        auditLogger.log(AuditLogger.AuditEvent.ofPrincipal(
                "web.write.replay",
                principalId == null ? "web" : "web:" + principalId,
                principalId,
                "task/" + taskId,
                out.replayed() ? "ok" : "failed",
                null,
                null,
                taskId,
                null,
                details
        ));
        return out;
    }

    public ReplayBatchOutcome replayDeadLetterBatch(String status, int limit) {
        String normalizedStatus = (status == null || status.isBlank())
                ? TaskStatus.DEAD_LETTER.name()
                : status.trim().toUpperCase(Locale.ROOT);
        int safeLimit = Math.max(1, limit);
        List<TaskStore.TaskSummary> candidates = taskStore.listTasks(normalizedStatus, safeLimit, 0);
        List<ReplayOutcome> details = new ArrayList<>();
        int replayed = 0;
        for (TaskStore.TaskSummary summary : candidates) {
            ReplayOutcome single = replayDeadLetter(summary.taskId());
            details.add(single);
            if (single.replayed()) {
                replayed++;
            }
        }
        int failed = details.size() - replayed;
        ReplayBatchOutcome out = new ReplayBatchOutcome(normalizedStatus, details.size(), replayed, failed, details);
        auditLogger.log(AuditLogger.AuditEvent.of(
                "dead.replay.batch",
                "cli",
                "runtime/dead-letter",
                failed == 0 ? "ok" : "partial",
                null,
                null,
                null,
                null,
                Map.of(
                        "status", normalizedStatus,
                        "limit", safeLimit,
                        "candidates", details.size(),
                        "replayed", replayed,
                        "failed", failed
                )
        ));
        return out;
    }

    public ReplayBatchOutcome replayDeadLetterBatchViaWeb(String status, int limit, Map<String, Object> requestMeta) {
        ReplayBatchOutcome out = replayDeadLetterBatch(status, limit);
        String principalId = principalFromRequestMeta(requestMeta);
        Map<String, Object> details = mergeAuditDetails(
                requestMeta,
                Map.of(
                        "status", out.status(),
                        "total", out.total(),
                        "replayed", out.replayed(),
                        "failed", out.failed()
                )
        );
        auditLogger.log(AuditLogger.AuditEvent.ofPrincipal(
                "web.write.replay_batch",
                principalId == null ? "web" : "web:" + principalId,
                principalId,
                "runtime/dead-letter",
                out.failed() == 0 ? "ok" : "partial",
                null,
                null,
                null,
                null,
                details
        ));
        return out;
    }

    public long markWebGetWriteCompat(String route, Map<String, Object> requestMeta) {
        long total = webWriteGetCompatTotal.incrementAndGet();
        String principalId = principalFromRequestMeta(requestMeta);
        Map<String, Object> details = mergeAuditDetails(
                requestMeta,
                Map.of(
                        "route", route == null ? "" : route,
                        "deprecation", "GET write compatibility is deprecated; use POST",
                        "total", total
                )
        );
        auditLogger.log(AuditLogger.AuditEvent.ofPrincipal(
                "web.write.get_compat",
                principalId == null ? "web" : "web:" + principalId,
                principalId,
                route == null || route.isBlank() ? "runtime/web" : route,
                "deprecated",
                null,
                null,
                null,
                null,
                details
        ));
        return total;
    }

    public long markWebWriteRateLimited(String route, Map<String, Object> requestMeta) {
        long total = webWriteRateLimitedTotal.incrementAndGet();
        String principalId = principalFromRequestMeta(requestMeta);
        Map<String, Object> details = mergeAuditDetails(
                requestMeta,
                Map.of(
                        "route", route == null ? "" : route,
                        "total", total
                )
        );
        auditLogger.log(AuditLogger.AuditEvent.ofPrincipal(
                "web.write.rate_limited",
                principalId == null ? "web" : "web:" + principalId,
                principalId,
                route == null || route.isBlank() ? "runtime/web" : route,
                "rejected",
                null,
                null,
                null,
                null,
                details
        ));
        return total;
    }

    public long markReplicationSyncTick(
            String actor,
            int peers,
            int pulls,
            int pushes,
            int failures,
            List<String> errors
    ) {
        long total = replicationSyncTotal.incrementAndGet();
        auditLogger.log(AuditLogger.AuditEvent.of(
                "replication.sync.tick",
                actor == null || actor.isBlank() ? "replication-controller" : actor,
                "runtime/replication",
                failures > 0 ? "partial" : "ok",
                null,
                null,
                null,
                null,
                Map.of(
                        "namespace", config.namespace(),
                        "peers", Math.max(0, peers),
                        "pulls", Math.max(0, pulls),
                        "pushes", Math.max(0, pushes),
                        "failures", Math.max(0, failures),
                        "errors", errors == null ? List.of() : errors,
                        "total", total
                )
        ));
        return total;
    }

    public AgentRegistry agentRegistry() {
        return agentRegistry;
    }

    public List<TaskStore.MeshNode> members() {
        return taskStore.listNodes();
    }

    public ClusterStateOutcome clusterState() {
        TaskStore.ClusterStateValue row = taskStore.clusterEpoch();
        long epoch = parseLongOrDefault(row.value(), 1L);
        ClusterStateOutcome out = new ClusterStateOutcome(
                row.key(),
                row.value(),
                epoch,
                row.version(),
                row.updatedAtMs()
        );
        auditLogger.log(AuditLogger.AuditEvent.of(
                "cluster.state.get",
                "cli",
                "runtime/cluster-state",
                "ok",
                null,
                null,
                null,
                null,
                Map.of(
                        "key", out.key(),
                        "epoch", out.epoch(),
                        "version", out.version()
                )
        ));
        return out;
    }

    public ClusterEpochBumpOutcome bumpClusterEpoch(String reason, String actor) {
        String normalizedReason = reason == null ? "" : reason.trim();
        if (normalizedReason.isBlank()) {
            normalizedReason = "manual";
        }
        String normalizedActor = actor == null ? "" : actor.trim();
        if (normalizedActor.isBlank()) {
            normalizedActor = "cli";
        }
        TaskStore.ClusterStateValue before = taskStore.clusterEpoch();
        long beforeEpoch = parseLongOrDefault(before.value(), 1L);
        long nowMs = Instant.now().toEpochMilli();
        TaskStore.ClusterStateValue after = taskStore.bumpClusterEpoch(nowMs);
        long afterEpoch = parseLongOrDefault(after.value(), beforeEpoch + 1L);
        ClusterEpochBumpOutcome out = new ClusterEpochBumpOutcome(
                beforeEpoch,
                afterEpoch,
                after.version(),
                after.updatedAtMs(),
                normalizedReason
        );
        auditLogger.log(AuditLogger.AuditEvent.of(
                "cluster.epoch.bump",
                normalizedActor,
                "runtime/cluster-state",
                "ok",
                null,
                null,
                null,
                null,
                Map.of(
                        "previous_epoch", out.previousEpoch(),
                        "current_epoch", out.currentEpoch(),
                        "version", out.version(),
                        "reason", out.reason()
                )
        ));
        return out;
    }

    public MeshSummaryOutcome meshSummary() {
        long nowMs = Instant.now().toEpochMilli();
        TaskStore.MeshSnapshot snapshot = taskStore.loadMeshSnapshot(nowMs);
        MeshSummaryOutcome out = new MeshSummaryOutcome(
                snapshot.totalNodes(),
                snapshot.aliveNodes(),
                snapshot.suspectNodes(),
                snapshot.deadNodes(),
                snapshot.runningByAliveOwners(),
                snapshot.runningBySuspectOwners(),
                snapshot.runningByDeadOwners(),
                snapshot.runningByUnknownOwners(),
                snapshot.oldestDeadNodeAgeMs(),
                nowMs
        );
        auditLogger.log(AuditLogger.AuditEvent.of(
                "membership.summary",
                "cli",
                "runtime/membership",
                "ok",
                null,
                null,
                null,
                null,
                Map.ofEntries(
                        Map.entry("total_nodes", out.totalNodes()),
                        Map.entry("alive_nodes", out.aliveNodes()),
                        Map.entry("suspect_nodes", out.suspectNodes()),
                        Map.entry("dead_nodes", out.deadNodes()),
                        Map.entry("running_by_alive", out.runningByAliveOwners()),
                        Map.entry("running_by_suspect", out.runningBySuspectOwners()),
                        Map.entry("running_by_dead", out.runningByDeadOwners()),
                        Map.entry("running_by_unknown", out.runningByUnknownOwners()),
                        Map.entry("oldest_dead_node_age_ms", out.oldestDeadNodeAgeMs())
                )
        ));
        return out;
    }

    public GossipTickOutcome gossipTick(String nodeId, int bindPort, List<String> seedEndpoints, int receiveWindowMs) {
        return gossipTick(nodeId, bindPort, seedEndpoints, receiveWindowMs, null, null);
    }

    public GossipTickOutcome gossipTick(
            String nodeId,
            int bindPort,
            List<String> seedEndpoints,
            int receiveWindowMs,
            Integer fanoutOverride,
            Integer ttlOverride
    ) {
        if (bindPort <= 0 || bindPort > 65535) {
            throw new IllegalArgumentException("Invalid gossip port: " + bindPort);
        }
        RuntimeSettings settings = runtimeSettings;
        long nowMs = Instant.now().toEpochMilli();
        TaskStore.HeartbeatOutcome localHeartbeat = taskStore.heartbeatNode(
                nodeId,
                nowMs,
                settings.suspectRecoverMinMs(),
                settings.deadRecoverMinMs()
        );
        if (localHeartbeat.recoverySuppressed()) {
            membershipRecoverySuppressedTotal.incrementAndGet();
        }
        int fanout = Math.max(1, fanoutOverride == null ? settings.gossipFanout() : fanoutOverride);
        int ttl = Math.max(1, ttlOverride == null ? settings.gossipPacketTtl() : ttlOverride);
        String msgId = "gsp_" + UUID.randomUUID();
        List<GossipNodeState> syncNodes = buildGossipSyncNodes(nodeId, nowMs, settings.gossipSyncSampleSize());
        GossipPacket outboundUnsigned = new GossipPacket(nodeId, nowMs, msgId, nodeId, ttl, syncNodes, null);
        GossipPacket outbound = signGossipPacket(outboundUnsigned, settings.gossipSharedSecret());
        byte[] payload = Jsons.toJson(outbound).getBytes(StandardCharsets.UTF_8);
        int sent = 0;
        int relayed = 0;
        int received = 0;
        int accepted = 0;
        int deduplicated = 0;
        int invalid = 0;

        try (DatagramSocket socket = new DatagramSocket(null)) {
            socket.setReuseAddress(true);
            socket.bind(new InetSocketAddress(bindPort));
            socket.setSoTimeout(Math.max(100, receiveWindowMs));

            List<SeedEndpoint> seeds = parseSeedEndpoints(seedEndpoints);
            List<SeedEndpoint> fanoutTargets = selectFanoutSeeds(seeds, fanout);
            for (SeedEndpoint seed : fanoutTargets) {
                try {
                    DatagramPacket packet = new DatagramPacket(
                            payload,
                            payload.length,
                            InetAddress.getByName(seed.host()),
                            seed.port()
                    );
                    socket.send(packet);
                    sent++;
                } catch (Exception e) {
                    invalid++;
                }
            }

            long deadline = System.currentTimeMillis() + Math.max(100, receiveWindowMs);
            while (System.currentTimeMillis() <= deadline) {
                byte[] buf = new byte[8192];
                DatagramPacket incoming = new DatagramPacket(buf, buf.length);
                try {
                    socket.receive(incoming);
                } catch (SocketTimeoutException timeout) {
                    break;
                }
                received++;
                String body = new String(incoming.getData(), incoming.getOffset(), incoming.getLength(), StandardCharsets.UTF_8);
                try {
                    GossipPacket peer = Jsons.mapper().readValue(body, GossipPacket.class);
                    if (!verifyGossipPacket(peer, settings.gossipSharedSecret())) {
                        invalid++;
                        continue;
                    }
                    if (peer.nodeId() == null || peer.nodeId().isBlank() || nodeId.equals(peer.nodeId())) {
                        continue;
                    }
                    String peerMsgId = (peer.msgId() == null || peer.msgId().isBlank())
                            ? "legacy_" + Hashing.sha256Hex(body)
                            : peer.msgId().trim();
                    if (!registerGossipMessage(peerMsgId, nowMs, settings)) {
                        deduplicated++;
                        continue;
                    }
                    long heartbeatMs = peer.heartbeatMs() <= 0 ? nowMs : peer.heartbeatMs();
                    TaskStore.HeartbeatOutcome peerHeartbeat = taskStore.heartbeatNodeIfNewer(
                            peer.nodeId(),
                            heartbeatMs,
                            nowMs,
                            settings.suspectRecoverMinMs(),
                            settings.deadRecoverMinMs()
                    );
                    if (peerHeartbeat.recoverySuppressed()) {
                        membershipRecoverySuppressedTotal.incrementAndGet();
                    }
                    String origin = peer.originNode();
                    if (origin != null && !origin.isBlank() && !nodeId.equals(origin) && !origin.equals(peer.nodeId())) {
                        TaskStore.HeartbeatOutcome originHeartbeat = taskStore.heartbeatNodeIfNewer(
                                origin,
                                heartbeatMs,
                                nowMs,
                                settings.suspectRecoverMinMs(),
                                settings.deadRecoverMinMs()
                        );
                        if (originHeartbeat.recoverySuppressed()) {
                            membershipRecoverySuppressedTotal.incrementAndGet();
                        }
                    }
                    int mergedNodes = mergeGossipNodes(peer.nodes(), nowMs, settings);
                    if (mergedNodes > 0) {
                        antiEntropyMergeTotal.addAndGet(mergedNodes);
                        accepted += mergedNodes;
                    }
                    accepted++;

                    int peerTtl = peer.ttl() == null ? 1 : peer.ttl();
                    if (peerTtl > 1) {
                        GossipPacket relayedUnsigned = new GossipPacket(
                                peer.nodeId(),
                                heartbeatMs,
                                peerMsgId,
                                origin == null || origin.isBlank() ? peer.nodeId() : origin,
                                peerTtl - 1,
                                peer.nodes(),
                                null
                        );
                        GossipPacket relayedPacket = signGossipPacket(relayedUnsigned, settings.gossipSharedSecret());
                        byte[] relayPayload = Jsons.toJson(relayedPacket).getBytes(StandardCharsets.UTF_8);
                        List<SeedEndpoint> relayCandidates = relayCandidates(seeds, incoming, bindPort);
                        for (SeedEndpoint seed : selectFanoutSeeds(relayCandidates, fanout)) {
                            try {
                                DatagramPacket relay = new DatagramPacket(
                                        relayPayload,
                                        relayPayload.length,
                                        InetAddress.getByName(seed.host()),
                                        seed.port()
                                );
                                socket.send(relay);
                                relayed++;
                            } catch (Exception e) {
                                invalid++;
                            }
                        }
                    }
                } catch (Exception parseError) {
                    invalid++;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed gossip tick on UDP port " + bindPort, e);
        }

        long suspectBeforeMs = nowMs - settings.suspectAfterMs();
        long deadBeforeMs = nowMs - settings.deadAfterMs();
        TaskStore.NodeStateTransition nodeTransition = taskStore.reconcileNodeStates(nowMs, suspectBeforeMs, deadBeforeMs);
        GossipTickOutcome out = new GossipTickOutcome(
                nodeId,
                bindPort,
                sent,
                relayed,
                received,
                accepted,
                deduplicated,
                invalid,
                fanout,
                ttl,
                gossipDedupCache.size(),
                nodeTransition.suspected(),
                nodeTransition.dead()
        );
        auditLogger.log(AuditLogger.AuditEvent.of(
                "membership.gossip.tick",
                "system",
                "runtime/membership",
                "ok",
                null,
                null,
                null,
                null,
                Map.ofEntries(
                        Map.entry("node_id", nodeId),
                        Map.entry("bind_port", bindPort),
                        Map.entry("sent", sent),
                        Map.entry("relayed", relayed),
                        Map.entry("received", received),
                        Map.entry("accepted", accepted),
                        Map.entry("deduplicated", deduplicated),
                        Map.entry("invalid", invalid),
                        Map.entry("fanout", fanout),
                        Map.entry("ttl", ttl),
                        Map.entry("suspected_nodes_marked", out.suspectedNodesMarked()),
                        Map.entry("dead_nodes_marked", out.deadNodesMarked())
                )
        ));
        return out;
    }

    public GossipSyncOutcome gossipSync(
            String nodeId,
            int bindPort,
            List<String> seedEndpoints,
            int receiveWindowMs,
            int rounds,
            long intervalMs,
            Integer fanoutOverride,
            Integer ttlOverride
    ) {
        int safeRounds = Math.max(1, rounds);
        long safeInterval = Math.max(0L, intervalMs);
        List<GossipTickOutcome> ticks = new ArrayList<>(safeRounds);
        int sent = 0;
        int relayed = 0;
        int received = 0;
        int accepted = 0;
        int deduplicated = 0;
        int invalid = 0;
        int suspected = 0;
        int dead = 0;
        for (int i = 0; i < safeRounds; i++) {
            GossipTickOutcome tick = gossipTick(
                    nodeId,
                    bindPort,
                    seedEndpoints,
                    receiveWindowMs,
                    fanoutOverride,
                    ttlOverride
            );
            ticks.add(tick);
            sent += tick.sent();
            relayed += tick.relayed();
            received += tick.received();
            accepted += tick.accepted();
            deduplicated += tick.deduplicated();
            invalid += tick.invalid();
            suspected += tick.suspectedNodesMarked();
            dead += tick.deadNodesMarked();
            if (i < safeRounds - 1 && safeInterval > 0L) {
                try {
                    Thread.sleep(safeInterval);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        List<TaskStore.MeshNode> members = taskStore.listNodes();
        long nowMs = Instant.now().toEpochMilli();
        GossipSyncOutcome out = new GossipSyncOutcome(
                nodeId,
                bindPort,
                ticks.size(),
                sent,
                relayed,
                received,
                accepted,
                deduplicated,
                invalid,
                suspected,
                dead,
                members.size(),
                ticks
        );
        auditLogger.log(AuditLogger.AuditEvent.of(
                "membership.gossip.sync",
                "system",
                "runtime/membership",
                "ok",
                null,
                null,
                null,
                null,
                Map.ofEntries(
                        Map.entry("node_id", nodeId),
                        Map.entry("bind_port", bindPort),
                        Map.entry("rounds", out.roundsCompleted()),
                        Map.entry("member_count", out.memberCount()),
                        Map.entry("sent", out.sent()),
                        Map.entry("relayed", out.relayed()),
                        Map.entry("received", out.received()),
                        Map.entry("accepted", out.accepted()),
                        Map.entry("deduplicated", out.deduplicated()),
                        Map.entry("invalid", out.invalid()),
                        Map.entry("suspected_nodes_marked", out.suspectedNodesMarked()),
                        Map.entry("dead_nodes_marked", out.deadNodesMarked()),
                        Map.entry("checked_at_ms", nowMs)
                )
        ));
        return out;
    }

    public List<TaskStore.LeaseConflict> leaseConflicts(int limit, int sinceHours) {
        long sinceMs = Instant.now().minus(Duration.ofHours(Math.max(1, sinceHours))).toEpochMilli();
        List<TaskStore.LeaseConflict> rows = taskStore.listLeaseConflicts(Math.max(1, limit), sinceMs);
        auditLogger.log(AuditLogger.AuditEvent.of(
                "lease.conflicts.list",
                "cli",
                "runtime/lease-conflicts",
                "ok",
                null,
                null,
                null,
                null,
                Map.of("limit", limit, "since_hours", sinceHours, "count", rows.size())
        ));
        return rows;
    }

    public LeaseReportOutcome generateLeaseConflictReport(int hours, int limit, String outputPath) {
        int lookbackHours = Math.max(1, hours);
        int cap = Math.max(10, limit);
        long nowMs = Instant.now().toEpochMilli();
        List<TaskStore.LeaseConflict> rows = taskStore.listLeaseConflicts(cap, nowMs - Duration.ofHours(lookbackHours).toMillis());
        Map<String, Integer> byEventType = countBy(rows, TaskStore.LeaseConflict::eventType);
        Map<String, Integer> byStep = countBy(rows, TaskStore.LeaseConflict::stepId);
        Map<String, Integer> byWorker = countBy(rows, TaskStore.LeaseConflict::workerId);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("generated_at", Instant.ofEpochMilli(nowMs).toString());
        payload.put("window_hours", lookbackHours);
        payload.put("rows", rows.size());
        payload.put("by_event_type", byEventType);
        payload.put("top_steps", topN(byStep, 10));
        payload.put("top_workers", topN(byWorker, 10));
        payload.put("sample", rows);

        Path out = Paths.get(outputPath).toAbsolutePath().normalize();
        try {
            if (out.getParent() != null) {
                Files.createDirectories(out.getParent());
            }
            Files.writeString(out, Jsons.toJson(payload), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write lease report: " + out, e);
        }
        LeaseReportOutcome result = new LeaseReportOutcome(true, out.toString(), rows.size());
        auditLogger.log(AuditLogger.AuditEvent.of(
                "lease.report.generate",
                "cli",
                "runtime/lease-report",
                "ok",
                null,
                null,
                null,
                null,
                Map.of("output", result.path(), "rows", result.rows())
        ));
        return result;
    }

    public PurgeOutcome purge(int doneRetentionDays, int deadRetentionDays, int idempotencyTtlDays) {
        long nowMs = Instant.now().toEpochMilli();
        int idempotencyPurged = taskStore.purgeIdempotencyOlderThan(
                Instant.now().minus(Duration.ofDays(idempotencyTtlDays)).toEpochMilli()
        );
        int donePurged = purgeDoneDirectories(doneRetentionDays);
        int deadPurged = purgeDeadFiles(nowMs - Duration.ofDays(deadRetentionDays).toMillis());
        int retryPurged = purgeRetryFiles(nowMs - Duration.ofDays(deadRetentionDays).toMillis());
        PurgeOutcome out = new PurgeOutcome(donePurged, deadPurged, retryPurged, idempotencyPurged);
        auditLogger.log(AuditLogger.AuditEvent.of(
                "storage.purge",
                "cli",
                "runtime/storage",
                "ok",
                null,
                null,
                null,
                null,
                Map.of(
                        "done_dirs_purged", out.doneDirectoriesPurged(),
                        "dead_files_purged", out.deadFilesPurged(),
                        "retry_files_purged", out.retryFilesPurged(),
                        "idempotency_rows_purged", out.idempotencyRowsPurged()
                )
        ));
        return out;
    }

    public DeadExportOutcome exportDeadLetters(String outputPath, int limit) {
        Path output = Paths.get(outputPath).toAbsolutePath().normalize();
        List<DeadEntry> entries = new ArrayList<>();
        try (Stream<Path> stream = Files.list(config.deadRoot())) {
            List<Path> files = stream
                    .filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().endsWith(".meta.json"))
                    .sorted(Comparator.comparing(Path::toString))
                    .limit(limit)
                    .toList();
            for (Path file : files) {
                MessageEnvelope envelope = Jsons.mapper().readValue(file.toFile(), MessageEnvelope.class);
                entries.add(new DeadEntry(
                        envelope.msgId(),
                        envelope.taskId(),
                        envelope.stepId(),
                        envelope.toAgent(),
                        envelope.priority(),
                        envelope.createdAtMs(),
                        file.toAbsolutePath().normalize().toString()
                ));
            }
            Files.createDirectories(output.getParent() == null ? Paths.get(".") : output.getParent());
            Files.writeString(output, Jsons.toJson(entries), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to export dead letters", e);
        }
        DeadExportOutcome out = new DeadExportOutcome(entries.size(), output.toString());
        auditLogger.log(AuditLogger.AuditEvent.of(
                "dead.export",
                "cli",
                "runtime/dead",
                "ok",
                null,
                null,
                null,
                null,
                Map.of("exported", out.exported(), "output", out.outputPath())
        ));
        return out;
    }

    public SnapshotExportOutcome exportSnapshot(String outputDir) {
        long nowMs = Instant.now().toEpochMilli();
        Path out = Paths.get(outputDir).toAbsolutePath().normalize();
        Path source = config.rootDir();
        if (out.startsWith(source)) {
            throw new IllegalArgumentException("snapshot output must be outside runtime root: " + out);
        }
        Path snapshotRoot = out.resolve("relaymesh-root");
        try {
            if (Files.exists(snapshotRoot)) {
                deleteRecursively(snapshotRoot);
            }
            Files.createDirectories(snapshotRoot);
            CopySummary copied = copyDirectory(source, snapshotRoot);
            Path manifest = out.resolve("manifest.json");
            Map<String, Object> manifestBody = new LinkedHashMap<>();
            manifestBody.put("schema_version", "relaymesh.snapshot.v1");
            manifestBody.put("created_at", Instant.ofEpochMilli(nowMs).toString());
            manifestBody.put("source_root", source.toString());
            manifestBody.put("snapshot_root", snapshotRoot.toString());
            manifestBody.put("file_count", copied.files());
            manifestBody.put("bytes", copied.bytes());
            Files.writeString(manifest, Jsons.toJson(manifestBody), StandardCharsets.UTF_8);
            SnapshotExportOutcome outResult = new SnapshotExportOutcome(
                    out.toString(),
                    snapshotRoot.toString(),
                    copied.files(),
                    copied.bytes(),
                    nowMs
            );
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "snapshot.export",
                    "cli",
                    "runtime/snapshot",
                    "ok",
                    null,
                    null,
                    null,
                    null,
                    Map.of(
                            "output", outResult.outputDir(),
                            "files", outResult.fileCount(),
                            "bytes", outResult.totalBytes()
                    )
            ));
            return outResult;
        } catch (IOException e) {
            throw new RuntimeException("Failed to export snapshot to " + out, e);
        }
    }

    public SnapshotImportOutcome importSnapshot(String inputDir) {
        long nowMs = Instant.now().toEpochMilli();
        Path in = Paths.get(inputDir).toAbsolutePath().normalize();
        Path snapshotRoot = in.resolve("relaymesh-root");
        Path targetRoot = config.rootDir();
        if (!Files.isDirectory(snapshotRoot)) {
            throw new IllegalArgumentException("snapshot root does not exist: " + snapshotRoot);
        }
        if (snapshotRoot.startsWith(targetRoot)) {
            throw new IllegalArgumentException("snapshot input must be outside runtime root: " + snapshotRoot);
        }
        try {
            Files.createDirectories(targetRoot);
            clearDirectory(targetRoot);
            CopySummary restored = copyDirectory(snapshotRoot, targetRoot);
            SnapshotImportOutcome out = new SnapshotImportOutcome(
                    in.toString(),
                    snapshotRoot.toString(),
                    restored.files(),
                    restored.bytes(),
                    nowMs
            );
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "snapshot.import",
                    "cli",
                    "runtime/snapshot",
                    "ok",
                    null,
                    null,
                    null,
                    null,
                    Map.of(
                            "input", out.inputDir(),
                            "files", out.restoredFiles(),
                            "bytes", out.restoredBytes()
                    )
            ));
            return out;
        } catch (IOException e) {
            throw new RuntimeException("Failed to import snapshot from " + in, e);
        }
    }

    public StatsOutcome stats() {
        long nowMs = Instant.now().toEpochMilli();
        TaskStore.RuntimeStats db = taskStore.loadRuntimeStats();
        TaskStore.SlaSnapshot sla = taskStore.loadSlaSnapshot(nowMs);
        TaskStore.LeaseConflictSummary conflict = taskStore.loadLeaseConflictSummary(nowMs);
        TaskStore.MeshSnapshot mesh = taskStore.loadMeshSnapshot(nowMs);
        int inboxHigh = countFiles(config.inboxHigh(), "*.meta.json");
        int inboxNormal = countFiles(config.inboxNormal(), "*.meta.json");
        int inboxLow = countFiles(config.inboxLow(), "*.meta.json");
        int processing = countFiles(config.processingDir(), "*.meta.json");
        int dead = countFiles(config.deadRoot(), "*.meta.json");
        int retry = countFiles(config.retryRoot(), "*.meta.json");
        long antiEntropyMerged = antiEntropyMergeTotal.get();
        long webGetCompat = webWriteGetCompatTotal.get();
        long webRateLimited = webWriteRateLimitedTotal.get();
        long deadOwnerRecovered = deadOwnerReclaimedTotal.get();
        long membershipRecoverySuppressed = membershipRecoverySuppressedTotal.get();
        long meshNodesPruned = meshNodesPrunedTotal.get();
        long workerPaused = workerPausedTotal.get();
        long replicationImports = replicationImportTotal.get();
        long replicationTiesResolved = replicationTieResolvedTotal.get();
        long replicationTiesKeptLocal = replicationTieKeptLocalTotal.get();
        long replicationLastLagMs = replicationLastImportLagMs.get();
        long replicationSyncRuns = replicationSyncTotal.get();
        long sloAlertFires = sloAlertFireTotal.get();
        long submitRejected = submitRejectedTotal.get();
        long gossipSigningEnabled = runtimeSettings.gossipSharedSecret() == null || runtimeSettings.gossipSharedSecret().isBlank() ? 0L : 1L;
        long lowStarvation = fileBus.lowStarvationCount();
        long clusterEpoch = parseLongOrDefault(taskStore.clusterEpoch().value(), 1L);
        long convergenceSeconds = computeGossipConvergenceSeconds(taskStore.listNodes());
        long diskFreeBytes = currentDiskFreeBytes();
        long minFreeDiskBytes = Math.max(0L, runtimeSettings.minFreeDiskBytes());
        long diskPressure = (minFreeDiskBytes > 0L && diskFreeBytes >= 0L && diskFreeBytes < minFreeDiskBytes) ? 1L : 0L;
        StatsOutcome out = new StatsOutcome(
                db.taskStatus(),
                db.stepStatus(),
                db.messageState(),
                inboxHigh,
                inboxNormal,
                inboxLow,
                processing,
                dead,
                retry,
                sla.queueLagMs(),
                sla.stepLatencyP95Ms(),
                sla.stepLatencyP99Ms(),
                sla.deadLetterGrowth1h(),
                conflict.total(),
                conflict.last1h(),
                conflict.byEventType(),
                antiEntropyMerged,
                webGetCompat,
                webRateLimited,
                deadOwnerRecovered,
                membershipRecoverySuppressed,
                meshNodesPruned,
                workerPaused,
                replicationImports,
                replicationTiesResolved,
                replicationTiesKeptLocal,
                replicationLastLagMs,
                gossipSigningEnabled,
                lowStarvation,
                clusterEpoch,
                mesh.totalNodes(),
                mesh.aliveNodes(),
                mesh.suspectNodes(),
                mesh.deadNodes(),
                mesh.runningByAliveOwners(),
                mesh.runningBySuspectOwners(),
                mesh.runningByDeadOwners(),
                mesh.runningByUnknownOwners(),
                mesh.oldestDeadNodeAgeMs(),
                convergenceSeconds,
                diskFreeBytes,
                minFreeDiskBytes,
                diskPressure,
                replicationSyncRuns,
                sloAlertFires,
                submitRejected,
                config.namespace()
        );
        auditLogger.log(AuditLogger.AuditEvent.of(
                "runtime.stats",
                "cli",
                "runtime/stats",
                "ok",
                null,
                null,
                null,
                null,
                Map.ofEntries(
                        Map.entry("inbox_high", out.inboxHigh()),
                        Map.entry("inbox_normal", out.inboxNormal()),
                        Map.entry("inbox_low", out.inboxLow()),
                        Map.entry("dead", out.deadMetaFiles()),
                        Map.entry("queue_lag_ms", out.queueLagMs()),
                        Map.entry("step_latency_p95_ms", out.stepLatencyP95Ms()),
                        Map.entry("step_latency_p99_ms", out.stepLatencyP99Ms()),
                        Map.entry("dead_letter_growth_1h", out.deadLetterGrowth1h()),
                        Map.entry("lease_conflict_total", out.leaseConflictTotal()),
                        Map.entry("lease_conflict_1h", out.leaseConflict1h()),
                        Map.entry("anti_entropy_merge_total", out.antiEntropyMergeTotal()),
                        Map.entry("web_write_get_compat_total", out.webWriteGetCompatTotal()),
                        Map.entry("web_write_rate_limited_total", out.webWriteRateLimitedTotal()),
                        Map.entry("dead_owner_reclaimed_total", out.deadOwnerReclaimedTotal()),
                        Map.entry("membership_recovery_suppressed_total", out.membershipRecoverySuppressedTotal()),
                        Map.entry("mesh_nodes_pruned_total", out.meshNodesPrunedTotal()),
                        Map.entry("worker_paused_total", out.workerPausedTotal()),
                        Map.entry("replication_import_total", out.replicationImportTotal()),
                        Map.entry("replication_tie_resolved_total", out.replicationTieResolvedTotal()),
                        Map.entry("replication_tie_kept_local_total", out.replicationTieKeptLocalTotal()),
                        Map.entry("replication_last_import_lag_ms", out.replicationLastImportLagMs()),
                        Map.entry("replication_sync_total", out.replicationSyncTotal()),
                        Map.entry("gossip_signing_enabled", out.gossipSigningEnabled()),
                        Map.entry("slo_alert_fire_total", out.sloAlertFireTotal()),
                        Map.entry("submit_rejected_total", out.submitRejectedTotal()),
                        Map.entry("low_starvation_count", out.lowStarvationCount()),
                        Map.entry("cluster_epoch", out.clusterEpoch()),
                        Map.entry("mesh_total_nodes", out.meshTotalNodes()),
                        Map.entry("mesh_alive_nodes", out.meshAliveNodes()),
                        Map.entry("mesh_suspect_nodes", out.meshSuspectNodes()),
                        Map.entry("mesh_dead_nodes", out.meshDeadNodes()),
                        Map.entry("mesh_running_on_dead_owners", out.meshRunningByDeadOwners()),
                        Map.entry("gossip_convergence_seconds", out.gossipConvergenceSeconds()),
                        Map.entry("disk_free_bytes", out.diskFreeBytes()),
                        Map.entry("min_free_disk_bytes", out.minFreeDiskBytes()),
                        Map.entry("disk_pressure", out.diskPressure())
                )
        ));
        return out;
    }

    public String metricsText() {
        StatsOutcome stats = stats();
        String text = PrometheusFormatter.format(stats, config.namespace());
        auditLogger.log(AuditLogger.AuditEvent.of(
                "runtime.metrics",
                "cli",
                "runtime/metrics",
                "ok",
                null,
                null,
                null,
                null,
                Map.of("bytes", text.length())
        ));
        return text;
    }

    public SloAlertOutcome evaluateSloAlerts(String outputPath) {
        StatsOutcome stats = stats();
        RuntimeSettings settings = runtimeSettings;
        long nowMs = Instant.now().toEpochMilli();
        List<SloAlert> alerts = new ArrayList<>();
        appendSloAlert(alerts, "queue_lag", stats.queueLagMs(), settings.sloQueueLagTargetMs(), "queue lag exceeds target");
        appendSloAlert(alerts, "step_p95", stats.stepLatencyP95Ms(), settings.sloStepP95TargetMs(), "step p95 exceeds target");
        appendSloAlert(alerts, "step_p99", stats.stepLatencyP99Ms(), settings.sloStepP99TargetMs(), "step p99 exceeds target");
        appendSloAlert(alerts, "dead_growth_1h", stats.deadLetterGrowth1h(), settings.sloDeadLetterGrowthTarget1h(), "dead letter growth exceeds target");
        appendSloAlert(alerts, "replication_lag", stats.replicationLastImportLagMs(), settings.sloReplicationLagTargetMs(), "replication lag exceeds target");
        long firedTotal = alerts.isEmpty() ? sloAlertFireTotal.get() : sloAlertFireTotal.addAndGet(alerts.size());

        String resolvedOutput = outputPath;
        if (resolvedOutput == null || resolvedOutput.isBlank()) {
            resolvedOutput = config.alertsRoot().resolve("slo-alerts-latest.json").toString();
        }
        Path out = Paths.get(resolvedOutput).toAbsolutePath().normalize();
        try {
            if (out.getParent() != null) {
                Files.createDirectories(out.getParent());
            }
            Map<String, Object> body = Map.of(
                    "namespace", config.namespace(),
                    "evaluated_at_ms", nowMs,
                    "fired", !alerts.isEmpty(),
                    "alerts", alerts
            );
            Files.writeString(out, Jsons.toJson(body), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write SLO alert output: " + out, e);
        }

        auditLogger.log(AuditLogger.AuditEvent.of(
                "slo.evaluate",
                "cli",
                "runtime/slo",
                alerts.isEmpty() ? "ok" : "alert",
                null,
                null,
                null,
                null,
                Map.of(
                        "namespace", config.namespace(),
                        "alerts", alerts.size(),
                        "total_fired", firedTotal,
                        "output", out.toString()
                )
        ));
        return new SloAlertOutcome(
                config.namespace(),
                nowMs,
                alerts,
                firedTotal,
                out.toString()
        );
    }

    public AuditIntegrityOutcome verifyAuditIntegrity() {
        return verifyAuditIntegrity(0);
    }

    public AuditIntegrityOutcome verifyAuditIntegrity(int limit) {
        Path file = config.auditRoot().resolve("audit.log");
        if (!Files.exists(file)) {
            return new AuditIntegrityOutcome(true, 0, 0, 0, 0, "", "");
        }
        int safeLimit = Math.max(0, limit);
        int totalRows = 0;
        int checkedRows = 0;
        int legacyRows = 0;
        int brokenLine = 0;
        String reason = "";
        String expectedPrev = "";
        try {
            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            int start = 0;
            if (safeLimit > 0 && safeLimit < lines.size()) {
                start = lines.size() - safeLimit;
            }
            for (int i = start; i < lines.size(); i++) {
                String line = lines.get(i);
                if (line == null || line.isBlank()) {
                    continue;
                }
                totalRows++;
                JsonNode parsed;
                try {
                    parsed = Jsons.mapper().readTree(line);
                } catch (Exception e) {
                    brokenLine = i + 1;
                    reason = "invalid_json";
                    break;
                }
                String hash = parsed.path("hash").asText("");
                if (hash.isBlank()) {
                    legacyRows++;
                    expectedPrev = "";
                    continue;
                }
                String prevHash = parsed.path("prev_hash").asText("");
                if (!expectedPrev.isBlank() && !prevHash.equals(expectedPrev)) {
                    brokenLine = i + 1;
                    reason = "prev_hash_mismatch";
                    break;
                }
                ObjectNode canonical = (ObjectNode) parsed.deepCopy();
                canonical.remove("hash");
                canonical.remove("signature");
                String expectedHash = Hashing.sha256Hex(COMPACT_JSON.writeValueAsString(canonical));
                if (!expectedHash.equals(hash)) {
                    brokenLine = i + 1;
                    reason = "hash_mismatch";
                    break;
                }
                String signature = parsed.path("signature").asText("");
                if (!signature.isBlank() && !auditSigningSecret.isBlank()) {
                    String expectedSig = Hashing.hmacSha256Hex(auditSigningSecret, hash);
                    if (!expectedSig.equals(signature)) {
                        brokenLine = i + 1;
                        reason = "signature_mismatch";
                        break;
                    }
                }
                checkedRows++;
                expectedPrev = hash;
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to verify audit integrity", e);
        }
        boolean ok = brokenLine == 0;
        AuditIntegrityOutcome out = new AuditIntegrityOutcome(
                ok,
                totalRows,
                checkedRows,
                legacyRows,
                brokenLine,
                reason,
                expectedPrev
        );
        auditLogger.log(AuditLogger.AuditEvent.of(
                "audit.verify",
                "cli",
                "runtime/audit",
                ok ? "ok" : "failed",
                null,
                null,
                null,
                null,
                Map.of(
                        "checked_rows", out.checkedRows(),
                        "legacy_rows", out.legacyRows(),
                        "broken_line", out.brokenLine(),
                        "reason", out.reason()
                )
        ));
        return out;
    }

    public SiemExportOutcome exportAuditToSiem(String outputPath, int limit, String action, String taskId) {
        int safeLimit = Math.max(1, limit);
        List<String> rows = auditQuery(taskId, action, null, null, safeLimit);
        Path out = Paths.get(outputPath).toAbsolutePath().normalize();
        int exported = 0;
        try {
            if (out.getParent() != null) {
                Files.createDirectories(out.getParent());
            }
            List<String> payload = new ArrayList<>();
            for (String row : rows) {
                if (row == null || row.isBlank()) {
                    continue;
                }
                JsonNode node;
                try {
                    node = Jsons.mapper().readTree(row);
                } catch (Exception ignored) {
                    continue;
                }
                Map<String, Object> event = new LinkedHashMap<>();
                event.put("@timestamp", node.path("timestamp").asText(""));
                event.put("event.action", node.path("action").asText(""));
                event.put("event.outcome", node.path("result").asText(""));
                event.put("event.actor", node.path("actor").asText(""));
                event.put("trace.id", node.path("trace_id").asText(""));
                event.put("span.id", node.path("span_id").asText(""));
                event.put("task.id", node.path("task_id").asText(""));
                event.put("step.id", node.path("step_id").asText(""));
                event.put("relaymesh.namespace", node.path("namespace").asText(config.namespace()));
                event.put("relaymesh.principal_id", node.path("principal_id").asText(""));
                event.put("relaymesh.hash", node.path("hash").asText(""));
                event.put("relaymesh.signature", node.path("signature").asText(""));
                event.put("relaymesh.details", node.path("details"));
                payload.add(Jsons.mapper().writeValueAsString(event));
                exported++;
            }
            Files.write(out, payload, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to export SIEM payload", e);
        }
        auditLogger.log(AuditLogger.AuditEvent.of(
                "audit.siem.export",
                "cli",
                "runtime/audit",
                "ok",
                null,
                null,
                null,
                null,
                Map.of("output", out.toString(), "exported", exported)
        ));
        return new SiemExportOutcome(config.namespace(), out.toString(), exported);
    }

    public PayloadCrypto.KeyringStatus payloadKeyStatus() {
        return fileBus.payloadKeyStatus();
    }

    public PayloadCrypto.RotationOutcome rotatePayloadKey() {
        PayloadCrypto.RotationOutcome out = fileBus.rotatePayloadKey();
        auditLogger.log(AuditLogger.AuditEvent.of(
                "payload.key.rotate",
                "cli",
                "runtime/security",
                "ok",
                null,
                null,
                null,
                null,
                Map.of("active_kid", out.activeKid(), "total_keys", out.totalKeys(), "key_file", out.keyFile())
        ));
        return out;
    }

    public List<String> auditTail(int lines) {
        Path file = config.auditRoot().resolve("audit.log");
        if (!Files.exists(file)) return List.of();
        try {
            List<String> all = Files.readAllLines(file, StandardCharsets.UTF_8);
            if (all.isEmpty()) return List.of();
            int n = Math.max(1, lines);
            int from = Math.max(0, all.size() - n);
            List<String> out = new ArrayList<>(all.subList(from, all.size()));
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "audit.tail",
                    "cli",
                    "runtime/audit",
                    "ok",
                    null,
                    null,
                    null,
                    null,
                    Map.of("lines", n, "returned", out.size())
            ));
            return out;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read audit tail", e);
        }
    }

    public List<String> auditQuery(String taskId, String action, String fromIso, String toIso, int limit) {
        Path file = config.auditRoot().resolve("audit.log");
        if (!Files.exists(file)) return List.of();
        Instant from = parseOptionalInstant(fromIso, "--from");
        Instant to = parseOptionalInstant(toIso, "--to");
        if (from != null && to != null && from.isAfter(to)) {
            throw new IllegalArgumentException("--from must be <= --to");
        }
        int safeLimit = Math.max(1, limit);
        String normalizedTaskId = taskId == null ? "" : taskId.trim();
        String normalizedAction = action == null ? "" : action.trim();
        try {
            List<String> all = Files.readAllLines(file, StandardCharsets.UTF_8);
            List<String> out = new ArrayList<>();
            for (int i = all.size() - 1; i >= 0 && out.size() < safeLimit; i--) {
                String line = all.get(i);
                JsonNode node;
                try {
                    node = Jsons.mapper().readTree(line);
                } catch (Exception ignored) {
                    continue;
                }

                if (!normalizedTaskId.isEmpty() && !normalizedTaskId.equals(node.path("task_id").asText(""))) {
                    continue;
                }
                if (!normalizedAction.isEmpty() && !normalizedAction.equals(node.path("action").asText(""))) {
                    continue;
                }
                String ts = node.path("timestamp").asText("");
                Instant eventTime = parseInstantSafe(ts);
                if (from != null && (eventTime == null || eventTime.isBefore(from))) {
                    continue;
                }
                if (to != null && (eventTime == null || eventTime.isAfter(to))) {
                    continue;
                }
                out.add(line);
            }
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "audit.query",
                    "cli",
                    "runtime/audit",
                    "ok",
                    null,
                    null,
                    normalizedTaskId.isEmpty() ? null : normalizedTaskId,
                    null,
                    Map.of(
                            "action", normalizedAction,
                            "from", fromIso == null ? "" : fromIso,
                            "to", toIso == null ? "" : toIso,
                            "limit", safeLimit,
                            "returned", out.size()
                    )
            ));
            return out;
        } catch (IOException e) {
            throw new RuntimeException("Failed to query audit log", e);
        }
    }

    public List<OwnershipEvent> ownershipEvents(int limit, int sinceHours) {
        int safeLimit = Math.max(1, limit);
        long sinceMs = Instant.now().minus(Duration.ofHours(Math.max(1, sinceHours))).toEpochMilli();
        List<OwnershipEvent> out = new ArrayList<>();

        for (TaskStore.LeaseConflict row : taskStore.listLeaseConflicts(safeLimit, sinceMs)) {
            Map<String, Object> details = Map.ofEntries(
                    Map.entry("event_type", row.eventType()),
                    Map.entry("expected_status", row.expectedStatus() == null ? "" : row.expectedStatus()),
                    Map.entry("expected_lease_token", row.expectedLeaseToken() == null ? "" : row.expectedLeaseToken()),
                    Map.entry("expected_lease_epoch", row.expectedLeaseEpoch() == null ? -1L : row.expectedLeaseEpoch()),
                    Map.entry("actual_status", row.actualStatus() == null ? "" : row.actualStatus()),
                    Map.entry("actual_lease_owner", row.actualLeaseOwner() == null ? "" : row.actualLeaseOwner()),
                    Map.entry("actual_lease_token", row.actualLeaseToken() == null ? "" : row.actualLeaseToken()),
                    Map.entry("actual_lease_epoch", row.actualLeaseEpoch() == null ? -1L : row.actualLeaseEpoch())
            );
            out.add(new OwnershipEvent(
                    "lease_conflicts",
                    row.eventType(),
                    row.workerId(),
                    row.taskId(),
                    row.stepId(),
                    "conflict",
                    row.occurredAtMs(),
                    details
            ));
        }

        Path auditFile = config.auditRoot().resolve("audit.log");
        if (Files.exists(auditFile)) {
            try {
                List<String> all = Files.readAllLines(auditFile, StandardCharsets.UTF_8);
                for (int i = all.size() - 1; i >= 0; i--) {
                    JsonNode node;
                    try {
                        node = Jsons.mapper().readTree(all.get(i));
                    } catch (Exception ignored) {
                        continue;
                    }
                    String action = node.path("action").asText("");
                    if (!"ownership.recover".equals(action)
                            && !"ownership.recover.dead_node".equals(action)
                            && !"ownership.conflict.decision".equals(action)) {
                        continue;
                    }
                    Instant eventTime = parseInstantSafe(node.path("timestamp").asText(""));
                    if (eventTime == null || eventTime.toEpochMilli() < sinceMs) {
                        continue;
                    }
                    JsonNode detailsNode = node.path("details");
                    Map<String, Object> details = Jsons.mapper().convertValue(
                            detailsNode,
                            new TypeReference<Map<String, Object>>() {}
                    );
                    String nodeHint = "";
                    if ("ownership.conflict.decision".equals(action)) {
                        String winner = detailsNode.path("winner_owner").asText("");
                        if (!winner.isBlank()) {
                            nodeHint = winner;
                        }
                    } else {
                        JsonNode deadNodes = detailsNode.path("dead_nodes");
                        if (deadNodes.isArray() && deadNodes.size() > 0) {
                            List<String> ids = new ArrayList<>();
                            for (JsonNode id : deadNodes) {
                                String v = id.asText("");
                                if (!v.isBlank()) {
                                    ids.add(v);
                                }
                            }
                            nodeHint = String.join(",", ids);
                        }
                    }
                    out.add(new OwnershipEvent(
                            "audit",
                            action,
                            nodeHint,
                            node.path("task_id").asText(""),
                            node.path("step_id").asText(""),
                            node.path("result").asText(""),
                            eventTime.toEpochMilli(),
                            details
                    ));
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to read ownership events from audit log", e);
            }
        }

        out.sort((a, b) -> Long.compare(b.occurredAtMs(), a.occurredAtMs()));
        if (out.size() > safeLimit) {
            out = new ArrayList<>(out.subList(0, safeLimit));
        }
        auditLogger.log(AuditLogger.AuditEvent.of(
                "ownership.events.list",
                "cli",
                "runtime/ownership",
                "ok",
                null,
                null,
                null,
                null,
                Map.of("limit", safeLimit, "since_hours", sinceHours, "count", out.size())
        ));
        return out;
    }

    public List<SettingsHistoryEntry> settingsHistory(int limit) {
        Path file = config.auditRoot().resolve("audit.log");
        if (!Files.exists(file)) return List.of();
        int safeLimit = Math.max(1, limit);
        try {
            List<String> all = Files.readAllLines(file, StandardCharsets.UTF_8);
            List<SettingsHistoryEntry> out = new ArrayList<>();
            for (int i = all.size() - 1; i >= 0 && out.size() < safeLimit; i--) {
                JsonNode node;
                try {
                    node = Jsons.mapper().readTree(all.get(i));
                } catch (Exception ignored) {
                    continue;
                }
                if (!"runtime.settings.load".equals(node.path("action").asText(""))) {
                    continue;
                }
                JsonNode details = node.path("details");
                List<String> changedFields = new ArrayList<>();
                JsonNode changedFieldNode = details.path("changed_fields");
                if (changedFieldNode.isArray()) {
                    for (JsonNode field : changedFieldNode) {
                        String value = field == null ? "" : field.asText("");
                        if (!value.isBlank()) {
                            changedFields.add(value);
                        }
                    }
                }
                int changedCount = details.path("changed_count").asInt(changedFields.size());
                Long configMtimeMs = null;
                JsonNode configMtimeNode = details.get("config_mtime_ms");
                if (configMtimeNode != null && configMtimeNode.isNumber()) {
                    configMtimeMs = configMtimeNode.asLong();
                }
                String source = details.path("source").asText("");
                if (source.isBlank()) {
                    source = null;
                }
                String configPath = details.path("config").asText("");
                if (configPath.isBlank()) {
                    configPath = null;
                }
                out.add(new SettingsHistoryEntry(
                        node.path("timestamp").asText(""),
                        node.path("result").asText(""),
                        details.path("changed").asBoolean(false),
                        changedCount,
                        changedFields,
                        configMtimeMs,
                        source,
                        configPath
                ));
            }
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "settings.history",
                    "cli",
                    "runtime/settings",
                    "ok",
                    null,
                    null,
                    null,
                    null,
                    Map.of("limit", safeLimit, "returned", out.size())
            ));
            return out;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read settings history", e);
        }
    }

    public ReplicationExportOutcome exportReplicationEnvelope(long sinceMs, int limit, String outputPath) {
        return exportReplicationEnvelope(sinceMs, limit, outputPath, "");
    }

    public ReplicationExportOutcome exportReplicationEnvelope(
            long sinceMs,
            int limit,
            String outputPath,
            String sourceNodeId
    ) {
        long safeSinceMs = Math.max(0L, sinceMs);
        int safeLimit = Math.max(1, limit);
        long generatedAtMs = Instant.now().toEpochMilli();
        long clusterEpoch = parseLongOrDefault(taskStore.clusterEpoch().value(), 1L);
        String normalizedSourceNodeId = sourceNodeId == null ? "" : sourceNodeId.trim();

        List<TaskStore.TaskDelta> taskDeltas = taskStore.listTaskDeltas(safeSinceMs, safeLimit);
        List<TaskStore.StepDelta> stepDeltas = taskStore.listStepDeltas(safeSinceMs, safeLimit);
        List<TaskStore.MessageDelta> messageDeltas = taskStore.listMessageDeltas(safeSinceMs, safeLimit);

        long cursorMaxMs = safeSinceMs;
        cursorMaxMs = Math.max(cursorMaxMs, maxTaskDeltaUpdatedAt(taskDeltas));
        cursorMaxMs = Math.max(cursorMaxMs, maxStepDeltaUpdatedAt(stepDeltas));
        cursorMaxMs = Math.max(cursorMaxMs, maxMessageDeltaUpdatedAt(messageDeltas));

        Path out = Paths.get(outputPath).toAbsolutePath().normalize();
        Map<String, Object> envelope = new LinkedHashMap<>();
        envelope.put("schema_version", REPLICATION_SCHEMA_VERSION);
        envelope.put("namespace", config.namespace());
        envelope.put("generated_at_ms", generatedAtMs);
        envelope.put("cluster_epoch", clusterEpoch);
        envelope.put("source_node_id", normalizedSourceNodeId);
        envelope.put("since_ms", safeSinceMs);
        envelope.put("limit", safeLimit);
        envelope.put("cursor_max_ms", cursorMaxMs);
        envelope.put("task_deltas", taskDeltas);
        envelope.put("step_deltas", stepDeltas);
        envelope.put("message_deltas", messageDeltas);
        envelope.put("counts", Map.of(
                "tasks", taskDeltas.size(),
                "steps", stepDeltas.size(),
                "messages", messageDeltas.size()
        ));

        try {
            if (out.getParent() != null) {
                Files.createDirectories(out.getParent());
            }
            Files.writeString(out, Jsons.toJson(envelope), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to export replication envelope", e);
        }

        auditLogger.log(AuditLogger.AuditEvent.of(
                "replication.export",
                "cli",
                "runtime/replication",
                "ok",
                null,
                null,
                null,
                null,
                Map.ofEntries(
                        Map.entry("schema_version", REPLICATION_SCHEMA_VERSION),
                        Map.entry("namespace", config.namespace()),
                        Map.entry("since_ms", safeSinceMs),
                        Map.entry("limit", safeLimit),
                        Map.entry("cursor_max_ms", cursorMaxMs),
                        Map.entry("source_node_id", normalizedSourceNodeId),
                        Map.entry("task_deltas", taskDeltas.size()),
                        Map.entry("step_deltas", stepDeltas.size()),
                        Map.entry("message_deltas", messageDeltas.size()),
                        Map.entry("cluster_epoch", clusterEpoch),
                        Map.entry("output", out.toString())
                )
        ));
        return new ReplicationExportOutcome(
                REPLICATION_SCHEMA_VERSION,
                safeSinceMs,
                safeLimit,
                cursorMaxMs,
                taskDeltas.size(),
                stepDeltas.size(),
                messageDeltas.size(),
                clusterEpoch,
                generatedAtMs,
                normalizedSourceNodeId,
                out.toString()
        );
    }

    public ReplicationImportOutcome importReplicationEnvelope(String inputPath, String actor) {
        if (inputPath == null || inputPath.isBlank()) {
            throw new IllegalArgumentException("inputPath must not be blank");
        }
        String normalizedActor = actor == null || actor.isBlank() ? "cli" : actor.trim();
        Path in = Paths.get(inputPath).toAbsolutePath().normalize();
        JsonNode envelope;
        try {
            envelope = Jsons.mapper().readTree(Files.readString(in, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException("Failed to read replication envelope: " + in, e);
        }
        String schemaVersion = readTextField(envelope, "schema_version", "schemaVersion");
        if (schemaVersion.isBlank()) {
            throw new IllegalArgumentException("Replication envelope missing schema_version");
        }
        if (!REPLICATION_SCHEMA_VERSION.equals(schemaVersion)) {
            throw new IllegalArgumentException("Unsupported replication schema version: " + schemaVersion);
        }
        String envelopeNamespace = readTextField(envelope, "namespace", "namespace");
        if (envelopeNamespace.isBlank()) {
            envelopeNamespace = RelayMeshConfig.DEFAULT_NAMESPACE;
        }
        if (!config.namespace().equalsIgnoreCase(envelopeNamespace)) {
            throw new IllegalArgumentException("Replication envelope namespace mismatch: envelope="
                    + envelopeNamespace + ", runtime=" + config.namespace());
        }
        long sinceMs = readLongField(envelope, "since_ms", "sinceMs", 0L);
        int limit = readIntField(envelope, "limit", "limit", 0);
        long cursorMaxMs = readLongField(envelope, "cursor_max_ms", "cursorMaxMs", sinceMs);
        long sourceClusterEpoch = readLongField(envelope, "cluster_epoch", "clusterEpoch", 1L);
        String sourceNodeId = readTextField(envelope, "source_node_id", "sourceNodeId");
        long localClusterEpoch = parseLongOrDefault(taskStore.clusterEpoch().value(), 1L);

        List<TaskStore.TaskDelta> taskDeltas = parseTaskDeltas(readField(envelope, "task_deltas", "taskDeltas"));
        List<TaskStore.StepDelta> rawStepDeltas = parseStepDeltas(readField(envelope, "step_deltas", "stepDeltas"));
        List<TaskStore.StepDelta> stepDeltas = normalizeIncomingStepDeltas(rawStepDeltas);
        List<TaskStore.MessageDelta> rawMessageDeltas = parseMessageDeltas(readField(envelope, "message_deltas", "messageDeltas"));
        List<TaskStore.MessageDelta> messageDeltas = normalizeIncomingMessageDeltas(rawMessageDeltas, stepDeltas);

        TaskStore.ReplicationApplySummary apply = taskStore.applyReplicationDeltas(
                taskDeltas,
                stepDeltas,
                messageDeltas,
                sourceClusterEpoch,
                localClusterEpoch,
                sourceNodeId
        );
        ReplicationQueueMaterializeOutcome queue = materializeImportedQueue(messageDeltas, stepDeltas);
        long importLagMs = Math.max(0L, Instant.now().toEpochMilli() - Math.max(0L, cursorMaxMs));
        long importTotal = replicationImportTotal.incrementAndGet();
        long tieResolvedTotal = replicationTieResolvedTotal.addAndGet(Math.max(0, apply.tieResolved()));
        long tieKeptLocalTotal = replicationTieKeptLocalTotal.addAndGet(Math.max(0, apply.tieKeptLocal()));
        replicationLastImportLagMs.set(importLagMs);

        auditLogger.log(AuditLogger.AuditEvent.of(
                "replication.import",
                normalizedActor,
                "runtime/replication",
                "ok",
                null,
                null,
                null,
                null,
                Map.ofEntries(
                        Map.entry("schema_version", schemaVersion),
                        Map.entry("namespace", config.namespace()),
                        Map.entry("source_cluster_epoch", sourceClusterEpoch),
                        Map.entry("local_cluster_epoch", localClusterEpoch),
                        Map.entry("source_node_id", sourceNodeId == null ? "" : sourceNodeId),
                        Map.entry("since_ms", sinceMs),
                        Map.entry("limit", limit),
                        Map.entry("cursor_max_ms", cursorMaxMs),
                        Map.entry("import_lag_ms", importLagMs),
                        Map.entry("import_total", importTotal),
                        Map.entry("task_deltas", taskDeltas.size()),
                        Map.entry("step_deltas", stepDeltas.size()),
                        Map.entry("message_deltas", messageDeltas.size()),
                        Map.entry("task_applied", apply.taskApplied()),
                        Map.entry("step_applied", apply.stepApplied()),
                        Map.entry("message_applied", apply.messageApplied()),
                        Map.entry("stale_skipped", apply.staleSkipped()),
                        Map.entry("tie_resolved", apply.tieResolved()),
                        Map.entry("tie_kept_local", apply.tieKeptLocal()),
                        Map.entry("tie_resolved_total", tieResolvedTotal),
                        Map.entry("tie_kept_local_total", tieKeptLocalTotal),
                        Map.entry("queue_enqueued", queue.enqueued()),
                        Map.entry("queue_skipped", queue.skipped()),
                        Map.entry("input", in.toString())
                )
        ));
        return new ReplicationImportOutcome(
                schemaVersion,
                in.toString(),
                sinceMs,
                limit,
                cursorMaxMs,
                sourceClusterEpoch,
                localClusterEpoch,
                taskDeltas.size(),
                stepDeltas.size(),
                messageDeltas.size(),
                apply.taskApplied(),
                apply.stepApplied(),
                apply.messageApplied(),
                apply.staleSkipped(),
                apply.tieResolved(),
                apply.tieKeptLocal(),
                queue.enqueued(),
                queue.skipped(),
                importLagMs
        );
    }

    public ConvergenceReportOutcome convergenceReport(int sinceHours, int limit, String outputPath) {
        int safeHours = Math.max(1, sinceHours);
        int safeLimit = Math.max(1, limit);
        long nowMs = Instant.now().toEpochMilli();
        long sinceMs = nowMs - Duration.ofHours(safeHours).toMillis();
        Path auditFile = config.auditRoot().resolve("audit.log");
        if (!Files.exists(auditFile)) {
            ConvergenceReportOutcome empty = new ConvergenceReportOutcome(
                    safeHours,
                    safeLimit,
                    sinceMs,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0L,
                    0L,
                    0L,
                    Map.of(),
                    nowMs,
                    null
            );
            maybeWriteConvergenceReport(outputPath, empty);
            return empty;
        }
        int imports = 0;
        int tieResolved = 0;
        int tieKeptLocal = 0;
        int queueEnqueued = 0;
        int staleSkipped = 0;
        int appliedRows = 0;
        List<Long> lagSamples = new ArrayList<>();
        Map<String, Integer> sourceNodeCounts = new LinkedHashMap<>();

        try {
            List<String> all = Files.readAllLines(auditFile, StandardCharsets.UTF_8);
            for (int i = all.size() - 1; i >= 0 && imports < safeLimit; i--) {
                JsonNode node;
                try {
                    node = Jsons.mapper().readTree(all.get(i));
                } catch (Exception ignored) {
                    continue;
                }
                if (!"replication.import".equals(node.path("action").asText(""))) {
                    continue;
                }
                Instant ts = parseInstantSafe(node.path("timestamp").asText(""));
                if (ts == null || ts.toEpochMilli() < sinceMs) {
                    continue;
                }
                JsonNode details = node.path("details");
                imports++;
                tieResolved += details.path("tie_resolved").asInt(0);
                tieKeptLocal += details.path("tie_kept_local").asInt(0);
                queueEnqueued += details.path("queue_enqueued").asInt(0);
                staleSkipped += details.path("stale_skipped").asInt(0);
                appliedRows += details.path("task_applied").asInt(0)
                        + details.path("step_applied").asInt(0)
                        + details.path("message_applied").asInt(0);
                long lag = details.path("import_lag_ms").asLong(-1L);
                if (lag >= 0L) {
                    lagSamples.add(lag);
                }
                String sourceNodeId = details.path("source_node_id").asText("");
                if (sourceNodeId.isBlank()) {
                    sourceNodeId = "_unknown";
                }
                sourceNodeCounts.merge(sourceNodeId, 1, Integer::sum);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to build convergence report from audit log", e);
        }
        Collections.sort(lagSamples);
        long lagAvgMs = 0L;
        if (!lagSamples.isEmpty()) {
            long total = 0L;
            for (Long lag : lagSamples) {
                total += lag;
            }
            lagAvgMs = total / lagSamples.size();
        }
        long lagP95Ms = percentileFromSorted(lagSamples, 95.0);
        long lagMaxMs = lagSamples.isEmpty() ? 0L : lagSamples.get(lagSamples.size() - 1);

        ConvergenceReportOutcome report = new ConvergenceReportOutcome(
                safeHours,
                safeLimit,
                sinceMs,
                imports,
                appliedRows,
                tieResolved,
                tieKeptLocal,
                queueEnqueued,
                staleSkipped,
                lagAvgMs,
                lagP95Ms,
                lagMaxMs,
                sourceNodeCounts,
                nowMs,
                outputPath == null || outputPath.isBlank()
                        ? null
                        : Paths.get(outputPath).toAbsolutePath().normalize().toString()
        );
        maybeWriteConvergenceReport(outputPath, report);
        auditLogger.log(AuditLogger.AuditEvent.of(
                "convergence.report",
                "cli",
                "runtime/convergence",
                "ok",
                null,
                null,
                null,
                null,
                Map.ofEntries(
                        Map.entry("since_hours", safeHours),
                        Map.entry("limit", safeLimit),
                        Map.entry("imports", report.imports()),
                        Map.entry("applied_rows", report.appliedRows()),
                        Map.entry("tie_resolved", report.tieResolved()),
                        Map.entry("tie_kept_local", report.tieKeptLocal()),
                        Map.entry("queue_enqueued", report.queueEnqueued()),
                        Map.entry("stale_skipped", report.staleSkipped()),
                        Map.entry("lag_avg_ms", report.lagAvgMs()),
                        Map.entry("lag_p95_ms", report.lagP95Ms()),
                        Map.entry("lag_max_ms", report.lagMaxMs()),
                        Map.entry("source_nodes", report.sourceNodeCounts().size()),
                        Map.entry("output", report.outputPath() == null ? "" : report.outputPath())
                )
        ));
        return report;
    }

    public TaskExportOutcome exportTask(String taskId, String outputPath) {
        Optional<TaskView> task = getTask(taskId);
        Optional<TaskStore.WorkflowDetail> wf = workflow(taskId);
        if (task.isEmpty() || wf.isEmpty()) {
            return new TaskExportOutcome(taskId, false, 0, 0, "task not found");
        }
        Path out = Paths.get(outputPath).toAbsolutePath().normalize();
        List<String> auditRows = auditByTaskId(taskId);
        Map<String, Object> payload = new HashMap<>();
        payload.put("task", task.get());
        payload.put("workflow", wf.get());
        payload.put("audit_events", auditRows);
        payload.put("exported_at", Instant.now().toString());
        try {
            if (out.getParent() != null) Files.createDirectories(out.getParent());
            Files.writeString(out, Jsons.toJson(payload), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to export task: " + taskId, e);
        }
        auditLogger.log(AuditLogger.AuditEvent.of(
                "task.export",
                "cli",
                "task/" + taskId,
                "ok",
                task.get().traceId(),
                null,
                taskId,
                null,
                Map.of("output", out.toString(), "audit_rows", auditRows.size())
        ));
        return new TaskExportOutcome(taskId, true, wf.get().steps().size(), auditRows.size(), out.toString());
    }

    public HealthOutcome health() {
        boolean dbOk;
        try (Connection ignored = database.openConnection()) {
            dbOk = true;
        } catch (Exception e) {
            dbOk = false;
        }
        boolean inbox = Files.isDirectory(config.inboxRoot());
        boolean processing = Files.isDirectory(config.processingDir());
        boolean meta = Files.isDirectory(config.metaDir());
        boolean payload = Files.isDirectory(config.payloadDir());
        boolean audit = Files.isDirectory(config.auditRoot());
        long diskFreeBytes = currentDiskFreeBytes();
        long minFreeDiskBytes = Math.max(0L, runtimeSettings.minFreeDiskBytes());
        boolean diskPressure = minFreeDiskBytes > 0L
                && diskFreeBytes >= 0L
                && diskFreeBytes < minFreeDiskBytes;
        boolean ok = dbOk && inbox && processing && meta && payload && audit && !diskPressure;
        HealthOutcome out = new HealthOutcome(
                ok,
                dbOk,
                inbox,
                processing,
                meta,
                payload,
                audit,
                diskFreeBytes,
                minFreeDiskBytes,
                diskPressure,
                Instant.now().toString()
        );
        auditLogger.log(AuditLogger.AuditEvent.of(
                "runtime.health",
                "cli",
                "runtime/health",
                ok ? "ok" : "degraded",
                null,
                null,
                null,
                null,
                Map.of(
                        "db_ok", dbOk,
                        "inbox_ok", inbox,
                        "processing_ok", processing,
                        "disk_free_bytes", diskFreeBytes,
                        "min_free_disk_bytes", minFreeDiskBytes,
                        "disk_pressure", diskPressure
                )
        ));
        return out;
    }

    private long currentDiskFreeBytes() {
        try {
            return config.rootDir().toFile().getUsableSpace();
        } catch (Exception ignored) {
            return -1L;
        }
    }

    private int dispatchDueRetries(int limit) {
        long now = Instant.now().toEpochMilli();
        int count = 0;
        for (TaskStore.RetryDispatch dispatch : taskStore.claimDueRetries(now, limit)) {
            String msgId = "msg_" + UUID.randomUUID();
            Path metaPath = config.metaDir().resolve(msgId + ".meta.json");
            MessageEnvelope envelope = new MessageEnvelope(
                    msgId,
                    dispatch.taskId(),
                    dispatch.stepId(),
                    "system",
                    dispatch.agentId(),
                    dispatch.priority(),
                    now,
                    dispatch.payloadPath(),
                    dispatch.nextAttempt()
            );
            TaskStore.RetryMessage retryMessage = new TaskStore.RetryMessage(
                    msgId,
                    dispatch.taskId(),
                    dispatch.stepId(),
                    dispatch.agentId(),
                    dispatch.priority(),
                    metaPath.toString(),
                    dispatch.payloadPath(),
                    now
            );
            taskStore.insertRetryMessage(retryMessage);
            fileBus.enqueueMetaOnly(envelope);
            count++;
        }
        return count;
    }

    private int dispatchReadySteps(List<TaskStore.ReadyStepDispatch> readySteps, long now) {
        int count = 0;
        for (TaskStore.ReadyStepDispatch dispatch : readySteps) {
            String msgId = "msg_" + UUID.randomUUID();
            Path metaPath = config.metaDir().resolve(msgId + ".meta.json");
            MessageEnvelope envelope = new MessageEnvelope(
                    msgId,
                    dispatch.taskId(),
                    dispatch.stepId(),
                    "system",
                    dispatch.agentId(),
                    dispatch.priority(),
                    now,
                    dispatch.payloadPath(),
                    dispatch.nextAttempt()
            );
            TaskStore.RetryMessage message = new TaskStore.RetryMessage(
                    msgId,
                    dispatch.taskId(),
                    dispatch.stepId(),
                    dispatch.agentId(),
                    dispatch.priority(),
                    metaPath.toString(),
                    dispatch.payloadPath(),
                    now
            );
            taskStore.insertRetryMessage(message);
            fileBus.enqueueMetaOnly(envelope);
            count++;
        }
        return count;
    }

    private void validateWorkflow(List<WorkflowStepSpec> stepSpecs) {
        Set<String> ids = new HashSet<>();
        for (WorkflowStepSpec step : stepSpecs) {
            if (step.id() == null || step.id().isBlank()) {
                throw new IllegalArgumentException("Workflow step id cannot be empty");
            }
            if (!ids.add(step.id())) {
                throw new IllegalArgumentException("Duplicate workflow step id: " + step.id());
            }
        }
        Map<String, List<String>> graph = new HashMap<>();
        for (WorkflowStepSpec step : stepSpecs) {
            graph.put(step.id(), new ArrayList<>(step.dependsOn()));
        }
        for (WorkflowStepSpec step : stepSpecs) {
            for (String dep : step.dependsOn()) {
                if (!ids.contains(dep)) {
                    throw new IllegalArgumentException("Unknown dependsOn step: " + dep);
                }
            }
        }
        Set<String> visiting = new HashSet<>();
        Set<String> visited = new HashSet<>();
        for (String id : ids) {
            dfsCycleCheck(id, graph, visiting, visited);
        }
    }

    private void dfsCycleCheck(String id, Map<String, List<String>> graph, Set<String> visiting, Set<String> visited) {
        if (visited.contains(id)) return;
        if (!visiting.add(id)) {
            throw new IllegalArgumentException("Workflow contains cycle at step: " + id);
        }
        for (String dep : graph.getOrDefault(id, List.of())) {
            dfsCycleCheck(dep, graph, visiting, visited);
        }
        visiting.remove(id);
        visited.add(id);
    }

    private void applyFailureDisposition(
            FileBus.ClaimedMessage claimed,
            String msgId,
            TaskStore.FailureResolution resolution
    ) {
        long now = Instant.now().toEpochMilli();
        if (resolution.outcome() == TaskStore.FailureOutcome.RETRY_SCHEDULED) {
            taskStore.markMessageState(msgId, MessageState.FAILED, now);
            fileBus.completeRetry(claimed);
            return;
        }
        taskStore.markMessageState(msgId, MessageState.DEAD, now);
        fileBus.completeFailure(claimed);
    }

    private String workerMessageForFailure(TaskStore.FailureResolution resolution) {
        return switch (resolution.outcome()) {
            case RETRY_SCHEDULED ->
                    "Retry scheduled at " + resolution.nextRetryAtMs() + " (attempt=" + resolution.attempt() + ")";
            case DEAD_LETTERED ->
                    "Moved to dead letter after attempt " + resolution.attempt();
            case STALE_LEASE ->
                    "Lease lost, failure result ignored";
        };
    }

    private void registerDefaultAgents() {
        agentRegistry.register(new EchoAgent());
        agentRegistry.register(new FailAgent());
    }

    private void enforceAdmissionGate(String source, String priorityRaw, int requestedSteps) {
        RuntimeSettings settings = runtimeSettings;
        int safeSteps = Math.max(1, requestedSteps);
        int queueDepth = countFiles(config.inboxHigh(), "*.meta.json")
                + countFiles(config.inboxNormal(), "*.meta.json")
                + countFiles(config.inboxLow(), "*.meta.json")
                + countFiles(config.processingDir(), "*.meta.json");
        int runningSteps = taskStore.countStepsByStatus(TaskStatus.RUNNING.name());
        long nowMs = Instant.now().toEpochMilli();
        String priority = priorityRaw == null ? "" : priorityRaw;

        if (settings.maxIngressQueueDepth() > 0 && (queueDepth + safeSteps) > settings.maxIngressQueueDepth()) {
            rejectAdmission(
                    "queue_depth_limit",
                    "Ingress queue depth limit reached",
                    source,
                    priority,
                    queueDepth,
                    runningSteps,
                    settings.maxIngressQueueDepth(),
                    settings.maxRunningSteps(),
                    settings.submitRateLimitPerMin()
            );
        }
        if (settings.maxRunningSteps() > 0 && runningSteps >= settings.maxRunningSteps()) {
            rejectAdmission(
                    "running_step_limit",
                    "Running step limit reached",
                    source,
                    priority,
                    queueDepth,
                    runningSteps,
                    settings.maxIngressQueueDepth(),
                    settings.maxRunningSteps(),
                    settings.submitRateLimitPerMin()
            );
        }
        if (settings.submitRateLimitPerMin() > 0) {
            long windowStartMs = nowMs - (nowMs % 60_000L);
            long inWindow;
            synchronized (submitRateLock) {
                if (submitWindowStartMs != windowStartMs) {
                    submitWindowStartMs = windowStartMs;
                    submitWindowCount.set(0L);
                }
                inWindow = submitWindowCount.addAndGet(safeSteps);
            }
            if (inWindow > settings.submitRateLimitPerMin()) {
                rejectAdmission(
                        "submit_rate_limit",
                        "Submit rate limit exceeded",
                        source,
                        priority,
                        queueDepth,
                        runningSteps,
                        settings.maxIngressQueueDepth(),
                        settings.maxRunningSteps(),
                        settings.submitRateLimitPerMin()
                );
            }
        }
    }

    private void rejectAdmission(
            String reason,
            String message,
            String source,
            String priority,
            int queueDepth,
            int runningSteps,
            int maxQueueDepth,
            int maxRunningSteps,
            int submitRateLimitPerMin
    ) {
        long totalRejected = submitRejectedTotal.incrementAndGet();
        auditLogger.log(AuditLogger.AuditEvent.of(
                "admission.reject",
                "runtime",
                "runtime/admission",
                reason,
                null,
                null,
                null,
                null,
                Map.ofEntries(
                        Map.entry("reason", reason),
                        Map.entry("source", source == null ? "" : source),
                        Map.entry("priority", priority == null ? "" : priority),
                        Map.entry("queue_depth", queueDepth),
                        Map.entry("running_steps", runningSteps),
                        Map.entry("max_queue_depth", maxQueueDepth),
                        Map.entry("max_running_steps", maxRunningSteps),
                        Map.entry("submit_rate_limit_per_min", submitRateLimitPerMin),
                        Map.entry("rejected_total", totalRejected)
                )
        ));
        throw new IllegalStateException(message);
    }

    private String loadOrCreateAuditSigningSecret(Path keyFile) {
        try {
            if (keyFile.getParent() != null) {
                Files.createDirectories(keyFile.getParent());
            }
            if (Files.exists(keyFile)) {
                String existing = Files.readString(keyFile, StandardCharsets.UTF_8).trim();
                if (!existing.isBlank()) {
                    return existing;
                }
            }
            byte[] random = new byte[32];
            new SecureRandom().nextBytes(random);
            String generated = Base64.getEncoder().encodeToString(random);
            Files.writeString(keyFile, generated, StandardCharsets.UTF_8);
            return generated;
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize audit signing secret: " + keyFile, e);
        }
    }

    private SettingsReloadOutcome loadRuntimeSettings(boolean force) {
        RuntimeSettings defaults = RuntimeSettings.defaults();
        Path cfg = config.rootDir().resolve("relaymesh-settings.json");
        long checkedAtMs = Instant.now().toEpochMilli();
        long mtime = resolveFileMtimeMs(cfg);
        if (!force && mtime == runtimeSettingsFileMtimeMs) {
            RuntimeSettings current = runtimeSettings;
            return new SettingsReloadOutcome(
                    false,
                    mtime >= 0L,
                    cfg.toString(),
                    current.toView(),
                    "unchanged",
                    checkedAtMs,
                    List.of()
            );
        }
        if (mtime < 0L) {
            RuntimeSettings current = runtimeSettings;
            runtimeSettings = defaults;
            runtimeSettingsFileMtimeMs = -1L;
            boolean changed = !defaults.equals(current);
            List<String> changedFields = changed ? diffSettingFields(current, defaults) : List.of();
            if (changed) {
                auditLogger.log(AuditLogger.AuditEvent.of(
                        "runtime.settings.load",
                        "system",
                        "runtime/settings",
                        "ok_default",
                        null,
                        null,
                        null,
                        null,
                        Map.ofEntries(
                                Map.entry("config", cfg.toString()),
                                Map.entry("source", "defaults"),
                                Map.entry("changed", true),
                                Map.entry("changed_count", changedFields.size()),
                                Map.entry("changed_fields", changedFields)
                        )
                ));
            }
            return new SettingsReloadOutcome(
                    changed,
                    false,
                    cfg.toString(),
                    defaults.toView(),
                    "defaults",
                    checkedAtMs,
                    changedFields
            );
        }
        try {
            RuntimeSettingsFile file = Jsons.mapper().readValue(cfg.toFile(), RuntimeSettingsFile.class);
            RuntimeSettings resolved = RuntimeSettings.fromFile(file, defaults);
            RuntimeSettings previous = runtimeSettings;
            runtimeSettings = resolved;
            runtimeSettingsFileMtimeMs = mtime;
            boolean changed = !resolved.equals(previous);
            List<String> changedFields = changed ? diffSettingFields(previous, resolved) : List.of();
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "runtime.settings.load",
                    "system",
                    "runtime/settings",
                    changed ? "reloaded" : "ok",
                    null,
                    null,
                    null,
                    null,
                    Map.ofEntries(
                            Map.entry("config", cfg.toString()),
                            Map.entry("changed", changed),
                            Map.entry("changed_count", changedFields.size()),
                            Map.entry("changed_fields", changedFields),
                            Map.entry("config_mtime_ms", mtime)
                    )
            ));
            return new SettingsReloadOutcome(
                    changed,
                    true,
                    cfg.toString(),
                    resolved.toView(),
                    changed ? "reloaded" : "unchanged_content",
                    checkedAtMs,
                    changedFields
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to load runtime settings: " + cfg, e);
        }
    }

    private long resolveFileMtimeMs(Path path) {
        if (!Files.exists(path)) {
            return -1L;
        }
        try {
            return Files.getLastModifiedTime(path).toMillis();
        } catch (IOException e) {
            throw new RuntimeException("Failed to read settings mtime: " + path, e);
        }
    }

    private List<String> diffSettingFields(RuntimeSettings before, RuntimeSettings after) {
        if (before == null || after == null) {
            return List.of();
        }
        List<String> changed = new ArrayList<>();
        if (before.leaseTimeoutMs() != after.leaseTimeoutMs()) changed.add("leaseTimeoutMs");
        if (before.suspectAfterMs() != after.suspectAfterMs()) changed.add("suspectAfterMs");
        if (before.deadAfterMs() != after.deadAfterMs()) changed.add("deadAfterMs");
        if (before.suspectRecoverMinMs() != after.suspectRecoverMinMs()) changed.add("suspectRecoverMinMs");
        if (before.deadRecoverMinMs() != after.deadRecoverMinMs()) changed.add("deadRecoverMinMs");
        if (before.pauseWorkerWhenLocalNotAlive() != after.pauseWorkerWhenLocalNotAlive()) changed.add("pauseWorkerWhenLocalNotAlive");
        if (before.maxAttempts() != after.maxAttempts()) changed.add("maxAttempts");
        if (before.baseBackoffMs() != after.baseBackoffMs()) changed.add("baseBackoffMs");
        if (before.maxBackoffMs() != after.maxBackoffMs()) changed.add("maxBackoffMs");
        if (before.retryDispatchLimit() != after.retryDispatchLimit()) changed.add("retryDispatchLimit");
        if (before.gossipFanout() != after.gossipFanout()) changed.add("gossipFanout");
        if (before.gossipPacketTtl() != after.gossipPacketTtl()) changed.add("gossipPacketTtl");
        if (before.gossipSyncSampleSize() != after.gossipSyncSampleSize()) changed.add("gossipSyncSampleSize");
        if (before.gossipDedupWindowMs() != after.gossipDedupWindowMs()) changed.add("gossipDedupWindowMs");
        if (before.gossipDedupMaxEntries() != after.gossipDedupMaxEntries()) changed.add("gossipDedupMaxEntries");
        if (!safeString(before.gossipSharedSecret()).equals(safeString(after.gossipSharedSecret()))) changed.add("gossipSharedSecret");
        if (before.meshPruneOlderThanMs() != after.meshPruneOlderThanMs()) changed.add("meshPruneOlderThanMs");
        if (before.meshPruneLimit() != after.meshPruneLimit()) changed.add("meshPruneLimit");
        if (before.meshPruneIntervalMs() != after.meshPruneIntervalMs()) changed.add("meshPruneIntervalMs");
        if (before.minFreeDiskBytes() != after.minFreeDiskBytes()) changed.add("minFreeDiskBytes");
        if (before.maxIngressQueueDepth() != after.maxIngressQueueDepth()) changed.add("maxIngressQueueDepth");
        if (before.maxRunningSteps() != after.maxRunningSteps()) changed.add("maxRunningSteps");
        if (before.submitRateLimitPerMin() != after.submitRateLimitPerMin()) changed.add("submitRateLimitPerMin");
        if (before.sloQueueLagTargetMs() != after.sloQueueLagTargetMs()) changed.add("sloQueueLagTargetMs");
        if (before.sloStepP95TargetMs() != after.sloStepP95TargetMs()) changed.add("sloStepP95TargetMs");
        if (before.sloStepP99TargetMs() != after.sloStepP99TargetMs()) changed.add("sloStepP99TargetMs");
        if (before.sloDeadLetterGrowthTarget1h() != after.sloDeadLetterGrowthTarget1h()) changed.add("sloDeadLetterGrowthTarget1h");
        if (before.sloReplicationLagTargetMs() != after.sloReplicationLagTargetMs()) changed.add("sloReplicationLagTargetMs");
        return changed;
    }

    private void loadRetryPolicies() {
        retryPolicyByAgent.clear();
        Path cfg = config.rootDir().resolve("agents").resolve("retry-policies.json");
        if (!Files.exists(cfg)) {
            return;
        }
        try {
            RetryPolicyFile file = Jsons.mapper().readValue(cfg.toFile(), RetryPolicyFile.class);
            if (file == null || file.agents() == null || file.agents().isEmpty()) {
                return;
            }
            int loaded = 0;
            int skipped = 0;
            for (RetryPolicySpec spec : file.agents()) {
                if (spec == null || spec.id() == null || spec.id().isBlank()) {
                    skipped++;
                    continue;
                }
                Integer maxAttempts = spec.maxAttempts();
                Long baseBackoffMs = spec.baseBackoffMs();
                Long maxBackoffMs = spec.maxBackoffMs();
                if (maxAttempts == null || maxAttempts <= 0
                        || baseBackoffMs == null || baseBackoffMs <= 0
                        || maxBackoffMs == null || maxBackoffMs <= 0
                        || baseBackoffMs > maxBackoffMs) {
                    skipped++;
                    continue;
                }
                retryPolicyByAgent.put(spec.id(), new TaskStore.RetryPolicy(maxAttempts, baseBackoffMs, maxBackoffMs));
                loaded++;
            }
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "retry.policy.load",
                    "system",
                    "runtime/retry-policies",
                    "ok",
                    null,
                    null,
                    null,
                    null,
                    Map.of("config", cfg.toString(), "loaded", loaded, "skipped", skipped)
            ));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load retry policy config: " + cfg, e);
        }
    }

    private TaskStore.RetryPolicy retryPolicyForAgent(String agentId) {
        TaskStore.RetryPolicy policy = retryPolicyByAgent.get(agentId);
        if (policy != null) {
            return policy;
        }
        RuntimeSettings settings = runtimeSettings;
        return new TaskStore.RetryPolicy(
                settings.maxAttempts(),
                settings.baseBackoffMs(),
                settings.maxBackoffMs()
        );
    }

    private void registerConfiguredScriptAgents() {
        Path cfg = config.rootDir().resolve("agents").resolve("scripts.json");
        if (!Files.exists(cfg)) {
            return;
        }
        try {
            ScriptAgentFile file = Jsons.mapper().readValue(cfg.toFile(), ScriptAgentFile.class);
            if (file == null || file.agents() == null || file.agents().isEmpty()) {
                return;
            }
            int loaded = 0;
            int skipped = 0;
            for (ScriptAgentSpec spec : file.agents()) {
                if (spec == null || spec.id() == null || spec.id().isBlank() || spec.command() == null || spec.command().isEmpty()) {
                    skipped++;
                    continue;
                }
                try {
                    long timeoutMs = spec.timeoutMs() == null ? 60_000L : spec.timeoutMs();
                    agentRegistry.register(new ScriptAgent(spec.id(), resolveScriptCommand(spec.command()), timeoutMs));
                    loaded++;
                } catch (Exception e) {
                    skipped++;
                    auditLogger.log(AuditLogger.AuditEvent.of(
                            "agent.script.register",
                            "system",
                            "runtime/agents",
                            "invalid_spec",
                            null,
                            null,
                            null,
                            null,
                            Map.of("agent_id", spec.id(), "error", e.getMessage())
                    ));
                }
            }
            auditLogger.log(AuditLogger.AuditEvent.of(
                    "agent.script.load",
                    "system",
                    "runtime/agents",
                    "ok",
                    null,
                    null,
                    null,
                    null,
                    Map.of("config", cfg.toString(), "loaded", loaded, "skipped", skipped)
            ));
        } catch (IOException e) {
            throw new RuntimeException("Failed to load script agent config: " + cfg, e);
        }
    }

    private List<String> resolveScriptCommand(List<String> rawCommand) {
        List<String> resolved = new ArrayList<>(rawCommand.size());
        for (String token : rawCommand) {
            if (token == null || token.isBlank()) {
                continue;
            }
            Path candidate = config.rootDir().resolve(token).normalize();
            if (Files.exists(candidate)) {
                resolved.add(candidate.toString());
            } else {
                resolved.add(token);
            }
        }
        if (resolved.isEmpty()) {
            throw new IllegalArgumentException("script command became empty after normalization");
        }
        return resolved;
    }

    private List<SeedEndpoint> parseSeedEndpoints(List<String> rawSeeds) {
        if (rawSeeds == null || rawSeeds.isEmpty()) {
            return List.of();
        }
        Set<String> seen = new HashSet<>();
        List<SeedEndpoint> out = new ArrayList<>();
        for (String seed : rawSeeds) {
            if (seed == null || seed.isBlank()) {
                continue;
            }
            String trimmed = seed.trim();
            int idx = trimmed.lastIndexOf(':');
            if (idx <= 0 || idx == trimmed.length() - 1) {
                continue;
            }
            String host = trimmed.substring(0, idx).trim();
            String portText = trimmed.substring(idx + 1).trim();
            if (host.isEmpty()) {
                continue;
            }
            try {
                int port = Integer.parseInt(portText);
                if (port <= 0 || port > 65535) {
                    continue;
                }
                String dedupKey = host.toLowerCase(Locale.ROOT) + ":" + port;
                if (seen.add(dedupKey)) {
                    out.add(new SeedEndpoint(host, port));
                }
            } catch (NumberFormatException ignored) {
                // Skip invalid seed endpoint format.
            }
        }
        return out;
    }

    private List<GossipNodeState> buildGossipSyncNodes(String nodeId, long nowMs, int sampleSize) {
        int cap = Math.max(1, sampleSize);
        long staleCutoff = nowMs - Math.max(10_000L, runtimeSettings.deadAfterMs() * 2L);
        Map<String, Long> merged = new LinkedHashMap<>();
        merged.put(nodeId, nowMs);
        for (TaskStore.MeshNode node : taskStore.listNodes()) {
            if (node == null || node.nodeId() == null || node.nodeId().isBlank()) {
                continue;
            }
            long heartbeat = node.lastHeartbeatMs();
            if (heartbeat < staleCutoff) {
                continue;
            }
            merged.merge(node.nodeId(), heartbeat, Math::max);
        }
        List<GossipNodeState> snapshot = new ArrayList<>(merged.size());
        for (Map.Entry<String, Long> entry : merged.entrySet()) {
            snapshot.add(new GossipNodeState(entry.getKey(), entry.getValue()));
        }
        if (snapshot.size() <= cap) {
            return snapshot;
        }
        Collections.shuffle(snapshot);
        return snapshot.subList(0, cap);
    }

    private int mergeGossipNodes(List<GossipNodeState> nodes, long nowMs, RuntimeSettings settings) {
        if (nodes == null || nodes.isEmpty()) {
            return 0;
        }
        int merged = 0;
        // Accept only plausible heartbeat timestamps to avoid poisoning local membership with stale/future data.
        long staleCutoff = nowMs - Math.max(10_000L, settings.deadAfterMs() * 2L);
        for (GossipNodeState node : nodes) {
            if (node == null || node.nodeId() == null || node.nodeId().isBlank()) {
                continue;
            }
            long heartbeat = node.heartbeatMs();
            if (heartbeat <= 0 || heartbeat < staleCutoff || heartbeat > (nowMs + 300_000L)) {
                continue;
            }
            TaskStore.HeartbeatOutcome mergedHeartbeat = taskStore.heartbeatNodeIfNewer(
                    node.nodeId(),
                    heartbeat,
                    nowMs,
                    settings.suspectRecoverMinMs(),
                    settings.deadRecoverMinMs()
            );
            if (mergedHeartbeat.recoverySuppressed()) {
                membershipRecoverySuppressedTotal.incrementAndGet();
            }
            merged++;
        }
        return merged;
    }

    private List<SeedEndpoint> selectFanoutSeeds(List<SeedEndpoint> seeds, int fanout) {
        if (seeds == null || seeds.isEmpty()) {
            return List.of();
        }
        int safeFanout = Math.max(1, fanout);
        if (seeds.size() <= safeFanout) {
            return seeds;
        }
        List<SeedEndpoint> copy = new ArrayList<>(seeds);
        Collections.shuffle(copy);
        return copy.subList(0, safeFanout);
    }

    private List<SeedEndpoint> relayCandidates(List<SeedEndpoint> seeds, DatagramPacket incoming, int bindPort) {
        if (seeds == null || seeds.isEmpty()) {
            return List.of();
        }
        List<SeedEndpoint> out = new ArrayList<>();
        for (SeedEndpoint seed : seeds) {
            if (seed.port() == bindPort) {
                continue;
            }
            if (isSameEndpoint(seed, incoming.getAddress(), incoming.getPort())) {
                continue;
            }
            out.add(seed);
        }
        return out;
    }

    private boolean isSameEndpoint(SeedEndpoint seed, InetAddress addr, int port) {
        if (seed.port() != port) {
            return false;
        }
        try {
            InetAddress resolved = InetAddress.getByName(seed.host());
            return resolved.equals(addr);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean registerGossipMessage(String msgId, long nowMs, RuntimeSettings settings) {
        if (msgId == null || msgId.isBlank()) {
            return false;
        }
        long expireAt = nowMs + settings.gossipDedupWindowMs();
        Long existing = gossipDedupCache.get(msgId);
        if (existing != null && existing > nowMs) {
            return false;
        }
        gossipDedupCache.put(msgId, expireAt);
        maybeCleanupGossipDedup(nowMs, settings);
        return true;
    }

    private void maybeCleanupGossipDedup(long nowMs, RuntimeSettings settings) {
        long cleanupInterval = Math.max(5_000L, Math.min(settings.gossipDedupWindowMs(), 30_000L));
        int softCap = settings.gossipDedupMaxEntries() * 2;
        if ((nowMs - lastGossipDedupCleanupMs) < cleanupInterval && gossipDedupCache.size() <= softCap) {
            return;
        }
        synchronized (gossipDedupLock) {
            if ((nowMs - lastGossipDedupCleanupMs) < cleanupInterval && gossipDedupCache.size() <= softCap) {
                return;
            }
            for (Map.Entry<String, Long> entry : new ArrayList<>(gossipDedupCache.entrySet())) {
                if (entry.getValue() <= nowMs) {
                    gossipDedupCache.remove(entry.getKey(), entry.getValue());
                }
            }
            int maxEntries = Math.max(64, settings.gossipDedupMaxEntries());
            int oversize = gossipDedupCache.size() - maxEntries;
            if (oversize > 0) {
                List<Map.Entry<String, Long>> byExpire = new ArrayList<>(gossipDedupCache.entrySet());
                byExpire.sort(Map.Entry.comparingByValue());
                for (int i = 0; i < oversize && i < byExpire.size(); i++) {
                    Map.Entry<String, Long> victim = byExpire.get(i);
                    gossipDedupCache.remove(victim.getKey(), victim.getValue());
                }
            }
            lastGossipDedupCleanupMs = nowMs;
        }
    }

    private int purgeDoneDirectories(int retentionDays) {
        LocalDate cutoff = LocalDate.now().minusDays(retentionDays);
        int deleted = 0;
        try (Stream<Path> stream = Files.list(config.doneRoot())) {
            for (Path child : stream.toList()) {
                if (!Files.isDirectory(child)) {
                    continue;
                }
                LocalDate date = parseDoneDirectoryDate(child.getFileName().toString());
                if (date != null && date.isBefore(cutoff)) {
                    deleteRecursively(child);
                    deleted++;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to purge done directory", e);
        }
        return deleted;
    }

    private int purgeDeadFiles(long cutoffEpochMs) {
        int deleted = 0;
        try (Stream<Path> stream = Files.list(config.deadRoot())) {
            for (Path file : stream.toList()) {
                if (!Files.isRegularFile(file)) {
                    continue;
                }
                long lastModified = Files.getLastModifiedTime(file).toMillis();
                if (lastModified < cutoffEpochMs) {
                    Files.deleteIfExists(file);
                    deleted++;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to purge dead files", e);
        }
        return deleted;
    }

    private int purgeRetryFiles(long cutoffEpochMs) {
        int deleted = 0;
        try (Stream<Path> stream = Files.list(config.retryRoot())) {
            for (Path file : stream.toList()) {
                if (!Files.isRegularFile(file)) {
                    continue;
                }
                long lastModified = Files.getLastModifiedTime(file).toMillis();
                if (lastModified < cutoffEpochMs) {
                    Files.deleteIfExists(file);
                    deleted++;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to purge retry files", e);
        }
        return deleted;
    }

    private LocalDate parseDoneDirectoryDate(String fileName) {
        try {
            return LocalDate.parse(fileName);
        } catch (Exception ignored) {
            return null;
        }
    }

    private void clearDirectory(Path root) throws IOException {
        if (!Files.exists(root) || !Files.isDirectory(root)) {
            return;
        }
        try (Stream<Path> stream = Files.list(root)) {
            for (Path child : stream.toList()) {
                deleteRecursively(child);
            }
        }
    }

    private CopySummary copyDirectory(Path sourceRoot, Path targetRoot) throws IOException {
        if (!Files.exists(sourceRoot)) {
            return new CopySummary(0, 0L);
        }
        int files = 0;
        long bytes = 0L;
        try (Stream<Path> walk = Files.walk(sourceRoot)) {
            for (Path source : walk.toList()) {
                Path relative = sourceRoot.relativize(source);
                Path target = targetRoot.resolve(relative);
                if (Files.isDirectory(source)) {
                    Files.createDirectories(target);
                    continue;
                }
                if (!Files.isRegularFile(source)) {
                    continue;
                }
                Path parent = target.getParent();
                if (parent != null) {
                    Files.createDirectories(parent);
                }
                Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.COPY_ATTRIBUTES);
                files++;
                bytes += Files.size(source);
            }
        }
        return new CopySummary(files, bytes);
    }

    private void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (Stream<Path> stream = Files.walk(root)) {
            for (Path p : stream.sorted((a, b) -> b.getNameCount() - a.getNameCount()).toList()) {
                Files.deleteIfExists(p);
            }
        }
    }

    private int countFiles(Path root, String glob) {
        if (!Files.exists(root)) return 0;
        int count = 0;
        if (Files.isDirectory(root)) {
            try (Stream<Path> stream = Files.walk(root)) {
                for (Path p : stream.toList()) {
                    if (!Files.isRegularFile(p)) continue;
                    String name = p.getFileName().toString();
                    if ("*.meta.json".equals(glob) && name.endsWith(".meta.json")) {
                        count++;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to count files in " + root, e);
            }
        }
        return count;
    }

    private Map<String, Object> mergeAuditDetails(Map<String, Object> base, Map<String, Object> extras) {
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        if (base != null) {
            out.putAll(base);
        }
        if (extras != null) {
            out.putAll(extras);
        }
        return out;
    }

    private String principalFromRequestMeta(Map<String, Object> requestMeta) {
        if (requestMeta == null || requestMeta.isEmpty()) {
            return null;
        }
        Object raw = requestMeta.get("principal_id");
        if (raw == null) {
            return null;
        }
        String value = String.valueOf(raw).trim();
        return value.isBlank() ? null : value;
    }

    private List<String> auditByTaskId(String taskId) {
        Path file = config.auditRoot().resolve("audit.log");
        if (!Files.exists(file)) return List.of();
        List<String> out = new ArrayList<>();
        String token = "\"task_id\":\"" + taskId + "\"";
        try {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (line.contains(token)) out.add(line);
            }
            return out;
        } catch (IOException e) {
            throw new RuntimeException("Failed to read audit file for export", e);
        }
    }

    private LeaseReportOutcome maybeWriteLeaseConflictReport(long nowMs) {
        long bucket = nowMs / 3_600_000L;
        if (bucket == lastLeaseReportBucket) {
            return new LeaseReportOutcome(false, null, 0);
        }
        synchronized (leaseReportLock) {
            if (bucket == lastLeaseReportBucket) {
                return new LeaseReportOutcome(false, null, 0);
            }
            String stamp = DateTimeFormatter.ofPattern("yyyyMMddHH").withZone(ZoneOffset.UTC).format(Instant.ofEpochMilli(nowMs));
            Path output = config.rootDir().resolve("reports").resolve("lease-conflicts-" + stamp + ".json");
            LeaseReportOutcome out = generateLeaseConflictReport(24, 5000, output.toString());
            lastLeaseReportBucket = bucket;
            return out;
        }
    }

    private Map<String, Integer> countBy(List<TaskStore.LeaseConflict> rows, java.util.function.Function<TaskStore.LeaseConflict, String> keyFn) {
        Map<String, Integer> out = new LinkedHashMap<>();
        for (TaskStore.LeaseConflict row : rows) {
            String k = keyFn.apply(row);
            if (k == null || k.isBlank()) {
                k = "_unknown";
            }
            out.merge(k, 1, Integer::sum);
        }
        return out;
    }

    private Map<String, Integer> topN(Map<String, Integer> values, int n) {
        Map<String, Integer> out = new LinkedHashMap<>();
        values.entrySet().stream()
                .sorted((a, b) -> Integer.compare(b.getValue(), a.getValue()))
                .limit(Math.max(1, n))
                .forEach(e -> out.put(e.getKey(), e.getValue()));
        return out;
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

    private String safeString(String raw) {
        return raw == null ? "" : raw;
    }

    private JsonNode readField(JsonNode node, String primaryName, String fallbackName) {
        if (node == null || node.isMissingNode()) {
            return com.fasterxml.jackson.databind.node.NullNode.getInstance();
        }
        JsonNode primary = node.path(primaryName);
        if (!primary.isMissingNode() && !primary.isNull()) {
            return primary;
        }
        JsonNode fallback = node.path(fallbackName);
        if (!fallback.isMissingNode() && !fallback.isNull()) {
            return fallback;
        }
        return com.fasterxml.jackson.databind.node.NullNode.getInstance();
    }

    private String readTextField(JsonNode node, String primaryName, String fallbackName) {
        JsonNode field = readField(node, primaryName, fallbackName);
        if (field.isNull() || field.isMissingNode()) {
            return "";
        }
        return field.asText("");
    }

    private long readLongField(JsonNode node, String primaryName, String fallbackName, long fallbackValue) {
        JsonNode field = readField(node, primaryName, fallbackName);
        if (field.isNull() || field.isMissingNode()) {
            return fallbackValue;
        }
        if (field.isNumber()) {
            return field.asLong(fallbackValue);
        }
        return parseLongOrDefault(field.asText(""), fallbackValue);
    }

    private int readIntField(JsonNode node, String primaryName, String fallbackName, int fallbackValue) {
        JsonNode field = readField(node, primaryName, fallbackName);
        if (field.isNull() || field.isMissingNode()) {
            return fallbackValue;
        }
        if (field.isNumber()) {
            return field.asInt(fallbackValue);
        }
        try {
            return Integer.parseInt(field.asText("").trim());
        } catch (Exception ignored) {
            return fallbackValue;
        }
    }

    private List<TaskStore.TaskDelta> parseTaskDeltas(JsonNode node) {
        if (node == null || !node.isArray()) {
            return List.of();
        }
        List<TaskStore.TaskDelta> out = new ArrayList<>();
        for (JsonNode row : node) {
            out.add(Jsons.mapper().convertValue(row, TaskStore.TaskDelta.class));
        }
        return out;
    }

    private List<TaskStore.StepDelta> parseStepDeltas(JsonNode node) {
        if (node == null || !node.isArray()) {
            return List.of();
        }
        List<TaskStore.StepDelta> out = new ArrayList<>();
        for (JsonNode row : node) {
            out.add(Jsons.mapper().convertValue(row, TaskStore.StepDelta.class));
        }
        return out;
    }

    private List<TaskStore.MessageDelta> parseMessageDeltas(JsonNode node) {
        if (node == null || !node.isArray()) {
            return List.of();
        }
        List<TaskStore.MessageDelta> out = new ArrayList<>();
        for (JsonNode row : node) {
            out.add(Jsons.mapper().convertValue(row, TaskStore.MessageDelta.class));
        }
        return out;
    }

    private List<TaskStore.StepDelta> normalizeIncomingStepDeltas(List<TaskStore.StepDelta> incoming) {
        if (incoming == null || incoming.isEmpty()) {
            return List.of();
        }
        List<TaskStore.StepDelta> out = new ArrayList<>(incoming.size());
        for (TaskStore.StepDelta row : incoming) {
            if (row == null) {
                continue;
            }
            String payloadText = row.payloadText();
            if (payloadText == null || payloadText.isBlank()) {
                payloadText = readPayloadIfExists(row.payloadPath());
            }
            String payloadPath = row.payloadPath();
            if (payloadText != null) {
                // Imported replicas may reference remote payload paths; materialize a local copy before apply.
                Path localPayload = config.payloadDir().resolve("replica-" + row.stepId() + ".payload.json");
                try {
                    Files.createDirectories(localPayload.getParent());
                    Files.writeString(localPayload, payloadText, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new RuntimeException("Failed to materialize replicated payload: " + row.stepId(), e);
                }
                payloadPath = localPayload.toString();
            }
            out.add(new TaskStore.StepDelta(
                    row.stepId(),
                    row.taskId(),
                    row.agentId(),
                    row.priority(),
                    payloadPath,
                    payloadText,
                    row.enqueued(),
                    row.resultPayload(),
                    row.status(),
                    row.attempt(),
                    row.nextRetryAtMs(),
                    row.leaseOwner(),
                    row.leaseToken(),
                    row.leaseEpoch(),
                    row.dependsOn(),
                    row.createdAtMs(),
                    row.updatedAtMs()
            ));
        }
        return out;
    }

    private List<TaskStore.MessageDelta> normalizeIncomingMessageDeltas(
            List<TaskStore.MessageDelta> incoming,
            List<TaskStore.StepDelta> steps
    ) {
        if (incoming == null || incoming.isEmpty()) {
            return List.of();
        }
        Map<String, String> payloadPathByStep = new HashMap<>();
        for (TaskStore.StepDelta step : steps) {
            if (step == null || step.stepId() == null) {
                continue;
            }
            payloadPathByStep.put(step.stepId(), step.payloadPath());
        }
        List<TaskStore.MessageDelta> out = new ArrayList<>(incoming.size());
        for (TaskStore.MessageDelta row : incoming) {
            if (row == null) {
                continue;
            }
            String payloadPath = payloadPathByStep.getOrDefault(row.stepId(), row.payloadPath());
            String metaPath = config.metaDir().resolve(row.msgId() + ".meta.json").toString();
            out.add(new TaskStore.MessageDelta(
                    row.msgId(),
                    row.taskId(),
                    row.stepId(),
                    row.fromAgent(),
                    row.toAgent(),
                    row.priority(),
                    row.state(),
                    metaPath,
                    payloadPath,
                    row.createdAtMs(),
                    row.updatedAtMs()
            ));
        }
        return out;
    }

    private ReplicationQueueMaterializeOutcome materializeImportedQueue(
            List<TaskStore.MessageDelta> messages,
            List<TaskStore.StepDelta> steps
    ) {
        if (messages == null || messages.isEmpty()) {
            return new ReplicationQueueMaterializeOutcome(0, 0);
        }
        Map<String, Integer> attemptByStep = new HashMap<>();
        Map<String, String> agentByStep = new HashMap<>();
        for (TaskStore.StepDelta step : steps) {
            if (step == null || step.stepId() == null) {
                continue;
            }
            attemptByStep.put(step.stepId(), Math.max(1, step.attempt()));
            if (step.agentId() != null && !step.agentId().isBlank()) {
                agentByStep.put(step.stepId(), step.agentId());
            }
        }

        int enqueued = 0;
        int skipped = 0;
        for (TaskStore.MessageDelta message : messages) {
            if (message == null || message.msgId() == null || message.msgId().isBlank()) {
                skipped++;
                continue;
            }
            if (!MessageState.QUEUED.name().equalsIgnoreCase(message.state())) {
                continue;
            }
            Path localMeta = config.metaDir().resolve(message.msgId() + ".meta.json");
            if (Files.exists(localMeta)) {
                skipped++;
                continue;
            }
            String payload = readPayloadIfExists(message.payloadPath());
            if (payload == null) {
                skipped++;
                continue;
            }
            String toAgent = message.toAgent();
            if (toAgent == null || toAgent.isBlank()) {
                toAgent = agentByStep.getOrDefault(message.stepId(), "");
            }
            if (toAgent.isBlank()) {
                skipped++;
                continue;
            }
            String fromAgent = message.fromAgent();
            if (fromAgent == null || fromAgent.isBlank()) {
                fromAgent = "system";
            }
            String priority = message.priority();
            if (priority == null || priority.isBlank()) {
                priority = Priority.NORMAL.name();
            }
            int attempt = Math.max(1, attemptByStep.getOrDefault(message.stepId(), 1));
            long createdAtMs = Math.max(0L, message.createdAtMs());
            MessageEnvelope envelope = new MessageEnvelope(
                    message.msgId(),
                    message.taskId(),
                    message.stepId(),
                    fromAgent,
                    toAgent,
                    priority,
                    createdAtMs,
                    message.payloadPath(),
                    attempt
            );
            try {
                fileBus.enqueue(envelope, payload);
                enqueued++;
            } catch (Exception e) {
                skipped++;
            }
        }
        return new ReplicationQueueMaterializeOutcome(enqueued, skipped);
    }

    private String readPayloadIfExists(String payloadPath) {
        if (payloadPath == null || payloadPath.isBlank()) {
            return null;
        }
        try {
            Path path = Path.of(payloadPath);
            if (!Files.exists(path) || !Files.isRegularFile(path)) {
                return null;
            }
            return Files.readString(path, StandardCharsets.UTF_8);
        } catch (Exception ignored) {
            return null;
        }
    }

    private GossipPacket signGossipPacket(GossipPacket packet, String sharedSecret) {
        if (packet == null) {
            return null;
        }
        String secret = sharedSecret == null ? "" : sharedSecret.trim();
        if (secret.isBlank()) {
            return packet;
        }
        GossipPacket unsigned = new GossipPacket(
                packet.nodeId(),
                packet.heartbeatMs(),
                packet.msgId(),
                packet.originNode(),
                packet.ttl(),
                packet.nodes(),
                null
        );
        String signature = Hashing.hmacSha256Hex(secret, gossipSignPayload(unsigned));
        return new GossipPacket(
                unsigned.nodeId(),
                unsigned.heartbeatMs(),
                unsigned.msgId(),
                unsigned.originNode(),
                unsigned.ttl(),
                unsigned.nodes(),
                signature
        );
    }

    private boolean verifyGossipPacket(GossipPacket packet, String sharedSecret) {
        if (packet == null) {
            return false;
        }
        String secret = sharedSecret == null ? "" : sharedSecret.trim();
        if (secret.isBlank()) {
            return true;
        }
        String signature = packet.signature() == null ? "" : packet.signature().trim();
        if (signature.isBlank()) {
            return false;
        }
        GossipPacket unsigned = new GossipPacket(
                packet.nodeId(),
                packet.heartbeatMs(),
                packet.msgId(),
                packet.originNode(),
                packet.ttl(),
                packet.nodes(),
                null
        );
        String expected = Hashing.hmacSha256Hex(secret, gossipSignPayload(unsigned));
        return signature.equalsIgnoreCase(expected);
    }

    private String gossipSignPayload(GossipPacket packet) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("node_id", packet.nodeId() == null ? "" : packet.nodeId());
        body.put("heartbeat_ms", packet.heartbeatMs());
        body.put("msg_id", packet.msgId() == null ? "" : packet.msgId());
        body.put("origin_node", packet.originNode() == null ? "" : packet.originNode());
        body.put("ttl", packet.ttl() == null ? 0 : packet.ttl());
        List<Map<String, Object>> nodes = new ArrayList<>();
        if (packet.nodes() != null) {
            for (GossipNodeState node : packet.nodes()) {
                if (node == null) {
                    continue;
                }
                nodes.add(Map.of(
                        "node_id", node.nodeId() == null ? "" : node.nodeId(),
                        "heartbeat_ms", node.heartbeatMs()
                ));
            }
        }
        body.put("nodes", nodes);
        return Jsons.toJson(body);
    }

    private long maxTaskDeltaUpdatedAt(List<TaskStore.TaskDelta> rows) {
        long max = 0L;
        for (TaskStore.TaskDelta row : rows) {
            if (row == null) {
                continue;
            }
            max = Math.max(max, row.updatedAtMs());
        }
        return max;
    }

    private long maxStepDeltaUpdatedAt(List<TaskStore.StepDelta> rows) {
        long max = 0L;
        for (TaskStore.StepDelta row : rows) {
            if (row == null) {
                continue;
            }
            max = Math.max(max, row.updatedAtMs());
        }
        return max;
    }

    private long maxMessageDeltaUpdatedAt(List<TaskStore.MessageDelta> rows) {
        long max = 0L;
        for (TaskStore.MessageDelta row : rows) {
            if (row == null) {
                continue;
            }
            max = Math.max(max, row.updatedAtMs());
        }
        return max;
    }

    private long percentileFromSorted(List<Long> sorted, double percentile) {
        if (sorted == null || sorted.isEmpty()) {
            return 0L;
        }
        int idx = (int) Math.ceil((percentile / 100.0) * sorted.size()) - 1;
        idx = Math.max(0, Math.min(sorted.size() - 1, idx));
        return sorted.get(idx);
    }

    private void appendSloAlert(List<SloAlert> alerts, String metric, long observed, long target, String message) {
        long safeTarget = Math.max(1L, target);
        if (observed <= safeTarget) {
            return;
        }
        double burnRate = (double) observed / (double) safeTarget;
        String severity = burnRate >= 2.0d ? "critical" : "warning";
        alerts.add(new SloAlert(metric, observed, safeTarget, burnRate, severity, message));
    }

    private void maybeWriteConvergenceReport(String outputPath, ConvergenceReportOutcome report) {
        if (outputPath == null || outputPath.isBlank() || report == null) {
            return;
        }
        Path out = Paths.get(outputPath).toAbsolutePath().normalize();
        try {
            if (out.getParent() != null) {
                Files.createDirectories(out.getParent());
            }
            Files.writeString(out, Jsons.toJson(report), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write convergence report: " + out, e);
        }
    }

    private String decideOwnershipWinner(String candidateOwner, String currentOwner) {
        String candidate = candidateOwner == null ? "" : candidateOwner.trim();
        String current = currentOwner == null ? "" : currentOwner.trim();
        if (!current.isBlank()) {
            return current;
        }
        if (candidate.isBlank()) {
            return "";
        }
        return candidate;
    }

    private long computeGossipConvergenceSeconds(List<TaskStore.MeshNode> members) {
        if (members == null || members.size() <= 1) {
            return 0L;
        }
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        int count = 0;
        for (TaskStore.MeshNode node : members) {
            if (node == null) {
                continue;
            }
            long heartbeat = node.lastHeartbeatMs();
            if (heartbeat <= 0) {
                continue;
            }
            min = Math.min(min, heartbeat);
            max = Math.max(max, heartbeat);
            count++;
        }
        if (count <= 1 || min == Long.MAX_VALUE || max == Long.MIN_VALUE) {
            return 0L;
        }
        return Math.max(0L, (max - min) / 1000L);
    }

    private Instant parseOptionalInstant(String raw, String fieldName) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(raw.trim());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid " + fieldName + " value: " + raw + ", expected ISO-8601 instant");
        }
    }

    private Instant parseInstantSafe(String raw) {
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return Instant.parse(raw);
        } catch (Exception ignored) {
            return null;
        }
    }

    public record SubmitOutcome(String taskId, boolean deduplicated, String message) {
    }

    public record WorkflowStepSpec(String id, String agent, String input, String priority, List<String> dependsOn) {
        public WorkflowStepSpec {
            if (dependsOn == null) dependsOn = List.of();
            if (priority == null || priority.isBlank()) priority = Priority.NORMAL.name();
        }
    }

    public record WorkerOutcome(boolean processed, String taskId, String msgId, String message) {
    }

    public record MaintenanceOutcome(
            String nodeId,
            int suspectedNodesMarked,
            int deadNodesMarked,
            int leaseHeartbeats,
            int monotonicReclaimed,
            int monotonicRetryScheduled,
            int monotonicDeadLettered,
            int deadOwnerReclaimed,
            int deadOwnerRetryScheduled,
            int deadOwnerDeadLettered,
            int wallReclaimed,
            int wallRetryScheduled,
            int wallDeadLettered,
            int retryMessagesDispatched,
            int deadNodesPruned,
            boolean leaseReportGenerated
    ) {
    }

    public record DeadOwnerRecoveryOutcome(
            int deadNodeCount,
            List<String> deadNodes,
            int reclaimed,
            int retryScheduled,
            int deadLettered
    ) {
    }

    public record MeshPruneOutcome(
            long olderThanMs,
            long cutoffMs,
            int limit,
            int pruned,
            long total
    ) {
    }

    public record GossipTickOutcome(
            String nodeId,
            int bindPort,
            int sent,
            int relayed,
            int received,
            int accepted,
            int deduplicated,
            int invalid,
            int fanout,
            int ttl,
            int dedupCacheSize,
            int suspectedNodesMarked,
            int deadNodesMarked
    ) {
    }

    public record GossipSyncOutcome(
            String nodeId,
            int bindPort,
            int roundsCompleted,
            int sent,
            int relayed,
            int received,
            int accepted,
            int deduplicated,
            int invalid,
            int suspectedNodesMarked,
            int deadNodesMarked,
            int memberCount,
            List<GossipTickOutcome> ticks
    ) {
    }

    public record RuntimeSettingsView(
            long leaseTimeoutMs,
            long suspectAfterMs,
            long deadAfterMs,
            long suspectRecoverMinMs,
            long deadRecoverMinMs,
            boolean pauseWorkerWhenLocalNotAlive,
            int maxAttempts,
            long baseBackoffMs,
            long maxBackoffMs,
            int retryDispatchLimit,
            int gossipFanout,
            int gossipPacketTtl,
            int gossipSyncSampleSize,
            long gossipDedupWindowMs,
            int gossipDedupMaxEntries,
            boolean gossipPacketSigningEnabled,
            long meshPruneOlderThanMs,
            int meshPruneLimit,
            long meshPruneIntervalMs,
            long minFreeDiskBytes,
            int maxIngressQueueDepth,
            int maxRunningSteps,
            int submitRateLimitPerMin,
            long sloQueueLagTargetMs,
            long sloStepP95TargetMs,
            long sloStepP99TargetMs,
            int sloDeadLetterGrowthTarget1h,
            long sloReplicationLagTargetMs
    ) {
    }

    public record SettingsReloadOutcome(
            boolean changed,
            boolean configExists,
            String sourcePath,
            RuntimeSettingsView settings,
            String message,
            long checkedAtMs,
            List<String> changedFields
    ) {
    }

    public record SettingsHistoryEntry(
            String timestamp,
            String result,
            boolean changed,
            int changedCount,
            List<String> changedFields,
            Long configMtimeMs,
            String source,
            String configPath
    ) {
    }

    public record OwnershipEvent(
            String source,
            String action,
            String nodeId,
            String taskId,
            String stepId,
            String result,
            long occurredAtMs,
            Map<String, Object> details
    ) {
    }

    public record MeshSummaryOutcome(
            int totalNodes,
            int aliveNodes,
            int suspectNodes,
            int deadNodes,
            int runningByAliveOwners,
            int runningBySuspectOwners,
            int runningByDeadOwners,
            int runningByUnknownOwners,
            long oldestDeadNodeAgeMs,
            long generatedAtMs
    ) {
    }

    public record ClusterStateOutcome(
            String key,
            String rawValue,
            long epoch,
            long version,
            long updatedAtMs
    ) {
    }

    public record ClusterEpochBumpOutcome(
            long previousEpoch,
            long currentEpoch,
            long version,
            long updatedAtMs,
            String reason
    ) {
    }

    public record PurgeOutcome(int doneDirectoriesPurged, int deadFilesPurged, int retryFilesPurged, int idempotencyRowsPurged) {
    }

    public record ReplayOutcome(String taskId, boolean replayed, String message) {
    }

    public record ReplayBatchOutcome(String status, int total, int replayed, int failed, List<ReplayOutcome> details) {
    }

    public record LeaseReportOutcome(boolean generated, String path, int rows) {
    }

    public record DeadExportOutcome(int exported, String outputPath) {
    }

    public record SnapshotExportOutcome(
            String outputDir,
            String snapshotRoot,
            int fileCount,
            long totalBytes,
            long createdAtMs
    ) {
    }

    public record SnapshotImportOutcome(
            String inputDir,
            String snapshotRoot,
            int restoredFiles,
            long restoredBytes,
            long restoredAtMs
    ) {
    }

    public record HealthOutcome(
            boolean ok,
            boolean dbOk,
            boolean inboxDirOk,
            boolean processingDirOk,
            boolean metaDirOk,
            boolean payloadDirOk,
            boolean auditDirOk,
            long diskFreeBytes,
            long minFreeDiskBytes,
            boolean diskPressure,
            String checkedAt
    ) {
    }

    public record TaskExportOutcome(
            String taskId,
            boolean exported,
            int stepCount,
            int auditEvents,
            String output
    ) {
    }

    public record ReplicationExportOutcome(
            String schemaVersion,
            long sinceMs,
            int limit,
            long cursorMaxMs,
            int taskDeltaCount,
            int stepDeltaCount,
            int messageDeltaCount,
            long clusterEpoch,
            long generatedAtMs,
            String sourceNodeId,
            String outputPath
    ) {
    }

    public record ReplicationImportOutcome(
            String schemaVersion,
            String inputPath,
            long sinceMs,
            int limit,
            long cursorMaxMs,
            long sourceClusterEpoch,
            long localClusterEpoch,
            int taskDeltaCount,
            int stepDeltaCount,
            int messageDeltaCount,
            int taskApplied,
            int stepApplied,
            int messageApplied,
            int staleSkipped,
            int tieResolved,
            int tieKeptLocal,
            int queueEnqueued,
            int queueSkipped,
            long importLagMs
    ) {
    }

    public record ConvergenceReportOutcome(
            int sinceHours,
            int limit,
            long sinceMs,
            int imports,
            int appliedRows,
            int tieResolved,
            int tieKeptLocal,
            int queueEnqueued,
            int staleSkipped,
            long lagAvgMs,
            long lagP95Ms,
            long lagMaxMs,
            Map<String, Integer> sourceNodeCounts,
            long generatedAtMs,
            String outputPath
    ) {
    }

    public record StatsOutcome(
            Map<String, Integer> taskStatus,
            Map<String, Integer> stepStatus,
            Map<String, Integer> messageState,
            int inboxHigh,
            int inboxNormal,
            int inboxLow,
            int processingMetaFiles,
            int deadMetaFiles,
            int retryMetaFiles,
            long queueLagMs,
            long stepLatencyP95Ms,
            long stepLatencyP99Ms,
            int deadLetterGrowth1h,
            int leaseConflictTotal,
            int leaseConflict1h,
            Map<String, Integer> leaseConflictByType,
            long antiEntropyMergeTotal,
            long webWriteGetCompatTotal,
            long webWriteRateLimitedTotal,
            long deadOwnerReclaimedTotal,
            long membershipRecoverySuppressedTotal,
            long meshNodesPrunedTotal,
            long workerPausedTotal,
            long replicationImportTotal,
            long replicationTieResolvedTotal,
            long replicationTieKeptLocalTotal,
            long replicationLastImportLagMs,
            long gossipSigningEnabled,
            long lowStarvationCount,
            long clusterEpoch,
            int meshTotalNodes,
            int meshAliveNodes,
            int meshSuspectNodes,
            int meshDeadNodes,
            int meshRunningByAliveOwners,
            int meshRunningBySuspectOwners,
            int meshRunningByDeadOwners,
            int meshRunningByUnknownOwners,
            long meshOldestDeadNodeAgeMs,
            long gossipConvergenceSeconds,
            long diskFreeBytes,
            long minFreeDiskBytes,
            long diskPressure,
            long replicationSyncTotal,
            long sloAlertFireTotal,
            long submitRejectedTotal,
            String namespace
    ) {
    }

    public record SloAlert(
            String metric,
            long observed,
            long target,
            double burnRate,
            String severity,
            String message
    ) {
    }

    public record SloAlertOutcome(
            String namespace,
            long evaluatedAtMs,
            List<SloAlert> alerts,
            long totalFired,
            String outputPath
    ) {
        public boolean fired() {
            return alerts != null && !alerts.isEmpty();
        }
    }

    public record AuditIntegrityOutcome(
            boolean ok,
            int totalRows,
            int checkedRows,
            int legacyRows,
            int brokenLine,
            String reason,
            String tailHash
    ) {
    }

    public record SiemExportOutcome(
            String namespace,
            String outputPath,
            int exported
    ) {
    }

    private record LeaseWatch(String taskId, String stepId, String leaseToken, long leaseEpoch, long lastHeartbeatMonoNs) {
    }

    private record DeadEntry(
            String msgId,
            String taskId,
            String stepId,
            String toAgent,
            String priority,
            long createdAtMs,
            String metaPath
    ) {
    }

    private record ReplicationQueueMaterializeOutcome(int enqueued, int skipped) {
    }

    private record CopySummary(int files, long bytes) {
    }

    private record ScriptAgentFile(List<ScriptAgentSpec> agents) {
    }

    private record ScriptAgentSpec(String id, List<String> command, Long timeoutMs) {
    }

    private record RetryPolicyFile(List<RetryPolicySpec> agents) {
    }

    private record RetryPolicySpec(String id, Integer maxAttempts, Long baseBackoffMs, Long maxBackoffMs) {
    }

    private record GossipPacket(
            String nodeId,
            long heartbeatMs,
            String msgId,
            String originNode,
            Integer ttl,
            List<GossipNodeState> nodes,
            String signature
    ) {
    }

    private record SeedEndpoint(String host, int port) {
    }

    private record GossipNodeState(String nodeId, long heartbeatMs) {
    }

    private record RuntimeSettingsFile(
            Long leaseTimeoutMs,
            Long suspectAfterMs,
            Long deadAfterMs,
            Long suspectRecoverMinMs,
            Long deadRecoverMinMs,
            Boolean pauseWorkerWhenLocalNotAlive,
            Integer maxAttempts,
            Long baseBackoffMs,
            Long maxBackoffMs,
            Integer retryDispatchLimit,
            Integer gossipFanout,
            Integer gossipPacketTtl,
            Integer gossipSyncSampleSize,
            Long gossipDedupWindowMs,
            Integer gossipDedupMaxEntries,
            String gossipSharedSecret,
            Long meshPruneOlderThanMs,
            Integer meshPruneLimit,
            Long meshPruneIntervalMs,
            Long minFreeDiskBytes,
            Integer maxIngressQueueDepth,
            Integer maxRunningSteps,
            Integer submitRateLimitPerMin,
            Long sloQueueLagTargetMs,
            Long sloStepP95TargetMs,
            Long sloStepP99TargetMs,
            Integer sloDeadLetterGrowthTarget1h,
            Long sloReplicationLagTargetMs
    ) {
    }

    private record RuntimeSettings(
            long leaseTimeoutMs,
            long suspectAfterMs,
            long deadAfterMs,
            long suspectRecoverMinMs,
            long deadRecoverMinMs,
            boolean pauseWorkerWhenLocalNotAlive,
            int maxAttempts,
            long baseBackoffMs,
            long maxBackoffMs,
            int retryDispatchLimit,
            int gossipFanout,
            int gossipPacketTtl,
            int gossipSyncSampleSize,
            long gossipDedupWindowMs,
            int gossipDedupMaxEntries,
            String gossipSharedSecret,
            long meshPruneOlderThanMs,
            int meshPruneLimit,
            long meshPruneIntervalMs,
            long minFreeDiskBytes,
            int maxIngressQueueDepth,
            int maxRunningSteps,
            int submitRateLimitPerMin,
            long sloQueueLagTargetMs,
            long sloStepP95TargetMs,
            long sloStepP99TargetMs,
            int sloDeadLetterGrowthTarget1h,
            long sloReplicationLagTargetMs
    ) {
        static RuntimeSettings defaults() {
            long lease = RelayMeshConfig.DEFAULT_LEASE_TIMEOUT_MS;
            return new RuntimeSettings(
                    lease,
                    lease * 2L,
                    lease * 4L,
                    5_000L,
                    30_000L,
                    true,
                    RelayMeshConfig.DEFAULT_MAX_ATTEMPTS,
                    RelayMeshConfig.DEFAULT_BASE_BACKOFF_MS,
                    RelayMeshConfig.DEFAULT_MAX_BACKOFF_MS,
                    RelayMeshConfig.DEFAULT_RETRY_DISPATCH_LIMIT,
                    3,
                    2,
                    32,
                    60_000L,
                    4_096,
                    "",
                    7L * 24L * 60L * 60L * 1000L,
                    256,
                    60L * 60L * 1000L,
                    0L,
                    5_000,
                    2_000,
                    240,
                    5_000L,
                    2_000L,
                    5_000L,
                    10,
                    30_000L
            );
        }

        static RuntimeSettings fromFile(RuntimeSettingsFile file, RuntimeSettings defaults) {
            if (file == null) {
                return defaults;
            }
            long leaseTimeout = sanitizeLong(file.leaseTimeoutMs(), defaults.leaseTimeoutMs(), 100L);
            long suspectAfter = sanitizeLong(file.suspectAfterMs(), defaults.suspectAfterMs(), leaseTimeout);
            if (suspectAfter < leaseTimeout) {
                suspectAfter = leaseTimeout;
            }
            long deadAfter = sanitizeLong(file.deadAfterMs(), defaults.deadAfterMs(), suspectAfter);
            if (deadAfter < suspectAfter) {
                deadAfter = suspectAfter;
            }
            long suspectRecoverMin = sanitizeLong(file.suspectRecoverMinMs(), defaults.suspectRecoverMinMs(), 0L);
            long deadRecoverMin = sanitizeLong(file.deadRecoverMinMs(), defaults.deadRecoverMinMs(), 0L);
            boolean pauseWorkerWhenLocalNotAlive = sanitizeBoolean(
                    file.pauseWorkerWhenLocalNotAlive(),
                    defaults.pauseWorkerWhenLocalNotAlive()
            );
            int maxAttempts = sanitizeInt(file.maxAttempts(), defaults.maxAttempts(), 1);
            long baseBackoff = sanitizeLong(file.baseBackoffMs(), defaults.baseBackoffMs(), 1L);
            long maxBackoff = sanitizeLong(file.maxBackoffMs(), defaults.maxBackoffMs(), baseBackoff);
            if (maxBackoff < baseBackoff) {
                maxBackoff = baseBackoff;
            }
            int retryDispatchLimit = sanitizeInt(file.retryDispatchLimit(), defaults.retryDispatchLimit(), 1);
            int gossipFanout = sanitizeInt(file.gossipFanout(), defaults.gossipFanout(), 1);
            int gossipPacketTtl = sanitizeInt(file.gossipPacketTtl(), defaults.gossipPacketTtl(), 1);
            int gossipSyncSampleSize = sanitizeInt(file.gossipSyncSampleSize(), defaults.gossipSyncSampleSize(), 1);
            long gossipDedupWindowMs = sanitizeLong(file.gossipDedupWindowMs(), defaults.gossipDedupWindowMs(), 1_000L);
            int gossipDedupMaxEntries = sanitizeInt(file.gossipDedupMaxEntries(), defaults.gossipDedupMaxEntries(), 64);
            String gossipSharedSecret = sanitizeSecret(file.gossipSharedSecret(), defaults.gossipSharedSecret());
            long meshPruneOlderThanMs = sanitizeLong(file.meshPruneOlderThanMs(), defaults.meshPruneOlderThanMs(), 0L);
            int meshPruneLimit = sanitizeInt(file.meshPruneLimit(), defaults.meshPruneLimit(), 1);
            long meshPruneIntervalMs = sanitizeLong(file.meshPruneIntervalMs(), defaults.meshPruneIntervalMs(), 0L);
            long minFreeDiskBytes = sanitizeLong(file.minFreeDiskBytes(), defaults.minFreeDiskBytes(), 0L);
            int maxIngressQueueDepth = sanitizeInt(file.maxIngressQueueDepth(), defaults.maxIngressQueueDepth(), 1);
            int maxRunningSteps = sanitizeInt(file.maxRunningSteps(), defaults.maxRunningSteps(), 1);
            int submitRateLimitPerMin = sanitizeInt(file.submitRateLimitPerMin(), defaults.submitRateLimitPerMin(), 0);
            long sloQueueLagTargetMs = sanitizeLong(file.sloQueueLagTargetMs(), defaults.sloQueueLagTargetMs(), 1L);
            long sloStepP95TargetMs = sanitizeLong(file.sloStepP95TargetMs(), defaults.sloStepP95TargetMs(), 1L);
            long sloStepP99TargetMs = sanitizeLong(file.sloStepP99TargetMs(), defaults.sloStepP99TargetMs(), sloStepP95TargetMs);
            int sloDeadLetterGrowthTarget1h = sanitizeInt(
                    file.sloDeadLetterGrowthTarget1h(),
                    defaults.sloDeadLetterGrowthTarget1h(),
                    1
            );
            long sloReplicationLagTargetMs = sanitizeLong(
                    file.sloReplicationLagTargetMs(),
                    defaults.sloReplicationLagTargetMs(),
                    1L
            );
            return new RuntimeSettings(
                    leaseTimeout,
                    suspectAfter,
                    deadAfter,
                    suspectRecoverMin,
                    deadRecoverMin,
                    pauseWorkerWhenLocalNotAlive,
                    maxAttempts,
                    baseBackoff,
                    maxBackoff,
                    retryDispatchLimit,
                    gossipFanout,
                    gossipPacketTtl,
                    gossipSyncSampleSize,
                    gossipDedupWindowMs,
                    gossipDedupMaxEntries,
                    gossipSharedSecret,
                    meshPruneOlderThanMs,
                    meshPruneLimit,
                    meshPruneIntervalMs,
                    minFreeDiskBytes,
                    maxIngressQueueDepth,
                    maxRunningSteps,
                    submitRateLimitPerMin,
                    sloQueueLagTargetMs,
                    sloStepP95TargetMs,
                    sloStepP99TargetMs,
                    sloDeadLetterGrowthTarget1h,
                    sloReplicationLagTargetMs
            );
        }

        RuntimeSettingsView toView() {
            return new RuntimeSettingsView(
                    leaseTimeoutMs,
                    suspectAfterMs,
                    deadAfterMs,
                    suspectRecoverMinMs,
                    deadRecoverMinMs,
                    pauseWorkerWhenLocalNotAlive,
                    maxAttempts,
                    baseBackoffMs,
                    maxBackoffMs,
                    retryDispatchLimit,
                    gossipFanout,
                    gossipPacketTtl,
                    gossipSyncSampleSize,
                    gossipDedupWindowMs,
                    gossipDedupMaxEntries,
                    gossipSharedSecret != null && !gossipSharedSecret.isBlank(),
                    meshPruneOlderThanMs,
                    meshPruneLimit,
                    meshPruneIntervalMs,
                    minFreeDiskBytes,
                    maxIngressQueueDepth,
                    maxRunningSteps,
                    submitRateLimitPerMin,
                    sloQueueLagTargetMs,
                    sloStepP95TargetMs,
                    sloStepP99TargetMs,
                    sloDeadLetterGrowthTarget1h,
                    sloReplicationLagTargetMs
            );
        }

        private static int sanitizeInt(Integer raw, int fallback, int min) {
            if (raw == null) {
                return fallback;
            }
            return Math.max(min, raw);
        }

        private static boolean sanitizeBoolean(Boolean raw, boolean fallback) {
            if (raw == null) {
                return fallback;
            }
            return raw;
        }

        private static long sanitizeLong(Long raw, long fallback, long min) {
            if (raw == null) {
                return fallback;
            }
            return Math.max(min, raw);
        }

        private static String sanitizeSecret(String raw, String fallback) {
            if (raw == null) {
                return fallback == null ? "" : fallback;
            }
            return raw.trim();
        }
    }
}
