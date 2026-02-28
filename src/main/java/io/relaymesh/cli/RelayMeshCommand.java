package io.relaymesh.cli;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.relaymesh.config.RelayMeshConfig;
import io.relaymesh.model.TaskView;
import io.relaymesh.runtime.RelayMeshRuntime;
import io.relaymesh.security.NodeRpcTls;
import io.relaymesh.storage.Database;
import io.relaymesh.util.Jsons;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.ParentCommand;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.Callable;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Locale;
import java.time.Instant;
import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.io.OutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.Duration;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;

@Command(
        name = "relaymesh",
        mixinStandardHelpOptions = true,
        description = "RelayMesh single-node runtime CLI",
        subcommands = {
                RelayMeshCommand.InitCommand.class,
                RelayMeshCommand.SubmitCommand.class,
                RelayMeshCommand.SubmitWorkflowCommand.class,
                RelayMeshCommand.WorkerCommand.class,
                RelayMeshCommand.TaskCommand.class,
                RelayMeshCommand.CancelCommand.class,
                RelayMeshCommand.TaskExportCommand.class,
                RelayMeshCommand.TasksCommand.class,
                RelayMeshCommand.WorkflowCommand.class,
                RelayMeshCommand.AgentsCommand.class,
                RelayMeshCommand.MembersCommand.class,
                RelayMeshCommand.ClusterStateCommand.class,
                RelayMeshCommand.ClusterEpochBumpCommand.class,
                RelayMeshCommand.ReplicationExportCommand.class,
                RelayMeshCommand.ReplicationImportCommand.class,
                RelayMeshCommand.MeshSummaryCommand.class,
                RelayMeshCommand.MaintenanceCommand.class,
                RelayMeshCommand.StatsCommand.class,
                RelayMeshCommand.BenchmarkRunCommand.class,
                RelayMeshCommand.MetricsCommand.class,
                RelayMeshCommand.ServeMetricsCommand.class,
                RelayMeshCommand.GossipCommand.class,
                RelayMeshCommand.GossipSyncCommand.class,
                RelayMeshCommand.ReloadSettingsCommand.class,
                RelayMeshCommand.SettingsHistoryCommand.class,
                RelayMeshCommand.MeshRecoverCommand.class,
                RelayMeshCommand.MeshPruneCommand.class,
                RelayMeshCommand.OwnershipEventsCommand.class,
                RelayMeshCommand.ConvergenceReportCommand.class,
                RelayMeshCommand.LeaseConflictsCommand.class,
                RelayMeshCommand.LeaseReportCommand.class,
                RelayMeshCommand.ServeWebCommand.class,
                RelayMeshCommand.ServeNodeRpcCommand.class,
                RelayMeshCommand.NodeRpcRevocationTemplateCommand.class,
                RelayMeshCommand.AuditTailCommand.class,
                RelayMeshCommand.AuditQueryCommand.class,
                RelayMeshCommand.HealthCommand.class,
                RelayMeshCommand.SnapshotExportCommand.class,
                RelayMeshCommand.SnapshotImportCommand.class,
                RelayMeshCommand.PurgeCommand.class,
                RelayMeshCommand.ReplayCommand.class,
                RelayMeshCommand.ReplayBatchCommand.class,
                RelayMeshCommand.DeadExportCommand.class,
                RelayMeshCommand.ReplicationControllerCommand.class,
                RelayMeshCommand.DrAutomationCommand.class,
                RelayMeshCommand.SloEvaluateCommand.class,
                RelayMeshCommand.PayloadKeyStatusCommand.class,
                RelayMeshCommand.PayloadKeyRotateCommand.class,
                RelayMeshCommand.AuditVerifyCommand.class,
                RelayMeshCommand.AuditSiemExportCommand.class,
                RelayMeshCommand.SchemaMigrationsCommand.class,
                RelayMeshCommand.SchemaRollbackCommand.class
        }
)
public final class RelayMeshCommand implements Runnable {
    private static final Object CONTROL_ROOM_LAYOUTS_LOCK = new Object();

    @Option(names = {"--root"}, description = "Runtime data root directory", defaultValue = "data")
    String root;

    @Option(names = {"--namespace"}, description = "Runtime namespace (tenant scope)", defaultValue = "default")
    String namespace;

    @Override
    public void run() {
        System.out.println("Use subcommands: init | submit | submit-workflow | worker | task | cancel | task-export | tasks | workflow | agents | members | cluster-state | cluster-epoch-bump | replication-export | replication-import | replication-controller | mesh-summary | maintenance | stats | benchmark-run | metrics | serve-metrics | gossip | gossip-sync | reload-settings | settings-history | mesh-recover | mesh-prune | ownership-events | convergence-report | lease-conflicts | lease-report | serve-web | serve-node-rpc | node-rpc-revocation-template | audit-tail | audit-query | audit-verify | audit-siem-export | health | snapshot-export | snapshot-import | dr-automation | slo-evaluate | payload-key-status | payload-key-rotate | schema-migrations | schema-rollback | purge | replay | replay-batch | dead-export");
    }

    RelayMeshRuntime runtime() {
        RelayMeshConfig config = RelayMeshConfig.fromRoot(root, namespace);
        return new RelayMeshRuntime(config);
    }

    @Command(name = "init", description = "Initialize directories and SQLite schema")
    static final class InitCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println("Initialized RelayMesh at: " + RelayMeshConfig.fromRoot(parent.root, parent.namespace).rootDir());
            return 0;
        }
    }

    @Command(name = "submit", description = "Submit a single-step task")
    static final class SubmitCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--agent"}, required = true, description = "Target agent id")
        String agent;

        @Option(names = {"--input"}, required = true, description = "Task input payload")
        String input;

        @Option(names = {"--priority"}, defaultValue = "normal", description = "Priority: high|normal|low")
        String priority;

        @Option(names = {"--idempotency-key"}, description = "Optional idempotency key")
        String idempotencyKey;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.SubmitOutcome outcome = runtime.submit(agent, input, priority, idempotencyKey);
            System.out.println(Jsons.toJson(outcome));
            return 0;
        }
    }

    @Command(name = "submit-workflow", description = "Submit workflow from a JSON file")
    static final class SubmitWorkflowCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--file"}, required = true, description = "Workflow JSON file path")
        String file;

        @Option(names = {"--idempotency-key"}, description = "Optional idempotency key")
        String idempotencyKey;

        @Override
        public Integer call() throws Exception {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            WorkflowFile wf = Jsons.mapper().readValue(Path.of(file).toFile(), WorkflowFile.class);
            List<RelayMeshRuntime.WorkflowStepSpec> steps = wf.steps().stream()
                    .map(s -> new RelayMeshRuntime.WorkflowStepSpec(
                            s.id(),
                            s.agent(),
                            s.input(),
                            s.priority(),
                            s.dependsOn() == null ? List.of() : s.dependsOn()
                    ))
                    .toList();
            RelayMeshRuntime.SubmitOutcome outcome = runtime.submitWorkflow(steps, idempotencyKey);
            System.out.println(Jsons.toJson(outcome));
            return 0;
        }
    }

    @Command(name = "worker", description = "Run worker loop or single poll")
    static final class WorkerCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--once"}, defaultValue = "false", description = "Run only one poll cycle")
        boolean once;

        @Option(names = {"--worker-id"}, defaultValue = "worker-local", description = "Worker identity")
        String workerId;

        @Option(names = {"--node-id"}, defaultValue = "node-local", description = "Node identity for membership heartbeat")
        String nodeId;

        @Option(names = {"--interval-ms"}, defaultValue = "1000", description = "Loop interval in ms")
        long intervalMs;

        @Option(names = {"--maintenance"}, defaultValue = "true",
                description = "Run maintenance tick (lease heartbeat/reclaim/retry-dispatch)")
        boolean maintenance;

        @Option(names = {"--graceful-timeout-ms"}, defaultValue = "0",
                description = "Graceful shutdown timeout; 0 means platform default")
        long gracefulTimeoutMs;

        @Option(names = {"--settings-reload-ms"}, defaultValue = "10000",
                description = "Check interval for hot-reloading relaymesh-settings.json")
        long settingsReloadMs;

        @Override
        public Integer call() throws Exception {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            if (once) {
                RelayMeshRuntime.SettingsReloadOutcome reload = runtime.maybeReloadSettings(0L);
                if (reload.changed()) {
                    System.out.println(Jsons.toJson(reload));
                }
                if (maintenance) {
                    RelayMeshRuntime.MaintenanceOutcome maintenanceOutcome = runtime.runMaintenance(nodeId);
                    System.out.println(Jsons.toJson(maintenanceOutcome));
                }
                RelayMeshRuntime.WorkerOutcome result = runtime.runWorkerOnce(workerId, nodeId);
                System.out.println(Jsons.toJson(result));
                return 0;
            }
            long effectiveGraceful = effectiveGracefulTimeoutMs(gracefulTimeoutMs);
            AtomicBoolean running = new AtomicBoolean(true);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                running.set(false);
                try {
                    Thread.sleep(effectiveGraceful);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }, "relaymesh-shutdown-hook"));

            while (running.get()) {
                RelayMeshRuntime.SettingsReloadOutcome reload = runtime.maybeReloadSettings(settingsReloadMs);
                if (reload.changed()) {
                    System.out.println(Jsons.toJson(reload));
                }
                if (maintenance) {
                    RelayMeshRuntime.MaintenanceOutcome maintenanceOutcome = runtime.runMaintenance(nodeId);
                    System.out.println(Jsons.toJson(maintenanceOutcome));
                }
                RelayMeshRuntime.WorkerOutcome result = runtime.runWorkerOnce(workerId, nodeId);
                System.out.println(Jsons.toJson(result));
                Thread.sleep(intervalMs);
            }
            return 0;
        }

        private long effectiveGracefulTimeoutMs(long configured) {
            if (configured > 0) {
                return configured;
            }
            String os = System.getProperty("os.name", "").toLowerCase();
            if (os.contains("win")) {
                return 5_000L;
            }
            return 15_000L;
        }
    }

    @Command(name = "task", description = "Query task status by task id")
    static final class TaskCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Parameters(index = "0", description = "Task id")
        String taskId;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            Optional<TaskView> task = runtime.getTask(taskId);
            if (task.isEmpty()) {
                System.out.println("{\"error\":\"task not found\"}");
                return 1;
            }
            System.out.println(Jsons.toJson(task.get()));
            return 0;
        }
    }

    @Command(name = "cancel", description = "Cancel a running/pending task")
    static final class CancelCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Parameters(index = "0", description = "Task id")
        String taskId;

        @Option(names = {"--reason"}, description = "Cancellation reason")
        String reason;

        @Option(names = {"--mode"}, defaultValue = "hard", description = "Cancellation mode: hard|soft")
        String mode;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            var out = runtime.cancelTask(taskId, reason, mode);
            System.out.println(Jsons.toJson(out));
            return out.cancelled() ? 0 : 1;
        }
    }

    @Command(name = "task-export", description = "Export task snapshot (task/workflow/audit) to JSON")
    static final class TaskExportCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Parameters(index = "0", description = "Task id")
        String taskId;

        @Option(names = {"--out"}, required = true, description = "Output JSON path")
        String out;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.TaskExportOutcome result = runtime.exportTask(taskId, out);
            System.out.println(Jsons.toJson(result));
            return result.exported() ? 0 : 1;
        }
    }

    @Command(name = "tasks", description = "List tasks with optional filters")
    static final class TasksCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--status"}, description = "Filter by task status")
        String status;

        @Option(names = {"--limit"}, defaultValue = "50", description = "Max number of rows")
        int limit;

        @Option(names = {"--offset"}, defaultValue = "0", description = "Pagination offset")
        int offset;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.tasks(status, limit, offset)));
            return 0;
        }
    }

    @Command(name = "workflow", description = "Show workflow steps and dependencies by task id")
    static final class WorkflowCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Parameters(index = "0", description = "Task id")
        String taskId;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            var wf = runtime.workflow(taskId);
            if (wf.isEmpty()) {
                System.out.println("{\"error\":\"workflow not found\"}");
                return 1;
            }
            System.out.println(Jsons.toJson(wf.get()));
            return 0;
        }
    }

    @Command(name = "agents", description = "List registered local agents")
    static final class AgentsCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.agentRegistry().listAgentIds()));
            return 0;
        }
    }

    @Command(name = "stats", description = "Show runtime status counters")
    static final class StatsCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.stats()));
            return 0;
        }
    }

    @Command(name = "benchmark-run",
            description = "Run local sustained benchmark and output throughput/latency summary")
    static final class BenchmarkRunCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--tasks"}, defaultValue = "1000", description = "Number of tasks to submit")
        int tasks;

        @Option(names = {"--agent"}, defaultValue = "echo", description = "Target agent id")
        String agent;

        @Option(names = {"--priority"}, defaultValue = "normal", description = "Task priority")
        String priority;

        @Option(names = {"--input-size"}, defaultValue = "64", description = "Payload size per task (bytes)")
        int inputSize;

        @Option(names = {"--worker-id"}, defaultValue = "bench-worker", description = "Worker identity")
        String workerId;

        @Option(names = {"--node-id"}, defaultValue = "bench-node", description = "Node identity")
        String nodeId;

        @Option(names = {"--max-seconds"}, defaultValue = "300", description = "Max benchmark run time in seconds")
        int maxSeconds;

        @Option(names = {"--poll-batch"}, defaultValue = "64",
                description = "Worker polls per loop before status check")
        int pollBatch;

        @Option(names = {"--report-out"}, description = "Optional output JSON report path")
        String reportOut;

        @Override
        public Integer call() throws Exception {
            int taskCount = Math.max(1, tasks);
            int payloadSize = Math.max(1, inputSize);
            int maxRunSeconds = Math.max(5, maxSeconds);
            int polls = Math.max(1, pollBatch);

            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();

            String payloadPrefix = "bench-" + "x".repeat(payloadSize);
            List<String> taskIds = new ArrayList<>(taskCount);
            long submitStartedAtMs = Instant.now().toEpochMilli();
            for (int i = 0; i < taskCount; i++) {
                RelayMeshRuntime.SubmitOutcome out = runtime.submit(
                        agent,
                        payloadPrefix + "-" + i,
                        priority,
                        null
                );
                taskIds.add(out.taskId());
            }
            long submitFinishedAtMs = Instant.now().toEpochMilli();

            LinkedHashMap<String, TaskView> terminal = new LinkedHashMap<>();
            LinkedHashSet<String> remaining = new LinkedHashSet<>(taskIds);
            long runStartedAtMs = Instant.now().toEpochMilli();
            long deadlineMs = runStartedAtMs + (maxRunSeconds * 1000L);
            int loops = 0;

            while (!remaining.isEmpty() && Instant.now().toEpochMilli() < deadlineMs) {
                runtime.runMaintenance(nodeId);
                for (int i = 0; i < polls; i++) {
                    runtime.runWorkerOnce(workerId, nodeId);
                }
                loops++;
                collectTerminalTasks(runtime, remaining, terminal);
                if ((loops % 5) == 0) {
                    Thread.sleep(5L);
                }
            }
            collectTerminalTasks(runtime, remaining, terminal);

            long runFinishedAtMs = Instant.now().toEpochMilli();
            long runDurationMs = Math.max(1L, runFinishedAtMs - runStartedAtMs);
            boolean timedOut = !remaining.isEmpty();

            List<Long> latencies = new ArrayList<>();
            LinkedHashMap<String, Integer> statusCounts = new LinkedHashMap<>();
            for (TaskView view : terminal.values()) {
                statusCounts.merge(view.status(), 1, Integer::sum);
                long latency = Math.max(0L, view.updatedAtMs() - view.createdAtMs());
                latencies.add(latency);
            }
            latencies.sort(Long::compareTo);
            int completed = terminal.size();
            long p50 = percentile(latencies, 0.50d);
            long p95 = percentile(latencies, 0.95d);
            long p99 = percentile(latencies, 0.99d);
            double throughput = (completed * 1000.0d) / runDurationMs;
            RelayMeshRuntime.StatsOutcome stats = runtime.stats();

            BenchmarkRunOutcome outcome = new BenchmarkRunOutcome(
                    taskCount,
                    completed,
                    remaining.size(),
                    timedOut,
                    submitStartedAtMs,
                    submitFinishedAtMs,
                    runStartedAtMs,
                    runFinishedAtMs,
                    runDurationMs,
                    loops,
                    throughput,
                    p50,
                    p95,
                    p99,
                    statusCounts,
                    stats
            );

            if (reportOut != null && !reportOut.isBlank()) {
                Path out = Path.of(reportOut);
                if (out.getParent() != null) {
                    Files.createDirectories(out.getParent());
                }
                Files.writeString(out, Jsons.toJson(outcome), StandardCharsets.UTF_8);
            }

            System.out.println(Jsons.toJson(outcome));
            return timedOut ? 1 : 0;
        }

        private void collectTerminalTasks(
                RelayMeshRuntime runtime,
                Set<String> remaining,
                Map<String, TaskView> terminal
        ) {
            List<String> ids = new ArrayList<>(remaining);
            for (String taskId : ids) {
                Optional<TaskView> maybe = runtime.getTask(taskId);
                if (maybe.isEmpty()) {
                    continue;
                }
                TaskView view = maybe.get();
                if (isTerminalStatus(view.status())) {
                    terminal.put(taskId, view);
                    remaining.remove(taskId);
                }
            }
        }

        private boolean isTerminalStatus(String status) {
            if (status == null) {
                return false;
            }
            String upper = status.trim().toUpperCase(Locale.ROOT);
            return !("PENDING".equals(upper) || "RUNNING".equals(upper) || "RETRYING".equals(upper));
        }

        private long percentile(List<Long> sortedValues, double quantile) {
            if (sortedValues == null || sortedValues.isEmpty()) {
                return 0L;
            }
            double q = Math.max(0.0d, Math.min(1.0d, quantile));
            int index = (int) Math.ceil(q * sortedValues.size()) - 1;
            if (index < 0) {
                index = 0;
            }
            if (index >= sortedValues.size()) {
                index = sortedValues.size() - 1;
            }
            return sortedValues.get(index);
        }

        private record BenchmarkRunOutcome(
                int submittedTasks,
                int completedTasks,
                int pendingTasks,
                boolean timedOut,
                long submitStartedAtMs,
                long submitFinishedAtMs,
                long runStartedAtMs,
                long runFinishedAtMs,
                long runDurationMs,
                int loops,
                double throughputTasksPerSec,
                long latencyP50Ms,
                long latencyP95Ms,
                long latencyP99Ms,
                Map<String, Integer> statusCounts,
                RelayMeshRuntime.StatsOutcome stats
        ) {
        }
    }

    @Command(name = "members", description = "List mesh member nodes")
    static final class MembersCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.members()));
            return 0;
        }
    }

    @Command(name = "cluster-state", description = "Show cluster-wide state baseline (cluster_epoch)")
    static final class ClusterStateCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.clusterState()));
            return 0;
        }
    }

    @Command(name = "cluster-epoch-bump", description = "Bump cluster_epoch for mesh-wide fencing/maintenance operations")
    static final class ClusterEpochBumpCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--reason"}, defaultValue = "manual", description = "Reason for epoch bump")
        String reason;

        @Option(names = {"--actor"}, defaultValue = "cli", description = "Actor label for audit logs")
        String actor;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.bumpClusterEpoch(reason, actor)));
            return 0;
        }
    }

    @Command(name = "replication-export", description = "Export task/step/message deltas as a replication envelope")
    static final class ReplicationExportCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--since-ms"}, defaultValue = "0", description = "Inclusive updated_at_ms cursor")
        long sinceMs;

        @Option(names = {"--limit"}, defaultValue = "1000", description = "Max rows per delta collection")
        int limit;

        @Option(names = {"--source-node-id"}, defaultValue = "node-local",
                description = "Source node id written into the envelope for deterministic merge tie-break")
        String sourceNodeId;

        @Option(names = {"--out"}, required = true, description = "Output JSON envelope path")
        String out;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.ReplicationExportOutcome result =
                    runtime.exportReplicationEnvelope(sinceMs, limit, out, sourceNodeId);
            System.out.println(Jsons.toJson(result));
            return 0;
        }
    }

    @Command(name = "replication-import", description = "Import and apply task/step/message deltas from a replication envelope")
    static final class ReplicationImportCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--in"}, required = true, description = "Input envelope JSON path")
        String input;

        @Option(names = {"--actor"}, defaultValue = "cli", description = "Actor label for audit logs")
        String actor;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.ReplicationImportOutcome result =
                    runtime.importReplicationEnvelope(input, actor);
            System.out.println(Jsons.toJson(result));
            return 0;
        }
    }

    @Command(name = "mesh-summary", description = "Show mesh status and running ownership risk summary")
    static final class MeshSummaryCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.meshSummary()));
            return 0;
        }
    }

    @Command(name = "maintenance", description = "Run one maintenance tick (membership + lease + retry)")
    static final class MaintenanceCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--node-id"}, defaultValue = "node-local", description = "Node identity for membership heartbeat")
        String nodeId;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.MaintenanceOutcome out = runtime.runMaintenance(nodeId);
            System.out.println(Jsons.toJson(out));
            return 0;
        }
    }

    @Command(name = "metrics", description = "Print Prometheus metrics text")
    static final class MetricsCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.print(runtime.metricsText());
            return 0;
        }
    }

    @Command(name = "serve-metrics", description = "Expose Prometheus metrics over HTTP")
    static final class ServeMetricsCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--port"}, defaultValue = "9464", description = "Bind port")
        int port;

        @Option(names = {"--path"}, defaultValue = "/metrics", description = "Metrics path")
        String path;

        @Override
        public Integer call() throws Exception {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext(path, exchange -> {
                String body = runtime.metricsText();
                byte[] bytes = body.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            });
            server.setExecutor(null);
            server.start();
            System.out.println("Metrics server listening on http://127.0.0.1:" + port + path);
            Thread.currentThread().join();
            return 0;
        }
    }

    @Command(name = "gossip", description = "Run UDP gossip membership tick (prototype)")
    static final class GossipCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--node-id"}, defaultValue = "node-local", description = "Local node id")
        String nodeId;

        @Option(names = {"--bind-port"}, defaultValue = "17888", description = "UDP bind port")
        int bindPort;

        @Option(names = {"--seeds"}, split = ",", description = "Seed endpoints host:port, comma-separated")
        List<String> seeds;

        @Option(names = {"--seeds-file"}, description = "Seed endpoints file (one host:port per line)")
        String seedsFile;

        @Option(names = {"--window-ms"}, defaultValue = "300", description = "Receive window for each tick in ms")
        int windowMs;

        @Option(names = {"--fanout"}, defaultValue = "-1",
                description = "Seeds to send per tick (-1 uses runtime settings)")
        int fanout;

        @Option(names = {"--ttl"}, defaultValue = "-1",
                description = "Gossip packet TTL hop count (-1 uses runtime settings)")
        int ttl;

        @Option(names = {"--interval-ms"}, defaultValue = "1000", description = "Interval between ticks in loop mode")
        long intervalMs;

        @Option(names = {"--once"}, defaultValue = "true", description = "Run single tick if true")
        boolean once;

        @Override
        public Integer call() throws Exception {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            List<String> mergedSeeds = resolveSeedEndpoints(seeds, seedsFile);
            if (once) {
                System.out.println(Jsons.toJson(runtime.gossipTick(
                        nodeId,
                        bindPort,
                        mergedSeeds,
                        windowMs,
                        fanout <= 0 ? null : fanout,
                        ttl <= 0 ? null : ttl
                )));
                return 0;
            }
            while (true) {
                System.out.println(Jsons.toJson(runtime.gossipTick(
                        nodeId,
                        bindPort,
                        mergedSeeds,
                        windowMs,
                        fanout <= 0 ? null : fanout,
                        ttl <= 0 ? null : ttl
                )));
                Thread.sleep(intervalMs);
            }
        }
    }

    @Command(name = "gossip-sync", description = "Run anti-entropy gossip sync rounds")
    static final class GossipSyncCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--node-id"}, defaultValue = "node-local", description = "Local node id")
        String nodeId;

        @Option(names = {"--bind-port"}, defaultValue = "17888", description = "UDP bind port")
        int bindPort;

        @Option(names = {"--seeds"}, split = ",", description = "Seed endpoints host:port, comma-separated")
        List<String> seeds;

        @Option(names = {"--seeds-file"}, description = "Seed endpoints file (one host:port per line)")
        String seedsFile;

        @Option(names = {"--window-ms"}, defaultValue = "1200", description = "Receive window for each round in ms")
        int windowMs;

        @Option(names = {"--rounds"}, defaultValue = "3", description = "Number of sync rounds")
        int rounds;

        @Option(names = {"--interval-ms"}, defaultValue = "250", description = "Sleep interval between rounds")
        long intervalMs;

        @Option(names = {"--fanout"}, defaultValue = "-1",
                description = "Seeds to send per round (-1 uses runtime settings)")
        int fanout;

        @Option(names = {"--ttl"}, defaultValue = "-1",
                description = "Gossip packet TTL hop count (-1 uses runtime settings)")
        int ttl;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            List<String> mergedSeeds = resolveSeedEndpoints(seeds, seedsFile);
            RelayMeshRuntime.GossipSyncOutcome out = runtime.gossipSync(
                    nodeId,
                    bindPort,
                    mergedSeeds,
                    windowMs,
                    rounds,
                    intervalMs,
                    fanout <= 0 ? null : fanout,
                    ttl <= 0 ? null : ttl
            );
            System.out.println(Jsons.toJson(out));
            return 0;
        }
    }

    @Command(name = "reload-settings", description = "Force reload relaymesh-settings.json and print effective values")
    static final class ReloadSettingsCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.SettingsReloadOutcome out = runtime.reloadSettings();
            System.out.println(Jsons.toJson(out));
            return 0;
        }
    }

    @Command(name = "settings-history", description = "Show recent runtime.settings.load audit summaries")
    static final class SettingsHistoryCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--limit"}, defaultValue = "20", description = "Max rows")
        int limit;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.settingsHistory(limit)));
            return 0;
        }
    }

    @Command(name = "mesh-recover", description = "Recover running steps owned by DEAD mesh nodes")
    static final class MeshRecoverCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--limit"}, defaultValue = "128", description = "Max running steps to recover")
        int limit;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.recoverDeadOwnerLeases(limit)));
            return 0;
        }
    }

    @Command(name = "mesh-prune", description = "Prune stale DEAD mesh member rows")
    static final class MeshPruneCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--older-than-ms"}, defaultValue = "604800000",
                description = "Prune rows older than this age in ms (default 7d)")
        long olderThanMs;

        @Option(names = {"--limit"}, defaultValue = "256", description = "Max rows pruned in one run")
        int limit;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.MeshPruneOutcome out = runtime.pruneDeadNodesByAge(olderThanMs, limit);
            System.out.println(Jsons.toJson(out));
            return 0;
        }
    }

    @Command(name = "ownership-events", description = "List ownership timeline events (lease conflicts + recoveries)")
    static final class OwnershipEventsCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--limit"}, defaultValue = "100", description = "Max rows")
        int limit;

        @Option(names = {"--since-hours"}, defaultValue = "24", description = "Lookback window in hours")
        int sinceHours;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.ownershipEvents(limit, sinceHours)));
            return 0;
        }
    }

    @Command(name = "convergence-report", description = "Generate split-brain convergence report from replication imports")
    static final class ConvergenceReportCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--since-hours"}, defaultValue = "24", description = "Lookback window in hours")
        int sinceHours;

        @Option(names = {"--limit"}, defaultValue = "200", description = "Max replication import events included")
        int limit;

        @Option(names = {"--out"}, description = "Optional output JSON path")
        String out;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.convergenceReport(sinceHours, limit, out)));
            return 0;
        }
    }

    @Command(name = "lease-conflicts", description = "List ownership/lease CAS conflicts")
    static final class LeaseConflictsCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--limit"}, defaultValue = "100", description = "Max rows")
        int limit;

        @Option(names = {"--since-hours"}, defaultValue = "24", description = "Lookback window in hours")
        int sinceHours;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.leaseConflicts(limit, sinceHours)));
            return 0;
        }
    }

    @Command(name = "lease-report", description = "Generate aggregated lease conflict report")
    static final class LeaseReportCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--hours"}, defaultValue = "24", description = "Lookback window in hours")
        int hours;

        @Option(names = {"--limit"}, defaultValue = "5000", description = "Max source rows to aggregate")
        int limit;

        @Option(names = {"--out"}, description = "Output file path (default under <root>/reports)")
        String out;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            String output = out;
            if (output == null || output.isBlank()) {
                output = Path.of(parent.root, "reports", "lease-conflicts-manual.json").toString();
            }
            RelayMeshRuntime.LeaseReportOutcome result = runtime.generateLeaseConflictReport(hours, limit, output);
            System.out.println(Jsons.toJson(result));
            return 0;
        }
    }

    @Command(name = "serve-web", description = "Run minimal web console (tasks/workflow/dead/metrics)")
    static final class ServeWebCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--port"}, defaultValue = "8080", description = "Bind port")
        int port;

        @Option(names = {"--task-limit"}, defaultValue = "100", description = "Default task list limit")
        int taskLimit;

        @Option(names = {"--dead-limit"}, defaultValue = "100", description = "Default dead list limit")
        int deadLimit;

        @Option(names = {"--ro-token"}, description = "Read-only token for web APIs/events")
        String roToken;

        @Option(names = {"--ro-token-next"}, description = "Secondary read-only token during rotation")
        String roTokenNext;

        @Option(names = {"--rw-token"}, description = "Read-write token for mutating web APIs")
        String rwToken;

        @Option(names = {"--rw-token-next"}, description = "Secondary read-write token during rotation")
        String rwTokenNext;

        @Option(names = {"--auth-file"}, description = "Optional JSON auth file (principals/tokens/roles/service accounts)")
        String authFile;

        @Option(names = {"--allow-get-writes"}, defaultValue = "false",
                description = "Allow GET on write APIs (default false: POST only)")
        boolean allowGetWrites;

        @Option(names = {"--write-rate-limit-per-min"}, defaultValue = "120",
                description = "Max write requests per minute per remote address (0 disables)")
        int writeRateLimitPerMin;

        @Override
        public Integer call() throws Exception {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshConfig baseConfig = RelayMeshConfig.fromRoot(parent.root, parent.namespace);
            String defaultNamespace = baseConfig.namespace();
            Path rootBaseDir = baseConfig.rootBaseDir();
            ConcurrentMap<String, RelayMeshRuntime> namespaceRuntimeCache = new ConcurrentHashMap<>();
            namespaceRuntimeCache.put(defaultNamespace, runtime);
            WebAuth auth = WebAuth.load(roToken, rwToken, roTokenNext, rwTokenNext, authFile, parent.namespace);
            WriteRateLimiter writeRateLimiter = new WriteRateLimiter(Math.max(0, writeRateLimitPerMin));
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/", exchange -> {
                byte[] body = dashboardHtml().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            });
            server.createContext("/control-room", exchange -> {
                byte[] body = controlRoomHtml().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/html; charset=utf-8");
                exchange.sendResponseHeaders(200, body.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(body);
                }
            });
            server.createContext("/api/namespaces", exchange -> {
                Map<String, String> q = parseParams(exchange);
                AuthPrincipal principal = authorizePrincipal(exchange, q, auth, false);
                if (principal == null) return;
                List<String> discovered = discoverNamespaces(rootBaseDir, defaultNamespace);
                List<String> visible = filterVisibleNamespaces(discovered, principal);
                writeJson(exchange, Map.of(
                        "activeNamespace", defaultNamespace,
                        "namespaces", visible,
                        "timestamp", Instant.now().toString()
                ), 200);
            });
            server.createContext("/api/control-room/snapshot", exchange -> {
                Map<String, String> q = parseParams(exchange);
                AuthPrincipal principal = authorizePrincipal(exchange, q, auth, false);
                if (principal == null) return;
                int snapshotTaskLimit = clamp(parseIntOrDefault(q.get("taskLimit"), taskLimit), 1, 200);
                int snapshotDeadLimit = clamp(parseIntOrDefault(q.get("deadLimit"), deadLimit), 1, 200);
                int snapshotConflictLimit = clamp(parseIntOrDefault(q.get("conflictLimit"), 20), 1, 200);
                int snapshotSinceHours = clamp(parseIntOrDefault(q.get("sinceHours"), 24), 1, 24 * 30);
                String statusFilter = q.get("status");
                List<String> discovered = discoverNamespaces(rootBaseDir, defaultNamespace);
                List<String> requested = resolveRequestedNamespaces(
                        q.get("namespaces"),
                        q.get("ns"),
                        defaultNamespace,
                        discovered
                );
                if (requested.isEmpty()) {
                    requested = List.of(defaultNamespace);
                }
                for (String requestedNamespace : requested) {
                    if (!discovered.contains(requestedNamespace)) {
                        writeJson(
                                exchange,
                                Map.of("error", "namespace_not_found", "namespace", requestedNamespace),
                                404
                        );
                        return;
                    }
                    if (!principalCanAccessNamespace(principal, requestedNamespace)) {
                        writeJson(
                                exchange,
                                Map.of("error", "forbidden_namespace", "namespace", requestedNamespace),
                                403
                        );
                        return;
                    }
                }
                LinkedHashMap<String, Object> payload = buildControlRoomSnapshotPayload(
                        parent.root,
                        defaultNamespace,
                        namespaceRuntimeCache,
                        requested,
                        statusFilter,
                        snapshotTaskLimit,
                        snapshotDeadLimit,
                        snapshotConflictLimit,
                        snapshotSinceHours
                );
                writeJson(exchange, payload, 200);
            });
            server.createContext("/api/control-room/action", exchange -> {
                if (!allowWriteMethod(exchange, allowGetWrites, runtime, "/api/control-room/action")) return;
                if (!allowWriteRate(exchange, writeRateLimiter, runtime, "/api/control-room/action")) return;
                Map<String, String> q = parseParams(exchange);
                AuthPrincipal principal = authorizePrincipal(exchange, q, auth, true);
                if (principal == null) return;
                String action = q.get("action");
                if (action == null || action.isBlank()) {
                    writeJson(exchange, Map.of("error", "missing_action"), 400);
                    return;
                }
                String requestedNamespace = normalizeNamespace(q.get("namespace"), defaultNamespace);
                List<String> discovered = discoverNamespaces(rootBaseDir, defaultNamespace);
                if (!discovered.contains(requestedNamespace)) {
                    writeJson(
                            exchange,
                            Map.of("error", "namespace_not_found", "namespace", requestedNamespace),
                            404
                    );
                    return;
                }
                if (!principalCanAccessNamespace(principal, requestedNamespace)) {
                    writeJson(
                            exchange,
                            Map.of("error", "forbidden_namespace", "namespace", requestedNamespace),
                            403
                    );
                    return;
                }
                RelayMeshRuntime scoped = runtimeForNamespace(parent.root, requestedNamespace, namespaceRuntimeCache);
                Map<String, Object> auditMeta = new LinkedHashMap<>(requestAuditMeta(exchange, "/api/control-room/action"));
                auditMeta.put("namespace", requestedNamespace);
                auditMeta.put("control_action", action);
                LinkedHashMap<String, Object> response = new LinkedHashMap<>();
                response.put("timestamp", Instant.now().toString());
                response.put("namespace", requestedNamespace);
                response.put("action", action);
                switch (action.trim().toLowerCase(Locale.ROOT)) {
                    case "cancel" -> {
                        String taskId = q.get("taskId");
                        if (taskId == null || taskId.isBlank()) {
                            writeJson(exchange, Map.of("error", "missing_task_id"), 400);
                            return;
                        }
                        String mode = q.get("mode");
                        String reason = q.get("reason");
                        Object result = scoped.cancelTaskViaWeb(
                                taskId,
                                reason,
                                mode == null || mode.isBlank() ? "hard" : mode,
                                auditMeta
                        );
                        response.put("result", result);
                    }
                    case "replay" -> {
                        String taskId = q.get("taskId");
                        if (taskId == null || taskId.isBlank()) {
                            writeJson(exchange, Map.of("error", "missing_task_id"), 400);
                            return;
                        }
                        Object result = scoped.replayDeadLetterViaWeb(taskId, auditMeta);
                        response.put("result", result);
                    }
                    case "replay_batch" -> {
                        String status = q.get("status");
                        int limit = clamp(parseIntOrDefault(q.get("limit"), 100), 1, 500);
                        Object result = scoped.replayDeadLetterBatchViaWeb(
                                status == null || status.isBlank() ? "DEAD_LETTER" : status,
                                limit,
                                auditMeta
                        );
                        response.put("result", result);
                    }
                    default -> {
                        writeJson(exchange, Map.of("error", "unsupported_action", "action", action), 400);
                        return;
                    }
                }
                writeJson(exchange, response, 200);
            });
            server.createContext("/api/control-room/workflow", exchange -> {
                Map<String, String> q = parseParams(exchange);
                AuthPrincipal principal = authorizePrincipal(exchange, q, auth, false);
                if (principal == null) return;
                String requestedNamespace = normalizeNamespace(q.get("namespace"), defaultNamespace);
                String taskId = q.get("taskId");
                if (taskId == null || taskId.isBlank()) {
                    writeJson(exchange, Map.of("error", "missing_task_id"), 400);
                    return;
                }
                List<String> discovered = discoverNamespaces(rootBaseDir, defaultNamespace);
                if (!discovered.contains(requestedNamespace)) {
                    writeJson(
                            exchange,
                            Map.of("error", "namespace_not_found", "namespace", requestedNamespace),
                            404
                    );
                    return;
                }
                if (!principalCanAccessNamespace(principal, requestedNamespace)) {
                    writeJson(
                            exchange,
                            Map.of("error", "forbidden_namespace", "namespace", requestedNamespace),
                            403
                    );
                    return;
                }
                RelayMeshRuntime scoped = runtimeForNamespace(parent.root, requestedNamespace, namespaceRuntimeCache);
                var wf = scoped.workflow(taskId);
                if (wf.isEmpty()) {
                    writeJson(exchange, Map.of("error", "workflow_not_found", "taskId", taskId), 404);
                    return;
                }
                LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
                payload.put("timestamp", Instant.now().toString());
                payload.put("namespace", requestedNamespace);
                payload.put("taskId", taskId);
                payload.put("workflow", wf.get());
                payload.put("edges", buildWorkflowEdges(wf.get()));
                writeJson(exchange, payload, 200);
            });
            server.createContext("/api/control-room/command", exchange -> {
                if (!allowMethods(exchange, "POST")) return;
                Map<String, String> q = parseParams(exchange);
                String command = q.get("command");
                if (command == null || command.isBlank()) {
                    writeJson(exchange, Map.of("error", "missing_command"), 400);
                    return;
                }
                List<String> tokens = parseControlCommandTokens(command);
                if (tokens.isEmpty()) {
                    writeJson(exchange, Map.of("error", "empty_command"), 400);
                    return;
                }
                String op = tokens.get(0).toLowerCase(Locale.ROOT);
                boolean writeRequired = isControlRoomWriteCommand(op);
                if (writeRequired) {
                    if (!allowWriteRate(exchange, writeRateLimiter, runtime, "/api/control-room/command")) return;
                }
                AuthPrincipal principal = authorizePrincipal(exchange, q, auth, writeRequired);
                if (principal == null) return;
                List<String> discovered = discoverNamespaces(rootBaseDir, defaultNamespace);
                LinkedHashMap<String, Object> response = new LinkedHashMap<>();
                response.put("timestamp", Instant.now().toString());
                response.put("command", command);
                switch (op) {
                    case "help" -> {
                        response.put("result", Map.of(
                                "commands", List.of(
                                        "help",
                                        "namespaces",
                                        "stats <namespace>",
                                        "members <namespace>",
                                        "tasks <namespace> [status] [limit]",
                                        "workflow <namespace> <taskId>",
                                        "cancel <namespace> <taskId> [soft|hard] [reason...]",
                                        "replay <namespace> <taskId>",
                                        "replay-batch <namespace> [limit]"
                                )
                        ));
                    }
                    case "namespaces" -> {
                        response.put("result", Map.of(
                                "activeNamespace", defaultNamespace,
                                "namespaces", filterVisibleNamespaces(discovered, principal)
                        ));
                    }
                    case "stats", "members", "tasks", "workflow", "cancel", "replay", "replay-batch", "replay_batch" -> {
                        if (tokens.size() < 2) {
                            writeJson(exchange, Map.of("error", "missing_namespace", "command", op), 400);
                            return;
                        }
                        String requestedNamespace = normalizeNamespace(tokens.get(1), defaultNamespace);
                        if (!discovered.contains(requestedNamespace)) {
                            writeJson(
                                    exchange,
                                    Map.of("error", "namespace_not_found", "namespace", requestedNamespace),
                                    404
                            );
                            return;
                        }
                        if (!principalCanAccessNamespace(principal, requestedNamespace)) {
                            writeJson(
                                    exchange,
                                    Map.of("error", "forbidden_namespace", "namespace", requestedNamespace),
                                    403
                            );
                            return;
                        }
                        RelayMeshRuntime scoped = runtimeForNamespace(parent.root, requestedNamespace, namespaceRuntimeCache);
                        response.put("namespace", requestedNamespace);
                        switch (op) {
                            case "stats" -> response.put("result", scoped.stats());
                            case "members" -> response.put("result", scoped.members());
                            case "tasks" -> {
                                String status = tokens.size() >= 3 ? tokens.get(2) : "";
                                if ("all".equalsIgnoreCase(status) || "-".equals(status)) {
                                    status = "";
                                }
                                int limit = tokens.size() >= 4 ? clamp(parseIntOrDefault(tokens.get(3), 20), 1, 200) : 20;
                                response.put("result", scoped.tasks(status, limit, 0));
                            }
                            case "workflow" -> {
                                if (tokens.size() < 3) {
                                    writeJson(exchange, Map.of("error", "missing_task_id", "command", op), 400);
                                    return;
                                }
                                String taskId = tokens.get(2);
                                var wf = scoped.workflow(taskId);
                                if (wf.isEmpty()) {
                                    writeJson(exchange, Map.of("error", "workflow_not_found", "taskId", taskId), 404);
                                    return;
                                }
                                response.put("taskId", taskId);
                                response.put("result", Map.of(
                                        "workflow", wf.get(),
                                        "edges", buildWorkflowEdges(wf.get())
                                ));
                            }
                            case "cancel" -> {
                                if (tokens.size() < 3) {
                                    writeJson(exchange, Map.of("error", "missing_task_id", "command", op), 400);
                                    return;
                                }
                                String taskId = tokens.get(2);
                                String mode = tokens.size() >= 4 ? tokens.get(3) : "hard";
                                String reason = joinCommandTail(tokens, 4);
                                Map<String, Object> audit = new LinkedHashMap<>(requestAuditMeta(exchange, "/api/control-room/command"));
                                audit.put("namespace", requestedNamespace);
                                audit.put("control_command", command);
                                Object result = scoped.cancelTaskViaWeb(taskId, reason, mode, audit);
                                response.put("taskId", taskId);
                                response.put("mode", mode);
                                response.put("result", result);
                            }
                            case "replay" -> {
                                if (tokens.size() < 3) {
                                    writeJson(exchange, Map.of("error", "missing_task_id", "command", op), 400);
                                    return;
                                }
                                String taskId = tokens.get(2);
                                Map<String, Object> audit = new LinkedHashMap<>(requestAuditMeta(exchange, "/api/control-room/command"));
                                audit.put("namespace", requestedNamespace);
                                audit.put("control_command", command);
                                Object result = scoped.replayDeadLetterViaWeb(taskId, audit);
                                response.put("taskId", taskId);
                                response.put("result", result);
                            }
                            case "replay-batch", "replay_batch" -> {
                                int limit = tokens.size() >= 3 ? clamp(parseIntOrDefault(tokens.get(2), 50), 1, 500) : 50;
                                Map<String, Object> audit = new LinkedHashMap<>(requestAuditMeta(exchange, "/api/control-room/command"));
                                audit.put("namespace", requestedNamespace);
                                audit.put("control_command", command);
                                Object result = scoped.replayDeadLetterBatchViaWeb("DEAD_LETTER", limit, audit);
                                response.put("limit", limit);
                                response.put("result", result);
                            }
                            default -> {
                                writeJson(exchange, Map.of("error", "unsupported_command", "command", op), 400);
                                return;
                            }
                        }
                    }
                    default -> {
                        writeJson(exchange, Map.of("error", "unsupported_command", "command", op), 400);
                        return;
                    }
                }
                writeJson(exchange, response, 200);
            });
            server.createContext("/api/control-room/layouts", exchange -> {
                Map<String, String> q = parseParams(exchange);
                AuthPrincipal principal = authorizePrincipal(exchange, q, auth, false);
                if (principal == null) return;
                String profile = normalizeLayoutProfileName(q.get("name"));
                LinkedHashMap<String, JsonNode> profiles;
                synchronized (CONTROL_ROOM_LAYOUTS_LOCK) {
                    profiles = readControlRoomLayouts(rootBaseDir);
                }
                if (!profile.isEmpty()) {
                    JsonNode layout = profiles.get(profile);
                    if (layout == null) {
                        writeJson(exchange, Map.of("error", "profile_not_found", "name", profile), 404);
                        return;
                    }
                    writeJson(exchange, Map.of(
                            "timestamp", Instant.now().toString(),
                            "name", profile,
                            "layout", layout
                    ), 200);
                    return;
                }
                List<String> names = new ArrayList<>(profiles.keySet());
                names.sort(String::compareTo);
                writeJson(exchange, Map.of(
                        "timestamp", Instant.now().toString(),
                        "profiles", names,
                        "count", names.size()
                ), 200);
            });
            server.createContext("/api/control-room/layouts/save", exchange -> {
                if (!allowMethods(exchange, "POST")) return;
                if (!allowWriteRate(exchange, writeRateLimiter, runtime, "/api/control-room/layouts/save")) return;
                Map<String, String> q = parseParams(exchange);
                AuthPrincipal principal = authorizePrincipal(exchange, q, auth, true);
                if (principal == null) return;
                String profile = normalizeLayoutProfileName(q.get("name"));
                if (profile.isEmpty()) {
                    writeJson(exchange, Map.of("error", "invalid_profile_name"), 400);
                    return;
                }
                String layoutRaw = q.get("layout");
                if (layoutRaw == null || layoutRaw.isBlank()) {
                    writeJson(exchange, Map.of("error", "missing_layout"), 400);
                    return;
                }
                JsonNode layout;
                try {
                    layout = Jsons.mapper().readTree(layoutRaw);
                } catch (Exception e) {
                    writeJson(exchange, Map.of("error", "invalid_layout_json"), 400);
                    return;
                }
                if (layout == null || !layout.isObject()) {
                    writeJson(exchange, Map.of("error", "invalid_layout_model"), 400);
                    return;
                }
                int count;
                synchronized (CONTROL_ROOM_LAYOUTS_LOCK) {
                    LinkedHashMap<String, JsonNode> profiles = readControlRoomLayouts(rootBaseDir);
                    profiles.put(profile, layout);
                    writeControlRoomLayouts(rootBaseDir, profiles);
                    count = profiles.size();
                }
                writeJson(exchange, Map.of(
                        "timestamp", Instant.now().toString(),
                        "saved", profile,
                        "count", count
                ), 200);
            });
            server.createContext("/api/control-room/layouts/delete", exchange -> {
                if (!allowMethods(exchange, "POST")) return;
                if (!allowWriteRate(exchange, writeRateLimiter, runtime, "/api/control-room/layouts/delete")) return;
                Map<String, String> q = parseParams(exchange);
                AuthPrincipal principal = authorizePrincipal(exchange, q, auth, true);
                if (principal == null) return;
                String profile = normalizeLayoutProfileName(q.get("name"));
                if (profile.isEmpty()) {
                    writeJson(exchange, Map.of("error", "invalid_profile_name"), 400);
                    return;
                }
                boolean deleted;
                int count;
                synchronized (CONTROL_ROOM_LAYOUTS_LOCK) {
                    LinkedHashMap<String, JsonNode> profiles = readControlRoomLayouts(rootBaseDir);
                    deleted = profiles.remove(profile) != null;
                    writeControlRoomLayouts(rootBaseDir, profiles);
                    count = profiles.size();
                }
                writeJson(exchange, Map.of(
                        "timestamp", Instant.now().toString(),
                        "deleted", profile,
                        "removed", deleted,
                        "count", count
                ), 200);
            });
            server.createContext("/events/control-room", exchange -> {
                Map<String, String> q = parseParams(exchange);
                AuthPrincipal principal = authorizePrincipal(exchange, q, auth, false);
                if (principal == null) return;
                int snapshotTaskLimit = clamp(parseIntOrDefault(q.get("taskLimit"), taskLimit), 1, 200);
                int snapshotDeadLimit = clamp(parseIntOrDefault(q.get("deadLimit"), deadLimit), 1, 200);
                int snapshotConflictLimit = clamp(parseIntOrDefault(q.get("conflictLimit"), 20), 1, 200);
                int snapshotSinceHours = clamp(parseIntOrDefault(q.get("sinceHours"), 24), 1, 24 * 30);
                int intervalMs = clamp(parseIntOrDefault(q.get("intervalMs"), 3000), 1000, 30000);
                String statusFilter = q.get("status");
                List<String> discovered = discoverNamespaces(rootBaseDir, defaultNamespace);
                List<String> requested = resolveRequestedNamespaces(
                        q.get("namespaces"),
                        q.get("ns"),
                        defaultNamespace,
                        discovered
                );
                if (requested.isEmpty()) {
                    requested = List.of(defaultNamespace);
                }
                for (String requestedNamespace : requested) {
                    if (!discovered.contains(requestedNamespace)) {
                        writeJson(
                                exchange,
                                Map.of("error", "namespace_not_found", "namespace", requestedNamespace),
                                404
                        );
                        return;
                    }
                    if (!principalCanAccessNamespace(principal, requestedNamespace)) {
                        writeJson(
                                exchange,
                                Map.of("error", "forbidden_namespace", "namespace", requestedNamespace),
                                403
                        );
                        return;
                    }
                }
                exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
                exchange.getResponseHeaders().set("Cache-Control", "no-cache");
                exchange.getResponseHeaders().set("Connection", "keep-alive");
                exchange.sendResponseHeaders(200, 0);
                try (OutputStream os = exchange.getResponseBody()) {
                    while (true) {
                        LinkedHashMap<String, Object> payload = buildControlRoomSnapshotPayload(
                                parent.root,
                                defaultNamespace,
                                namespaceRuntimeCache,
                                requested,
                                statusFilter,
                                snapshotTaskLimit,
                                snapshotDeadLimit,
                                snapshotConflictLimit,
                                snapshotSinceHours
                        );
                        String data = Jsons.toJson(payload).replace("\r", " ").replace("\n", " ");
                        String frame = "event: control_snapshot\ndata: " + data + "\n\n";
                        os.write(frame.getBytes(StandardCharsets.UTF_8));
                        os.flush();
                        Thread.sleep(intervalMs);
                    }
                } catch (Exception ignored) {
                    // Client disconnected or stream interrupted.
                }
            });
            server.createContext("/api/tasks", exchange -> {
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, false)) return;
                int limit = parseIntOrDefault(q.get("limit"), taskLimit);
                int offset = parseIntOrDefault(q.get("offset"), 0);
                String status = q.get("status");
                writeJson(exchange, runtime.tasks(status, limit, offset), 200);
            });
            server.createContext("/api/dead", exchange -> {
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, false)) return;
                int limit = parseIntOrDefault(q.get("limit"), deadLimit);
                writeJson(exchange, runtime.tasks("DEAD_LETTER", limit, 0), 200);
            });
            server.createContext("/api/workflow", exchange -> {
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, false)) return;
                String taskId = q.get("taskId");
                if (taskId == null || taskId.isBlank()) {
                    writeJson(exchange, Map.of("error", "missing taskId"), 400);
                    return;
                }
                var wf = runtime.workflow(taskId);
                if (wf.isEmpty()) {
                    writeJson(exchange, Map.of("error", "workflow not found"), 404);
                    return;
                }
                writeJson(exchange, wf.get(), 200);
            });
            server.createContext("/api/stats", exchange -> {
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, false)) return;
                writeJson(exchange, runtime.stats(), 200);
            });
            server.createContext("/api/members", exchange -> {
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, false)) return;
                writeJson(exchange, runtime.members(), 200);
            });
            server.createContext("/api/lease-conflicts", exchange -> {
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, false)) return;
                int limit = parseIntOrDefault(q.get("limit"), 100);
                int since = parseIntOrDefault(q.get("sinceHours"), 24);
                writeJson(exchange, runtime.leaseConflicts(limit, since), 200);
            });
            server.createContext("/api/cancel", exchange -> {
                if (!allowWriteMethod(exchange, allowGetWrites, runtime, "/api/cancel")) return;
                if (!allowWriteRate(exchange, writeRateLimiter, runtime, "/api/cancel")) return;
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, true)) return;
                String taskId = q.get("taskId");
                if (taskId == null || taskId.isBlank()) {
                    writeJson(exchange, Map.of("error", "missing taskId"), 400);
                    return;
                }
                String mode = q.get("mode");
                String reason = q.get("reason");
                writeJson(
                        exchange,
                        runtime.cancelTaskViaWeb(
                                taskId,
                                reason,
                                mode == null || mode.isBlank() ? "hard" : mode,
                                requestAuditMeta(exchange, "/api/cancel")
                        ),
                        200
                );
            });
            server.createContext("/api/replay", exchange -> {
                if (!allowWriteMethod(exchange, allowGetWrites, runtime, "/api/replay")) return;
                if (!allowWriteRate(exchange, writeRateLimiter, runtime, "/api/replay")) return;
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, true)) return;
                String taskId = q.get("taskId");
                if (taskId == null || taskId.isBlank()) {
                    writeJson(exchange, Map.of("error", "missing taskId"), 400);
                    return;
                }
                writeJson(exchange, runtime.replayDeadLetterViaWeb(taskId, requestAuditMeta(exchange, "/api/replay")), 200);
            });
            server.createContext("/api/replay-batch", exchange -> {
                if (!allowWriteMethod(exchange, allowGetWrites, runtime, "/api/replay-batch")) return;
                if (!allowWriteRate(exchange, writeRateLimiter, runtime, "/api/replay-batch")) return;
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, true)) return;
                String status = q.get("status");
                int limit = parseIntOrDefault(q.get("limit"), 100);
                writeJson(
                        exchange,
                        runtime.replayDeadLetterBatchViaWeb(
                                status == null ? "DEAD_LETTER" : status,
                                limit,
                                requestAuditMeta(exchange, "/api/replay-batch")
                        ),
                        200
                );
            });
            server.createContext("/api/audit-query", exchange -> {
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, false)) return;
                String taskId = q.get("taskId");
                String action = q.get("action");
                String from = q.get("from");
                String to = q.get("to");
                int limit = parseIntOrDefault(q.get("limit"), 100);
                writeJson(exchange, runtime.auditQuery(taskId, action, from, to, limit), 200);
            });
            server.createContext("/api/metrics", exchange -> {
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, false)) return;
                String body = runtime.metricsText();
                byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            });
            server.createContext("/events", exchange -> {
                Map<String, String> q = parseParams(exchange);
                if (!authorize(exchange, q, auth, false)) return;
                exchange.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
                exchange.getResponseHeaders().set("Cache-Control", "no-cache");
                exchange.getResponseHeaders().set("Connection", "keep-alive");
                exchange.sendResponseHeaders(200, 0);
                try (OutputStream os = exchange.getResponseBody()) {
                    while (true) {
                        Map<String, Object> snapshot = Map.of(
                                "timestamp", Instant.now().toString(),
                                "members", runtime.members(),
                                "stats", runtime.stats(),
                                "conflicts", runtime.leaseConflicts(20, 24)
                        );
                        String data = Jsons.toJson(snapshot).replace("\r", " ").replace("\n", " ");
                        String frame = "event: snapshot\ndata: " + data + "\n\n";
                        os.write(frame.getBytes(StandardCharsets.UTF_8));
                        os.flush();
                        Thread.sleep(5000L);
                    }
                } catch (Exception ignored) {
                    // Client disconnected or stream interrupted.
                }
            });
            server.setExecutor(null);
            server.start();
            String authMode = auth.enabled()
                    ? "enabled (principals=" + auth.principalCount() + ", namespace=" + parent.namespace + ")"
                    : "disabled";
            String writeMethodMode = allowGetWrites ? "GET+POST" : "POST-only";
            System.out.println("Web console listening on http://127.0.0.1:" + port + "/, auth=" + authMode + ", writeMethods=" + writeMethodMode + ", writeRateLimitPerMin=" + Math.max(0, writeRateLimitPerMin));
            if (allowGetWrites) {
                System.out.println("DEPRECATION: --allow-get-writes is temporary compatibility mode and will be removed in a future release; use POST for write APIs.");
            }
            Thread.currentThread().join();
            return 0;
        }
    }

    @Command(name = "serve-node-rpc", description = "Run HTTPS node RPC for replication export/import (supports optional mTLS)")
    static final class ServeNodeRpcCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--bind"}, defaultValue = "127.0.0.1", description = "Bind host")
        String bind;

        @Option(names = {"--port"}, defaultValue = "18443", description = "Bind port")
        int port;

        @Option(names = {"--keystore"}, required = true, description = "TLS keystore path (PKCS12/JKS)")
        String keystorePath;

        @Option(names = {"--keystore-pass"}, required = true, description = "TLS keystore password")
        String keystorePass;

        @Option(names = {"--keystore-type"}, defaultValue = "PKCS12", description = "Keystore type")
        String keystoreType;

        @Option(names = {"--truststore"}, description = "Optional truststore path for client cert validation")
        String truststorePath;

        @Option(names = {"--truststore-pass"}, description = "Truststore password")
        String truststorePass;

        @Option(names = {"--truststore-type"}, defaultValue = "PKCS12", description = "Truststore type")
        String truststoreType;

        @Option(names = {"--require-client-auth"}, defaultValue = "false", description = "Require client certificate (mTLS)")
        boolean requireClientAuth;

        @Option(names = {"--source-node-id"}, defaultValue = "node-rpc", description = "Default source node id for export endpoint")
        String sourceNodeId;

        @Option(names = {"--revocation-file"}, description = "Optional revocation list file (serial:<hex> / sha256:<hex>)")
        String revocationFile;

        @Option(names = {"--reload-interval-ms"}, defaultValue = "5000",
                description = "Reload TLS materials when keystore/truststore/revocation file changes; 0 disables reload")
        long reloadIntervalMs;

        @Override
        public Integer call() throws Exception {
            if (requireClientAuth && (truststorePath == null || truststorePath.isBlank())) {
                throw new IllegalArgumentException("--truststore is required when --require-client-auth=true");
            }
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();

            AtomicReference<NodeRpcTls.SslBundle> tlsBundleRef = new AtomicReference<>(loadTlsBundle());
            HttpsServer server = HttpsServer.create(new InetSocketAddress(bind, port), 0);
            server.setHttpsConfigurator(buildHttpsConfigurator(tlsBundleRef));
            server.createContext("/node/health", exchange -> {
                if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    writeJson(exchange, Map.of("error", "method_not_allowed"), 405);
                    return;
                }
                writeJson(exchange, runtime.health(), 200);
            });
            server.createContext("/node/tls/status", exchange -> {
                if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    writeJson(exchange, Map.of("error", "method_not_allowed"), 405);
                    return;
                }
                NodeRpcTls.SslBundle tls = tlsBundleRef.get();
                writeJson(exchange, tlsStatusPayload(tls), 200);
            });
            server.createContext("/node/replication/export", exchange -> {
                if (!"GET".equalsIgnoreCase(exchange.getRequestMethod())) {
                    writeJson(exchange, Map.of("error", "method_not_allowed"), 405);
                    return;
                }
                Map<String, String> q = parseParams(exchange);
                long sinceMs = parseLongOrDefault(q.get("sinceMs"), 0L);
                int limit = parseIntOrDefault(q.get("limit"), 1000);
                String source = q.get("sourceNodeId");
                if (source == null || source.isBlank()) {
                    source = sourceNodeId;
                }
                Path tmpDir = Paths.get(parent.root).resolve("tmp");
                Files.createDirectories(tmpDir);
                Path tmp = Files.createTempFile(tmpDir, "node-rpc-export-", ".json");
                try {
                    runtime.exportReplicationEnvelope(sinceMs, limit, tmp.toString(), source);
                    JsonNode envelope = Jsons.mapper().readTree(Files.readString(tmp, StandardCharsets.UTF_8));
                    writeJson(exchange, envelope, 200);
                } finally {
                    Files.deleteIfExists(tmp);
                }
            });
            server.createContext("/node/replication/import", exchange -> {
                if (!"POST".equalsIgnoreCase(exchange.getRequestMethod())) {
                    writeJson(exchange, Map.of("error", "method_not_allowed"), 405);
                    return;
                }
                Map<String, String> q = parseParams(exchange);
                String actor = q.get("actor");
                if (actor == null || actor.isBlank()) {
                    actor = "node-rpc";
                }
                String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
                Path tmpDir = Paths.get(parent.root).resolve("tmp");
                Files.createDirectories(tmpDir);
                Path tmp = Files.createTempFile(tmpDir, "node-rpc-import-", ".json");
                try {
                    Files.writeString(tmp, body, StandardCharsets.UTF_8);
                    RelayMeshRuntime.ReplicationImportOutcome out = runtime.importReplicationEnvelope(tmp.toString(), actor);
                    writeJson(exchange, out, 200);
                } finally {
                    Files.deleteIfExists(tmp);
                }
            });
            server.setExecutor(null);

            AtomicBoolean running = new AtomicBoolean(true);
            AtomicReference<Thread> reloadThreadRef = new AtomicReference<>();
            if (reloadIntervalMs > 0L) {
                long sleepMs = Math.max(1_000L, reloadIntervalMs);
                Thread reloadThread = new Thread(() -> {
                    while (running.get()) {
                        try {
                            Thread.sleep(sleepMs);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                        try {
                            NodeRpcTls.SslBundle current = tlsBundleRef.get();
                            if (!tlsMaterialChanged(current)) {
                                continue;
                            }
                            NodeRpcTls.SslBundle reloaded = loadTlsBundle();
                            tlsBundleRef.set(reloaded);
                            server.setHttpsConfigurator(buildHttpsConfigurator(tlsBundleRef));
                            System.out.println(
                                    "Node RPC TLS reloaded: fingerprintSha256=" + reloaded.serverCertFingerprintSha256() +
                                            ", revokedEntries=" + reloaded.revocationPolicy().entryCount()
                            );
                        } catch (Exception e) {
                            System.err.println("WARN node-rpc tls reload failed: " + e.getMessage());
                        }
                    }
                }, "relaymesh-node-rpc-tls-reload");
                reloadThread.setDaemon(true);
                reloadThread.start();
                reloadThreadRef.set(reloadThread);
            }

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                running.set(false);
                Thread reloadThread = reloadThreadRef.get();
                if (reloadThread != null) {
                    try {
                        reloadThread.interrupt();
                    } catch (Exception ignored) {
                    }
                }
                try {
                    server.stop(0);
                } catch (Exception ignored) {
                }
            }, "relaymesh-node-rpc-shutdown"));

            server.start();
            NodeRpcTls.SslBundle loaded = tlsBundleRef.get();
            System.out.println("Node RPC listening on https://" + bind + ":" + port +
                    ", requireClientAuth=" + requireClientAuth +
                    ", sourceNodeId=" + sourceNodeId +
                    ", tlsFingerprintSha256=" + loaded.serverCertFingerprintSha256() +
                    ", revokedEntries=" + loaded.revocationPolicy().entryCount() +
                    ", reloadIntervalMs=" + Math.max(0L, reloadIntervalMs));
            Thread.currentThread().join();
            return 0;
        }

        private HttpsConfigurator buildHttpsConfigurator(AtomicReference<NodeRpcTls.SslBundle> tlsBundleRef) {
            NodeRpcTls.SslBundle initial = tlsBundleRef.get();
            return new HttpsConfigurator(initial.sslContext()) {
                @Override
                public void configure(HttpsParameters params) {
                    SSLContext currentSsl = tlsBundleRef.get().sslContext();
                    SSLParameters sslParams = currentSsl.getDefaultSSLParameters();
                    sslParams.setNeedClientAuth(requireClientAuth);
                    params.setSSLParameters(sslParams);
                }
            };
        }

        private NodeRpcTls.SslBundle loadTlsBundle() throws Exception {
            return NodeRpcTls.build(
                    keystorePath,
                    keystorePass,
                    keystoreType,
                    truststorePath,
                    truststorePass,
                    truststoreType,
                    revocationFile
            );
        }

        private boolean tlsMaterialChanged(NodeRpcTls.SslBundle current) {
            if (current == null) {
                return true;
            }
            long keyMtime = NodeRpcTls.fileMtimeMs(keystorePath);
            long trustMtime = NodeRpcTls.fileMtimeMs(truststorePath);
            long revokeMtime = NodeRpcTls.fileMtimeMs(revocationFile);
            return keyMtime != current.keystoreMtimeMs()
                    || trustMtime != current.truststoreMtimeMs()
                    || revokeMtime != current.revocationMtimeMs();
        }

        private Map<String, Object> tlsStatusPayload(NodeRpcTls.SslBundle bundle) {
            LinkedHashMap<String, Object> out = new LinkedHashMap<>();
            out.put("ok", true);
            out.put("requireClientAuth", requireClientAuth);
            out.put("reloadIntervalMs", Math.max(0L, reloadIntervalMs));
            out.put("loadedAtMs", bundle.loadedAtMs());
            out.put("serverCertFingerprintSha256", bundle.serverCertFingerprintSha256());
            out.put("serverCertSerialHex", bundle.serverCertSerialHex());
            out.put("serverCertSubject", bundle.serverCertSubject());
            out.put("serverCertNotAfterIso", bundle.serverCertNotAfterIso());
            out.put("revocationSourcePath", bundle.revocationPolicy().sourcePath());
            out.put("revocationLoadedAtMs", bundle.revocationPolicy().loadedAtMs());
            out.put("revokedSerialCount", bundle.revocationPolicy().revokedSerialNumbers().size());
            out.put("revokedFingerprintCount", bundle.revocationPolicy().revokedSha256Fingerprints().size());
            out.put("revokedEntryCount", bundle.revocationPolicy().entryCount());
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
    }

    @Command(name = "node-rpc-revocation-template",
            description = "Generate a revocation list template for serve-node-rpc --revocation-file")
    static final class NodeRpcRevocationTemplateCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--out"}, required = true, description = "Output revocation file path")
        String out;

        @Option(names = {"--serial"}, split = ",",
                description = "Optional comma-separated certificate serial hex entries")
        List<String> serials = List.of();

        @Option(names = {"--fingerprint"}, split = ",",
                description = "Optional comma-separated sha256 fingerprint entries")
        List<String> fingerprints = List.of();

        @Override
        public Integer call() throws Exception {
            Path outPath = Path.of(out);
            if (outPath.getParent() != null) {
                Files.createDirectories(outPath.getParent());
            }
            NodeRpcTls.writeRevocationTemplate(outPath, serials, fingerprints);
            NodeRpcTls.RevocationPolicy policy = NodeRpcTls.loadRevocationPolicy(outPath.toString());
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("output", outPath.toString());
            result.put("revokedSerialCount", policy.revokedSerialNumbers().size());
            result.put("revokedFingerprintCount", policy.revokedSha256Fingerprints().size());
            result.put("revokedEntryCount", policy.entryCount());
            System.out.println(Jsons.toJson(result));
            return 0;
        }
    }

    @Command(name = "audit-tail", description = "Show latest audit log lines")
    static final class AuditTailCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--lines"}, defaultValue = "50", description = "Number of latest lines")
        int lines;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            var rows = runtime.auditTail(lines);
            for (String row : rows) {
                System.out.println(row);
            }
            return 0;
        }
    }

    @Command(name = "audit-query", description = "Query audit log with structured filters")
    static final class AuditQueryCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--task-id"}, description = "Filter by task id")
        String taskId;

        @Option(names = {"--action"}, description = "Filter by action name")
        String action;

        @Option(names = {"--from"}, description = "Filter from ISO-8601 timestamp (inclusive)")
        String from;

        @Option(names = {"--to"}, description = "Filter to ISO-8601 timestamp (inclusive)")
        String to;

        @Option(names = {"--limit"}, defaultValue = "100", description = "Max number of matching rows")
        int limit;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            var rows = runtime.auditQuery(taskId, action, from, to, limit);
            for (String row : rows) {
                System.out.println(row);
            }
            return 0;
        }
    }

    @Command(name = "health", description = "Check runtime health")
    static final class HealthCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.HealthOutcome out = runtime.health();
            System.out.println(Jsons.toJson(out));
            return out.ok() ? 0 : 1;
        }
    }

    @Command(name = "snapshot-export", description = "Export full runtime snapshot for upgrade/rollback workflows")
    static final class SnapshotExportCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--out"}, required = true, description = "Snapshot output directory")
        String out;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.SnapshotExportOutcome result = runtime.exportSnapshot(out);
            System.out.println(Jsons.toJson(result));
            return 0;
        }
    }

    @Command(name = "snapshot-import", description = "Import full runtime snapshot (destructive overwrite of current root)")
    static final class SnapshotImportCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--in"}, required = true, description = "Snapshot input directory")
        String input;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.SnapshotImportOutcome result = runtime.importSnapshot(input);
            System.out.println(Jsons.toJson(result));
            return 0;
        }
    }

    @Command(name = "purge", description = "Purge historical done/dead files and expired idempotency records")
    static final class PurgeCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--done-retention-days"}, defaultValue = "7",
                description = "Keep done directories newer than this value")
        int doneRetentionDays;

        @Option(names = {"--dead-retention-days"}, defaultValue = "7",
                description = "Keep dead files newer than this value")
        int deadRetentionDays;

        @Option(names = {"--idempotency-ttl-days"}, defaultValue = "7",
                description = "Keep idempotency rows newer than this value")
        int idempotencyTtlDays;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.PurgeOutcome outcome = runtime.purge(
                    doneRetentionDays,
                    deadRetentionDays,
                    idempotencyTtlDays
            );
            System.out.println(Jsons.toJson(outcome));
            return 0;
        }
    }

    @Command(name = "replay", description = "Replay a DEAD_LETTER task back to queue")
    static final class ReplayCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Parameters(index = "0", description = "Task id")
        String taskId;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.ReplayOutcome outcome = runtime.replayDeadLetter(taskId);
            System.out.println(Jsons.toJson(outcome));
            return outcome.replayed() ? 0 : 1;
        }
    }

    @Command(name = "replay-batch", description = "Replay multiple tasks by status (default DEAD_LETTER)")
    static final class ReplayBatchCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--status"}, defaultValue = "DEAD_LETTER", description = "Task status to scan")
        String status;

        @Option(names = {"--limit"}, defaultValue = "100", description = "Maximum tasks to replay")
        int limit;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.ReplayBatchOutcome outcome = runtime.replayDeadLetterBatch(status, limit);
            System.out.println(Jsons.toJson(outcome));
            return outcome.failed() == 0 ? 0 : 1;
        }
    }

    @Command(name = "dead-export", description = "Export dead-letter metadata to a JSON file")
    static final class DeadExportCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--out"}, required = true, description = "Output json file path")
        String out;

        @Option(names = {"--limit"}, defaultValue = "1000", description = "Maximum number of dead entries")
        int limit;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.DeadExportOutcome outcome = runtime.exportDeadLetters(out, limit);
            System.out.println(Jsons.toJson(outcome));
            return 0;
        }
    }

    @Command(name = "replication-controller", description = "Run continuous replication controller (pull/push with peer state)")
    static final class ReplicationControllerCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--peers"}, split = ",", description = "Peer base URLs (e.g. https://127.0.0.1:18443)")
        List<String> peers = List.of();

        @Option(names = {"--peers-file"}, description = "Optional peer URL file (one URL per line)")
        String peersFile;

        @Option(names = {"--interval-ms"}, defaultValue = "10000", description = "Controller loop interval in ms")
        long intervalMs;

        @Option(names = {"--limit"}, defaultValue = "500", description = "Replication delta limit per pull/push")
        int limit;

        @Option(names = {"--state-file"}, description = "Controller state JSON file path")
        String stateFile;

        @Option(names = {"--actor"}, defaultValue = "replication-controller", description = "Actor for replication import logs")
        String actor;

        @Option(names = {"--push"}, defaultValue = "true", description = "Enable push to peers after pull")
        boolean push;

        @Option(names = {"--once"}, defaultValue = "false", description = "Run one tick and exit")
        boolean once;

        @Override
        public Integer call() throws Exception {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshConfig config = RelayMeshConfig.fromRoot(parent.root, parent.namespace);
            List<String> resolvedPeers = resolvePeerUrls(peers, peersFile);
            if (resolvedPeers.isEmpty()) {
                throw new IllegalArgumentException("No replication peers configured");
            }
            Path statePath = (stateFile == null || stateFile.isBlank())
                    ? config.replicationRoot().resolve("controller-state.json")
                    : Path.of(stateFile).toAbsolutePath().normalize();
            long sleepMs = Math.max(1_000L, intervalMs);
            while (true) {
                ReplicationControllerTickOutcome out = runReplicationControllerTick(
                        runtime,
                        config,
                        resolvedPeers,
                        Math.max(1, limit),
                        statePath,
                        actor,
                        push
                );
                System.out.println(Jsons.toJson(out));
                if (once) {
                    return out.failures() == 0 ? 0 : 1;
                }
                Thread.sleep(sleepMs);
            }
        }
    }

    @Command(name = "dr-automation", description = "Run DR automation: scheduled snapshot, offsite copy, restore drill report")
    static final class DrAutomationCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--snapshot-root"}, description = "Snapshot output root (default <root>/dr/snapshots)")
        String snapshotRoot;

        @Option(names = {"--offsite-root"}, description = "Offsite mirror root (default <root>/dr/offsite)")
        String offsiteRoot;

        @Option(names = {"--report-out"}, description = "Latest drill report file path")
        String reportOut;

        @Option(names = {"--interval-ms"}, defaultValue = "0", description = "Loop interval; 0 runs once")
        long intervalMs;

        @Option(names = {"--drill"}, defaultValue = "false", description = "Run restore drill after snapshot copy")
        boolean drill;

        @Override
        public Integer call() throws Exception {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshConfig config = RelayMeshConfig.fromRoot(parent.root, parent.namespace);
            Path drBase = config.rootBaseDir().resolve("dr").resolve(config.namespace());
            Path snapshots = snapshotRoot == null || snapshotRoot.isBlank()
                    ? drBase.resolve("snapshots")
                    : Path.of(snapshotRoot).toAbsolutePath().normalize();
            Path offsite = offsiteRoot == null || offsiteRoot.isBlank()
                    ? drBase.resolve("offsite")
                    : Path.of(offsiteRoot).toAbsolutePath().normalize();
            Path report = reportOut == null || reportOut.isBlank()
                    ? drBase.resolve("report-latest.json")
                    : Path.of(reportOut).toAbsolutePath().normalize();
            long sleepMs = Math.max(1_000L, intervalMs);
            while (true) {
                long nowMs = Instant.now().toEpochMilli();
                Path snapshotTarget = snapshots.resolve("snapshot-" + nowMs);
                RelayMeshRuntime.SnapshotExportOutcome exported = runtime.exportSnapshot(snapshotTarget.toString());
                Path offsiteTarget = offsite.resolve(snapshotTarget.getFileName().toString());
                copyDirectory(snapshotTarget, offsiteTarget);

                boolean drillOk = true;
                String drillMessage = "skipped";
                String drillRoot = null;
                if (drill) {
                    Path tempRuntimeRoot = Files.createTempDirectory(drBase, "drill-runtime-");
                    drillRoot = tempRuntimeRoot.toString();
                    try {
                        RelayMeshRuntime drillRuntime = new RelayMeshRuntime(RelayMeshConfig.fromRoot(tempRuntimeRoot.toString(), parent.namespace));
                        drillRuntime.init();
                        drillRuntime.importSnapshot(offsiteTarget.toString());
                        RelayMeshRuntime.HealthOutcome health = drillRuntime.health();
                        drillOk = health.ok();
                        drillMessage = drillOk ? "restore_ok" : "restore_health_failed";
                    } finally {
                        deleteRecursively(tempRuntimeRoot);
                    }
                }
                Map<String, Object> summary = new LinkedHashMap<>();
                summary.put("namespace", parent.namespace);
                summary.put("timestamp", Instant.ofEpochMilli(nowMs).toString());
                summary.put("snapshot_output", exported.outputDir());
                summary.put("snapshot_root", exported.snapshotRoot());
                summary.put("snapshot_files", exported.fileCount());
                summary.put("snapshot_bytes", exported.totalBytes());
                summary.put("offsite_output", offsiteTarget.toString());
                summary.put("drill_enabled", drill);
                summary.put("drill_ok", drillOk);
                summary.put("drill_message", drillMessage);
                summary.put("drill_root", drillRoot == null ? "" : drillRoot);
                if (report.getParent() != null) {
                    Files.createDirectories(report.getParent());
                }
                Files.writeString(report, Jsons.toJson(summary), StandardCharsets.UTF_8);
                System.out.println(Jsons.toJson(summary));
                if (intervalMs <= 0L) {
                    return drillOk ? 0 : 1;
                }
                Thread.sleep(sleepMs);
            }
        }
    }

    @Command(name = "slo-evaluate", description = "Evaluate SLO burn-rate alerts and write latest alert state")
    static final class SloEvaluateCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--out"}, description = "Optional output JSON path for latest alerts")
        String out;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.SloAlertOutcome result = runtime.evaluateSloAlerts(out);
            System.out.println(Jsons.toJson(result));
            return result.fired() ? 2 : 0;
        }
    }

    @Command(name = "payload-key-status", description = "Show payload encryption keyring status")
    static final class PayloadKeyStatusCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.payloadKeyStatus()));
            return 0;
        }
    }

    @Command(name = "payload-key-rotate", description = "Rotate payload encryption key and set new active key id")
    static final class PayloadKeyRotateCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            System.out.println(Jsons.toJson(runtime.rotatePayloadKey()));
            return 0;
        }
    }

    @Command(name = "audit-verify", description = "Verify audit tamper-evidence chain and signatures")
    static final class AuditVerifyCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--limit"}, defaultValue = "0", description = "Optional tail row limit; 0 verifies full log")
        int limit;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.AuditIntegrityOutcome out = runtime.verifyAuditIntegrity(limit);
            System.out.println(Jsons.toJson(out));
            return out.ok() ? 0 : 1;
        }
    }

    @Command(name = "audit-siem-export", description = "Export audit events to SIEM-friendly NDJSON")
    static final class AuditSiemExportCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--out"}, required = true, description = "Output NDJSON path")
        String out;

        @Option(names = {"--limit"}, defaultValue = "200", description = "Max rows to export")
        int limit;

        @Option(names = {"--action"}, description = "Optional action filter")
        String action;

        @Option(names = {"--task-id"}, description = "Optional task id filter")
        String taskId;

        @Override
        public Integer call() {
            RelayMeshRuntime runtime = parent.runtime();
            runtime.init();
            RelayMeshRuntime.SiemExportOutcome outResult = runtime.exportAuditToSiem(out, limit, action, taskId);
            System.out.println(Jsons.toJson(outResult));
            return 0;
        }
    }

    @Command(name = "schema-migrations", description = "Show applied schema migration versions")
    static final class SchemaMigrationsCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--limit"}, defaultValue = "50", description = "Max rows to print")
        int limit;

        @Override
        public Integer call() {
            RelayMeshConfig config = RelayMeshConfig.fromRoot(parent.root, parent.namespace);
            Database database = new Database(config);
            database.init();
            System.out.println(Jsons.toJson(database.listSchemaMigrations(limit)));
            return 0;
        }
    }

    @Command(name = "schema-rollback", description = "Rollback a known migration version (best-effort, safe subset)")
    static final class SchemaRollbackCommand implements Callable<Integer> {
        @ParentCommand
        RelayMeshCommand parent;

        @Option(names = {"--version"}, required = true, description = "Migration version to rollback")
        String version;

        @Override
        public Integer call() {
            RelayMeshConfig config = RelayMeshConfig.fromRoot(parent.root, parent.namespace);
            Database database = new Database(config);
            database.init();
            Database.RollbackOutcome out = database.rollbackMigration(version);
            System.out.println(Jsons.toJson(out));
            return out.removedMigrationRow() ? 0 : 1;
        }
    }

    private static List<String> resolvePeerUrls(List<String> cliPeers, String peersFile) throws IOException {
        LinkedHashSet<String> dedup = new LinkedHashSet<>();
        if (cliPeers != null) {
            for (String raw : cliPeers) {
                String normalized = normalizePeerUrl(raw);
                if (!normalized.isBlank()) {
                    dedup.add(normalized);
                }
            }
        }
        if (peersFile != null && !peersFile.isBlank()) {
            Path file = Path.of(peersFile.trim()).toAbsolutePath().normalize();
            if (!Files.exists(file)) {
                throw new IllegalArgumentException("peers file not found: " + file);
            }
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (line == null) {
                    continue;
                }
                String trimmed = line.trim();
                if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                    continue;
                }
                for (String token : trimmed.split(",")) {
                    String normalized = normalizePeerUrl(token);
                    if (!normalized.isBlank()) {
                        dedup.add(normalized);
                    }
                }
            }
        }
        return new ArrayList<>(dedup);
    }

    private static String normalizePeerUrl(String raw) {
        if (raw == null || raw.isBlank()) {
            return "";
        }
        String value = raw.trim();
        if (!value.startsWith("http://") && !value.startsWith("https://")) {
            value = "https://" + value;
        }
        while (value.endsWith("/")) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }

    private static ReplicationControllerTickOutcome runReplicationControllerTick(
            RelayMeshRuntime runtime,
            RelayMeshConfig config,
            List<String> peers,
            int limit,
            Path stateFile,
            String actor,
            boolean push
    ) throws Exception {
        ControllerState state = loadControllerState(stateFile);
        LinkedHashMap<String, ControllerPeerState> next = new LinkedHashMap<>(state.peers());
        HttpClient http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        int pulls = 0;
        int pushes = 0;
        int failures = 0;
        List<String> errors = new ArrayList<>();
        long nowMs = Instant.now().toEpochMilli();
        for (String peer : peers) {
            ControllerPeerState peerState = next.getOrDefault(peer, new ControllerPeerState(0L, 0L, 0));
            long pullCursor = Math.max(0L, peerState.pullCursorMs());
            long pushCursor = Math.max(0L, peerState.pushCursorMs());
            int failureCount = peerState.failures();
            try {
                String pullUrl = peer + "/node/replication/export?sinceMs=" + pullCursor + "&limit=" + limit;
                HttpRequest pullReq = HttpRequest.newBuilder(URI.create(pullUrl))
                        .timeout(Duration.ofSeconds(20))
                        .GET()
                        .build();
                HttpResponse<String> pullResp = http.send(pullReq, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (pullResp.statusCode() / 100 != 2) {
                    throw new IOException("pull failed status=" + pullResp.statusCode());
                }
                String pullBody = pullResp.body();
                JsonNode pullEnvelope = Jsons.mapper().readTree(pullBody);
                long cursorMaxMs = pullEnvelope.path("cursor_max_ms").asLong(pullCursor);
                Files.createDirectories(config.replicationRoot());
                Path pullFile = Files.createTempFile(config.replicationRoot(), "controller-pull-", ".json");
                try {
                    Files.writeString(pullFile, pullBody, StandardCharsets.UTF_8);
                    runtime.importReplicationEnvelope(pullFile.toString(), actor);
                } finally {
                    Files.deleteIfExists(pullFile);
                }
                pullCursor = Math.max(pullCursor, cursorMaxMs);
                pulls++;

                if (push) {
                    Path pushFile = Files.createTempFile(config.replicationRoot(), "controller-push-", ".json");
                    try {
                        RelayMeshRuntime.ReplicationExportOutcome exported =
                                runtime.exportReplicationEnvelope(pushCursor, limit, pushFile.toString(), "controller");
                        String pushBody = Files.readString(pushFile, StandardCharsets.UTF_8);
                        String pushUrl = peer + "/node/replication/import?actor="
                                + URLEncoder.encode(actor, StandardCharsets.UTF_8);
                        HttpRequest pushReq = HttpRequest.newBuilder(URI.create(pushUrl))
                                .timeout(Duration.ofSeconds(20))
                                .header("Content-Type", "application/json")
                                .POST(HttpRequest.BodyPublishers.ofString(pushBody, StandardCharsets.UTF_8))
                                .build();
                        HttpResponse<String> pushResp = http.send(pushReq, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                        if (pushResp.statusCode() / 100 != 2) {
                            throw new IOException("push failed status=" + pushResp.statusCode());
                        }
                        pushCursor = Math.max(pushCursor, exported.cursorMaxMs());
                        pushes++;
                    } finally {
                        Files.deleteIfExists(pushFile);
                    }
                }
                failureCount = 0;
            } catch (Exception e) {
                failures++;
                failureCount++;
                errors.add(peer + ": " + e.getMessage());
            }
            next.put(peer, new ControllerPeerState(pullCursor, pushCursor, failureCount));
        }
        ControllerState outState = new ControllerState(next);
        saveControllerState(stateFile, outState);
        runtime.markReplicationSyncTick(actor, peers.size(), pulls, pushes, failures, errors);
        return new ReplicationControllerTickOutcome(
                config.namespace(),
                nowMs,
                peers.size(),
                pulls,
                pushes,
                failures,
                stateFile.toString(),
                errors
        );
    }

    private static ControllerState loadControllerState(Path file) {
        try {
            if (!Files.exists(file)) {
                return new ControllerState(Map.of());
            }
            JsonNode node = Jsons.mapper().readTree(Files.readString(file, StandardCharsets.UTF_8));
            JsonNode peersNode = node.path("peers");
            LinkedHashMap<String, ControllerPeerState> peers = new LinkedHashMap<>();
            if (peersNode.isObject()) {
                peersNode.fieldNames().forEachRemaining(peerName -> {
                    JsonNode value = peersNode.path(peerName);
                    peers.put(
                            peerName,
                            new ControllerPeerState(
                                    value.path("pull_cursor_ms").asLong(0L),
                                    value.path("push_cursor_ms").asLong(0L),
                                    value.path("failures").asInt(0)
                            )
                    );
                });
            }
            return new ControllerState(peers);
        } catch (Exception e) {
            return new ControllerState(Map.of());
        }
    }

    private static void saveControllerState(Path file, ControllerState state) throws IOException {
        if (file.getParent() != null) {
            Files.createDirectories(file.getParent());
        }
        LinkedHashMap<String, Object> peers = new LinkedHashMap<>();
        for (Map.Entry<String, ControllerPeerState> entry : state.peers().entrySet()) {
            peers.put(entry.getKey(), Map.of(
                    "pull_cursor_ms", entry.getValue().pullCursorMs(),
                    "push_cursor_ms", entry.getValue().pushCursorMs(),
                    "failures", entry.getValue().failures()
            ));
        }
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("schema", "relaymesh.replication.controller.v1");
        body.put("updated_at", Instant.now().toString());
        body.put("peers", peers);
        Files.writeString(file, Jsons.toJson(body), StandardCharsets.UTF_8);
    }

    private static void copyDirectory(Path sourceRoot, Path targetRoot) throws IOException {
        if (!Files.exists(sourceRoot)) {
            return;
        }
        try (var walk = Files.walk(sourceRoot)) {
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
                if (target.getParent() != null) {
                    Files.createDirectories(target.getParent());
                }
                Files.copy(source, target, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
                        java.nio.file.StandardCopyOption.COPY_ATTRIBUTES);
            }
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        try (var walk = Files.walk(root)) {
            for (Path path : walk.sorted((a, b) -> Integer.compare(b.getNameCount(), a.getNameCount())).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }

    private record ControllerPeerState(long pullCursorMs, long pushCursorMs, int failures) {
    }

    private record ControllerState(Map<String, ControllerPeerState> peers) {
    }

    private record ReplicationControllerTickOutcome(
            String namespace,
            long tickAtMs,
            int peers,
            int pulls,
            int pushes,
            int failures,
            String stateFile,
            List<String> errors
    ) {
    }

    private static void writeJson(com.sun.net.httpserver.HttpExchange exchange, Object body, int status) throws java.io.IOException {
        byte[] bytes = Jsons.toJson(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static boolean authorize(
            com.sun.net.httpserver.HttpExchange exchange,
            Map<String, String> query,
            WebAuth auth,
            boolean writeRequired
    ) throws IOException {
        AuthPrincipal principal = authorizePrincipal(exchange, query, auth, writeRequired);
        return principal != null;
    }

    private static AuthPrincipal authorizePrincipal(
            com.sun.net.httpserver.HttpExchange exchange,
            Map<String, String> query,
            WebAuth auth,
            boolean writeRequired
    ) throws IOException {
        if (!auth.enabled()) {
            AuthPrincipal bypass = AuthPrincipal.bypass(auth.namespace());
            applyPrincipal(exchange, bypass);
            return bypass;
        }
        String token = extractToken(exchange, query);
        if (token == null || token.isBlank()) {
            writeJson(exchange, Map.of("error", "missing_token"), 401);
            return null;
        }
        AuthPrincipal principal = auth.resolve(token);
        if (principal == null) {
            writeJson(exchange, Map.of("error", "forbidden_token"), 403);
            return null;
        }
        if (!auth.namespaceAllowed(principal)) {
            writeJson(exchange, Map.of("error", "forbidden_namespace", "namespace", auth.namespace()), 403);
            return null;
        }
        if (writeRequired) {
            if (!auth.canWrite(principal)) {
                writeJson(exchange, Map.of("error", "forbidden_write"), 403);
                return null;
            }
            applyPrincipal(exchange, principal);
            return principal;
        }
        if (!auth.canRead(principal)) {
            writeJson(exchange, Map.of("error", "forbidden_read"), 403);
            return null;
        }
        applyPrincipal(exchange, principal);
        return principal;
    }

    private static void applyPrincipal(com.sun.net.httpserver.HttpExchange exchange, AuthPrincipal principal) {
        if (exchange == null || principal == null) {
            return;
        }
        exchange.setAttribute("principal_id", principal.id());
        exchange.setAttribute("principal_role", principal.role().name().toLowerCase(Locale.ROOT));
        exchange.setAttribute("principal_service_account", principal.serviceAccount());
        exchange.setAttribute("namespace", principal.namespace());
    }

    private static boolean allowMethods(com.sun.net.httpserver.HttpExchange exchange, String... methods) throws IOException {
        String method = exchange.getRequestMethod();
        if (method == null) {
            writeJson(exchange, Map.of("error", "method_not_allowed"), 405);
            return false;
        }
        for (String allowed : methods) {
            if (allowed.equalsIgnoreCase(method)) {
                return true;
            }
        }
        exchange.getResponseHeaders().set("Allow", String.join(",", methods));
        writeJson(exchange, Map.of("error", "method_not_allowed", "method", method), 405);
        return false;
    }

    private static boolean allowWriteMethod(
            com.sun.net.httpserver.HttpExchange exchange,
            boolean allowGetWrites,
            RelayMeshRuntime runtime,
            String route
    ) throws IOException {
        if (!allowGetWrites) {
            return allowMethods(exchange, "POST");
        }
        boolean allowed = allowMethods(exchange, "GET", "POST");
        if (!allowed) {
            return false;
        }
        String method = exchange.getRequestMethod();
        if ("GET".equalsIgnoreCase(method)) {
            exchange.getResponseHeaders().add("Warning", "299 RelayMesh \"GET write API compatibility is deprecated; use POST\"");
            if (runtime != null) {
                runtime.markWebGetWriteCompat(route, requestAuditMeta(exchange, route));
            }
        }
        return true;
    }

    private static boolean allowWriteRate(
            com.sun.net.httpserver.HttpExchange exchange,
            WriteRateLimiter limiter,
            RelayMeshRuntime runtime,
            String route
    ) throws IOException {
        if (limiter == null || limiter.disabled()) {
            return true;
        }
        String remote = exchange.getRemoteAddress() == null ? "unknown" : exchange.getRemoteAddress().toString();
        if (limiter.tryAcquire(remote, Instant.now().toEpochMilli())) {
            return true;
        }
        exchange.getResponseHeaders().set("Retry-After", "60");
        writeJson(exchange, Map.of("error", "rate_limited", "route", route), 429);
        if (runtime != null) {
            runtime.markWebWriteRateLimited(route, requestAuditMeta(exchange, route));
        }
        return false;
    }

    private static String extractToken(com.sun.net.httpserver.HttpExchange exchange, Map<String, String> query) {
        String authz = exchange.getRequestHeaders().getFirst("Authorization");
        if (authz != null && authz.regionMatches(true, 0, "Bearer ", 0, 7)) {
            String token = authz.substring(7).trim();
            if (!token.isEmpty()) {
                return token;
            }
        }
        String queryToken = query.get("token");
        if (queryToken != null && !queryToken.isBlank()) {
            return queryToken.trim();
        }
        return null;
    }

    private static Map<String, Object> requestAuditMeta(com.sun.net.httpserver.HttpExchange exchange, String route) {
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        out.put("route", route);
        out.put("method", exchange.getRequestMethod());
        out.put("remote_addr", exchange.getRemoteAddress() == null ? "" : exchange.getRemoteAddress().toString());
        out.put("user_agent", nullableHeader(exchange, "User-Agent"));
        out.put("x_forwarded_for", nullableHeader(exchange, "X-Forwarded-For"));
        out.put("origin", nullableHeader(exchange, "Origin"));
        out.put("referer", nullableHeader(exchange, "Referer"));
        out.put("principal_id", attribute(exchange, "principal_id"));
        out.put("principal_role", attribute(exchange, "principal_role"));
        out.put("principal_service_account", attribute(exchange, "principal_service_account"));
        out.put("namespace", attribute(exchange, "namespace"));
        return out;
    }

    private static String nullableHeader(com.sun.net.httpserver.HttpExchange exchange, String header) {
        String value = exchange.getRequestHeaders().getFirst(header);
        return value == null ? "" : value;
    }

    private static String attribute(com.sun.net.httpserver.HttpExchange exchange, String key) {
        Object value = exchange.getAttribute(key);
        return value == null ? "" : String.valueOf(value);
    }

    private static Map<String, String> parseParams(com.sun.net.httpserver.HttpExchange exchange) throws IOException {
        Map<String, String> out = new LinkedHashMap<>(parseQuery(exchange.getRequestURI()));
        String method = exchange.getRequestMethod();
        if (method == null) {
            return out;
        }
        if (!"POST".equalsIgnoreCase(method) && !"PUT".equalsIgnoreCase(method) && !"PATCH".equalsIgnoreCase(method)) {
            return out;
        }
        byte[] raw = exchange.getRequestBody().readAllBytes();
        if (raw.length == 0) {
            return out;
        }
        String body = new String(raw, StandardCharsets.UTF_8).trim();
        if (body.isEmpty()) {
            return out;
        }
        String contentType = exchange.getRequestHeaders().getFirst("Content-Type");
        String normalized = contentType == null ? "" : contentType.toLowerCase(Locale.ROOT);
        if (normalized.contains("application/json")) {
            try {
                JsonNode node = Jsons.mapper().readTree(body);
                if (node != null && node.isObject()) {
                    node.fieldNames().forEachRemaining(key -> {
                        JsonNode value = node.path(key);
                        if (value == null || value.isNull()) {
                            out.put(key, "");
                        } else if (value.isValueNode()) {
                            out.put(key, value.asText());
                        } else {
                            out.put(key, value.toString());
                        }
                    });
                }
                return out;
            } catch (Exception ignored) {
                // Fall back to querystring parser.
            }
        }
        out.putAll(parseQueryString(body));
        return out;
    }

    private static int parseIntOrDefault(String raw, int fallback) {
        if (raw == null || raw.isBlank()) {
            return fallback;
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static int clamp(int value, int min, int max) {
        if (value < min) {
            return min;
        }
        if (value > max) {
            return max;
        }
        return value;
    }

    private static RelayMeshRuntime runtimeForNamespace(
            String root,
            String namespace,
            ConcurrentMap<String, RelayMeshRuntime> cache
    ) {
        String normalized = normalizeNamespace(namespace, RelayMeshConfig.DEFAULT_NAMESPACE);
        return cache.computeIfAbsent(normalized, ns -> {
            RelayMeshRuntime scoped = new RelayMeshRuntime(RelayMeshConfig.fromRoot(root, ns));
            scoped.init();
            return scoped;
        });
    }

    private static List<String> discoverNamespaces(Path rootBaseDir, String activeNamespace) {
        LinkedHashSet<String> out = new LinkedHashSet<>();
        String normalizedActive = normalizeNamespace(activeNamespace, RelayMeshConfig.DEFAULT_NAMESPACE);
        if (Files.exists(rootBaseDir.resolve("relaymesh.db"))) {
            out.add(RelayMeshConfig.DEFAULT_NAMESPACE);
        }
        Path namespacesRoot = rootBaseDir.resolve(RelayMeshConfig.DEFAULT_NAMESPACES_DIR);
        if (Files.isDirectory(namespacesRoot)) {
            try (var children = Files.newDirectoryStream(namespacesRoot)) {
                for (Path child : children) {
                    if (!Files.isDirectory(child)) {
                        continue;
                    }
                    if (!Files.exists(child.resolve("relaymesh.db"))) {
                        continue;
                    }
                    out.add(normalizeNamespace(child.getFileName().toString(), normalizedActive));
                }
            } catch (IOException ignored) {
                // Keep best-effort discovery for control-room.
            }
        }
        out.add(normalizedActive);
        List<String> namespaces = new ArrayList<>(out);
        namespaces.sort(String::compareTo);
        if (namespaces.remove(normalizedActive)) {
            namespaces.add(0, normalizedActive);
        }
        return namespaces;
    }

    private static List<String> resolveRequestedNamespaces(
            String rawNamespaces,
            String singleNamespace,
            String activeNamespace,
            List<String> discovered
    ) {
        String value = rawNamespaces == null || rawNamespaces.isBlank() ? singleNamespace : rawNamespaces;
        if (value == null || value.isBlank()) {
            return List.of(normalizeNamespace(activeNamespace, RelayMeshConfig.DEFAULT_NAMESPACE));
        }
        LinkedHashSet<String> out = new LinkedHashSet<>();
        for (String token : parseNamespaceList(value)) {
            if ("all".equalsIgnoreCase(token)) {
                if (discovered != null) {
                    for (String ns : discovered) {
                        out.add(normalizeNamespace(ns, activeNamespace));
                    }
                }
                continue;
            }
            out.add(normalizeNamespace(token, activeNamespace));
        }
        if (out.isEmpty()) {
            out.add(normalizeNamespace(activeNamespace, RelayMeshConfig.DEFAULT_NAMESPACE));
        }
        return new ArrayList<>(out);
    }

    private static List<String> parseNamespaceList(String raw) {
        List<String> out = new ArrayList<>();
        if (raw == null || raw.isBlank()) {
            return out;
        }
        for (String token : raw.split(",")) {
            if (token == null) {
                continue;
            }
            String trimmed = token.trim();
            if (!trimmed.isEmpty()) {
                out.add(trimmed);
            }
        }
        return out;
    }

    private static String normalizeNamespace(String raw, String fallback) {
        String candidate = raw == null || raw.isBlank() ? fallback : raw.trim();
        if (candidate == null || candidate.isBlank()) {
            candidate = RelayMeshConfig.DEFAULT_NAMESPACE;
        }
        return RelayMeshConfig.fromRoot("data", candidate).namespace();
    }

    private static List<String> filterVisibleNamespaces(List<String> discovered, AuthPrincipal principal) {
        if (principal == null) {
            return List.of();
        }
        if (principal.allowedNamespaces() == null || principal.allowedNamespaces().isEmpty()) {
            return discovered;
        }
        List<String> out = new ArrayList<>();
        for (String namespace : discovered) {
            if (principal.allowedNamespaces().contains(namespace)) {
                out.add(namespace);
            }
        }
        return out;
    }

    private static boolean principalCanAccessNamespace(AuthPrincipal principal, String namespace) {
        if (principal == null) {
            return false;
        }
        if (principal.allowedNamespaces() == null || principal.allowedNamespaces().isEmpty()) {
            return true;
        }
        String normalized = normalizeNamespace(namespace, principal.namespace());
        return principal.allowedNamespaces().contains(normalized);
    }

    private static LinkedHashMap<String, Object> buildControlRoomSnapshotPayload(
            String root,
            String defaultNamespace,
            ConcurrentMap<String, RelayMeshRuntime> runtimeCache,
            List<String> requestedNamespaces,
            String statusFilter,
            int taskLimit,
            int deadLimit,
            int conflictLimit,
            int sinceHours
    ) {
        LinkedHashMap<String, Object> data = new LinkedHashMap<>();
        for (String ns : requestedNamespaces) {
            RelayMeshRuntime scoped = runtimeForNamespace(root, ns, runtimeCache);
            LinkedHashMap<String, Object> snap = new LinkedHashMap<>();
            snap.put("stats", scoped.stats());
            snap.put("members", scoped.members());
            snap.put("tasks", scoped.tasks(statusFilter, taskLimit, 0));
            snap.put("dead", scoped.tasks("DEAD_LETTER", deadLimit, 0));
            snap.put("conflicts", scoped.leaseConflicts(conflictLimit, sinceHours));
            data.put(ns, snap);
        }
        LinkedHashMap<String, Object> payload = new LinkedHashMap<>();
        payload.put("timestamp", Instant.now().toString());
        payload.put("activeNamespace", defaultNamespace);
        payload.put("requestedNamespaces", requestedNamespaces);
        payload.put("statusFilter", statusFilter == null ? "" : statusFilter);
        payload.put("data", data);
        return payload;
    }

    private static List<String> buildWorkflowEdges(Object workflowPayload) {
        List<String> edges = new ArrayList<>();
        if (workflowPayload == null) {
            return edges;
        }
        JsonNode root = Jsons.mapper().valueToTree(workflowPayload);
        JsonNode steps = root.path("steps");
        if (!steps.isArray()) {
            return edges;
        }
        for (JsonNode step : steps) {
            String stepId = step.path("stepId").asText();
            if (stepId == null || stepId.isBlank()) {
                stepId = step.path("id").asText();
            }
            if (stepId == null || stepId.isBlank()) {
                continue;
            }
            JsonNode dependsOn = step.path("dependsOn");
            if (!dependsOn.isArray() || dependsOn.isEmpty()) {
                edges.add("[ROOT] -> " + stepId);
                continue;
            }
            for (JsonNode dep : dependsOn) {
                String depId = dep.asText();
                if (depId == null || depId.isBlank()) {
                    continue;
                }
                edges.add(depId + " -> " + stepId);
            }
        }
        return edges;
    }

    private static List<String> parseControlCommandTokens(String raw) {
        List<String> out = new ArrayList<>();
        if (raw == null || raw.isBlank()) {
            return out;
        }
        for (String token : raw.trim().split("\\s+")) {
            if (token != null && !token.isBlank()) {
                out.add(token.trim());
            }
        }
        return out;
    }

    private static String joinCommandTail(List<String> tokens, int startIndex) {
        if (tokens == null || tokens.isEmpty() || startIndex >= tokens.size()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = startIndex; i < tokens.size(); i++) {
            if (i > startIndex) {
                sb.append(' ');
            }
            sb.append(tokens.get(i));
        }
        return sb.toString();
    }

    private static boolean isControlRoomWriteCommand(String op) {
        if (op == null || op.isBlank()) {
            return false;
        }
        String value = op.trim().toLowerCase(Locale.ROOT);
        return "cancel".equals(value)
                || "replay".equals(value)
                || "replay-batch".equals(value)
                || "replay_batch".equals(value);
    }

    private static String normalizeLayoutProfileName(String raw) {
        if (raw == null || raw.isBlank()) {
            return "";
        }
        String value = raw.trim();
        if (value.length() > 64) {
            return "";
        }
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            boolean ok = (ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '_' || ch == '-' || ch == '.';
            if (!ok) {
                return "";
            }
        }
        return value;
    }

    private static Path controlRoomLayoutsPath(Path rootBaseDir) {
        return rootBaseDir.resolve("control-room-layouts.json");
    }

    private static LinkedHashMap<String, JsonNode> readControlRoomLayouts(Path rootBaseDir) throws IOException {
        LinkedHashMap<String, JsonNode> out = new LinkedHashMap<>();
        Path file = controlRoomLayoutsPath(rootBaseDir);
        if (!Files.exists(file)) {
            return out;
        }
        JsonNode root = Jsons.mapper().readTree(file.toFile());
        if (root == null || !root.isObject()) {
            return out;
        }
        root.fields().forEachRemaining(entry -> {
            String key = normalizeLayoutProfileName(entry.getKey());
            JsonNode value = entry.getValue();
            if (!key.isEmpty() && value != null && value.isObject()) {
                out.put(key, value);
            }
        });
        return out;
    }

    private static void writeControlRoomLayouts(Path rootBaseDir, Map<String, JsonNode> profiles) throws IOException {
        Path file = controlRoomLayoutsPath(rootBaseDir);
        Path parent = file.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        ObjectNode root = Jsons.mapper().createObjectNode();
        if (profiles != null) {
            profiles.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> {
                        String key = normalizeLayoutProfileName(entry.getKey());
                        JsonNode value = entry.getValue();
                        if (!key.isEmpty() && value != null && value.isObject()) {
                            root.set(key, value);
                        }
                    });
        }
        Jsons.mapper().writerWithDefaultPrettyPrinter().writeValue(file.toFile(), root);
    }

    private static List<String> resolveSeedEndpoints(List<String> cliSeeds, String seedsFile) {
        Set<String> unique = new LinkedHashSet<>();
        if (cliSeeds != null) {
            for (String seed : cliSeeds) {
                if (seed != null && !seed.isBlank()) {
                    String normalized = seed.trim();
                    if (normalized.startsWith("\uFEFF")) {
                        normalized = normalized.substring(1);
                    }
                    unique.add(normalized);
                }
            }
        }
        if (seedsFile != null && !seedsFile.isBlank()) {
            Path file = Path.of(seedsFile.trim());
            if (!Files.exists(file)) {
                throw new IllegalArgumentException("seeds file not found: " + file);
            }
            try {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (line == null) continue;
                    String trimmed = line.trim();
                    if (trimmed.isEmpty() || trimmed.startsWith("#")) {
                        continue;
                    }
                    for (String token : trimmed.split(",")) {
                        String seed = token.trim();
                        if (seed.startsWith("\uFEFF")) {
                            seed = seed.substring(1);
                        }
                        if (!seed.isEmpty()) {
                            unique.add(seed);
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("failed to read seeds file: " + file, e);
            }
        }
        return new ArrayList<>(unique);
    }

    private static Map<String, String> parseQuery(URI uri) {
        String query = uri.getRawQuery();
        return parseQueryString(query);
    }

    private static Map<String, String> parseQueryString(String query) {
        Map<String, String> out = new LinkedHashMap<>();
        if (query == null || query.isBlank()) {
            return out;
        }
        for (String pair : query.split("&")) {
            if (pair.isBlank()) {
                continue;
            }
            int idx = pair.indexOf('=');
            if (idx < 0) {
                String key = URLDecoder.decode(pair, StandardCharsets.UTF_8);
                out.put(key, "");
            } else {
                String key = URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8);
                String value = URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8);
                out.put(key, value);
            }
        }
        return out;
    }

    private static String dashboardHtml() {
        return """
                <!doctype html>
                <html lang="en">
                <head>
                  <meta charset="utf-8">
                  <meta name="viewport" content="width=device-width, initial-scale=1">
                  <title>RelayMesh Console</title>
                  <style>
                    :root {
                      --bg: radial-gradient(circle at 20% 10%, #f0f9ff 0%, #f8fafc 45%, #e2e8f0 100%);
                      --ink: #0f172a;
                      --muted: #475569;
                      --panel: rgba(255,255,255,0.86);
                      --line: #cbd5e1;
                      --accent: #0f766e;
                      --accent2: #0369a1;
                    }
                    * { box-sizing: border-box; }
                    body {
                      margin: 0;
                      font-family: "Space Grotesk", "Segoe UI", sans-serif;
                      color: var(--ink);
                      background: var(--bg);
                    }
                    .wrap { max-width: 1200px; margin: 24px auto; padding: 0 16px; }
                    .hero { display: flex; gap: 12px; align-items: baseline; margin-bottom: 16px; }
                    .hero h1 { margin: 0; font-size: 30px; letter-spacing: 0.4px; }
                    .hero p { margin: 0; color: var(--muted); }
                    .grid { display: grid; grid-template-columns: 2fr 1fr; gap: 14px; }
                    .panel {
                      background: var(--panel);
                      border: 1px solid var(--line);
                      border-radius: 14px;
                      box-shadow: 0 8px 24px rgba(2, 6, 23, 0.08);
                      padding: 14px;
                    }
                    h2 { margin: 0 0 10px; font-size: 16px; text-transform: uppercase; color: var(--accent2); }
                    table { width: 100%; border-collapse: collapse; font-size: 13px; }
                    th, td { border-bottom: 1px solid #e2e8f0; padding: 6px 8px; text-align: left; }
                    th { color: var(--muted); font-weight: 600; }
                    .mono {
                      font-family: "IBM Plex Mono", "JetBrains Mono", Consolas, monospace;
                      font-size: 12px;
                      white-space: pre-wrap;
                      word-break: break-word;
                    }
                    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
                    .tag { display: inline-block; padding: 2px 8px; border-radius: 999px; background: #ecfeff; color: var(--accent); border: 1px solid #99f6e4; }
                    .toolbar { display: flex; gap: 8px; margin-bottom: 8px; }
                    input, button {
                      border-radius: 10px;
                      border: 1px solid #94a3b8;
                      padding: 7px 10px;
                      font: inherit;
                    }
                    button { background: #0f766e; color: white; border-color: #0f766e; cursor: pointer; }
                    @media (max-width: 960px) {
                      .grid { grid-template-columns: 1fr; }
                      .row { grid-template-columns: 1fr; }
                    }
                  </style>
                </head>
                <body>
                  <div class="wrap">
                    <div class="hero">
                      <h1>RelayMesh Console</h1>
                      <p>tasks, workflow graph, dead letters, metrics, membership, and control-room mode</p>
                    </div>
                    <div id="sseStatus" class="mono" style="margin-bottom:10px;color:#0369a1;">SSE: connecting...</div>
                    <div class="grid">
                      <section class="panel">
                        <h2>Tasks</h2>
                        <div class="toolbar">
                          <input id="tokenInput" placeholder="token (optional)">
                          <input id="taskIdInput" placeholder="task id for workflow view">
                          <input id="reasonInput" placeholder="reason (optional)">
                          <button onclick="loadWorkflow()">Load Workflow</button>
                          <button onclick="cancelTask('soft')">Soft Cancel</button>
                          <button onclick="cancelTask('hard')">Hard Cancel</button>
                          <button onclick="replayTask()">Replay Task</button>
                          <button onclick="refreshAll()">Refresh</button>
                          <button onclick="window.location.href='/control-room'">Control Room</button>
                        </div>
                        <div id="actionResult" class="mono" style="margin-bottom:8px;color:#0f766e;"></div>
                        <table id="taskTable"><thead><tr><th>Task</th><th>Status</th><th>Updated(ms)</th></tr></thead><tbody></tbody></table>
                      </section>
                      <section class="panel">
                        <h2>Members</h2>
                        <div id="members" class="mono">loading...</div>
                      </section>
                    </div>
                    <div class="row" style="margin-top:14px;">
                      <section class="panel">
                        <h2>Workflow Graph</h2>
                        <div id="workflow" class="mono">Select a task and click "Load Workflow".</div>
                      </section>
                      <section class="panel">
                        <h2>Dead Letters</h2>
                        <div class="toolbar">
                          <button onclick="replayBatch()">Replay Batch</button>
                        </div>
                        <div id="dead" class="mono">loading...</div>
                      </section>
                    </div>
                    <div class="row" style="margin-top:14px;">
                      <section class="panel">
                        <h2>Lease Conflicts <span class="tag">CAS</span></h2>
                        <div id="conflicts" class="mono">loading...</div>
                      </section>
                      <section class="panel">
                        <h2>Metrics Preview</h2>
                        <div id="metrics" class="mono">loading...</div>
                      </section>
                    </div>
                    <div class="row" style="margin-top:14px;">
                      <section class="panel">
                        <h2>Audit Query</h2>
                        <div class="toolbar">
                          <input id="auditActionInput" placeholder="action filter (optional)">
                          <button onclick="loadAudit()">Load Audit</button>
                        </div>
                        <div id="auditRows" class="mono">loading...</div>
                      </section>
                      <section class="panel">
                        <h2>Quick Hints</h2>
                        <div class="mono">1. Select task id then Load Workflow.\n2. Use Soft Cancel to stop pending/retry steps.\n3. Replay Batch requeues DEAD_LETTER tasks.\n4. Audit Query accepts task id + action.</div>
                      </section>
                    </div>
                  </div>
                  <script>
                    function authToken() {
                      return document.getElementById('tokenInput').value.trim();
                    }
                    function withAuth(url) {
                      const token = authToken();
                      if (!token) return url;
                      const sep = url.includes('?') ? '&' : '?';
                      return url + sep + 'token=' + encodeURIComponent(token);
                    }
                    async function fetchJson(url, options = {}) {
                      const r = await fetch(withAuth(url), options);
                      if (!r.ok) throw new Error(await r.text());
                      return r.json();
                    }
                    async function postForm(url, params) {
                      const body = new URLSearchParams(params).toString();
                      return fetchJson(url, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                        body
                      });
                    }
                    function selectedTaskId() {
                      return document.getElementById('taskIdInput').value.trim();
                    }
                    function actionReason() {
                      return document.getElementById('reasonInput').value.trim();
                    }
                    async function refreshAll() {
                      const tasks = await fetchJson('/api/tasks?limit=30');
                      const tbody = document.querySelector('#taskTable tbody');
                      tbody.innerHTML = tasks.map(t => `<tr><td>${t.taskId}</td><td>${t.status}</td><td>${t.updatedAtMs}</td></tr>`).join('');
                      const members = await fetchJson('/api/members');
                      document.getElementById('members').textContent = JSON.stringify(members, null, 2);
                      const dead = await fetchJson('/api/dead?limit=20');
                      document.getElementById('dead').textContent = JSON.stringify(dead, null, 2);
                      const conflicts = await fetchJson('/api/lease-conflicts?limit=20&sinceHours=24');
                      document.getElementById('conflicts').textContent = JSON.stringify(conflicts, null, 2);
                      await loadAudit(true);
                      const metrics = await fetch(withAuth('/api/metrics')).then(r => r.text());
                      document.getElementById('metrics').textContent = metrics.split('\\n').slice(0, 40).join('\\n');
                    }
                    async function loadWorkflow() {
                      const taskId = selectedTaskId();
                      if (!taskId) return;
                      try {
                        const wf = await fetchJson('/api/workflow?taskId=' + encodeURIComponent(taskId));
                        const edges = [];
                        for (const s of wf.steps || []) {
                          if (!s.dependsOn || s.dependsOn.length === 0) edges.push(`[ROOT] -> ${s.stepId}`);
                          else for (const d of s.dependsOn) edges.push(`${d} -> ${s.stepId}`);
                        }
                        document.getElementById('workflow').textContent =
                          `task=${wf.task.taskId} status=${wf.task.status}\\n\\n` +
                          edges.join('\\n');
                      } catch (e) {
                        document.getElementById('workflow').textContent = 'workflow load failed: ' + e.message;
                      }
                    }
                    async function cancelTask(mode) {
                      const taskId = selectedTaskId();
                      if (!taskId) return;
                      const reason = actionReason();
                      try {
                        const out = await postForm('/api/cancel', { taskId, mode, reason });
                        document.getElementById('actionResult').textContent = JSON.stringify(out, null, 2);
                        await refreshAll();
                      } catch (e) {
                        document.getElementById('actionResult').textContent = 'cancel failed: ' + e.message;
                      }
                    }
                    async function replayTask() {
                      const taskId = selectedTaskId();
                      if (!taskId) return;
                      try {
                        const out = await postForm('/api/replay', { taskId });
                        document.getElementById('actionResult').textContent = JSON.stringify(out, null, 2);
                        await refreshAll();
                      } catch (e) {
                        document.getElementById('actionResult').textContent = 'replay failed: ' + e.message;
                      }
                    }
                    async function replayBatch() {
                      try {
                        const out = await postForm('/api/replay-batch', { status: 'DEAD_LETTER', limit: 50 });
                        document.getElementById('actionResult').textContent = JSON.stringify(out, null, 2);
                        await refreshAll();
                      } catch (e) {
                        document.getElementById('actionResult').textContent = 'replay-batch failed: ' + e.message;
                      }
                    }
                    async function loadAudit(silent = false) {
                      const taskId = selectedTaskId();
                      const action = document.getElementById('auditActionInput').value.trim();
                      let url = '/api/audit-query?limit=40';
                      if (taskId) url += '&taskId=' + encodeURIComponent(taskId);
                      if (action) url += '&action=' + encodeURIComponent(action);
                      try {
                        const rows = await fetchJson(url);
                        document.getElementById('auditRows').textContent = JSON.stringify(rows, null, 2);
                      } catch (e) {
                        if (!silent) {
                          document.getElementById('auditRows').textContent = 'audit load failed: ' + e.message;
                        }
                      }
                    }
                    function connectEvents() {
                      const status = document.getElementById('sseStatus');
                      const es = new EventSource(withAuth('/events'));
                      es.onopen = () => { status.textContent = 'SSE: connected'; };
                      es.onerror = () => { status.textContent = 'SSE: reconnecting...'; };
                      es.addEventListener('snapshot', ev => {
                        try {
                          const snap = JSON.parse(ev.data);
                          status.textContent = 'SSE: live ' + (snap.timestamp || '');
                          if (snap.members) {
                            document.getElementById('members').textContent = JSON.stringify(snap.members, null, 2);
                          }
                          if (snap.conflicts) {
                            document.getElementById('conflicts').textContent = JSON.stringify(snap.conflicts, null, 2);
                          }
                        } catch (e) {
                          status.textContent = 'SSE: parse error';
                        }
                      });
                    }
                    function initTokenFromQuery() {
                      const qp = new URLSearchParams(window.location.search);
                      const t = qp.get('token') || '';
                      if (t) {
                        document.getElementById('tokenInput').value = t;
                      }
                    }
                    initTokenFromQuery();
                    connectEvents();
                    refreshAll();
                    setInterval(refreshAll, 8000);
                  </script>
                </body>
                </html>
                """;
    }

    private static String controlRoomHtml() {
        return """
                <!doctype html>
                <html lang="en">
                <head>
                  <meta charset="utf-8">
                  <meta name="viewport" content="width=device-width, initial-scale=1">
                  <title>RelayMesh Control Room</title>
                  <style>
                    :root {
                      --bg: radial-gradient(circle at 10% 10%, #f8fafc 0%, #e2e8f0 45%, #cbd5e1 100%);
                      --ink: #0f172a;
                      --muted: #475569;
                      --line: #94a3b8;
                      --panel: rgba(248, 250, 252, 0.95);
                      --accent: #0f766e;
                      --accent2: #0369a1;
                      --warning: #b45309;
                      --active: #fef3c7;
                      --active-line: #f59e0b;
                    }
                    * { box-sizing: border-box; }
                    body {
                      margin: 0;
                      color: var(--ink);
                      font-family: "Space Grotesk", "Segoe UI", sans-serif;
                      background: var(--bg);
                    }
                    .wrap {
                      max-width: 1480px;
                      margin: 16px auto 20px;
                      padding: 0 14px;
                    }
                    .topbar {
                      background: var(--panel);
                      border: 1px solid var(--line);
                      border-radius: 14px;
                      padding: 12px;
                      box-shadow: 0 8px 20px rgba(2, 6, 23, 0.08);
                      margin-bottom: 12px;
                    }
                    .title {
                      display: flex;
                      flex-wrap: wrap;
                      gap: 10px;
                      align-items: baseline;
                      margin-bottom: 10px;
                    }
                    .title h1 {
                      margin: 0;
                      font-size: 28px;
                      letter-spacing: 0.3px;
                    }
                    .title p {
                      margin: 0;
                      color: var(--muted);
                    }
                    .controls {
                      display: grid;
                      grid-template-columns: repeat(11, minmax(0, 1fr));
                      gap: 8px;
                    }
                    .ops {
                      display: grid;
                      grid-template-columns: repeat(8, minmax(0, 1fr));
                      gap: 8px;
                      margin-top: 8px;
                    }
                    .profiles {
                      display: grid;
                      grid-template-columns: repeat(6, minmax(0, 1fr));
                      gap: 8px;
                      margin-top: 8px;
                    }
                    .actions {
                      display: grid;
                      grid-template-columns: repeat(9, minmax(0, 1fr));
                      gap: 8px;
                      margin-top: 8px;
                    }
                    .commandbar {
                      display: grid;
                      grid-template-columns: 5fr 1fr 1fr;
                      gap: 8px;
                      margin-top: 8px;
                    }
                    .hint {
                      margin-top: 8px;
                      color: var(--muted);
                      font-size: 12px;
                    }
                    input, select, button {
                      width: 100%;
                      border: 1px solid var(--line);
                      border-radius: 10px;
                      padding: 7px 9px;
                      font: inherit;
                      color: var(--ink);
                      background: #fff;
                    }
                    button {
                      background: var(--accent);
                      border-color: var(--accent);
                      color: #fff;
                      cursor: pointer;
                    }
                    .secondary {
                      background: #0f172a;
                      border-color: #0f172a;
                    }
                    .warning {
                      background: var(--warning);
                      border-color: var(--warning);
                    }
                    .status {
                      margin-top: 8px;
                      font-family: "IBM Plex Mono", "JetBrains Mono", Consolas, monospace;
                      font-size: 12px;
                      color: var(--accent2);
                    }
                    .action-result {
                      margin-top: 8px;
                      font-family: "IBM Plex Mono", "JetBrains Mono", Consolas, monospace;
                      font-size: 12px;
                      color: #0f172a;
                      border: 1px solid #cbd5e1;
                      background: #f8fafc;
                      border-radius: 8px;
                      padding: 8px;
                      max-height: 180px;
                      overflow: auto;
                      white-space: pre-wrap;
                    }
                    .grid {
                      display: grid;
                      grid-template-columns: repeat(2, minmax(0, 1fr));
                      gap: 10px;
                    }
                    .pane {
                      background: var(--panel);
                      border: 1px solid var(--line);
                      border-radius: 14px;
                      padding: 10px;
                      min-height: 260px;
                      box-shadow: 0 8px 22px rgba(2, 6, 23, 0.08);
                    }
                    .pane.active {
                      border-color: var(--active-line);
                      background: var(--active);
                      box-shadow: 0 12px 28px rgba(245, 158, 11, 0.25);
                    }
                    .pane-head {
                      display: grid;
                      grid-template-columns: 120px 1fr 1fr 90px 1fr;
                      gap: 6px;
                      align-items: center;
                      margin-bottom: 8px;
                    }
                    .pane-title {
                      font-size: 14px;
                      font-weight: 700;
                    }
                    .mono {
                      font-family: "IBM Plex Mono", "JetBrains Mono", Consolas, monospace;
                      font-size: 12px;
                    }
                    .pane-body {
                      border: 1px solid #cbd5e1;
                      border-radius: 10px;
                      background: #f8fafc;
                      min-height: 192px;
                      max-height: 380px;
                      overflow: auto;
                      padding: 8px;
                      white-space: pre;
                    }
                    @media (max-width: 1200px) {
                      .controls {
                        grid-template-columns: repeat(6, minmax(0, 1fr));
                      }
                      .ops {
                        grid-template-columns: repeat(3, minmax(0, 1fr));
                      }
                      .profiles {
                        grid-template-columns: repeat(3, minmax(0, 1fr));
                      }
                      .actions {
                        grid-template-columns: repeat(3, minmax(0, 1fr));
                      }
                      .commandbar {
                        grid-template-columns: 1fr;
                      }
                      .pane-head {
                        grid-template-columns: 120px 1fr 1fr;
                      }
                    }
                    @media (max-width: 860px) {
                      .grid {
                        grid-template-columns: 1fr;
                      }
                      .controls {
                        grid-template-columns: 1fr;
                      }
                      .ops {
                        grid-template-columns: 1fr;
                      }
                      .profiles {
                        grid-template-columns: 1fr;
                      }
                      .actions {
                        grid-template-columns: 1fr;
                      }
                      .commandbar {
                        grid-template-columns: 1fr;
                      }
                    }
                  </style>
                </head>
                <body>
                  <div class="wrap">
                    <div class="topbar">
                      <div class="title">
                        <h1>RelayMesh Control Room</h1>
                        <p>multi-screen operator view with shared snapshots</p>
                      </div>
                      <div class="controls">
                        <input id="tokenInput" placeholder="token (optional)">
                        <input id="namespacesInput" placeholder="namespaces: all or default,project-a">
                        <input id="statusFilterInput" placeholder="task status filter (optional)">
                        <input id="focusTaskInput" placeholder="focus task id (workflow/actions)">
                        <input id="refreshSecInput" type="number" min="1" max="60" value="5" placeholder="refresh sec">
                        <select id="transportInput">
                          <option value="sse">transport: sse</option>
                          <option value="poll">transport: poll</option>
                        </select>
                        <select id="presetInput">
                          <option value="">preset (optional)</option>
                          <option value="ops">ops</option>
                          <option value="incident">incident</option>
                          <option value="throughput">throughput</option>
                          <option value="audit">audit</option>
                        </select>
                        <button onclick="manualRefresh()">Refresh Now</button>
                        <button onclick="applySelectedPreset()">Apply Preset</button>
                        <button onclick="saveLayoutToStorage()">Save Layout</button>
                        <button class="warning" onclick="clearStoredLayout()">Clear Layout</button>
                      </div>
                      <div class="ops">
                        <button class="secondary" onclick="copyShareLink()">Copy Share Link</button>
                        <button class="secondary" onclick="window.location.href='/'">Open Classic Console</button>
                        <button class="secondary" onclick="window.open('/?token=' + encodeURIComponent(authToken()), '_blank')">Classic In New Tab</button>
                        <button class="secondary" onclick="addPane()">Add Pane</button>
                        <button class="secondary" onclick="removePane()">Remove Pane</button>
                        <button class="secondary" onclick="openApiNamespaces()">Open /api/namespaces</button>
                        <button class="secondary" onclick="openApiSnapshot()">Open Snapshot API</button>
                        <button class="secondary" onclick="toggleFullscreen()">Toggle Fullscreen</button>
                      </div>
                      <div class="profiles">
                        <input id="profileNameInput" placeholder="layout profile name">
                        <select id="profileSelect"><option value="">profile list</option></select>
                        <button onclick="saveProfile()">Save Profile</button>
                        <button onclick="loadSelectedProfile()">Load Profile</button>
                        <button onclick="deleteSelectedProfile()">Delete Profile</button>
                        <button class="secondary" onclick="refreshProfiles()">Refresh Profiles</button>
                      </div>
                      <div class="actions">
                        <input id="actionNamespaceInput" placeholder="action namespace (blank = focused pane)">
                        <input id="actionTaskIdInput" placeholder="task id (blank = infer from pane)">
                        <input id="actionReasonInput" placeholder="reason (optional)">
                        <button onclick="runControlAction('cancel','soft')">Soft Cancel</button>
                        <button onclick="runControlAction('cancel','hard')">Hard Cancel</button>
                        <button onclick="runControlAction('replay')">Replay Task</button>
                        <button onclick="runControlAction('replay_batch')">Replay Batch</button>
                        <button class="secondary" onclick="clearActionInputs()">Clear Action Inputs</button>
                        <button class="secondary" onclick="clearActionResult()">Clear Action Output</button>
                      </div>
                      <div class="commandbar">
                        <input id="commandInput" placeholder="command: stats|members|tasks|workflow|cancel|replay|replay-batch <ns> ... | pane add|remove | preset <name> | refresh">
                        <button class="secondary" onclick="runCommandLine()">Run Cmd</button>
                        <button class="secondary" onclick="showCommandHelp()">Cmd Help</button>
                      </div>
                      <div class="hint">
                        Hotkeys: Alt+1..9 focus panel, Tab/Shift+Tab cycle, Ctrl+R refresh, Ctrl+S save layout, Ctrl+L copy link, Ctrl++ add pane, Ctrl+- remove pane, Ctrl+Enter run command.
                      </div>
                      <div id="statusLine" class="status mono">booting...</div>
                      <div id="actionResult" class="action-result">action output: ready</div>
                    </div>
                    <section id="paneGrid" class="grid"></section>
                  </div>
                  <script>
                    const STORAGE_KEY = 'relaymesh.control-room.layout.v2';
                    const MAX_PANES = 9;
                    const MIN_PANES = 2;
                    const PRESETS = {
                      ops: [
                        { namespace: 'default', view: 'tasks', limit: 20, status: '' },
                        { namespace: 'default', view: 'dead', limit: 20, status: '' },
                        { namespace: 'default', view: 'conflicts', limit: 20, status: '' },
                        { namespace: 'default', view: 'stats', limit: 1, status: '' }
                      ],
                      incident: [
                        { namespace: 'default', view: 'tasks', limit: 30, status: 'RETRYING' },
                        { namespace: 'default', view: 'dead', limit: 30, status: '' },
                        { namespace: 'default', view: 'conflicts', limit: 30, status: '' },
                        { namespace: 'default', view: 'members', limit: 1, status: '' }
                      ],
                      throughput: [
                        { namespace: 'default', view: 'tasks', limit: 40, status: 'RUNNING' },
                        { namespace: 'default', view: 'tasks', limit: 40, status: 'SUCCESS' },
                        { namespace: 'default', view: 'tasks', limit: 40, status: 'PENDING' },
                        { namespace: 'default', view: 'stats', limit: 1, status: '' }
                      ],
                      audit: [
                        { namespace: 'default', view: 'dead', limit: 30, status: '' },
                        { namespace: 'default', view: 'conflicts', limit: 30, status: '' },
                        { namespace: 'default', view: 'members', limit: 1, status: '' },
                        { namespace: 'default', view: 'stats', limit: 1, status: '' }
                      ]
                    };

                    function defaultPanes() {
                      return [
                        { index: 1, namespace: 'default', view: 'tasks', limit: 12, status: '' },
                        { index: 2, namespace: 'default', view: 'dead', limit: 12, status: '' },
                        { index: 3, namespace: 'default', view: 'conflicts', limit: 20, status: '' },
                        { index: 4, namespace: 'default', view: 'stats', limit: 1, status: '' }
                      ];
                    }

                    const state = {
                      namespaces: [],
                      snapshot: null,
                      workflowCache: {},
                      activePane: 0,
                      pollTimer: null,
                      stream: null,
                      persistTimer: null,
                      panes: defaultPanes(),
                      pendingLayout: null,
                      pendingPreset: '',
                      pendingProfile: ''
                    };

                    function authToken() {
                      return document.getElementById('tokenInput').value.trim();
                    }

                    function withAuth(url) {
                      const token = authToken();
                      if (!token) return url;
                      const sep = url.includes('?') ? '&' : '?';
                      return url + sep + 'token=' + encodeURIComponent(token);
                    }

                    async function fetchJson(url) {
                      const r = await fetch(withAuth(url));
                      if (!r.ok) {
                        throw new Error(await r.text());
                      }
                      return r.json();
                    }

                    async function postForm(url, params) {
                      const body = new URLSearchParams(params).toString();
                      const r = await fetch(withAuth(url), {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
                        body
                      });
                      if (!r.ok) {
                        throw new Error(await r.text());
                      }
                      return r.json();
                    }

                    function paneCount() {
                      return state.panes.length;
                    }

                    function ensurePaneShape(raw) {
                      const views = new Set(['tasks', 'dead', 'conflicts', 'members', 'stats']);
                      const source = Array.isArray(raw) ? raw : [];
                      const out = [];
                      const targetCount = Math.max(MIN_PANES, Math.min(MAX_PANES, source.length > 0 ? source.length : 4));
                      for (let i = 0; i < targetCount; i++) {
                        const src = source[i] || {};
                        const view = typeof src.view === 'string' && views.has(src.view) ? src.view : 'tasks';
                        const namespace = typeof src.namespace === 'string' && src.namespace.trim() ? src.namespace.trim() : 'default';
                        const status = typeof src.status === 'string' ? src.status.trim() : '';
                        const limitRaw = Number.parseInt(String(src.limit || ''), 10);
                        const limit = Number.isFinite(limitRaw) ? Math.max(1, Math.min(200, limitRaw)) : 20;
                        out.push({
                          index: i + 1,
                          namespace,
                          view,
                          limit,
                          status
                        });
                      }
                      return out;
                    }

                    function paneNamespaceOptions(selected) {
                      const values = state.namespaces.length > 0 ? state.namespaces : ['default'];
                      return values.map(ns => {
                        const sel = ns === selected ? 'selected' : '';
                        return `<option value="${ns}" ${sel}>${ns}</option>`;
                      }).join('');
                    }

                    function paneViewOptions(selected) {
                      const views = ['tasks', 'dead', 'conflicts', 'members', 'stats', 'workflow'];
                      return views.map(v => {
                        const sel = v === selected ? 'selected' : '';
                        return `<option value="${v}" ${sel}>${v}</option>`;
                      }).join('');
                    }

                    function reconcilePaneNamespaces() {
                      const fallback = state.namespaces.length > 0 ? state.namespaces[0] : 'default';
                      for (const pane of state.panes) {
                        if (!state.namespaces.includes(pane.namespace)) {
                          pane.namespace = fallback;
                        }
                      }
                    }

                    function renderPaneShells() {
                      const grid = document.getElementById('paneGrid');
                      grid.innerHTML = '';
                      for (const cfg of state.panes) {
                        const pane = document.createElement('article');
                        pane.className = 'pane';
                        pane.dataset.index = String(cfg.index - 1);
                        pane.innerHTML = `
                          <div class="pane-head">
                            <div class="pane-title">Pane ${cfg.index} (Alt+${cfg.index})</div>
                            <select class="nsSelect">${paneNamespaceOptions(cfg.namespace)}</select>
                            <select class="viewSelect">${paneViewOptions(cfg.view)}</select>
                            <input class="limitInput" type="number" min="1" max="200" value="${cfg.limit}">
                            <input class="statusInput" placeholder="status filter (tasks only)" value="${cfg.status}">
                          </div>
                          <div class="pane-body mono" id="paneBody${cfg.index}">loading...</div>
                        `;
                        grid.appendChild(pane);
                      }
                      bindPaneControls();
                      focusPane(state.activePane);
                    }

                    function bindPaneControls() {
                      const panes = document.querySelectorAll('.pane');
                      panes.forEach((pane, idx) => {
                        pane.addEventListener('click', () => focusPane(idx));
                        const nsSel = pane.querySelector('.nsSelect');
                        const viewSel = pane.querySelector('.viewSelect');
                        const limitInput = pane.querySelector('.limitInput');
                        const statusInput = pane.querySelector('.statusInput');
                        nsSel.addEventListener('change', () => {
                          state.panes[idx].namespace = nsSel.value;
                          renderPaneData(idx);
                          persistLayoutSoon();
                        });
                        viewSel.addEventListener('change', () => {
                          state.panes[idx].view = viewSel.value;
                          renderPaneData(idx);
                          persistLayoutSoon();
                        });
                        limitInput.addEventListener('change', () => {
                          const next = Number.parseInt(limitInput.value, 10);
                          state.panes[idx].limit = Number.isFinite(next) ? Math.max(1, Math.min(200, next)) : 20;
                          limitInput.value = String(state.panes[idx].limit);
                          renderPaneData(idx);
                          persistLayoutSoon();
                        });
                        statusInput.addEventListener('change', () => {
                          state.panes[idx].status = statusInput.value.trim();
                          renderPaneData(idx);
                          persistLayoutSoon();
                        });
                      });
                    }

                    function focusPane(index) {
                      const panes = document.querySelectorAll('.pane');
                      if (panes.length === 0) return;
                      const normalized = (index + panes.length) % panes.length;
                      state.activePane = normalized;
                      panes.forEach((pane, idx) => {
                        if (idx === normalized) pane.classList.add('active');
                        else pane.classList.remove('active');
                      });
                      persistLayoutSoon();
                    }

                    function activePaneConfig() {
                      if (state.panes.length === 0) return null;
                      const idx = Math.max(0, Math.min(state.panes.length - 1, state.activePane));
                      return state.panes[idx] || null;
                    }

                    function selectedNamespacesQuery() {
                      const raw = document.getElementById('namespacesInput').value.trim();
                      if (!raw) return 'all';
                      return raw;
                    }

                    function globalStatusFilter() {
                      return document.getElementById('statusFilterInput').value.trim();
                    }

                    function maxPaneLimit(view) {
                      let max = 20;
                      for (const pane of state.panes) {
                        if (view === 'task' && pane.view === 'tasks') {
                          max = Math.max(max, pane.limit || 20);
                        }
                        if (view === 'dead' && pane.view === 'dead') {
                          max = Math.max(max, pane.limit || 20);
                        }
                        if (view === 'conflicts' && pane.view === 'conflicts') {
                          max = Math.max(max, pane.limit || 20);
                        }
                      }
                      return Math.max(1, Math.min(200, max));
                    }

                    async function loadNamespaces() {
                      const body = await fetchJson('/api/namespaces');
                      state.namespaces = body.namespaces || [];
                      const active = body.activeNamespace || 'default';
                      if (!state.namespaces.includes(active)) {
                        state.namespaces.unshift(active);
                      }
                      if (!document.getElementById('namespacesInput').value.trim()) {
                        document.getElementById('namespacesInput').value = state.namespaces.join(',');
                      }
                      reconcilePaneNamespaces();
                      renderPaneShells();
                    }

                    function setStatusLine(msg) {
                      document.getElementById('statusLine').textContent = msg;
                    }

                    function maxPollIntervalSec() {
                      const secRaw = Number.parseInt(document.getElementById('refreshSecInput').value, 10);
                      const sec = Number.isFinite(secRaw) ? Math.max(1, Math.min(60, secRaw)) : 5;
                      document.getElementById('refreshSecInput').value = String(sec);
                      return sec;
                    }

                    function buildSnapshotParams(includeInterval) {
                      const qp = new URLSearchParams();
                      qp.set('namespaces', selectedNamespacesQuery());
                      qp.set('taskLimit', String(maxPaneLimit('task')));
                      qp.set('deadLimit', String(maxPaneLimit('dead')));
                      qp.set('conflictLimit', String(maxPaneLimit('conflicts')));
                      const status = globalStatusFilter();
                      if (status) qp.set('status', status);
                      if (includeInterval) {
                        qp.set('intervalMs', String(maxPollIntervalSec() * 1000));
                      }
                      return qp;
                    }

                    function updateSnapshot(body, source) {
                      state.snapshot = body;
                      renderAllPaneData();
                      const names = (body.requestedNamespaces || []).join(',');
                      const ts = body.timestamp || new Date().toISOString();
                      setStatusLine(source + ' ok @ ' + ts + ' namespaces=' + names + ' mode=' + currentTransport());
                    }

                    async function loadSnapshot() {
                      const qp = buildSnapshotParams(false);
                      const body = await fetchJson('/api/control-room/snapshot?' + qp.toString());
                      updateSnapshot(body, 'snapshot');
                    }

                    function safeArray(value) {
                      return Array.isArray(value) ? value : [];
                    }

                    function truncateLines(lines, limit) {
                      if (lines.length <= limit) return lines;
                      const copy = lines.slice(0, limit);
                      copy.push('... (' + (lines.length - limit) + ' more)');
                      return copy;
                    }

                    function workflowCacheKey(namespace, taskId) {
                      return namespace + '::' + taskId;
                    }

                    function firstTaskIdForNamespace(slot) {
                      const tasks = safeArray(slot.tasks);
                      if (tasks.length > 0 && tasks[0] && tasks[0].taskId) {
                        return String(tasks[0].taskId);
                      }
                      const dead = safeArray(slot.dead);
                      if (dead.length > 0 && dead[0] && dead[0].taskId) {
                        return String(dead[0].taskId);
                      }
                      return '';
                    }

                    function formatWorkflowPayload(payload, namespace, taskId) {
                      const wf = payload && payload.workflow ? payload.workflow : {};
                      const task = wf && wf.task ? wf.task : {};
                      const status = task && task.status ? String(task.status) : '';
                      const edges = Array.isArray(payload && payload.edges) ? payload.edges : [];
                      const head = 'workflow namespace=' + namespace + ' task=' + taskId + ' status=' + status;
                      if (edges.length === 0) {
                        return head + '\\n\\n(no edges)';
                      }
                      return head + '\\n\\n' + edges.join('\\n');
                    }

                    async function loadWorkflowForPane(index, namespace, taskId) {
                      const key = workflowCacheKey(namespace, taskId);
                      try {
                        let payload = state.workflowCache[key];
                        if (!payload) {
                          payload = await fetchJson('/api/control-room/workflow?namespace=' + encodeURIComponent(namespace) + '&taskId=' + encodeURIComponent(taskId));
                          state.workflowCache[key] = payload;
                        }
                        const pane = state.panes[index];
                        if (!pane || pane.namespace !== namespace || pane.view !== 'workflow') {
                          return;
                        }
                        const body = document.getElementById('paneBody' + pane.index);
                        if (!body) return;
                        body.textContent = formatWorkflowPayload(payload, namespace, taskId);
                      } catch (e) {
                        const pane = state.panes[index];
                        if (!pane || pane.namespace !== namespace || pane.view !== 'workflow') {
                          return;
                        }
                        const body = document.getElementById('paneBody' + pane.index);
                        if (!body) return;
                        body.textContent = 'workflow load failed: ' + e.message;
                      }
                    }

                    function renderPaneData(index) {
                      const cfg = state.panes[index];
                      const body = document.getElementById('paneBody' + cfg.index);
                      if (!body) return;
                      if (!state.snapshot || !state.snapshot.data) {
                        body.textContent = 'waiting for snapshot...';
                        return;
                      }
                      const ns = cfg.namespace;
                      const slot = state.snapshot.data[ns];
                      if (!slot) {
                        body.textContent = 'namespace not found in snapshot: ' + ns;
                        return;
                      }
                      const view = cfg.view;
                      if (view === 'tasks') {
                        let rows = safeArray(slot.tasks);
                        const localStatus = cfg.status || globalStatusFilter();
                        if (localStatus) {
                          rows = rows.filter(t => String(t.status || '').toUpperCase() === localStatus.toUpperCase());
                        }
                        const lines = rows.map(t => `${t.taskId} | ${t.status} | ${t.updatedAtMs}`);
                        body.textContent = truncateLines(lines, cfg.limit).join('\\n') || '(empty tasks)';
                        return;
                      }
                      if (view === 'dead') {
                        const rows = safeArray(slot.dead);
                        const lines = rows.map(t => `${t.taskId} | ${t.status} | ${t.lastError || ''}`);
                        body.textContent = truncateLines(lines, cfg.limit).join('\\n') || '(empty dead)';
                        return;
                      }
                      if (view === 'conflicts') {
                        const rows = safeArray(slot.conflicts);
                        const lines = rows.map(c => `${c.type || 'conflict'} | step=${c.stepId || ''} | task=${c.taskId || ''} | at=${c.occurredAtMs || ''}`);
                        body.textContent = truncateLines(lines, cfg.limit).join('\\n') || '(empty conflicts)';
                        return;
                      }
                      if (view === 'members') {
                        body.textContent = JSON.stringify(slot.members || {}, null, 2);
                        return;
                      }
                      if (view === 'workflow') {
                        const taskId = focusTaskId() || firstTaskIdForNamespace(slot);
                        if (!taskId) {
                          body.textContent = 'workflow view: set focus task id, or keep tasks available in this namespace';
                          return;
                        }
                        const key = workflowCacheKey(ns, taskId);
                        const cached = state.workflowCache[key];
                        if (cached) {
                          body.textContent = formatWorkflowPayload(cached, ns, taskId);
                          return;
                        }
                        body.textContent = 'loading workflow for task=' + taskId + ' ...';
                        loadWorkflowForPane(index, ns, taskId);
                        return;
                      }
                      body.textContent = JSON.stringify(slot.stats || {}, null, 2);
                    }

                    function renderAllPaneData() {
                      for (let i = 0; i < state.panes.length; i++) {
                        renderPaneData(i);
                      }
                    }

                    function setActionResult(text) {
                      document.getElementById('actionResult').textContent = text;
                    }

                    function clearActionResult() {
                      setActionResult('action output: cleared');
                    }

                    function clearActionInputs() {
                      document.getElementById('actionNamespaceInput').value = '';
                      document.getElementById('actionTaskIdInput').value = '';
                      document.getElementById('actionReasonInput').value = '';
                    }

                    function inferTaskIdFromActivePane() {
                      if (!state.snapshot || !state.snapshot.data) return '';
                      const active = activePaneConfig();
                      if (!active) return '';
                      const slot = state.snapshot.data[active.namespace];
                      if (!slot) return '';
                      const rows = active.view === 'dead' ? safeArray(slot.dead) : safeArray(slot.tasks);
                      if (rows.length === 0) return '';
                      const first = rows[0];
                      return first && first.taskId ? String(first.taskId) : '';
                    }

                    function focusTaskId() {
                      return document.getElementById('focusTaskInput').value.trim();
                    }

                    function actionNamespace() {
                      const raw = document.getElementById('actionNamespaceInput').value.trim();
                      if (raw) return raw;
                      const active = activePaneConfig();
                      if (active && active.namespace) return active.namespace;
                      return 'default';
                    }

                    function actionTaskId() {
                      const raw = document.getElementById('actionTaskIdInput').value.trim();
                      if (raw) return raw;
                      const focus = focusTaskId();
                      if (focus) return focus;
                      return inferTaskIdFromActivePane();
                    }

                    async function runControlAction(action, mode = '') {
                      const namespace = actionNamespace();
                      const reason = document.getElementById('actionReasonInput').value.trim();
                      const payload = { action, namespace };
                      if (mode) payload.mode = mode;
                      if (reason) payload.reason = reason;
                      if (action === 'cancel' || action === 'replay') {
                        const taskId = actionTaskId();
                        if (!taskId) {
                          setActionResult('action failed: missing task id');
                          return;
                        }
                        payload.taskId = taskId;
                      }
                      if (action === 'replay_batch') {
                        payload.status = 'DEAD_LETTER';
                        payload.limit = '50';
                      }
                      try {
                        const out = await postForm('/api/control-room/action', payload);
                        setActionResult(JSON.stringify(out, null, 2));
                        state.workflowCache = {};
                        await loadSnapshot();
                      } catch (e) {
                        setActionResult('action failed: ' + e.message);
                      }
                    }

                    function tokenizeCommand(raw) {
                      if (!raw) return [];
                      return raw.trim().split(/\s+/).filter(Boolean);
                    }

                    async function runCommandLine() {
                      const raw = document.getElementById('commandInput').value.trim();
                      if (!raw) return;
                      const parts = tokenizeCommand(raw);
                      if (parts.length === 0) return;
                      const cmd = parts[0].toLowerCase();
                      try {
                        if (cmd === 'refresh') {
                          await loadSnapshot();
                          return;
                        }
                        if (cmd === 'preset' && parts.length >= 2) {
                          const name = parts[1].toLowerCase();
                          document.getElementById('presetInput').value = name;
                          applyPreset(name);
                          await loadSnapshot();
                          return;
                        }
                        if (cmd === 'pane' && parts.length >= 2) {
                          const op = parts[1].toLowerCase();
                          if (op === 'add') {
                            addPane();
                            return;
                          }
                          if (op === 'remove') {
                            removePane();
                            return;
                          }
                        }
                        if (cmd === 'profile' && parts.length >= 2) {
                          const op = parts[1].toLowerCase();
                          if (op === 'list') {
                            await refreshProfiles();
                            return;
                          }
                          if (op === 'save' && parts.length >= 3) {
                            document.getElementById('profileNameInput').value = parts[2];
                            await saveProfile();
                            return;
                          }
                          if (op === 'load' && parts.length >= 3) {
                            await loadProfileByName(parts[2]);
                            return;
                          }
                          if (op === 'delete' && parts.length >= 3) {
                            document.getElementById('profileNameInput').value = parts[2];
                            await deleteSelectedProfile();
                            return;
                          }
                        }
                        const out = await postForm('/api/control-room/command', { command: raw });
                        setActionResult(JSON.stringify(out, null, 2));
                        const lower = cmd.toLowerCase();
                        if (lower === 'cancel' || lower === 'replay' || lower === 'replay-batch' || lower === 'replay_batch') {
                          state.workflowCache = {};
                        }
                        if (lower === 'workflow' && parts.length >= 3) {
                          document.getElementById('focusTaskInput').value = parts[2];
                        }
                        if (lower === 'help') {
                          return;
                        }
                        await loadSnapshot();
                      } catch (e) {
                        setActionResult('command failed: ' + e.message);
                      }
                    }

                    function showCommandHelp() {
                      const lines = [
                        'commands:',
                        'refresh',
                        'preset <ops|incident|throughput|audit>',
                        'pane add',
                        'pane remove',
                        'profile list',
                        'profile save <name>',
                        'profile load <name>',
                        'profile delete <name>',
                        'namespaces',
                        'stats <namespace>',
                        'members <namespace>',
                        'tasks <namespace> [status] [limit]',
                        'workflow <namespace> <taskId>',
                        'cancel <namespace> <taskId> [soft|hard] [reason...]',
                        'replay <namespace> <taskId>',
                        'replay-batch <namespace> [limit]'
                      ];
                      setActionResult(lines.join('\\n'));
                    }

                    function manualRefresh() {
                      loadSnapshot().catch(err => {
                        setStatusLine('snapshot failed: ' + err.message);
                      });
                    }

                    function stopRealtime() {
                      if (state.pollTimer) {
                        clearInterval(state.pollTimer);
                        state.pollTimer = null;
                      }
                      if (state.stream) {
                        state.stream.close();
                        state.stream = null;
                      }
                    }

                    function rearmPolling() {
                      stopRealtime();
                      const sec = maxPollIntervalSec();
                      state.pollTimer = setInterval(() => {
                        loadSnapshot().catch(err => {
                          setStatusLine('snapshot failed: ' + err.message);
                        });
                      }, sec * 1000);
                    }

                    function connectControlStream() {
                      stopRealtime();
                      const params = buildSnapshotParams(true);
                      const es = new EventSource(withAuth('/events/control-room?' + params.toString()));
                      state.stream = es;
                      es.onopen = () => {
                        setStatusLine('stream connected');
                      };
                      es.onerror = () => {
                        setStatusLine('stream reconnecting...');
                      };
                      es.addEventListener('control_snapshot', ev => {
                        try {
                          const payload = JSON.parse(ev.data);
                          updateSnapshot(payload, 'stream');
                        } catch (e) {
                          setStatusLine('stream parse failed: ' + e.message);
                        }
                      });
                    }

                    function currentTransport() {
                      const value = document.getElementById('transportInput').value;
                      return value === 'poll' ? 'poll' : 'sse';
                    }

                    function applyTransport() {
                      if (currentTransport() === 'poll') {
                        rearmPolling();
                        return;
                      }
                      connectControlStream();
                    }

                    function exportLayoutState() {
                      return {
                        panes: state.panes.map(p => ({
                          namespace: p.namespace,
                          view: p.view,
                          limit: p.limit,
                          status: p.status || ''
                        })),
                        namespaces: document.getElementById('namespacesInput').value.trim(),
                        statusFilter: document.getElementById('statusFilterInput').value.trim(),
                        focusTaskId: document.getElementById('focusTaskInput').value.trim(),
                        actionNs: document.getElementById('actionNamespaceInput').value.trim(),
                        profile: document.getElementById('profileNameInput').value.trim(),
                        refreshSec: maxPollIntervalSec(),
                        transport: currentTransport(),
                        preset: document.getElementById('presetInput').value || '',
                        activePane: state.activePane
                      };
                    }

                    function applyLayout(layout) {
                      if (!layout || typeof layout !== 'object') return;
                      if (typeof layout.namespaces === 'string') {
                        document.getElementById('namespacesInput').value = layout.namespaces;
                      }
                      if (typeof layout.statusFilter === 'string') {
                        document.getElementById('statusFilterInput').value = layout.statusFilter;
                      }
                      if (typeof layout.focusTaskId === 'string') {
                        document.getElementById('focusTaskInput').value = layout.focusTaskId;
                      }
                      if (typeof layout.actionNs === 'string') {
                        document.getElementById('actionNamespaceInput').value = layout.actionNs;
                      }
                      if (typeof layout.profile === 'string') {
                        document.getElementById('profileNameInput').value = layout.profile;
                      }
                      if (layout.refreshSec != null) {
                        document.getElementById('refreshSecInput').value = String(layout.refreshSec);
                      }
                      if (layout.transport === 'poll' || layout.transport === 'sse') {
                        document.getElementById('transportInput').value = layout.transport;
                      }
                      if (typeof layout.preset === 'string') {
                        document.getElementById('presetInput').value = layout.preset;
                      }
                      state.panes = ensurePaneShape(layout.panes);
                      state.activePane = Number.isFinite(layout.activePane) ? Math.max(0, Math.min(paneCount() - 1, layout.activePane)) : 0;
                    }

                    function saveLayoutToStorage() {
                      try {
                        const payload = exportLayoutState();
                        localStorage.setItem(STORAGE_KEY, JSON.stringify(payload));
                        setStatusLine('layout saved');
                      } catch (e) {
                        setStatusLine('save layout failed: ' + e.message);
                      }
                    }

                    function persistLayoutSoon() {
                      if (state.persistTimer) {
                        clearTimeout(state.persistTimer);
                      }
                      state.persistTimer = setTimeout(() => {
                        saveLayoutToStorage();
                        state.persistTimer = null;
                      }, 350);
                    }

                    function loadLayoutFromStorage() {
                      try {
                        const raw = localStorage.getItem(STORAGE_KEY);
                        if (!raw) return null;
                        return JSON.parse(raw);
                      } catch (e) {
                        return null;
                      }
                    }

                    function clearStoredLayout() {
                      try {
                        localStorage.removeItem(STORAGE_KEY);
                        setStatusLine('stored layout cleared');
                      } catch (e) {
                        setStatusLine('clear layout failed: ' + e.message);
                      }
                    }

                    function applyPreset(name) {
                      const preset = PRESETS[name];
                      if (!preset) return;
                      const fallbackNs = state.namespaces.length > 0 ? state.namespaces[0] : 'default';
                      const hydrated = preset.map((pane, idx) => ({
                        index: idx + 1,
                        namespace: state.namespaces.includes(pane.namespace) ? pane.namespace : fallbackNs,
                        view: pane.view,
                        limit: pane.limit,
                        status: pane.status || ''
                      }));
                      state.panes = ensurePaneShape(hydrated);
                      renderPaneShells();
                      renderAllPaneData();
                      persistLayoutSoon();
                    }

                    function applySelectedPreset() {
                      const name = document.getElementById('presetInput').value;
                      if (!name) return;
                      applyPreset(name);
                      manualRefresh();
                    }

                    function copyShareLink() {
                      const qp = new URLSearchParams();
                      qp.set('layout', JSON.stringify(exportLayoutState()));
                      const link = window.location.origin + window.location.pathname + '?' + qp.toString();
                      navigator.clipboard.writeText(link)
                        .then(() => setStatusLine('share link copied'))
                        .catch(err => setStatusLine('copy link failed: ' + err.message));
                    }

                    function profileNameFromInputs() {
                      const rawInput = document.getElementById('profileNameInput').value.trim();
                      if (rawInput) return rawInput;
                      const selected = document.getElementById('profileSelect').value.trim();
                      return selected;
                    }

                    async function refreshProfiles() {
                      try {
                        const out = await fetchJson('/api/control-room/layouts');
                        const profiles = Array.isArray(out.profiles) ? out.profiles : [];
                        const select = document.getElementById('profileSelect');
                        const current = select.value;
                        select.innerHTML = '<option value=\"\">profile list</option>';
                        for (const name of profiles) {
                          const opt = document.createElement('option');
                          opt.value = name;
                          opt.textContent = name;
                          select.appendChild(opt);
                        }
                        if (current && profiles.includes(current)) {
                          select.value = current;
                        }
                        setStatusLine('profiles loaded: ' + profiles.length);
                      } catch (e) {
                        setStatusLine('profile load failed: ' + e.message);
                      }
                    }

                    async function saveProfile() {
                      const name = profileNameFromInputs();
                      if (!name) {
                        setActionResult('profile save failed: missing profile name');
                        return;
                      }
                      try {
                        const payload = {
                          name,
                          layout: JSON.stringify(exportLayoutState())
                        };
                        const out = await postForm('/api/control-room/layouts/save', payload);
                        document.getElementById('profileNameInput').value = name;
                        await refreshProfiles();
                        document.getElementById('profileSelect').value = name;
                        setActionResult(JSON.stringify(out, null, 2));
                      } catch (e) {
                        setActionResult('profile save failed: ' + e.message);
                      }
                    }

                    async function loadProfileByName(name) {
                      if (!name) {
                        setActionResult('profile load failed: missing profile name');
                        return;
                      }
                      try {
                        const out = await fetchJson('/api/control-room/layouts?name=' + encodeURIComponent(name));
                        applyLayout(out.layout || {});
                        reconcilePaneNamespaces();
                        renderPaneShells();
                        await loadSnapshot();
                        applyTransport();
                        document.getElementById('profileNameInput').value = name;
                        document.getElementById('profileSelect').value = name;
                        setActionResult(JSON.stringify(out, null, 2));
                      } catch (e) {
                        setActionResult('profile load failed: ' + e.message);
                      }
                    }

                    async function loadSelectedProfile() {
                      const name = profileNameFromInputs();
                      await loadProfileByName(name);
                    }

                    async function deleteSelectedProfile() {
                      const name = profileNameFromInputs();
                      if (!name) {
                        setActionResult('profile delete failed: missing profile name');
                        return;
                      }
                      try {
                        const out = await postForm('/api/control-room/layouts/delete', { name });
                        await refreshProfiles();
                        if (document.getElementById('profileSelect').value === name) {
                          document.getElementById('profileSelect').value = '';
                        }
                        setActionResult(JSON.stringify(out, null, 2));
                      } catch (e) {
                        setActionResult('profile delete failed: ' + e.message);
                      }
                    }

                    function openApiNamespaces() {
                      window.open(withAuth('/api/namespaces'), '_blank');
                    }

                    function openApiSnapshot() {
                      const qp = buildSnapshotParams(false);
                      window.open(withAuth('/api/control-room/snapshot?' + qp.toString()), '_blank');
                    }

                    function toggleFullscreen() {
                      if (!document.fullscreenElement) {
                        document.documentElement.requestFullscreen().catch(() => {});
                        return;
                      }
                      document.exitFullscreen().catch(() => {});
                    }

                    function addPane() {
                      if (state.panes.length >= MAX_PANES) {
                        setStatusLine('max panes reached');
                        return;
                      }
                      const fallbackNs = state.namespaces.length > 0 ? state.namespaces[0] : 'default';
                      state.panes.push({
                        index: state.panes.length + 1,
                        namespace: fallbackNs,
                        view: 'tasks',
                        limit: 12,
                        status: ''
                      });
                      for (let i = 0; i < state.panes.length; i++) {
                        state.panes[i].index = i + 1;
                      }
                      renderPaneShells();
                      renderAllPaneData();
                      persistLayoutSoon();
                    }

                    function removePane() {
                      if (state.panes.length <= MIN_PANES) {
                        setStatusLine('min panes reached');
                        return;
                      }
                      state.panes.pop();
                      for (let i = 0; i < state.panes.length; i++) {
                        state.panes[i].index = i + 1;
                      }
                      state.activePane = Math.min(state.activePane, state.panes.length - 1);
                      renderPaneShells();
                      renderAllPaneData();
                      persistLayoutSoon();
                    }

                    function bindGlobalKeys() {
                      document.addEventListener('keydown', ev => {
                        if (ev.altKey && !ev.ctrlKey && !ev.shiftKey) {
                          const num = Number.parseInt(ev.key, 10);
                          if (Number.isFinite(num) && num >= 1 && num <= state.panes.length) {
                            ev.preventDefault();
                            focusPane(num - 1);
                            return;
                          }
                        }
                        if (ev.key === 'Tab') {
                          ev.preventDefault();
                          const direction = ev.shiftKey ? -1 : 1;
                          focusPane(state.activePane + direction);
                          return;
                        }
                        if (ev.ctrlKey && (ev.key === 'r' || ev.key === 'R')) {
                          ev.preventDefault();
                          manualRefresh();
                          return;
                        }
                        if (ev.ctrlKey && (ev.key === 's' || ev.key === 'S')) {
                          ev.preventDefault();
                          saveLayoutToStorage();
                          return;
                        }
                        if (ev.ctrlKey && (ev.key === 'l' || ev.key === 'L')) {
                          ev.preventDefault();
                          copyShareLink();
                          return;
                        }
                        if (ev.ctrlKey && (ev.key === '=' || ev.key === '+')) {
                          ev.preventDefault();
                          addPane();
                          return;
                        }
                        if (ev.ctrlKey && ev.key === '-') {
                          ev.preventDefault();
                          removePane();
                          return;
                        }
                        if (ev.ctrlKey && ev.key === 'Enter') {
                          ev.preventDefault();
                          runCommandLine();
                        }
                      });
                    }

                    function initFromQuery() {
                      const qp = new URLSearchParams(window.location.search);
                      const token = qp.get('token') || '';
                      if (token) {
                        document.getElementById('tokenInput').value = token;
                      }
                      const namespaces = qp.get('namespaces') || '';
                      if (namespaces) {
                        document.getElementById('namespacesInput').value = namespaces;
                      }
                      const transport = qp.get('transport') || '';
                      if (transport === 'poll' || transport === 'sse') {
                        document.getElementById('transportInput').value = transport;
                      }
                      const refreshSec = qp.get('refreshSec') || '';
                      if (refreshSec) {
                        document.getElementById('refreshSecInput').value = refreshSec;
                      }
                      const preset = qp.get('preset') || '';
                      if (preset) {
                        state.pendingPreset = preset;
                        document.getElementById('presetInput').value = preset;
                      }
                      const profile = qp.get('profile') || '';
                      if (profile) {
                        state.pendingProfile = profile;
                        document.getElementById('profileNameInput').value = profile;
                      }
                      const focusTask = qp.get('focusTask') || '';
                      if (focusTask) {
                        document.getElementById('focusTaskInput').value = focusTask;
                      }
                      const actionNs = qp.get('actionNs') || '';
                      if (actionNs) {
                        document.getElementById('actionNamespaceInput').value = actionNs;
                      }
                      const layout = qp.get('layout') || '';
                      if (layout) {
                        try {
                          state.pendingLayout = JSON.parse(layout);
                        } catch (e) {
                          setStatusLine('query layout parse failed');
                        }
                      }
                    }

                    function bindTopControls() {
                      const triggerRefresh = () => {
                        persistLayoutSoon();
                        manualRefresh();
                        applyTransport();
                      };
                      document.getElementById('namespacesInput').addEventListener('change', triggerRefresh);
                      document.getElementById('statusFilterInput').addEventListener('change', triggerRefresh);
                      document.getElementById('focusTaskInput').addEventListener('change', () => {
                        persistLayoutSoon();
                        renderAllPaneData();
                      });
                      document.getElementById('refreshSecInput').addEventListener('change', triggerRefresh);
                      document.getElementById('transportInput').addEventListener('change', triggerRefresh);
                      document.getElementById('presetInput').addEventListener('change', persistLayoutSoon);
                      document.getElementById('profileNameInput').addEventListener('change', persistLayoutSoon);
                      document.getElementById('profileSelect').addEventListener('change', () => {
                        const value = document.getElementById('profileSelect').value.trim();
                        if (value) {
                          document.getElementById('profileNameInput').value = value;
                        }
                        persistLayoutSoon();
                      });
                      document.getElementById('actionNamespaceInput').addEventListener('change', persistLayoutSoon);
                      document.getElementById('commandInput').addEventListener('keydown', ev => {
                        if (ev.key === 'Enter') {
                          ev.preventDefault();
                          runCommandLine();
                        }
                      });
                    }

                    async function bootstrap() {
                      initFromQuery();
                      bindGlobalKeys();
                      bindTopControls();
                      clearActionResult();
                      document.getElementById('tokenInput').addEventListener('change', () => {
                        stopRealtime();
                        loadNamespaces()
                          .then(refreshProfiles)
                          .then(() => {
                            renderPaneShells();
                            return loadSnapshot();
                          })
                          .then(applyTransport)
                          .catch(err => setStatusLine('reload failed: ' + err.message));
                      });
                      await loadNamespaces();
                      await refreshProfiles();
                      if (state.pendingProfile) {
                        await loadProfileByName(state.pendingProfile);
                        return;
                      }
                      const stored = state.pendingLayout == null ? loadLayoutFromStorage() : null;
                      if (state.pendingLayout != null) {
                        applyLayout(state.pendingLayout);
                      } else if (stored) {
                        applyLayout(stored);
                      } else {
                        state.panes = ensurePaneShape(state.panes);
                      }
                      if (state.pendingPreset) {
                        applyPreset(state.pendingPreset);
                      }
                      reconcilePaneNamespaces();
                      renderPaneShells();
                      await loadSnapshot();
                      applyTransport();
                    }

                    bootstrap().catch(err => {
                      setStatusLine('bootstrap failed: ' + err.message);
                    });
                  </script>
                </body>
                </html>
                """;
    }

    record WorkflowFile(List<WorkflowStepFile> steps) {
    }

    record WorkflowStepFile(String id, String agent, String input, String priority, List<String> dependsOn) {
    }

    private enum PrincipalRole {
        READER,
        WRITER,
        ADMIN;

        boolean canRead() {
            return true;
        }

        boolean canWrite() {
            return this == WRITER || this == ADMIN;
        }
    }

    private record AuthPrincipal(
            String id,
            PrincipalRole role,
            boolean serviceAccount,
            Set<String> allowedNamespaces,
            String namespace
    ) {
        static AuthPrincipal bypass(String namespace) {
            return new AuthPrincipal("principal:bypass", PrincipalRole.ADMIN, true, Set.of(), namespace);
        }
    }

    private record WebAuth(
            String namespace,
            Map<String, AuthPrincipal> tokenToPrincipal
    ) {
        static WebAuth load(
                String roToken,
                String rwToken,
                String roTokenNext,
                String rwTokenNext,
                String authFile,
                String namespace
        ) throws IOException {
            String normalizedNamespace = namespace == null || namespace.isBlank() ? "default" : namespace.trim().toLowerCase(Locale.ROOT);
            LinkedHashMap<String, AuthPrincipal> tokens = new LinkedHashMap<>();
            addLegacyTokens(tokens, roToken, roTokenNext, PrincipalRole.READER, normalizedNamespace);
            addLegacyTokens(tokens, rwToken, rwTokenNext, PrincipalRole.WRITER, normalizedNamespace);
            if (authFile != null && !authFile.isBlank()) {
                loadFromFile(tokens, Path.of(authFile.trim()), normalizedNamespace);
            }
            return new WebAuth(normalizedNamespace, Map.copyOf(tokens));
        }

        boolean enabled() {
            return !tokenToPrincipal.isEmpty();
        }

        int principalCount() {
            LinkedHashSet<String> ids = new LinkedHashSet<>();
            for (AuthPrincipal principal : tokenToPrincipal.values()) {
                if (principal != null && principal.id() != null) {
                    ids.add(principal.id());
                }
            }
            return ids.size();
        }

        AuthPrincipal resolve(String token) {
            if (token == null || token.isBlank()) {
                return null;
            }
            return tokenToPrincipal.get(token.trim());
        }

        boolean canRead(AuthPrincipal principal) {
            return principal != null && principal.role().canRead();
        }

        boolean canWrite(AuthPrincipal principal) {
            return principal != null && principal.role().canWrite();
        }

        boolean namespaceAllowed(AuthPrincipal principal) {
            if (principal == null) {
                return false;
            }
            if (principal.allowedNamespaces() == null || principal.allowedNamespaces().isEmpty()) {
                return true;
            }
            return principal.allowedNamespaces().contains(namespace);
        }

        private static void addLegacyTokens(
                Map<String, AuthPrincipal> target,
                String primary,
                String secondary,
                PrincipalRole role,
                String namespace
        ) {
            List<String> values = new ArrayList<>();
            values.addAll(parseTokenSet(primary));
            values.addAll(parseTokenSet(secondary));
            int index = 0;
            for (String token : values) {
                if (token == null || token.isBlank()) {
                    continue;
                }
                String principalId = "sa:legacy:" + role.name().toLowerCase(Locale.ROOT) + ":" + (++index);
                target.put(token, new AuthPrincipal(principalId, role, true, Set.of(namespace), namespace));
            }
        }

        private static List<String> parseTokenSet(String raw) {
            List<String> out = new ArrayList<>();
            if (raw == null || raw.isBlank()) {
                return out;
            }
            for (String token : raw.split(",")) {
                String trimmed = token == null ? "" : token.trim();
                if (!trimmed.isEmpty()) {
                    out.add(trimmed);
                }
            }
            return out;
        }

        private static void loadFromFile(Map<String, AuthPrincipal> target, Path file, String namespace) throws IOException {
            if (!Files.exists(file)) {
                throw new IllegalArgumentException("auth file not found: " + file);
            }
            WebAuthFile body = Jsons.mapper().readValue(file.toFile(), WebAuthFile.class);
            if (body == null || body.principals() == null) {
                return;
            }
            for (WebPrincipalFile principal : body.principals()) {
                if (principal == null || principal.id() == null || principal.id().isBlank()) {
                    continue;
                }
                PrincipalRole role = parseRole(principal.role());
                boolean serviceAccount = principal.serviceAccount() != null && principal.serviceAccount();
                Set<String> namespaces = new LinkedHashSet<>();
                if (principal.namespaces() != null) {
                    for (String ns : principal.namespaces()) {
                        if (ns != null && !ns.isBlank()) {
                            namespaces.add(ns.trim().toLowerCase(Locale.ROOT));
                        }
                    }
                }
                AuthPrincipal model = new AuthPrincipal(
                        principal.id().trim(),
                        role,
                        serviceAccount,
                        Set.copyOf(namespaces),
                        namespace
                );
                if (principal.tokens() == null) {
                    continue;
                }
                for (String token : principal.tokens()) {
                    if (token == null || token.isBlank()) {
                        continue;
                    }
                    target.put(token.trim(), model);
                }
            }
        }

        private static PrincipalRole parseRole(String raw) {
            if (raw == null || raw.isBlank()) {
                return PrincipalRole.READER;
            }
            String value = raw.trim().toLowerCase(Locale.ROOT);
            return switch (value) {
                case "read", "reader", "ro", "readonly" -> PrincipalRole.READER;
                case "write", "writer", "rw", "operator" -> PrincipalRole.WRITER;
                case "admin", "owner" -> PrincipalRole.ADMIN;
                default -> throw new IllegalArgumentException("Unsupported auth role: " + raw);
            };
        }
    }

    private record WebAuthFile(List<WebPrincipalFile> principals) {
    }

    private record WebPrincipalFile(
            String id,
            String role,
            Boolean serviceAccount,
            List<String> tokens,
            List<String> namespaces
    ) {
    }

    private static final class WriteRateLimiter {
        private final int limitPerMinute;
        private final ConcurrentMap<String, WindowCounter> counters;
        private final AtomicLong lastCleanupWindowStartMs;

        private WriteRateLimiter(int limitPerMinute) {
            this.limitPerMinute = Math.max(0, limitPerMinute);
            this.counters = new ConcurrentHashMap<>();
            this.lastCleanupWindowStartMs = new AtomicLong(Long.MIN_VALUE);
        }

        private boolean disabled() {
            return limitPerMinute <= 0;
        }

        private boolean tryAcquire(String key, long nowMs) {
            if (disabled()) {
                return true;
            }
            long windowStartMs = nowMs - (nowMs % 60_000L);
            WindowCounter current = counters.compute(key, (k, existing) -> {
                if (existing == null || existing.windowStartMs < windowStartMs) {
                    return new WindowCounter(windowStartMs, 1);
                }
                return new WindowCounter(windowStartMs, existing.count + 1);
            });
            cleanup(windowStartMs);
            return current.count <= limitPerMinute;
        }

        private void cleanup(long windowStartMs) {
            long prev = lastCleanupWindowStartMs.get();
            if (prev == windowStartMs) {
                return;
            }
            if (!lastCleanupWindowStartMs.compareAndSet(prev, windowStartMs)) {
                return;
            }
            long keepAfter = windowStartMs - 60_000L;
            counters.entrySet().removeIf(e -> e.getValue().windowStartMs < keepAfter);
        }
    }

    private static final class WindowCounter {
        private final long windowStartMs;
        private final int count;

        private WindowCounter(long windowStartMs, int count) {
            this.windowStartMs = windowStartMs;
            this.count = count;
        }
    }
}
