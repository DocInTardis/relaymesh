package io.relaymesh.cli;

import com.fasterxml.jackson.databind.JsonNode;
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
        if (!auth.enabled()) {
            AuthPrincipal bypass = AuthPrincipal.bypass(auth.namespace());
            applyPrincipal(exchange, bypass);
            return true;
        }
        String token = extractToken(exchange, query);
        if (token == null || token.isBlank()) {
            writeJson(exchange, Map.of("error", "missing_token"), 401);
            return false;
        }
        AuthPrincipal principal = auth.resolve(token);
        if (principal == null) {
            writeJson(exchange, Map.of("error", "forbidden_token"), 403);
            return false;
        }
        if (!auth.namespaceAllowed(principal)) {
            writeJson(exchange, Map.of("error", "forbidden_namespace", "namespace", auth.namespace()), 403);
            return false;
        }
        if (writeRequired) {
            if (!auth.canWrite(principal)) {
                writeJson(exchange, Map.of("error", "forbidden_write"), 403);
                return false;
            }
            applyPrincipal(exchange, principal);
            return true;
        }
        if (!auth.canRead(principal)) {
            writeJson(exchange, Map.of("error", "forbidden_read"), 403);
            return false;
        }
        applyPrincipal(exchange, principal);
        return true;
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
                      <p>tasks, workflow graph, dead letters, metrics, membership</p>
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
