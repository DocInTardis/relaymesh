package io.relaymesh.observability;

import io.relaymesh.runtime.RelayMeshRuntime;

import java.util.Map;

public final class PrometheusFormatter {
    private PrometheusFormatter() {
    }

    public static String format(RelayMeshRuntime.StatsOutcome stats) {
        return format(stats, null);
    }

    public static String format(RelayMeshRuntime.StatsOutcome stats, String namespace) {
        StringBuilder sb = new StringBuilder();
        appendMapGauge(sb, "relaymesh_tasks_total", "Tasks grouped by status", "status", stats.taskStatus());
        appendMapGauge(sb, "relaymesh_steps_total", "Steps grouped by status", "status", stats.stepStatus());
        appendMapGauge(sb, "relaymesh_messages_total", "Messages grouped by state", "state", stats.messageState());

        appendGauge(sb, "relaymesh_inbox_depth", "Inbox queue depth", "priority", "high", stats.inboxHigh());
        appendGauge(sb, "relaymesh_inbox_depth", "Inbox queue depth", "priority", "normal", stats.inboxNormal());
        appendGauge(sb, "relaymesh_inbox_depth", "Inbox queue depth", "priority", "low", stats.inboxLow());
        appendGauge(sb, "relaymesh_processing_meta_files", "Processing meta files", null, null, stats.processingMetaFiles());
        appendGauge(sb, "relaymesh_dead_meta_files", "Dead meta files", null, null, stats.deadMetaFiles());
        appendGauge(sb, "relaymesh_retry_meta_files", "Retry meta files", null, null, stats.retryMetaFiles());
        appendGauge(sb, "relaymesh_queue_lag_ms", "Queue lag in milliseconds", null, null, stats.queueLagMs());
        appendGauge(sb, "relaymesh_step_latency_ms", "Step latency percentiles in milliseconds", "quantile", "0.95", stats.stepLatencyP95Ms());
        appendGauge(sb, "relaymesh_step_latency_ms", "Step latency percentiles in milliseconds", "quantile", "0.99", stats.stepLatencyP99Ms());
        appendGauge(sb, "relaymesh_dead_letter_growth_1h", "Tasks moved to dead letter in the last 1h", null, null, stats.deadLetterGrowth1h());
        appendGauge(sb, "relaymesh_lease_conflict_total", "Total lease conflicts observed", null, null, stats.leaseConflictTotal());
        appendGauge(sb, "relaymesh_lease_conflict_1h", "Lease conflicts observed in the last 1h", null, null, stats.leaseConflict1h());
        appendMapGauge(sb, "relaymesh_lease_conflict_events_total", "Lease conflicts grouped by event type", "event_type", stats.leaseConflictByType());
        appendGauge(sb, "relaymesh_anti_entropy_merge_total", "Merged membership entries from anti-entropy gossip", null, null, stats.antiEntropyMergeTotal());
        appendGauge(sb, "relaymesh_web_write_get_compat_total", "Deprecated GET write compatibility hits", null, null, stats.webWriteGetCompatTotal());
        appendGauge(sb, "relaymesh_web_write_rate_limited_total", "Rejected web write requests due to rate limit", null, null, stats.webWriteRateLimitedTotal());
        appendGauge(sb, "relaymesh_dead_owner_reclaimed_total", "Running steps reclaimed from DEAD lease owners", null, null, stats.deadOwnerReclaimedTotal());
        appendGauge(sb, "relaymesh_membership_recovery_suppressed_total", "Membership recoveries suppressed by dwell windows", null, null, stats.membershipRecoverySuppressedTotal());
        appendGauge(sb, "relaymesh_mesh_nodes_pruned_total", "Pruned DEAD membership rows older than retention window", null, null, stats.meshNodesPrunedTotal());
        appendGauge(sb, "relaymesh_worker_paused_total", "Worker polls paused due to local SUSPECT/DEAD status", null, null, stats.workerPausedTotal());
        appendGauge(sb, "relaymesh_replication_import_total", "Replication envelopes imported", null, null, stats.replicationImportTotal());
        appendGauge(sb, "relaymesh_replication_tie_resolved_total", "Equal-timestamp RUNNING conflicts resolved in favor of incoming node", null, null, stats.replicationTieResolvedTotal());
        appendGauge(sb, "relaymesh_replication_tie_kept_local_total", "Equal-timestamp RUNNING conflicts kept on local node", null, null, stats.replicationTieKeptLocalTotal());
        appendGauge(sb, "relaymesh_replication_last_import_lag_ms", "Last observed replication import lag in milliseconds", null, null, stats.replicationLastImportLagMs());
        appendGauge(sb, "relaymesh_replication_sync_total", "Replication controller sync ticks completed", null, null, stats.replicationSyncTotal());
        appendGauge(sb, "relaymesh_slo_alert_fire_total", "Total SLO alerts fired", null, null, stats.sloAlertFireTotal());
        appendGauge(sb, "relaymesh_submit_rejected_total", "Rejected submits due to admission/backpressure controls", null, null, stats.submitRejectedTotal());
        appendGauge(sb, "relaymesh_gossip_signing_enabled", "Whether gossip packet signing is enabled (1=yes,0=no)", null, null, stats.gossipSigningEnabled());
        appendGauge(sb, "relaymesh_low_starvation_count", "Forced LOW-priority picks triggered by anti-starvation guard", null, null, stats.lowStarvationCount());
        appendGauge(sb, "relaymesh_cluster_epoch", "Cluster state epoch used for mesh-wide ownership fencing", null, null, stats.clusterEpoch());
        appendGauge(sb, "relaymesh_mesh_nodes_total", "Mesh nodes grouped by status", "status", "alive", stats.meshAliveNodes());
        appendGauge(sb, "relaymesh_mesh_nodes_total", "Mesh nodes grouped by status", "status", "suspect", stats.meshSuspectNodes());
        appendGauge(sb, "relaymesh_mesh_nodes_total", "Mesh nodes grouped by status", "status", "dead", stats.meshDeadNodes());
        appendGauge(sb, "relaymesh_mesh_nodes_total_all", "Total mesh node rows", null, null, stats.meshTotalNodes());
        appendGauge(sb, "relaymesh_running_steps_by_owner_status", "RUNNING steps grouped by lease owner node status", "owner_status", "alive", stats.meshRunningByAliveOwners());
        appendGauge(sb, "relaymesh_running_steps_by_owner_status", "RUNNING steps grouped by lease owner node status", "owner_status", "suspect", stats.meshRunningBySuspectOwners());
        appendGauge(sb, "relaymesh_running_steps_by_owner_status", "RUNNING steps grouped by lease owner node status", "owner_status", "dead", stats.meshRunningByDeadOwners());
        appendGauge(sb, "relaymesh_running_steps_by_owner_status", "RUNNING steps grouped by lease owner node status", "owner_status", "unknown", stats.meshRunningByUnknownOwners());
        appendGauge(sb, "relaymesh_oldest_dead_node_age_ms", "Age in milliseconds of the oldest DEAD node row", null, null, stats.meshOldestDeadNodeAgeMs());
        appendGauge(sb, "relaymesh_gossip_convergence_seconds", "Heartbeat spread across mesh members in seconds", null, null, stats.gossipConvergenceSeconds());
        appendGauge(sb, "relaymesh_disk_free_bytes", "Usable bytes on runtime root disk", null, null, stats.diskFreeBytes());
        appendGauge(sb, "relaymesh_min_free_disk_bytes", "Configured minimum free disk bytes before pressure mode", null, null, stats.minFreeDiskBytes());
        appendGauge(sb, "relaymesh_disk_pressure", "Disk pressure flag (1=pressure,0=ok)", null, null, stats.diskPressure());
        String base = sb.toString();
        String normalizedNamespace = namespace == null ? "" : namespace.trim();
        if (normalizedNamespace.isBlank()) {
            return base;
        }
        String escapedNs = escapeLabel(normalizedNamespace);
        StringBuilder withNamespace = new StringBuilder(base.length() * 2);
        withNamespace.append(base);
        for (String line : base.split("\\r?\\n")) {
            if (line == null || line.isBlank() || line.startsWith("#")) {
                continue;
            }
            int sep = line.lastIndexOf(' ');
            if (sep <= 0) {
                continue;
            }
            String sample = line.substring(0, sep);
            String value = line.substring(sep + 1);
            String namespacedSample;
            int brace = sample.indexOf('{');
            if (brace >= 0 && sample.endsWith("}")) {
                namespacedSample = sample.substring(0, brace + 1)
                        + "namespace=\"" + escapedNs + "\","
                        + sample.substring(brace + 1);
            } else {
                namespacedSample = sample + "{namespace=\"" + escapedNs + "\"}";
            }
            withNamespace.append(namespacedSample).append(' ').append(value).append('\n');
        }
        withNamespace.append("# HELP relaymesh_namespace_info Runtime namespace marker\n");
        withNamespace.append("# TYPE relaymesh_namespace_info gauge\n");
        withNamespace.append("relaymesh_namespace_info{namespace=\"").append(escapedNs).append("\"} 1\n");
        return withNamespace.toString();
    }

    private static void appendMapGauge(StringBuilder sb, String metric, String help, String label, Map<String, Integer> values) {
        sb.append("# HELP ").append(metric).append(" ").append(help).append('\n');
        sb.append("# TYPE ").append(metric).append(" gauge").append('\n');
        for (Map.Entry<String, Integer> e : values.entrySet()) {
            sb.append(metric).append('{')
                    .append(label).append("=\"").append(escapeLabel(e.getKey())).append("\"}")
                    .append(' ').append(e.getValue()).append('\n');
        }
    }

    private static void appendGauge(StringBuilder sb, String metric, String help, String label, String labelValue, long value) {
        if (!sb.toString().contains("# HELP " + metric + " ")) {
            sb.append("# HELP ").append(metric).append(" ").append(help).append('\n');
            sb.append("# TYPE ").append(metric).append(" gauge").append('\n');
        }
        sb.append(metric);
        if (label != null && labelValue != null) {
            sb.append('{').append(label).append("=\"").append(escapeLabel(labelValue)).append("\"}");
        }
        sb.append(' ').append(value).append('\n');
    }

    private static String escapeLabel(String v) {
        return v.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
