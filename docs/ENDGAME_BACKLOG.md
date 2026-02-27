# RelayMesh Endgame Backlog

Updated: `2026-02-25`

This file tracks the remaining gaps to move RelayMesh from `v2-beta` to a true decentralized endgame platform.

## E1 Strong-Consistency Mesh State Layer
- [x] Target: move core ownership/task state from local eventual-only semantics to a versioned cluster state contract with fencing.
- [x] Phase A (started): add `cluster_state` table + `cluster_epoch` baseline in SQLite.
- [x] Phase A (started): expose runtime and CLI controls (`cluster-state`, `cluster-epoch-bump`).
- [x] Phase A (started): expose observability (`relaymesh_cluster_epoch` metric).
- [x] Phase B: require ownership-changing writes to carry and validate `cluster_epoch` (claim/success/failure/reclaim/cancel core paths).
- [x] Phase C: add deterministic conflict policy for cross-node concurrent ownership claims.

## E2 Cross-Node Task Replication + Scheduling Loop
- [x] Target: task lifecycle survives single-node loss without manual replay.
- [x] Phase A: define replication envelope (`task/step/message` delta schema + version) and export CLI (`replication-export`).
- [x] Phase B: implement pull/push sync protocol for task deltas (`replication-export` + `replication-import`).
- [x] Phase C: add reconciliation rules for duplicate or out-of-order deltas (`updated_at_ms` newer-wins; equal timestamp keeps local).

## E3 Partition/Split-Brain Deterministic Convergence
- [x] Target: split brain converges predictably with auditable final ownership.
- [x] Phase A: add explicit partition simulation test harness (`scripts/v2_partition_sim_smoke.ps1` + production test).
- [x] Phase B: define tie-break rules (`epoch`, `version`, deterministic node ordering) in replication import conflict handling.
- [x] Phase C: produce convergence report and SLO metrics (`convergence-report`, replication tie/import lag metrics).

## E4 Node Security Baseline
- [x] Target: zero-trust node transport and identity validation.
- [x] Phase A: signed node packets (integrity + source authenticity) via `gossipSharedSecret` HMAC signing/verification.
- [x] Phase B: mTLS for node RPC channels.
- [x] Phase C: certificate rotation + revocation workflow.

## E5 Production-Scale Validation
- [x] Target: prove reliability under load/failure/upgrade.
- [x] Phase A: sustained throughput and latency benchmarks.
- [x] Phase B: chaos matrix (node crash, packet loss, clock skew, disk pressure).
- [x] Phase C: upgrade compatibility and rollback verification.

## Current Execution Focus
Now executing in order: `E1 -> E2 -> E3 -> E4 -> E5`.
Current active phase: `Completed (E1-E5 all done)`.
