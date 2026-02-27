# Reading Guide

This guide gives a practical reading order so you can build a mental model quickly.

## 1) Build and Run Baseline

1. Read `pom.xml` to understand runtime dependencies.
2. Read `src/main/java/io/relaymesh/Main.java` to see process entry.
3. Run a quick baseline:
   - `mvn test`
   - `mvn -q exec:java -Dexec.args="init --root tmp/guide-root"`

## 2) Understand Command Surface (CLI First)

1. Read `src/main/java/io/relaymesh/cli/RelayMeshCommand.java`.
2. Focus on command groups in this order:
   - bootstrap and workload: `init`, `submit`, `submit-workflow`, `worker`, `task`, `tasks`
   - operations: `stats`, `metrics`, `maintenance`, `members`, `mesh-summary`
   - replication and recovery: `replication-*`, `mesh-recover`, `mesh-prune`

Reason: CLI methods are a thin map of capabilities exposed by `RelayMeshRuntime`.

## 3) Follow Runtime Flow

1. Read `src/main/java/io/relaymesh/runtime/RelayMeshRuntime.java` as the core orchestrator.
2. Track one message through these steps:
   - submission (`submit` / `submitWorkflow`)
   - dispatch (`workerTick` and ready-step dispatch)
   - completion / retry / dead-letter handling
3. Then inspect supporting subsystems:
   - persistence: `src/main/java/io/relaymesh/storage/TaskStore.java`
   - file queue: `src/main/java/io/relaymesh/bus/FileBus.java`
   - agent execution: `src/main/java/io/relaymesh/agent/*`

## 4) Security and Observability

1. Security:
   - `src/main/java/io/relaymesh/security/PayloadCrypto.java`
   - `src/main/java/io/relaymesh/security/NodeRpcTls.java`
   - `src/main/java/io/relaymesh/security/SensitiveDataMasker.java`
2. Observability:
   - `src/main/java/io/relaymesh/observability/AuditLogger.java`
   - `src/main/java/io/relaymesh/observability/PrometheusFormatter.java`
   - `src/main/java/io/relaymesh/observability/OtelTraceExporter.java`

## 5) Validate Assumptions with Tests

Read tests as executable specs in this order:

1. `src/test/java/io/relaymesh/runtime/RelayMeshRuntimeProductionTest.java`
2. `src/test/java/io/relaymesh/runtime/RelayMeshRuntimeSecurityTest.java`
3. `src/test/java/io/relaymesh/runtime/RelayMeshRuntimeGovernanceTest.java`
4. `src/test/java/io/relaymesh/storage/TaskStoreLeaseEpochTest.java`
5. `src/test/java/io/relaymesh/bus/FileBusFairnessTest.java`
6. `src/test/java/io/relaymesh/security/NodeRpcTlsTest.java`

## 6) Use Scripts for End-to-End Scenarios

- See `scripts/` for smoke, chaos, and benchmark scripts.
- Start with `scripts/v2_mesh_smoke.ps1` and then `scripts/p8_smoke.ps1`.

## 7) Suggested Deep Dives

1. Lease epoch fencing and conflict resolution.
2. Replication import/export and payload materialization.
3. Gossip membership anti-entropy and stale-node recovery rules.
