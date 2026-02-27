# E5 Phase C: Upgrade Compatibility and Rollback Verification

Phase C validates that RelayMesh state can be snapshotted before an upgrade and safely restored (rollback) without semantic drift.

## Snapshot Commands

Export full runtime snapshot:

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-data snapshot-export --out demo-snapshots/before-upgrade"
```

Import snapshot (destructive overwrite of target root):

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-data snapshot-import --in demo-snapshots/before-upgrade"
```

Snapshot layout:

- `<snapshot>/manifest.json`
- `<snapshot>/relaymesh-root/**` (runtime root copy)

## Validation Script

Run end-to-end upgrade/rollback smoke:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_upgrade_rollback_phase_c.ps1 -Root tmp/v2-upgrade-rollback-phase-c
```

What it validates:

1. Baseline workload is executed and captured.
2. Snapshot is exported.
3. Upgrade simulation mutates state (`cluster_epoch` bump + additional workload).
4. Snapshot is imported (rollback).
5. Post-rollback state matches baseline invariants:
   - `SUCCESS` task count restored.
   - `clusterEpoch` restored.
   - health check passes.

## CI Coverage

CI includes `V2 Upgrade Rollback Smoke` step in `.github/workflows/ci.yml`.

