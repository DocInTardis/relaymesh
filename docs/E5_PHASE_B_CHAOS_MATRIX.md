# E5 Phase B: Chaos Matrix

RelayMesh Phase B validates failure handling across four production-relevant chaos classes.

## Matrix

1. `node_crash_recovery`
Description: owner node dies while a step is in `RUNNING`; lease recovery should reclaim and reschedule safely.
Script: `scripts/v2_mesh_smoke.ps1`

2. `packet_loss_partition_heal`
Description: nodes diverge under partition / packet loss and later heal; replicated state should converge deterministically.
Script: `scripts/v2_partition_sim_smoke.ps1`

3. `clock_skew_recovery_guard`
Description: aggressive heartbeat timing / recovery jitter should not produce unstable flapping without suppression.
Script: `scripts/v2_jitter_smoke.ps1`

4. `disk_pressure_guard`
Description: synthetic low-disk policy should force degraded health, emit pressure metrics, then recover after threshold removal.
Script: `scripts/v2_disk_pressure_smoke.ps1`

## One-shot Matrix Runner

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_chaos_matrix_phase_b.ps1 -RootPrefix tmp/v2-chaos-matrix
```

Output includes per-scenario payloads and final `passed=true/false`.

## Disk Pressure Controls

Runtime settings support:

```json
{
  "minFreeDiskBytes": 0
}
```

Semantics:

- `0`: disabled (default).
- `> 0`: if `usableSpace < minFreeDiskBytes`, health becomes degraded (`ok=false`, `diskPressure=true`).

Metrics:

- `relaymesh_disk_free_bytes`
- `relaymesh_min_free_disk_bytes`
- `relaymesh_disk_pressure`

## CI Coverage

CI now includes:

- `V2 Disk Pressure Smoke` (`scripts/v2_disk_pressure_smoke.ps1`)

Other matrix components are already covered by existing CI smoke steps:

- mesh crash recovery, jitter, and partition simulation.

