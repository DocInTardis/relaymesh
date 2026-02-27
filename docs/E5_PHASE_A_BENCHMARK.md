# E5 Phase A: Sustained Throughput and Latency Benchmark

This document defines the baseline benchmark workflow for RelayMesh endgame `E5 Phase A`.

## Goals

- Measure sustained task throughput (`tasks/sec`) under a controlled local workload.
- Measure end-to-end task latency (`p50`, `p95`, `p99`) from `createdAtMs -> updatedAtMs`.
- Produce a reproducible JSON report for regression tracking.

## Benchmark Command

Use the built-in benchmark runner:

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-data benchmark-run --tasks 2000 --agent echo --priority normal --input-size 64 --max-seconds 180 --poll-batch 64 --report-out demo-data/reports/benchmark-phase-a.json"
```

### Output fields

- `submittedTasks`
- `completedTasks`
- `pendingTasks`
- `timedOut`
- `throughputTasksPerSec`
- `latencyP50Ms`
- `latencyP95Ms`
- `latencyP99Ms`
- `statusCounts`
- `stats` (full runtime stats snapshot)

## Smoke Script

`scripts/v2_benchmark_phase_a.ps1` provides automated validation:

```powershell
powershell -ExecutionPolicy Bypass -File scripts/v2_benchmark_phase_a.ps1 -Root tmp/v2-benchmark-phase-a -Tasks 2000 -InputSize 64 -MaxSeconds 180
```

Script checks:

- Benchmark does not timeout.
- `submittedTasks == completedTasks`.
- `throughputTasksPerSec > 0`.
- Report file is generated.

## CI profile

CI uses a shorter run to keep pipeline time bounded:

- `Tasks=400`
- `InputSize=48`
- `MaxSeconds=120`

Configured in `.github/workflows/ci.yml` (`V2 Benchmark Phase A Smoke` step).

