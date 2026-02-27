param(
    [string]$Root = "tmp/v2-benchmark-phase-a",
    [int]$Tasks = 2000,
    [int]$InputSize = 64,
    [int]$MaxSeconds = 180,
    [string]$Out = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $ProjectRoot

try {
    function Invoke-RelayMeshJson {
        param([string]$CommandArgs)
        $execArg = "-Dexec.args=$CommandArgs"
        $output = & mvn -q exec:java "$execArg" 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "relaymesh command failed: $CommandArgs`n$output"
        }
        $text = ($output -join "`n")
        $start = $text.IndexOf("{")
        $end = $text.LastIndexOf("}")
        if ($start -lt 0 -or $end -lt $start) {
            throw "relaymesh command returned no json payload: $CommandArgs`n$output"
        }
        $json = $text.Substring($start, ($end - $start + 1))
        return ($json | ConvertFrom-Json)
    }

    # Normalize inputs and prepare an isolated benchmark root.
    $taskCount = [Math]::Max(1, $Tasks)
    $payloadSize = [Math]::Max(1, $InputSize)
    $seconds = [Math]::Max(5, $MaxSeconds)
    $rootPath = Join-Path $ProjectRoot $Root
    if (Test-Path $rootPath) {
        Remove-Item -Recurse -Force $rootPath
    }
    New-Item -ItemType Directory -Force -Path $rootPath | Out-Null

    $reportPath = $Out
    if ([string]::IsNullOrWhiteSpace($reportPath)) {
        $reportPath = Join-Path $rootPath "reports/benchmark-phase-a.json"
    }

    & mvn -q exec:java "-Dexec.args=--root=$Root init" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "relaymesh init failed for root=$Root"
    }

    # Execute sustained benchmark and assert basic success invariants.
    $benchArgs = "--root=$Root benchmark-run --tasks $taskCount --agent echo --priority normal --input-size $payloadSize --worker-id bench-worker --node-id bench-node --max-seconds $seconds --poll-batch 64 --report-out $reportPath"
    $result = Invoke-RelayMeshJson $benchArgs

    if ([bool]$result.timedOut) {
        throw "benchmark timed out"
    }
    if ([int]$result.submittedTasks -ne $taskCount) {
        throw "submittedTasks mismatch, expected=$taskCount actual=$($result.submittedTasks)"
    }
    if ([int]$result.completedTasks -ne $taskCount) {
        throw "completedTasks mismatch, expected=$taskCount actual=$($result.completedTasks)"
    }
    if ([double]$result.throughputTasksPerSec -le 0) {
        throw "throughputTasksPerSec must be > 0"
    }

    [ordered]@{
        smoke = "v2_benchmark_phase_a"
        tasks = [int]$result.submittedTasks
        completed = [int]$result.completedTasks
        throughputTasksPerSec = [double]$result.throughputTasksPerSec
        latencyP50Ms = [long]$result.latencyP50Ms
        latencyP95Ms = [long]$result.latencyP95Ms
        latencyP99Ms = [long]$result.latencyP99Ms
        report = $reportPath
    } | ConvertTo-Json
} finally {
    Pop-Location
}

