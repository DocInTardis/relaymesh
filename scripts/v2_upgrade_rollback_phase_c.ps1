param(
    [string]$Root = "tmp/v2-upgrade-rollback-phase-c"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $ProjectRoot

try {
    function Invoke-RelayMeshJson {
        param(
            [string]$CommandArgs,
            [switch]$AllowNonZero
        )
        $execArg = "-Dexec.args=$CommandArgs"
        $output = & mvn -q exec:java "$execArg" 2>&1
        if ($LASTEXITCODE -ne 0 -and -not $AllowNonZero) {
            throw "relaymesh command failed: $CommandArgs`n$($output -join "`n")"
        }
        $text = ($output -join "`n")
        $start = $text.IndexOf("{")
        $end = $text.LastIndexOf("}")
        if ($start -lt 0 -or $end -lt $start) {
            throw "relaymesh command returned no json payload: $CommandArgs`n$text"
        }
        $json = $text.Substring($start, ($end - $start + 1))
        return ($json | ConvertFrom-Json)
    }

    # Prepare clean baseline root and snapshot output area.
    $rootPath = Join-Path $ProjectRoot $Root
    $snapshotDir = Join-Path $ProjectRoot ($Root + "-snapshot")
    if (Test-Path $rootPath) {
        Remove-Item -Recurse -Force $rootPath
    }
    if (Test-Path $snapshotDir) {
        Remove-Item -Recurse -Force $snapshotDir
    }

    & mvn -q exec:java "-Dexec.args=--root=$Root init" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "relaymesh init failed"
    }

    # Baseline workload + state capture before "upgrade" mutation.
    $baselineBench = Invoke-RelayMeshJson "--root=$Root benchmark-run --tasks 120 --agent echo --priority normal --input-size 40 --max-seconds 120 --poll-batch 64 --report-out $Root/reports/phase-c-baseline.json"
    if ([bool]$baselineBench.timedOut) {
        throw "baseline benchmark timed out"
    }
    $baselineStats = Invoke-RelayMeshJson "--root=$Root stats"
    $baselineSuccess = 0
    if ($baselineStats.taskStatus.SUCCESS) {
        $baselineSuccess = [int]$baselineStats.taskStatus.SUCCESS
    }
    $baselineEpoch = [long]$baselineStats.clusterEpoch

    $snapshotExport = Invoke-RelayMeshJson "--root=$Root snapshot-export --out $snapshotDir"
    if ([int]$snapshotExport.fileCount -le 0) {
        throw "snapshot-export fileCount must be > 0"
    }

    # Simulate upgrade-side state changes.
    $null = Invoke-RelayMeshJson "--root=$Root cluster-epoch-bump --reason upgrade-sim --actor phase-c-smoke"
    $upgradeBench = Invoke-RelayMeshJson "--root=$Root benchmark-run --tasks 60 --agent echo --priority normal --input-size 48 --max-seconds 120 --poll-batch 64 --report-out $Root/reports/phase-c-upgrade.json"
    if ([bool]$upgradeBench.timedOut) {
        throw "upgrade benchmark timed out"
    }
    $upgradedStats = Invoke-RelayMeshJson "--root=$Root stats"
    $upgradedSuccess = 0
    if ($upgradedStats.taskStatus.SUCCESS) {
        $upgradedSuccess = [int]$upgradedStats.taskStatus.SUCCESS
    }
    if ($upgradedSuccess -le $baselineSuccess) {
        throw "upgrade stage did not increase SUCCESS task count"
    }

    # Roll back by importing snapshot and verify invariants were restored.
    $snapshotImport = Invoke-RelayMeshJson "--root=$Root snapshot-import --in $snapshotDir"
    if ([int]$snapshotImport.restoredFiles -le 0) {
        throw "snapshot-import restoredFiles must be > 0"
    }

    $restoredStats = Invoke-RelayMeshJson "--root=$Root stats"
    $restoredSuccess = 0
    if ($restoredStats.taskStatus.SUCCESS) {
        $restoredSuccess = [int]$restoredStats.taskStatus.SUCCESS
    }
    $restoredEpoch = [long]$restoredStats.clusterEpoch
    if ($restoredSuccess -ne $baselineSuccess) {
        throw "rollback validation failed: restored SUCCESS count mismatch"
    }
    if ($restoredEpoch -ne $baselineEpoch) {
        throw "rollback validation failed: restored clusterEpoch mismatch"
    }

    $health = Invoke-RelayMeshJson "--root=$Root health"
    if (-not [bool]$health.ok) {
        throw "health check failed after rollback"
    }

    [ordered]@{
        smoke = "v2_upgrade_rollback_phase_c"
        baselineSuccess = $baselineSuccess
        upgradedSuccess = $upgradedSuccess
        restoredSuccess = $restoredSuccess
        baselineEpoch = $baselineEpoch
        restoredEpoch = $restoredEpoch
        snapshotFileCount = [int]$snapshotExport.fileCount
        restoredFileCount = [int]$snapshotImport.restoredFiles
        rollbackValidated = $true
    } | ConvertTo-Json
} finally {
    Pop-Location
}

