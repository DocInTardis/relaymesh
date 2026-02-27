param(
    [string]$SourceRoot = "tmp/v2-convergence-source",
    [string]$TargetRoot = "tmp/v2-convergence-target"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $ProjectRoot

try {
    function Invoke-RelayMesh {
        param(
            [string]$Root,
            [string]$ArgsLine
        )
        $execArg = "-Dexec.args=--root=$Root $ArgsLine"
        $output = & mvn -q exec:java $execArg 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "relaymesh command failed: --root=$Root $ArgsLine`n$($output -join "`n")"
        }
        return ($output -join "`n")
    }

    $sourcePath = Join-Path $ProjectRoot $SourceRoot
    $targetPath = Join-Path $ProjectRoot $TargetRoot
    if (Test-Path $sourcePath) {
        Remove-Item -Recurse -Force $sourcePath
    }
    if (Test-Path $targetPath) {
        Remove-Item -Recurse -Force $targetPath
    }

    $null = Invoke-RelayMesh -Root $SourceRoot -ArgsLine "init"
    $null = Invoke-RelayMesh -Root $TargetRoot -ArgsLine "init"

    $submit = (Invoke-RelayMesh -Root $SourceRoot -ArgsLine "submit --agent echo --input convergence-report --priority normal" | ConvertFrom-Json)
    if ([string]::IsNullOrWhiteSpace([string]$submit.taskId)) {
        throw "submit did not return taskId"
    }

    $envelopePath = Join-Path $sourcePath "replication\convergence-envelope.json"
    $null = (Invoke-RelayMesh -Root $SourceRoot -ArgsLine "replication-export --since-ms 0 --limit 200 --source-node-id node-a --out $envelopePath" | ConvertFrom-Json)
    $import = (Invoke-RelayMesh -Root $TargetRoot -ArgsLine "replication-import --in $envelopePath --actor convergence-smoke" | ConvertFrom-Json)
    if ([int]$import.taskApplied -lt 1) {
        throw "expected replication import to apply task deltas"
    }

    $reportPath = Join-Path $targetPath "reports\convergence-report.json"
    $report = (Invoke-RelayMesh -Root $TargetRoot -ArgsLine "convergence-report --since-hours 24 --limit 200 --out $reportPath" | ConvertFrom-Json)
    if ([int]$report.imports -lt 1) {
        throw "expected convergence report imports >= 1"
    }
    if ([int]$report.appliedRows -lt 1) {
        throw "expected convergence report appliedRows >= 1"
    }
    if (-not (Test-Path $reportPath)) {
        throw "convergence report output file not found: $reportPath"
    }

    [ordered]@{
        smoke = "v2_convergence_report"
        imports = [int]$report.imports
        appliedRows = [int]$report.appliedRows
        tieResolved = [int]$report.tieResolved
        tieKeptLocal = [int]$report.tieKeptLocal
        lagP95Ms = [int64]$report.lagP95Ms
        output = [string]$reportPath
    } | ConvertTo-Json
} finally {
    Pop-Location
}

