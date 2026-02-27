param(
    [string]$Root = "tmp/v2-replication-envelope-smoke"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $ProjectRoot

try {
    function Invoke-RelayMesh {
        param([string]$ArgsLine)
        $execArg = "-Dexec.args=--root=$Root $ArgsLine"
        $output = & mvn -q exec:java $execArg 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "relaymesh command failed: $ArgsLine`n$($output -join "`n")"
        }
        return ($output -join "`n")
    }

    $rootPath = Join-Path $ProjectRoot $Root
    if (Test-Path $rootPath) {
        Remove-Item -Recurse -Force $rootPath
    }

    $null = Invoke-RelayMesh "init"
    $submit = (Invoke-RelayMesh "submit --agent echo --input replication-smoke --priority high" | ConvertFrom-Json)
    if ([string]::IsNullOrWhiteSpace([string]$submit.taskId)) {
        throw "submit did not return taskId"
    }

    $outFile = Join-Path $rootPath "exports\replication-envelope.json"
    $export = (Invoke-RelayMesh "replication-export --since-ms 0 --limit 200 --source-node-id smoke-node --out $outFile" | ConvertFrom-Json)
    if ($export.schemaVersion -ne "relaymesh.replication.v1") {
        throw "unexpected schemaVersion: $($export.schemaVersion)"
    }
    if ([int]$export.taskDeltaCount -lt 1 -or [int]$export.stepDeltaCount -lt 1 -or [int]$export.messageDeltaCount -lt 1) {
        throw "expected non-empty deltas, got tasks=$($export.taskDeltaCount), steps=$($export.stepDeltaCount), messages=$($export.messageDeltaCount)"
    }
    if (-not (Test-Path $outFile)) {
        throw "replication envelope file not found: $outFile"
    }

    $envelope = Get-Content -Raw $outFile | ConvertFrom-Json
    if ($envelope.schema_version -ne "relaymesh.replication.v1") {
        throw "unexpected schema_version in envelope: $($envelope.schema_version)"
    }
    if ([int64]$envelope.cursor_max_ms -lt 0) {
        throw "cursor_max_ms should be >= 0"
    }
    if ($envelope.task_deltas.Count -lt 1 -or $envelope.step_deltas.Count -lt 1 -or $envelope.message_deltas.Count -lt 1) {
        throw "envelope delta arrays are unexpectedly empty"
    }

    [ordered]@{
        smoke = "v2_replication_envelope"
        taskId = [string]$submit.taskId
        taskDeltas = [int]$export.taskDeltaCount
        stepDeltas = [int]$export.stepDeltaCount
        messageDeltas = [int]$export.messageDeltaCount
        cursorMaxMs = [int64]$export.cursorMaxMs
        output = [string]$outFile
    } | ConvertTo-Json
} finally {
    Pop-Location
}

