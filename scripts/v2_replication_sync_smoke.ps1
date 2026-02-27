param(
    [string]$SourceRoot = "tmp/v2-replication-sync-source",
    [string]$TargetRoot = "tmp/v2-replication-sync-target"
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

    $submit = (Invoke-RelayMesh -Root $SourceRoot -ArgsLine "submit --agent echo --input replication-sync --priority normal" | ConvertFrom-Json)
    if ([string]::IsNullOrWhiteSpace([string]$submit.taskId)) {
        throw "submit did not return taskId"
    }

    $envelopePath = Join-Path $sourcePath "replication\sync-envelope.json"
    $export = (Invoke-RelayMesh -Root $SourceRoot -ArgsLine "replication-export --since-ms 0 --limit 200 --source-node-id source-node --out $envelopePath" | ConvertFrom-Json)
    if ($export.schemaVersion -ne "relaymesh.replication.v1") {
        throw "unexpected replication export schemaVersion: $($export.schemaVersion)"
    }
    if (-not (Test-Path $envelopePath)) {
        throw "replication envelope file not found: $envelopePath"
    }

    $import = (Invoke-RelayMesh -Root $TargetRoot -ArgsLine "replication-import --in $envelopePath --actor smoke" | ConvertFrom-Json)
    if ($import.schemaVersion -ne "relaymesh.replication.v1") {
        throw "unexpected replication import schemaVersion: $($import.schemaVersion)"
    }
    if ([int]$import.taskApplied -lt 1 -or [int]$import.stepApplied -lt 1 -or [int]$import.messageApplied -lt 1) {
        throw "expected applied deltas >=1, got tasks=$($import.taskApplied), steps=$($import.stepApplied), messages=$($import.messageApplied)"
    }
    if ([int]$import.queueEnqueued -lt 1) {
        throw "expected queueEnqueued >=1 after import, got $($import.queueEnqueued)"
    }

    $worker = (Invoke-RelayMesh -Root $TargetRoot -ArgsLine "worker --once --maintenance=false --worker-id worker-target --node-id node-target" | ConvertFrom-Json)
    if (-not [bool]$worker.processed) {
        throw "expected imported queue to be consumable by target worker"
    }

    $duplicate = (Invoke-RelayMesh -Root $TargetRoot -ArgsLine "replication-import --in $envelopePath --actor smoke" | ConvertFrom-Json)
    if ([int]$duplicate.taskApplied -ne 0 -or [int]$duplicate.stepApplied -ne 0 -or [int]$duplicate.messageApplied -ne 0) {
        throw "duplicate import should not re-apply rows, got tasks=$($duplicate.taskApplied), steps=$($duplicate.stepApplied), messages=$($duplicate.messageApplied)"
    }

    $task = (Invoke-RelayMesh -Root $TargetRoot -ArgsLine "task $($submit.taskId)" | ConvertFrom-Json)
    if ([string]::IsNullOrWhiteSpace([string]$task.taskId)) {
        throw "target task query missing taskId"
    }
    if ($task.taskId -ne $submit.taskId) {
        throw "replicated task id mismatch: expected=$($submit.taskId), got=$($task.taskId)"
    }

    [ordered]@{
        smoke = "v2_replication_sync"
        taskId = [string]$submit.taskId
        envelope = [string]$envelopePath
        taskApplied = [int]$import.taskApplied
        stepApplied = [int]$import.stepApplied
        messageApplied = [int]$import.messageApplied
        queueEnqueued = [int]$import.queueEnqueued
        duplicateStaleSkipped = [int]$duplicate.staleSkipped
        workerProcessed = [bool]$worker.processed
    } | ConvertTo-Json
} finally {
    Pop-Location
}

