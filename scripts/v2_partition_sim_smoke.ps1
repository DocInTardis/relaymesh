param(
    [string]$NodeARoot = "tmp/v2-partition-node-a",
    [string]$NodeBRoot = "tmp/v2-partition-node-b"
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

    $nodeAPath = Join-Path $ProjectRoot $NodeARoot
    $nodeBPath = Join-Path $ProjectRoot $NodeBRoot
    if (Test-Path $nodeAPath) {
        Remove-Item -Recurse -Force $nodeAPath
    }
    if (Test-Path $nodeBPath) {
        Remove-Item -Recurse -Force $nodeBPath
    }

    $null = Invoke-RelayMesh -Root $NodeARoot -ArgsLine "init"
    $null = Invoke-RelayMesh -Root $NodeBRoot -ArgsLine "init"

    $submit = (Invoke-RelayMesh -Root $NodeARoot -ArgsLine "submit --agent echo --input partition-harness --priority normal" | ConvertFrom-Json)
    if ([string]::IsNullOrWhiteSpace([string]$submit.taskId)) {
        throw "submit did not return taskId"
    }
    $taskId = [string]$submit.taskId

    $bootEnvelope = Join-Path $nodeAPath "replication\partition-init.json"
    $null = (Invoke-RelayMesh -Root $NodeARoot -ArgsLine "replication-export --since-ms 0 --limit 1000 --source-node-id node-a --out $bootEnvelope" | ConvertFrom-Json)
    $bootImport = (Invoke-RelayMesh -Root $NodeBRoot -ArgsLine "replication-import --in $bootEnvelope --actor partition-smoke" | ConvertFrom-Json)
    if ([int]$bootImport.queueEnqueued -lt 1) {
        throw "expected queueEnqueued >=1 during partition bootstrap"
    }

    $runA = (Invoke-RelayMesh -Root $NodeARoot -ArgsLine "worker --once --maintenance=false --worker-id worker-a --node-id node-a" | ConvertFrom-Json)
    $runB = (Invoke-RelayMesh -Root $NodeBRoot -ArgsLine "worker --once --maintenance=false --worker-id worker-b --node-id node-b" | ConvertFrom-Json)
    if (-not [bool]$runA.processed -or -not [bool]$runB.processed) {
        throw "expected both nodes to process task during partition window"
    }

    $beforeA = (Invoke-RelayMesh -Root $NodeARoot -ArgsLine "task $taskId" | ConvertFrom-Json)
    $beforeB = (Invoke-RelayMesh -Root $NodeBRoot -ArgsLine "task $taskId" | ConvertFrom-Json)

    $healAEnvelope = Join-Path $nodeAPath "replication\partition-heal-a.json"
    $null = (Invoke-RelayMesh -Root $NodeARoot -ArgsLine "replication-export --since-ms 0 --limit 1000 --source-node-id node-a --out $healAEnvelope" | ConvertFrom-Json)
    $healAImport = (Invoke-RelayMesh -Root $NodeBRoot -ArgsLine "replication-import --in $healAEnvelope --actor partition-smoke" | ConvertFrom-Json)

    $healBEnvelope = Join-Path $nodeBPath "replication\partition-heal-b.json"
    $null = (Invoke-RelayMesh -Root $NodeBRoot -ArgsLine "replication-export --since-ms 0 --limit 1000 --source-node-id node-b --out $healBEnvelope" | ConvertFrom-Json)
    $healBImport = (Invoke-RelayMesh -Root $NodeARoot -ArgsLine "replication-import --in $healBEnvelope --actor partition-smoke" | ConvertFrom-Json)

    $afterA = (Invoke-RelayMesh -Root $NodeARoot -ArgsLine "task $taskId" | ConvertFrom-Json)
    $afterB = (Invoke-RelayMesh -Root $NodeBRoot -ArgsLine "task $taskId" | ConvertFrom-Json)
    if ($afterA.status -ne $afterB.status) {
        throw "post-heal status mismatch: nodeA=$($afterA.status), nodeB=$($afterB.status)"
    }

    [ordered]@{
        smoke = "v2_partition_sim"
        taskId = $taskId
        beforeHealNodeAStatus = [string]$beforeA.status
        beforeHealNodeBStatus = [string]$beforeB.status
        afterHealNodeAStatus = [string]$afterA.status
        afterHealNodeBStatus = [string]$afterB.status
        healAImportStaleSkipped = [int]$healAImport.staleSkipped
        healBImportStaleSkipped = [int]$healBImport.staleSkipped
    } | ConvertTo-Json
} finally {
    Pop-Location
}

