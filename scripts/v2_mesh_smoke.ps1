param(
    [string]$Root = "tmp/v2-mesh-smoke"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $ProjectRoot

try {
    function Invoke-RelayMesh {
        param([string]$ArgsLine)
        $execArg = "-Dexec.args=--root $Root $ArgsLine"
        $out = & mvn -q exec:java $execArg 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "relaymesh command failed: $ArgsLine`n$($out -join "`n")"
        }
        return ($out -join "`n")
    }

    if (Test-Path $Root) {
        Remove-Item -Recurse -Force $Root
    }

    & mvn -q -DskipTests package
    if ($LASTEXITCODE -ne 0) {
        throw "mvn package failed"
    }

    $null = Invoke-RelayMesh "init"
    $submitJson = Invoke-RelayMesh "submit --agent echo --input v2-smoke-owner --priority normal"
    $submit = $submitJson | ConvertFrom-Json
    $taskId = [string]$submit.taskId
    if ([string]::IsNullOrWhiteSpace($taskId)) {
        throw "submit missing taskId"
    }

    $workflowJson = Invoke-RelayMesh "workflow $taskId"
    $workflow = $workflowJson | ConvertFrom-Json
    $stepId = [string]$workflow.steps[0].stepId
    if ([string]::IsNullOrWhiteSpace($stepId)) {
        throw "workflow missing stepId"
    }

    @"
import sqlite3, time
db = r'$ProjectRoot\$Root\relaymesh.db'
task_id = '$taskId'
step_id = '$stepId'
now = int(time.time() * 1000)
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute("update tasks set status='RUNNING', updated_at_ms=? where task_id=?", (now, task_id))
cur.execute("update steps set status='RUNNING', attempt=1, lease_owner='node-dead-v2', lease_token='lease-v2', updated_at_ms=? where step_id=?", (now, step_id))
cur.execute("insert into mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) values(?,?,?,?,?) "
            "on conflict(node_id) do update set status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms",
            ('node-dead-v2', 'DEAD', now - 120000, now, now))
conn.commit()
print('ok')
"@ | python -

    $recoverJson = Invoke-RelayMesh "mesh-recover --limit 128"
    $recover = $recoverJson | ConvertFrom-Json
    if ([int]$recover.reclaimed -lt 1) {
        throw "mesh-recover reclaimed < 1"
    }

    $workflow2Json = Invoke-RelayMesh "workflow $taskId"
    $workflow2 = $workflow2Json | ConvertFrom-Json
    $stepStatus = [string]$workflow2.steps[0].status
    if ($stepStatus -ne "RETRYING") {
        throw "expected RETRYING after mesh-recover, got: $stepStatus"
    }

    $auditRows = Invoke-RelayMesh "audit-query --action ownership.recover --limit 10"
    if ($auditRows -notmatch "ownership.recover") {
        throw "ownership.recover audit row not found"
    }

    [ordered]@{
        taskId = $taskId
        stepId = $stepId
        deadNodeCount = [int]$recover.deadNodeCount
        recovered = [int]$recover.reclaimed
        retryScheduled = [int]$recover.retryScheduled
        deadLettered = [int]$recover.deadLettered
        finalStepStatus = $stepStatus
    } | ConvertTo-Json
} finally {
    Pop-Location
}

