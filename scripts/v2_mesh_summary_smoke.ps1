param(
    [string]$Root = "tmp/v2-mesh-summary-smoke"
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
    $t1 = (Invoke-RelayMesh "submit --agent echo --input sum-a --priority normal" | ConvertFrom-Json).taskId
    $t2 = (Invoke-RelayMesh "submit --agent echo --input sum-s --priority normal" | ConvertFrom-Json).taskId
    $t3 = (Invoke-RelayMesh "submit --agent echo --input sum-d --priority normal" | ConvertFrom-Json).taskId
    $t4 = (Invoke-RelayMesh "submit --agent echo --input sum-u --priority normal" | ConvertFrom-Json).taskId

    $s1 = ((Invoke-RelayMesh "workflow $t1" | ConvertFrom-Json).steps[0].stepId)
    $s2 = ((Invoke-RelayMesh "workflow $t2" | ConvertFrom-Json).steps[0].stepId)
    $s3 = ((Invoke-RelayMesh "workflow $t3" | ConvertFrom-Json).steps[0].stepId)
    $s4 = ((Invoke-RelayMesh "workflow $t4" | ConvertFrom-Json).steps[0].stepId)

    @"
import sqlite3, time
db = r'$ProjectRoot\$Root\relaymesh.db'
now = int(time.time() * 1000)
tasks = ['$t1', '$t2', '$t3', '$t4']
steps = [('$s1','node-a','lease-a'), ('$s2','node-s','lease-s'), ('$s3','node-d','lease-d'), ('$s4','node-u','lease-u')]
conn = sqlite3.connect(db)
cur = conn.cursor()
for task_id in tasks:
    cur.execute("update tasks set status='RUNNING', updated_at_ms=? where task_id=?", (now, task_id))
for step_id, owner, token in steps:
    cur.execute("update steps set status='RUNNING', lease_owner=?, lease_token=?, updated_at_ms=? where step_id=?", (owner, token, now, step_id))
cur.execute(
    "insert into mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) values(?,?,?,?,?) "
    "on conflict(node_id) do update set status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms",
    ('node-a', 'ALIVE', now, now, now)
)
cur.execute(
    "insert into mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) values(?,?,?,?,?) "
    "on conflict(node_id) do update set status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms",
    ('node-s', 'SUSPECT', now-2000, now-2000, now-2000)
)
cur.execute(
    "insert into mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) values(?,?,?,?,?) "
    "on conflict(node_id) do update set status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms",
    ('node-d', 'DEAD', now-60000, now-60000, now-60000)
)
conn.commit()
print('seeded')
"@ | python -

    $summary = Invoke-RelayMesh "mesh-summary" | ConvertFrom-Json
    if ([int]$summary.totalNodes -ne 3) { throw "expected totalNodes=3" }
    if ([int]$summary.aliveNodes -ne 1) { throw "expected aliveNodes=1" }
    if ([int]$summary.suspectNodes -ne 1) { throw "expected suspectNodes=1" }
    if ([int]$summary.deadNodes -ne 1) { throw "expected deadNodes=1" }
    if ([int]$summary.runningByDeadOwners -ne 1) { throw "expected runningByDeadOwners=1" }
    if ([int]$summary.runningByUnknownOwners -ne 1) { throw "expected runningByUnknownOwners=1" }

    $metrics = Invoke-RelayMesh "metrics"
    if ($metrics -notmatch 'relaymesh_mesh_nodes_total\{status="dead"\} 1') { throw "missing dead mesh nodes metric" }
    if ($metrics -notmatch 'relaymesh_running_steps_by_owner_status\{owner_status="unknown"\} 1') { throw "missing unknown ownership metric" }
    if ($metrics -notmatch 'relaymesh_oldest_dead_node_age_ms') { throw "missing oldest dead age metric" }

    [ordered]@{
        totalNodes = [int]$summary.totalNodes
        aliveNodes = [int]$summary.aliveNodes
        suspectNodes = [int]$summary.suspectNodes
        deadNodes = [int]$summary.deadNodes
        runningByDeadOwners = [int]$summary.runningByDeadOwners
        runningByUnknownOwners = [int]$summary.runningByUnknownOwners
    } | ConvertTo-Json
} finally {
    Pop-Location
}

