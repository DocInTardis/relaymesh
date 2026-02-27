param(
    [string]$Root = "tmp/v2-worker-pause-smoke"
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
    $submit = (Invoke-RelayMesh "submit --agent echo --input pause-smoke --priority normal") | ConvertFrom-Json
    $taskId = [string]$submit.taskId
    if ([string]::IsNullOrWhiteSpace($taskId)) {
        throw "submit missing taskId"
    }

    @"
import sqlite3, time
db = r'$ProjectRoot\$Root\relaymesh.db'
now = int(time.time() * 1000)
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute(
    "insert into mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) values(?,?,?,?,?) "
    "on conflict(node_id) do update set status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms",
    ('node-pause-smoke', 'SUSPECT', now, now, now)
)
conn.commit()
print('seeded')
"@ | python -

    $paused = (Invoke-RelayMesh "worker --once=true --worker-id worker-smoke --node-id node-pause-smoke --maintenance=false") | ConvertFrom-Json
    if ([bool]$paused.processed -ne $false) {
        throw "worker should be paused when local node is SUSPECT"
    }
    if ([string]$paused.message -notmatch "paused") {
        throw "worker pause message missing"
    }

    $task1 = (Invoke-RelayMesh "task $taskId") | ConvertFrom-Json
    if ([string]$task1.status -ne "PENDING") {
        throw "task should remain PENDING while worker is paused"
    }

    @"
import sqlite3, time
db = r'$ProjectRoot\$Root\relaymesh.db'
now = int(time.time() * 1000)
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute(
    "update mesh_nodes set status='ALIVE', last_heartbeat_ms=?, updated_at_ms=?, status_changed_at_ms=? where node_id=?",
    (now, now, now, 'node-pause-smoke')
)
conn.commit()
print('resumed')
"@ | python -

    $resumed = (Invoke-RelayMesh "worker --once=true --worker-id worker-smoke --node-id node-pause-smoke --maintenance=false") | ConvertFrom-Json
    if ([bool]$resumed.processed -ne $true) {
        throw "worker should resume when node status is ALIVE"
    }

    $task2 = (Invoke-RelayMesh "task $taskId") | ConvertFrom-Json
    if ([string]$task2.status -ne "SUCCESS") {
        throw "task should be SUCCESS after resume run"
    }

    $audit = Invoke-RelayMesh "audit-query --action worker.pause.local_status --limit 10"
    if ($audit -notmatch "worker.pause.local_status") {
        throw "worker.pause.local_status audit row not found"
    }

    [ordered]@{
        taskId = $taskId
        paused = $true
        resumed = $true
        finalStatus = [string]$task2.status
    } | ConvertTo-Json
} finally {
    Pop-Location
}

