param(
    [string]$Root = "tmp/v2-prune-smoke"
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

    @"
import sqlite3, time
db = r'$ProjectRoot\$Root\relaymesh.db'
now = int(time.time() * 1000)
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute(
    "insert into mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) values(?,?,?,?,?) "
    "on conflict(node_id) do update set status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms",
    ('node-prune-manual-old', 'DEAD', now - 10000, now - 10000, now - 10000)
)
cur.execute(
    "insert into mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) values(?,?,?,?,?) "
    "on conflict(node_id) do update set status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms",
    ('node-prune-manual-fresh', 'DEAD', now + 60000, now + 60000, now + 60000)
)
conn.commit()
print('seeded_manual')
"@ | python -

    $manualPrune = (Invoke-RelayMesh "mesh-prune --older-than-ms 5000 --limit 10") | ConvertFrom-Json
    if ([int]$manualPrune.pruned -lt 1) {
        throw "mesh-prune expected pruned >= 1"
    }

    $members1 = (Invoke-RelayMesh "members") | ConvertFrom-Json
    $ids1 = @($members1 | ForEach-Object { [string]$_.nodeId })
    if ($ids1 -contains "node-prune-manual-old") {
        throw "manual old node should be pruned"
    }
    if (-not ($ids1 -contains "node-prune-manual-fresh")) {
        throw "manual fresh node should remain"
    }

    $settings = @"
{
  "meshPruneOlderThanMs": 5000,
  "meshPruneLimit": 10,
  "meshPruneIntervalMs": 1
}
"@
    Set-Content -Path (Join-Path $Root "relaymesh-settings.json") -Value $settings -Encoding UTF8
    $null = Invoke-RelayMesh "reload-settings"

    @"
import sqlite3, time
db = r'$ProjectRoot\$Root\relaymesh.db'
now = int(time.time() * 1000)
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute(
    "insert into mesh_nodes(node_id,status,last_heartbeat_ms,updated_at_ms,status_changed_at_ms) values(?,?,?,?,?) "
    "on conflict(node_id) do update set status=excluded.status,last_heartbeat_ms=excluded.last_heartbeat_ms,updated_at_ms=excluded.updated_at_ms,status_changed_at_ms=excluded.status_changed_at_ms",
    ('node-prune-auto-old', 'DEAD', now - 10000, now - 10000, now - 10000)
)
conn.commit()
print('seeded_auto')
"@ | python -

    $maint = (Invoke-RelayMesh "maintenance --node-id node-prune-maint") | ConvertFrom-Json
    if ([int]$maint.deadNodesPruned -lt 1) {
        throw "maintenance expected deadNodesPruned >= 1"
    }

    $members2 = (Invoke-RelayMesh "members") | ConvertFrom-Json
    $ids2 = @($members2 | ForEach-Object { [string]$_.nodeId })
    if ($ids2 -contains "node-prune-auto-old") {
        throw "auto old node should be pruned by maintenance"
    }

    [ordered]@{
        manualPruned = [int]$manualPrune.pruned
        autoPruned = [int]$maint.deadNodesPruned
        remainingNodeCount = $ids2.Count
    } | ConvertTo-Json
} finally {
    Pop-Location
}

