param(
    [string]$Root = "tmp/v2-jitter-smoke"
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
    ('node-flap-v2', 'DEAD', now - 1000, now, now)
)
conn.commit()
print('seeded')
"@ | python -

    $null = Invoke-RelayMesh "maintenance --node-id node-flap-v2"
    $members1 = (Invoke-RelayMesh "members") | ConvertFrom-Json
    $node1 = $members1 | Where-Object { $_.nodeId -eq "node-flap-v2" } | Select-Object -First 1
    if ($null -eq $node1) {
        throw "node-flap-v2 missing after first maintenance"
    }
    if ([string]$node1.status -ne "DEAD") {
        throw "expected DEAD during dwell window, got: $($node1.status)"
    }

    $auditSuppressed = Invoke-RelayMesh "audit-query --action membership.recovery.suppressed --limit 5"
    if ($auditSuppressed -notmatch "membership.recovery.suppressed") {
        throw "expected membership.recovery.suppressed audit event"
    }

    @"
import sqlite3, time
db = r'$ProjectRoot\$Root\relaymesh.db'
now = int(time.time() * 1000)
conn = sqlite3.connect(db)
cur = conn.cursor()
cur.execute(
    "update mesh_nodes set status='DEAD', status_changed_at_ms=?, updated_at_ms=?, last_heartbeat_ms=? where node_id=?",
    (now - 31000, now - 31000, now - 1000, 'node-flap-v2')
)
conn.commit()
print('aged')
"@ | python -

    $null = Invoke-RelayMesh "maintenance --node-id node-flap-v2"
    $members2 = (Invoke-RelayMesh "members") | ConvertFrom-Json
    $node2 = $members2 | Where-Object { $_.nodeId -eq "node-flap-v2" } | Select-Object -First 1
    if ($null -eq $node2) {
        throw "node-flap-v2 missing after second maintenance"
    }
    if ([string]$node2.status -ne "ALIVE") {
        throw "expected ALIVE after dwell window elapsed, got: $($node2.status)"
    }

    [ordered]@{
        nodeId = "node-flap-v2"
        firstStatus = [string]$node1.status
        secondStatus = [string]$node2.status
        suppressedEventObserved = $true
    } | ConvertTo-Json
} finally {
    Pop-Location
}

