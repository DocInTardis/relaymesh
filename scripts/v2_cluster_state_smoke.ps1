param(
    [string]$Root = "tmp/v2-cluster-state-smoke"
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
    $before = (Invoke-RelayMesh "cluster-state" | ConvertFrom-Json)
    if ([long]$before.epoch -lt 1) {
        throw "expected initial epoch >= 1, got $($before.epoch)"
    }

    $bump = (Invoke-RelayMesh "cluster-epoch-bump --reason smoke --actor smoke" | ConvertFrom-Json)
    if ([long]$bump.currentEpoch -ne ([long]$before.epoch + 1)) {
        throw "expected bumped epoch to equal before+1, got before=$($before.epoch), current=$($bump.currentEpoch)"
    }

    $after = (Invoke-RelayMesh "cluster-state" | ConvertFrom-Json)
    if ([long]$after.epoch -ne [long]$bump.currentEpoch) {
        throw "cluster-state mismatch after bump, expected=$($bump.currentEpoch), got=$($after.epoch)"
    }

    $metrics = Invoke-RelayMesh "metrics"
    if ($metrics -notmatch "relaymesh_cluster_epoch\s+$($after.epoch)") {
        throw "metrics missing relaymesh_cluster_epoch=$($after.epoch)"
    }

    [ordered]@{
        smoke = "v2_cluster_state"
        beforeEpoch = [long]$before.epoch
        afterEpoch = [long]$after.epoch
        version = [long]$after.version
    } | ConvertTo-Json
} finally {
    Pop-Location
}

