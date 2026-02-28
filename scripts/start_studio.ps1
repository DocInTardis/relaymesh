param(
    [string]$Root = "",
    [string]$DefaultNamespace = "default",
    [int]$WebPort = 18080,
    [int]$MetricsPort = 19090,
    [int]$AutoWorkers = 0,
    [string]$AutoTopology = "dual",
    [switch]$NoWeb,
    [switch]$NoMetrics,
    [switch]$NoInteractive,
    [switch]$KeepRunningOnExit,
    [string[]]$RunCommands
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$HubScript = Join-Path $RepoRoot "scripts\agent_hub.ps1"
if (-not (Test-Path $HubScript)) {
    throw "agent_hub.ps1 not found: $HubScript"
}

$args = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", $HubScript,
    "-DefaultNamespace", $DefaultNamespace,
    "-WebPort", [string]$WebPort,
    "-MetricsPort", [string]$MetricsPort,
    "-AutoWorkers", [string]$AutoWorkers
)

if (-not [string]::IsNullOrWhiteSpace($Root)) {
    $args += @("-Root", $Root)
}
if (-not [string]::IsNullOrWhiteSpace($AutoTopology)) {
    $args += @("-AutoTopology", $AutoTopology)
}
if ($NoWeb) { $args += "-NoWeb" }
if ($NoMetrics) { $args += "-NoMetrics" }
if ($NoInteractive) { $args += "-NoInteractive" }
if ($KeepRunningOnExit) { $args += "-KeepRunningOnExit" }
if ($RunCommands -and $RunCommands.Count -gt 0) {
    $args += "-RunCommands"
    $args += $RunCommands
}

Write-Host ("[studio] launching RelayMesh Studio from {0}" -f $RepoRoot)
Write-Host ("[studio] web: http://127.0.0.1:{0}/control-room?token=relay_ro" -f $WebPort)

& powershell.exe @args
exit $LASTEXITCODE

