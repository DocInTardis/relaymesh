param(
    [string]$ShortcutPath = "$env:USERPROFILE\Desktop\RelayMesh Studio.lnk",
    [string]$IconPath = "",
    [string]$Root = "",
    [string]$DefaultNamespace = "default",
    [int]$WebPort = 18080,
    [int]$MetricsPort = 19090,
    [int]$AutoWorkers = 0,
    [string]$AutoTopology = "dual"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$StartScript = Join-Path $RepoRoot "scripts\start_studio.ps1"
if (-not (Test-Path $StartScript)) {
    throw "start_studio.ps1 not found: $StartScript"
}

$shortcutFull = [System.IO.Path]::GetFullPath($ShortcutPath)
$shortcutDir = Split-Path -Parent $shortcutFull
if (-not [string]::IsNullOrWhiteSpace($shortcutDir)) {
    New-Item -ItemType Directory -Path $shortcutDir -Force | Out-Null
}

$argParts = @(
    "-NoProfile",
    "-ExecutionPolicy", "Bypass",
    "-File", "`"$StartScript`"",
    "-DefaultNamespace", $DefaultNamespace,
    "-WebPort", [string]$WebPort,
    "-MetricsPort", [string]$MetricsPort,
    "-AutoWorkers", [string]$AutoWorkers
)
if (-not [string]::IsNullOrWhiteSpace($Root)) {
    $argParts += @("-Root", "`"$Root`"")
}
if (-not [string]::IsNullOrWhiteSpace($AutoTopology)) {
    $argParts += @("-AutoTopology", $AutoTopology)
}

$targetPath = (Get-Command "powershell.exe").Source
$arguments = $argParts -join " "
$workingDir = $RepoRoot

$resolvedIcon = $IconPath
if ([string]::IsNullOrWhiteSpace($resolvedIcon)) {
    $wt = Get-Command "wt.exe" -ErrorAction SilentlyContinue
    if ($null -ne $wt) {
        $resolvedIcon = $wt.Source
    } else {
        $resolvedIcon = $targetPath
    }
}

$wsh = New-Object -ComObject WScript.Shell
$shortcut = $wsh.CreateShortcut($shortcutFull)
$shortcut.TargetPath = $targetPath
$shortcut.Arguments = $arguments
$shortcut.WorkingDirectory = $workingDir
$shortcut.Description = "RelayMesh Studio launcher"
$shortcut.IconLocation = "$resolvedIcon,0"
$shortcut.Save()

Write-Host ("[studio] shortcut created: {0}" -f $shortcutFull)
Write-Host ("[studio] target: {0} {1}" -f $targetPath, $arguments)
Write-Host ("[studio] icon: {0}" -f $resolvedIcon)

