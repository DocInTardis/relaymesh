param(
    [string]$Root = "tmp/v2-disk-pressure-smoke"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $ProjectRoot

try {
    function Invoke-RelayMeshJson {
        param(
            [string]$CommandArgs,
            [switch]$AllowNonZero
        )
        $execArg = "-Dexec.args=$CommandArgs"
        $output = & mvn -q exec:java "$execArg" 2>&1
        if ($LASTEXITCODE -ne 0 -and -not $AllowNonZero) {
            throw "relaymesh command failed: $CommandArgs`n$output"
        }
        $text = ($output -join "`n")
        $start = $text.IndexOf("{")
        $end = $text.LastIndexOf("}")
        if ($start -lt 0 -or $end -lt $start) {
            throw "relaymesh command returned no json payload: $CommandArgs`n$output"
        }
        $json = $text.Substring($start, ($end - $start + 1))
        return ($json | ConvertFrom-Json)
    }

    $rootPath = Join-Path $ProjectRoot $Root
    if (Test-Path $rootPath) {
        Remove-Item -Recurse -Force $rootPath
    }
    New-Item -ItemType Directory -Force -Path $rootPath | Out-Null

    & mvn -q exec:java "-Dexec.args=--root=$Root init" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "relaymesh init failed"
    }

    $settingsPath = Join-Path $rootPath "relaymesh-settings.json"
    $pressureSettings = @"
{
  "minFreeDiskBytes": 900000000000000
}
"@
    Set-Content -Path $settingsPath -Value $pressureSettings -Encoding UTF8

    $null = Invoke-RelayMeshJson "--root=$Root reload-settings"
    $healthPressure = Invoke-RelayMeshJson "--root=$Root health" -AllowNonZero
    if ([bool]$healthPressure.ok) {
        throw "expected health.ok=false under synthetic disk pressure"
    }
    if (-not [bool]$healthPressure.diskPressure) {
        throw "expected diskPressure=true"
    }

    $metrics = & mvn -q exec:java "-Dexec.args=--root=$Root metrics" 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "metrics command failed"
    }
    $metricsText = ($metrics -join "`n")
    if ($metricsText -notmatch "relaymesh_disk_pressure 1") {
        throw "expected relaymesh_disk_pressure metric to be 1"
    }

    $normalSettings = @"
{
  "minFreeDiskBytes": 0
}
"@
    Set-Content -Path $settingsPath -Value $normalSettings -Encoding UTF8
    $null = Invoke-RelayMeshJson "--root=$Root reload-settings"
    $healthRecovered = Invoke-RelayMeshJson "--root=$Root health"
    if (-not [bool]$healthRecovered.ok) {
        throw "expected health.ok=true after removing disk pressure threshold"
    }
    if ([bool]$healthRecovered.diskPressure) {
        throw "expected diskPressure=false after recovery"
    }

    [ordered]@{
        smoke = "v2_disk_pressure"
        diskFreeBytes = [long]$healthPressure.diskFreeBytes
        pressuredMinFreeDiskBytes = [long]$healthPressure.minFreeDiskBytes
        pressureDetected = [bool]$healthPressure.diskPressure
        recovered = [bool]$healthRecovered.ok
    } | ConvertTo-Json
} finally {
    Pop-Location
}

