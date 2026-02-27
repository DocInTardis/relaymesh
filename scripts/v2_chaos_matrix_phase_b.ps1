param(
    [string]$RootPrefix = "tmp/v2-chaos-matrix"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $ProjectRoot

try {
    function Parse-JsonFromOutput {
        param([string[]]$Output)
        $text = ($Output -join "`n")
        $start = $text.IndexOf("{")
        $end = $text.LastIndexOf("}")
        if ($start -lt 0 -or $end -lt $start) {
            throw "missing json payload in output"
        }
        $json = $text.Substring($start, ($end - $start + 1))
        return ($json | ConvertFrom-Json)
    }

    # Run a smoke script as an isolated sub-scenario and parse its JSON outcome payload.
    function Invoke-SmokeScript {
        param(
            [Parameter(Mandatory = $true)][string]$ScriptName,
            [string[]]$Arguments
        )
        $scriptPath = Join-Path $ProjectRoot ("scripts/" + $ScriptName)
        if (-not (Test-Path $scriptPath)) {
            throw "missing smoke script: $scriptPath"
        }
        $exec = @("-ExecutionPolicy", "Bypass", "-File", $scriptPath)
        if ($Arguments -ne $null -and $Arguments.Count -gt 0) {
            $exec += $Arguments
        }
        $output = & powershell @exec 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "smoke failed: $ScriptName`n$($output -join "`n")"
        }
        return Parse-JsonFromOutput -Output $output
    }

    $results = @()

    # 1) Node crash / dead owner recovery semantics.
    $nodeCrash = Invoke-SmokeScript -ScriptName "v2_mesh_smoke.ps1" -Arguments @(
        "-Root", "$RootPrefix-node-crash"
    )
    $results += [ordered]@{
        scenario = "node_crash_recovery"
        pass = $true
        detail = $nodeCrash
    }

    # 2) Packet loss / partition healing and deterministic convergence.
    $packetLoss = Invoke-SmokeScript -ScriptName "v2_partition_sim_smoke.ps1" -Arguments @(
        "-NodeARoot", "$RootPrefix-packet-a",
        "-NodeBRoot", "$RootPrefix-packet-b"
    )
    $results += [ordered]@{
        scenario = "packet_loss_partition_heal"
        pass = $true
        detail = $packetLoss
    }

    # 3) Clock skew / jitter protection and suppression guard behavior.
    $clockSkew = Invoke-SmokeScript -ScriptName "v2_jitter_smoke.ps1" -Arguments @(
        "-Root", "$RootPrefix-clock-skew"
    )
    $results += [ordered]@{
        scenario = "clock_skew_recovery_guard"
        pass = $true
        detail = $clockSkew
    }

    # 4) Disk pressure detection and recovery after threshold rollback.
    $diskPressure = Invoke-SmokeScript -ScriptName "v2_disk_pressure_smoke.ps1" -Arguments @(
        "-Root", "$RootPrefix-disk-pressure"
    )
    $results += [ordered]@{
        scenario = "disk_pressure_guard"
        pass = $true
        detail = $diskPressure
    }

    [ordered]@{
        smoke = "v2_chaos_matrix_phase_b"
        scenarios = $results
        passed = $true
    } | ConvertTo-Json -Depth 8
} finally {
    Pop-Location
}

