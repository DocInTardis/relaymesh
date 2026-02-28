param(
    [string]$Root = "tmp/control-room-api-smoke",
    [int]$Port = 18890
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

    function Get-StatusCode {
        param(
            [string]$Uri,
            [string]$Method = "GET",
            [string]$Body = ""
        )
        try {
            if ($Method.ToUpperInvariant() -eq "POST") {
                $resp = Invoke-WebRequest -Uri $Uri -Method POST -ContentType "application/x-www-form-urlencoded" -Body $Body -TimeoutSec 10
            } else {
                $resp = Invoke-WebRequest -Uri $Uri -Method $Method -TimeoutSec 10
            }
            return [int]$resp.StatusCode
        } catch {
            $response = $_.Exception.Response
            if ($null -ne $response -and $null -ne $response.StatusCode) {
                try {
                    return [int]$response.StatusCode.value__
                } catch {
                    return [int]$response.StatusCode
                }
            }
            throw
        }
    }

    function Invoke-Body {
        param(
            [string]$Uri,
            [string]$Method = "GET",
            [string]$Body = ""
        )
        if ($Method.ToUpperInvariant() -eq "POST") {
            $resp = Invoke-WebRequest -Uri $Uri -Method POST -ContentType "application/x-www-form-urlencoded" -Body $Body -TimeoutSec 10
        } else {
            $resp = Invoke-WebRequest -Uri $Uri -Method $Method -TimeoutSec 10
        }
        return [string]$resp.Content
    }

    function Wait-WebReady {
        param(
            [int]$ReadyPort,
            [int]$TimeoutSec = 25
        )
        $deadline = (Get-Date).AddSeconds($TimeoutSec)
        while ((Get-Date) -lt $deadline) {
            try {
                $code = Get-StatusCode -Uri "http://127.0.0.1:$ReadyPort/"
                if ($code -eq 200) {
                    return
                }
            } catch {
                Start-Sleep -Milliseconds 250
                continue
            }
            Start-Sleep -Milliseconds 250
        }
        throw "web server not ready on port $ReadyPort"
    }

    function Start-WebJob {
        param(
            [string]$ExecArgs,
            [int]$ReadyPort
        )
        $job = Start-Job -ScriptBlock {
            param($ProjectRootArg, $ExecArgsArg)
            Set-Location $ProjectRootArg
            & mvn -q exec:java "-Dexec.args=$ExecArgsArg"
        } -ArgumentList $ProjectRoot, $ExecArgs
        Wait-WebReady -ReadyPort $ReadyPort
        return $job
    }

    function Stop-WebJob {
        param($Job)
        if ($null -eq $Job) {
            return
        }
        try {
            Stop-Job -Job $Job -Force -ErrorAction SilentlyContinue
        } catch {
            # Ignore cleanup failures in smoke script.
        }
        try {
            Receive-Job -Job $Job -Keep -ErrorAction SilentlyContinue | Out-Null
        } catch {
            # Ignore cleanup failures in smoke script.
        }
        try {
            Remove-Job -Job $Job -Force -ErrorAction SilentlyContinue
        } catch {
            # Ignore cleanup failures in smoke script.
        }
    }

    $rootPath = Join-Path $ProjectRoot $Root
    if (Test-Path $rootPath) {
        Remove-Item -Recurse -Force $rootPath
    }

    $null = Invoke-RelayMesh "init"

    $roToken = "relay_ro_cr_smoke"
    $rwToken = "relay_rw_cr_smoke"
    $summary = [ordered]@{}
    $server = $null

    try {
        $execArgs = "--root=$Root serve-web --port $Port --ro-token $roToken --rw-token $rwToken"
        $server = Start-WebJob -ExecArgs $execArgs -ReadyPort $Port

        $summary["control_room_html"] = Get-StatusCode -Uri "http://127.0.0.1:$Port/control-room?token=$roToken"
        $summary["snapshot_read_ro"] = Get-StatusCode -Uri "http://127.0.0.1:$Port/api/control-room/snapshot?token=$roToken"
        $summary["command_read_ro"] = Get-StatusCode -Uri "http://127.0.0.1:$Port/api/control-room/command?token=$roToken" -Method POST -Body "command=namespaces"
        $summary["command_write_ro"] = Get-StatusCode -Uri "http://127.0.0.1:$Port/api/control-room/command?token=$roToken" -Method POST -Body "command=cancel default fake-task hard"
        $summary["profiles_list_ro"] = Get-StatusCode -Uri "http://127.0.0.1:$Port/api/control-room/layouts?token=$roToken"
        $summary["profiles_save_rw"] = Get-StatusCode -Uri "http://127.0.0.1:$Port/api/control-room/layouts/save?token=$rwToken" -Method POST -Body "name=smoke_ops&layout={%22panes%22:[{%22namespace%22:%22default%22,%22view%22:%22tasks%22,%22limit%22:10,%22status%22:%22%22}]}"
        $summary["profiles_get_ro"] = Get-StatusCode -Uri "http://127.0.0.1:$Port/api/control-room/layouts?token=$roToken&name=smoke_ops"
        $summary["profiles_delete_rw"] = Get-StatusCode -Uri "http://127.0.0.1:$Port/api/control-room/layouts/delete?token=$rwToken" -Method POST -Body "name=smoke_ops"
        $summary["action_write_ro"] = Get-StatusCode -Uri "http://127.0.0.1:$Port/api/control-room/action?token=$roToken" -Method POST -Body "action=replay&namespace=default&taskId=fake-task"
        $summary["action_write_rw"] = Get-StatusCode -Uri "http://127.0.0.1:$Port/api/control-room/action?token=$rwToken" -Method POST -Body "action=replay&namespace=default&taskId=fake-task"

        $snapshotBody = Invoke-Body -Uri "http://127.0.0.1:$Port/api/control-room/snapshot?token=$roToken"
        if ($snapshotBody -notmatch '"data"') {
            throw "snapshot body missing data field"
        }

        if ($summary["control_room_html"] -ne 200) { throw "expected control-room 200, got $($summary["control_room_html"])" }
        if ($summary["snapshot_read_ro"] -ne 200) { throw "expected snapshot ro 200, got $($summary["snapshot_read_ro"])" }
        if ($summary["command_read_ro"] -ne 200) { throw "expected command read ro 200, got $($summary["command_read_ro"])" }
        if ($summary["command_write_ro"] -ne 403) { throw "expected command write ro 403, got $($summary["command_write_ro"])" }
        if ($summary["profiles_list_ro"] -ne 200) { throw "expected profiles list ro 200, got $($summary["profiles_list_ro"])" }
        if ($summary["profiles_save_rw"] -ne 200) { throw "expected profiles save rw 200, got $($summary["profiles_save_rw"])" }
        if ($summary["profiles_get_ro"] -ne 200) { throw "expected profiles get ro 200, got $($summary["profiles_get_ro"])" }
        if ($summary["profiles_delete_rw"] -ne 200) { throw "expected profiles delete rw 200, got $($summary["profiles_delete_rw"])" }
        if ($summary["action_write_ro"] -ne 403) { throw "expected action write ro 403, got $($summary["action_write_ro"])" }
        if ($summary["action_write_rw"] -eq 403) { throw "expected action write rw non-403, got 403" }
    } finally {
        Stop-WebJob -Job $server
    }

    Write-Host "Control-room API smoke checks passed."
    $summary | ConvertTo-Json
    $global:LASTEXITCODE = 0
} catch {
    Write-Error $_
    throw
} finally {
    Pop-Location
}

