param(
    [string]$Root = "tmp/p8-smoke",
    [int]$Port = 18888,
    [int]$WebReadyTimeoutSec = 60
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $ProjectRoot

try {
    $MvnCmd = Get-Command "mvn.cmd" -ErrorAction SilentlyContinue
    if ($null -eq $MvnCmd) {
        $MvnCmd = Get-Command "mvn" -ErrorAction SilentlyContinue
    }
    if ($null -eq $MvnCmd) {
        throw "Maven not found. Ensure mvn/mvn.cmd is available in PATH."
    }
    $MvnExecutable = $MvnCmd.Source

    function Invoke-RelayMesh {
        param([string]$ArgsLine)
        $execArg = "-Dexec.args=--root=$Root $ArgsLine"
        $output = & $script:MvnExecutable -q exec:java $execArg 2>&1
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
                $resp = Invoke-WebRequest -Uri $Uri -Method POST -ContentType "application/x-www-form-urlencoded" -Body $Body -TimeoutSec 8
            } else {
                $resp = Invoke-WebRequest -Uri $Uri -Method $Method -TimeoutSec 8
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

    function Wait-WebReady {
        param(
            [int]$ReadyPort,
            [int]$TimeoutSec = 60,
            [int]$ProcessId = 0,
            [string]$ErrorLogPath = ""
        )
        $deadline = (Get-Date).AddSeconds($TimeoutSec)
        while ((Get-Date) -lt $deadline) {
            if ($ProcessId -gt 0) {
                $proc = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
                if ($null -eq $proc) {
                    if (-not [string]::IsNullOrWhiteSpace($ErrorLogPath) -and (Test-Path $ErrorLogPath)) {
                        $errTail = (Get-Content -Path $ErrorLogPath -ErrorAction SilentlyContinue | Select-Object -Last 40) -join "`n"
                        throw "web process exited early; stderr tail:`n$errTail"
                    }
                    throw "web process exited early before readiness check"
                }
            }
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

    function Test-PortInUse {
        param([int]$CheckPort)
        $listener = $null
        try {
            $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $CheckPort)
            $listener.Start()
            return $false
        } catch {
            return $true
        } finally {
            if ($null -ne $listener) {
                try { $listener.Stop() } catch { }
            }
        }
    }

    function Find-FreePortPair {
        param([int]$PreferredBasePort)
        $candidate = if ($PreferredBasePort -lt 1024) { 1024 } else { $PreferredBasePort }
        if (-not (Test-PortInUse -CheckPort $candidate) -and -not (Test-PortInUse -CheckPort ($candidate + 1))) {
            return $candidate
        }
        for ($p = 20000; $p -le 60000; $p++) {
            if (-not (Test-PortInUse -CheckPort $p) -and -not (Test-PortInUse -CheckPort ($p + 1))) {
                return $p
            }
        }
        throw "unable to find available adjacent ports for smoke web checks"
    }

    function Start-WebProcess {
        param(
            [string]$ExecArgs,
            [int]$ReadyPort,
            [string]$LogPrefix
        )
        $prefix = if ([string]::IsNullOrWhiteSpace($LogPrefix)) { "web" } else { $LogPrefix.Trim() }
        $outLog = Join-Path $rootPath ("{0}.out.log" -f $prefix)
        $errLog = Join-Path $rootPath ("{0}.err.log" -f $prefix)
        if (Test-Path $outLog) { Remove-Item $outLog -Force }
        if (Test-Path $errLog) { Remove-Item $errLog -Force }

        $execProp = "-Dexec.args=`"$ExecArgs`""
        $proc = Start-Process -FilePath $script:MvnExecutable `
            -ArgumentList @("-q", "exec:java", $execProp) `
            -WorkingDirectory $ProjectRoot `
            -WindowStyle Hidden `
            -RedirectStandardOutput $outLog `
            -RedirectStandardError $errLog `
            -PassThru

        Wait-WebReady -ReadyPort $ReadyPort -TimeoutSec $WebReadyTimeoutSec -ProcessId $proc.Id -ErrorLogPath $errLog
        return [PSCustomObject]@{
            Process = $proc
            OutLog = $outLog
            ErrLog = $errLog
        }
    }

    function Stop-WebProcess {
        param(
            $Entry
        )
        if ($null -eq $Entry) {
            return
        }
        try {
            if ($null -ne $Entry.Process) {
                Stop-Process -Id $Entry.Process.Id -Force -ErrorAction SilentlyContinue
            }
        } catch {
            # Ignore cleanup failures in smoke script.
        }
    }

    $rootPath = Join-Path $ProjectRoot $Root
    if (Test-Path $rootPath) {
        Remove-Item -Recurse -Force $rootPath
    }

    & $script:MvnExecutable -q -DskipTests package
    if ($LASTEXITCODE -ne 0) {
        throw "mvn package failed"
    }

    $null = Invoke-RelayMesh "init"

    $settingsJson = @'
{
  "leaseTimeoutMs": 12000,
  "suspectAfterMs": 24000,
  "deadAfterMs": 48000,
  "maxAttempts": 3,
  "baseBackoffMs": 1000,
  "maxBackoffMs": 12000,
  "retryDispatchLimit": 16,
  "gossipFanout": 2,
  "gossipPacketTtl": 2,
  "gossipSyncSampleSize": 16,
  "gossipDedupWindowMs": 30000,
  "gossipDedupMaxEntries": 2048
}
'@
    Set-Content -Path (Join-Path $rootPath "relaymesh-settings.json") -Value $settingsJson -Encoding utf8

    $reloadOut = Invoke-RelayMesh "reload-settings"
    if ($reloadOut -notmatch '"changedFields"') {
        throw "reload-settings output missing changedFields"
    }

    $metricsOut = Invoke-RelayMesh "metrics"
    if ($metricsOut -notmatch "relaymesh_tasks_total") {
        throw "metrics output missing relaymesh_tasks_total"
    }
    if ($metricsOut -notmatch "relaymesh_web_write_get_compat_total") {
        throw "metrics output missing relaymesh_web_write_get_compat_total"
    }

    $roToken = "relay_ro_smoke"
    $rwToken = "relay_rw_smoke"
    $postPort = Find-FreePortPair -PreferredBasePort $Port
    $compatPort = $postPort + 1
    Write-Host ("[smoke] using ports post-only={0}, allow-get={1}" -f $postPort, $compatPort)
    $summary = [ordered]@{}

    $server = $null
    try {
        $postExecArgs = "--root=$Root serve-web --port $postPort --ro-token $roToken --rw-token $rwToken"
        $server = Start-WebProcess -ExecArgs $postExecArgs -ReadyPort $postPort -LogPrefix "web-post-only"

        $summary["post_only_read_no_token"] = Get-StatusCode -Uri "http://127.0.0.1:$postPort/api/tasks?limit=1"
        $summary["post_only_read_ro_token"] = Get-StatusCode -Uri "http://127.0.0.1:$postPort/api/tasks?limit=1&token=$roToken"
        $summary["post_only_write_get"] = Get-StatusCode -Uri "http://127.0.0.1:$postPort/api/replay-batch?status=DEAD_LETTER&limit=1&token=$rwToken"
        $summary["post_only_write_post"] = Get-StatusCode -Uri "http://127.0.0.1:$postPort/api/replay-batch?token=$rwToken" -Method "POST" -Body "status=DEAD_LETTER&limit=1"

        if ($summary["post_only_read_no_token"] -ne 401) { throw "expected 401 without token, got $($summary["post_only_read_no_token"])" }
        if ($summary["post_only_read_ro_token"] -ne 200) { throw "expected 200 with ro token, got $($summary["post_only_read_ro_token"])" }
        if ($summary["post_only_write_get"] -ne 405) { throw "expected 405 for GET write on post-only mode, got $($summary["post_only_write_get"])" }
        if ($summary["post_only_write_post"] -ne 200) { throw "expected 200 for POST write, got $($summary["post_only_write_post"])" }
    } finally {
        Stop-WebProcess -Entry $server
    }

    $server = $null
    try {
        $compatExecArgs = "--root=$Root serve-web --port $compatPort --ro-token $roToken --rw-token $rwToken --allow-get-writes"
        $server = Start-WebProcess -ExecArgs $compatExecArgs -ReadyPort $compatPort -LogPrefix "web-allow-get"

        $summary["allow_get_write_get"] = Get-StatusCode -Uri "http://127.0.0.1:$compatPort/api/replay-batch?status=DEAD_LETTER&limit=1&token=$rwToken"
        if ($summary["allow_get_write_get"] -ne 200) {
            throw "expected 200 for GET write with --allow-get-writes, got $($summary["allow_get_write_get"])"
        }

        $metricsResp = Invoke-WebRequest -Uri "http://127.0.0.1:$compatPort/api/metrics?token=$roToken" -Method GET -TimeoutSec 8
        $metricsText = [string]$metricsResp.Content
        if ($metricsText -notmatch "relaymesh_web_write_get_compat_total") {
            throw "api/metrics missing relaymesh_web_write_get_compat_total"
        }
        $match = [regex]::Match($metricsText, "relaymesh_web_write_get_compat_total\s+([0-9]+(?:\.[0-9]+)?)")
        if (-not $match.Success) {
            throw "cannot parse relaymesh_web_write_get_compat_total"
        }
        $summary["allow_get_metric_value"] = [double]$match.Groups[1].Value
        if ($summary["allow_get_metric_value"] -lt 1) {
            throw "expected relaymesh_web_write_get_compat_total >= 1, got $($summary["allow_get_metric_value"])"
        }
    } finally {
        Stop-WebProcess -Entry $server
    }

    $gossipOut = Invoke-RelayMesh "gossip-sync --node-id smoke-a --bind-port 18961 --seeds 127.0.0.1:18962 --window-ms 400 --rounds 1 --interval-ms 100 --fanout 1 --ttl 1"
    if ($gossipOut -notmatch '"roundsCompleted"\s*:\s*1') {
        throw "gossip-sync output missing roundsCompleted=1"
    }
    $summary["gossip_sync_checked"] = 1

    Write-Host "P8 smoke checks passed."
    $summary | ConvertTo-Json
    $global:LASTEXITCODE = 0
} catch {
    Write-Error $_
    throw
} finally {
    Pop-Location
}

