param(
    [string]$Root = "tmp/v2-node-rpc-rotation-smoke",
    [int]$Port = 19453
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$job = $null
Push-Location $ProjectRoot

try {
    function Get-FreePort {
        param([int]$StartPort)
        for ($p = [Math]::Max(1025, $StartPort); $p -lt ($StartPort + 256); $p++) {
            $listener = $null
            try {
                $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, $p)
                $listener.Start()
                return $p
            } catch {
                # in use
            } finally {
                if ($listener -ne $null) {
                    try { $listener.Stop() } catch {}
                }
            }
        }
        throw "no free port found near $StartPort"
    }

    function Invoke-NativeCommandQuiet {
        param(
            [Parameter(Mandatory = $true)][string]$FilePath,
            [Parameter(Mandatory = $true)][string[]]$Arguments
        )
        $prevErrorAction = $ErrorActionPreference
        $ErrorActionPreference = "Continue"
        try {
            & $FilePath @Arguments 2>$null | Out-Null
            $exitCode = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $prevErrorAction
        }
        if ($null -eq $exitCode) {
            $exitCode = 0
        }
        if ($exitCode -ne 0) {
            throw "native command failed: $FilePath (exit=$exitCode)"
        }
    }

    function Invoke-JsonGetInsecure {
        param([string]$Url)
        $raw = & curl.exe -sS -k --fail $Url 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw "curl request failed for $Url"
        }
        return $raw | ConvertFrom-Json
    }

    $tlsPort = Get-FreePort -StartPort $Port
    $rootPath = Join-Path $ProjectRoot $Root
    if (Test-Path $rootPath) {
        Remove-Item -Recurse -Force $rootPath
    }
    New-Item -ItemType Directory -Force -Path $rootPath | Out-Null

    $activeKeystore = Join-Path $rootPath "node-active.p12"
    $keystoreV1 = Join-Path $rootPath "node-v1.p12"
    $keystoreV2 = Join-Path $rootPath "node-v2.p12"
    $revocationFile = Join-Path $rootPath "revocations.txt"

    Invoke-NativeCommandQuiet -FilePath "keytool" -Arguments @(
        "-genkeypair",
        "-alias", "nodev1",
        "-keyalg", "RSA",
        "-storetype", "PKCS12",
        "-keystore", $keystoreV1,
        "-storepass", "changeit",
        "-keypass", "changeit",
        "-dname", "CN=127.0.0.1,OU=v1",
        "-validity", "2",
        "-noprompt"
    )

    Invoke-NativeCommandQuiet -FilePath "keytool" -Arguments @(
        "-genkeypair",
        "-alias", "nodev2",
        "-keyalg", "RSA",
        "-storetype", "PKCS12",
        "-keystore", $keystoreV2,
        "-storepass", "changeit",
        "-keypass", "changeit",
        "-dname", "CN=127.0.0.1,OU=v2",
        "-validity", "2",
        "-noprompt"
    )

    Copy-Item -Force $keystoreV1 $activeKeystore
    New-Item -ItemType File -Force -Path $revocationFile | Out-Null

    & mvn -q exec:java "-Dexec.args=--root=$Root init" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "relaymesh init failed"
    }

    $job = Start-Job -ScriptBlock {
        param($ProjectRootArg, $RootArg, $PortArg, $KeystoreArg, $RevocationArg)
        Set-Location $ProjectRootArg
        $execArg = "-Dexec.args=--root=$RootArg serve-node-rpc --bind 127.0.0.1 --port $PortArg --keystore $KeystoreArg --keystore-pass changeit --keystore-type PKCS12 --revocation-file $RevocationArg --reload-interval-ms 1000"
        & mvn -q exec:java $execArg 2>&1 | Out-String
    } -ArgumentList $ProjectRoot, $Root, $tlsPort, $activeKeystore, $revocationFile

    $status1 = $null
    $lastError = ""
    for ($i = 0; $i -lt 120; $i++) {
        if ($job.State -eq "Failed" -or $job.State -eq "Completed" -or $job.State -eq "Stopped") {
            $jobOut = (Receive-Job -Job $job -ErrorAction SilentlyContinue) -join "`n"
            throw "node rpc server exited before initial tls status: $jobOut"
        }
        try {
            $status1 = Invoke-JsonGetInsecure -Url "https://127.0.0.1:$tlsPort/node/tls/status"
            break
        } catch {
            $lastError = $_.Exception.Message
            Start-Sleep -Milliseconds 500
        }
    }
    if ($null -eq $status1) {
        throw "initial tls status was not ready: $lastError"
    }
    $fp1 = [string]$status1.serverCertFingerprintSha256
    if ([string]::IsNullOrWhiteSpace($fp1)) {
        throw "initial tls fingerprint missing"
    }
    if ([int]$status1.revokedEntryCount -ne 0) {
        throw "expected initial revokedEntryCount=0"
    }

    Copy-Item -Force $keystoreV2 $activeKeystore
    Add-Content -Path $revocationFile -Value "serial:DEADBEEF"

    $status2 = $null
    $rotated = $false
    $revocationReloaded = $false
    for ($i = 0; $i -lt 180; $i++) {
        if ($job.State -eq "Failed" -or $job.State -eq "Completed" -or $job.State -eq "Stopped") {
            $jobOut = (Receive-Job -Job $job -ErrorAction SilentlyContinue) -join "`n"
            throw "node rpc server exited before tls reload completed: $jobOut"
        }
        try {
            $status2 = Invoke-JsonGetInsecure -Url "https://127.0.0.1:$tlsPort/node/tls/status"
            if ([string]$status2.serverCertFingerprintSha256 -ne $fp1) {
                $rotated = $true
            }
            if ([int]$status2.revokedEntryCount -ge 1) {
                $revocationReloaded = $true
            }
            if ($rotated -and $revocationReloaded) {
                break
            }
        } catch {
            # keep waiting
        }
        Start-Sleep -Milliseconds 500
    }

    Stop-Job -Job $job -ErrorAction SilentlyContinue | Out-Null
    Receive-Job -Job $job -ErrorAction SilentlyContinue | Out-Null
    Remove-Job -Job $job -ErrorAction SilentlyContinue | Out-Null
    $job = $null

    if (-not $rotated) {
        throw "expected tls fingerprint to rotate after keystore replacement"
    }
    if (-not $revocationReloaded) {
        throw "expected revocation list reload to update revokedEntryCount"
    }

    [ordered]@{
        smoke = "v2_node_rpc_rotation"
        tlsPort = $tlsPort
        initialFingerprint = $fp1
        rotatedFingerprint = [string]$status2.serverCertFingerprintSha256
        rotationDetected = $rotated
        revocationReloaded = $revocationReloaded
        revokedEntryCount = [int]$status2.revokedEntryCount
    } | ConvertTo-Json
} finally {
    if ($null -ne $job) {
        try { Stop-Job -Job $job -ErrorAction SilentlyContinue | Out-Null } catch {}
        try { Receive-Job -Job $job -ErrorAction SilentlyContinue | Out-Null } catch {}
        try { Remove-Job -Job $job -ErrorAction SilentlyContinue | Out-Null } catch {}
    }
    Pop-Location
}

