param(
    [string]$Root = "tmp/v2-node-rpc-tls-smoke",
    [int]$Port = 19443
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
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

    $tlsPort = Get-FreePort -StartPort $Port
    $mtlsPort = Get-FreePort -StartPort ($tlsPort + 1)

    function Invoke-JsonGetInsecure {
        param([string]$Url)
        $raw = & curl.exe -sS -k --fail $Url 2>$null
        if ($LASTEXITCODE -ne 0) {
            throw "curl request failed for $Url"
        }
        return $raw | ConvertFrom-Json
    }

    $rootPath = Join-Path $ProjectRoot $Root
    if (Test-Path $rootPath) {
        Remove-Item -Recurse -Force $rootPath
    }
    New-Item -ItemType Directory -Force -Path $rootPath | Out-Null

    $keystore = Join-Path $rootPath "node-rpc-keystore.p12"
    Invoke-NativeCommandQuiet -FilePath "keytool" -Arguments @(
        "-genkeypair",
        "-alias", "node",
        "-keyalg", "RSA",
        "-storetype", "PKCS12",
        "-keystore", $keystore,
        "-storepass", "changeit",
        "-keypass", "changeit",
        "-dname", "CN=127.0.0.1",
        "-validity", "2",
        "-noprompt"
    )

    & mvn -q exec:java "-Dexec.args=--root=$Root init" | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "relaymesh init failed"
    }

    $jobTls = Start-Job -ScriptBlock {
        param($ProjectRootArg, $RootArg, $PortArg, $KeystoreArg)
        Set-Location $ProjectRootArg
        $execArg = "-Dexec.args=--root=$RootArg serve-node-rpc --bind 127.0.0.1 --port $PortArg --keystore $KeystoreArg --keystore-pass changeit --keystore-type PKCS12"
        & mvn -q exec:java $execArg 2>&1 | Out-String
    } -ArgumentList $ProjectRoot, $Root, $tlsPort, $keystore

    $health = $null
    $lastHealthError = ""
    for ($i = 0; $i -lt 120; $i++) {
        if ($jobTls.State -eq "Failed" -or $jobTls.State -eq "Completed" -or $jobTls.State -eq "Stopped") {
            $jobOut = (Receive-Job -Job $jobTls -ErrorAction SilentlyContinue) -join "`n"
            throw "node rpc tls server job exited before ready: $jobOut"
        }
        try {
            $health = Invoke-JsonGetInsecure -Url "https://127.0.0.1:$tlsPort/node/health"
            break
        } catch {
            $lastHealthError = $_.Exception.Message
            Start-Sleep -Milliseconds 500
        }
    }
    if ($null -eq $health) {
        throw "node rpc tls endpoint did not become ready: $lastHealthError"
    }
    if (-not [bool]$health.ok) {
        throw "node rpc health endpoint returned non-ok payload"
    }

    $export = Invoke-JsonGetInsecure -Url "https://127.0.0.1:$tlsPort/node/replication/export?sinceMs=0&limit=10&sourceNodeId=tls-smoke"
    if ($export.schema_version -ne "relaymesh.replication.v1") {
        throw "unexpected schema_version from tls export endpoint: $($export.schema_version)"
    }
    $tlsStatus = Invoke-JsonGetInsecure -Url "https://127.0.0.1:$tlsPort/node/tls/status"
    if (-not [bool]$tlsStatus.ok) {
        throw "node tls status endpoint returned non-ok payload"
    }
    if ([string]::IsNullOrWhiteSpace([string]$tlsStatus.serverCertFingerprintSha256)) {
        throw "node tls status endpoint missing fingerprint"
    }

    Stop-Job -Job $jobTls -ErrorAction SilentlyContinue | Out-Null
    Receive-Job -Job $jobTls -ErrorAction SilentlyContinue | Out-Null
    Remove-Job -Job $jobTls -ErrorAction SilentlyContinue | Out-Null

    $jobMtls = Start-Job -ScriptBlock {
        param($ProjectRootArg, $RootArg, $PortArg, $KeystoreArg)
        Set-Location $ProjectRootArg
        $execArg = "-Dexec.args=--root=$RootArg serve-node-rpc --bind 127.0.0.1 --port $PortArg --keystore $KeystoreArg --keystore-pass changeit --keystore-type PKCS12 --truststore $KeystoreArg --truststore-pass changeit --truststore-type PKCS12 --require-client-auth=true"
        & mvn -q exec:java $execArg 2>&1 | Out-String
    } -ArgumentList $ProjectRoot, $Root, $mtlsPort, $keystore

    $mtlsReady = $false
    for ($i = 0; $i -lt 120; $i++) {
        if ($jobMtls.State -eq "Failed" -or $jobMtls.State -eq "Completed" -or $jobMtls.State -eq "Stopped") {
            $jobOut = (Receive-Job -Job $jobMtls -ErrorAction SilentlyContinue) -join "`n"
            throw "node rpc mtls server job exited before ready: $jobOut"
        }
        try {
            $tcp = [System.Net.Sockets.TcpClient]::new()
            try {
                $iar = $tcp.BeginConnect("127.0.0.1", $mtlsPort, $null, $null)
                if ($iar.AsyncWaitHandle.WaitOne(200)) {
                    $tcp.EndConnect($iar) | Out-Null
                    $mtlsReady = $true
                    break
                }
            } finally {
                $tcp.Dispose()
            }
        } catch {
            # still booting
        }
        Start-Sleep -Milliseconds 500
    }
    if (-not $mtlsReady) {
        throw "node rpc mtls endpoint did not open listening port in time"
    }
    $mtlsRejected = $false
    try {
        $null = Invoke-JsonGetInsecure -Url "https://127.0.0.1:$mtlsPort/node/health"
    } catch {
        $mtlsRejected = $true
    }

    Stop-Job -Job $jobMtls -ErrorAction SilentlyContinue | Out-Null
    Receive-Job -Job $jobMtls -ErrorAction SilentlyContinue | Out-Null
    Remove-Job -Job $jobMtls -ErrorAction SilentlyContinue | Out-Null

    if (-not $mtlsRejected) {
        throw "expected mTLS endpoint to reject client without certificate"
    }

    [ordered]@{
        smoke = "v2_node_rpc_tls"
        tlsPort = $tlsPort
        mtlsPort = $mtlsPort
        tlsHealthOk = [bool]$health.ok
        exportSchema = [string]$export.schema_version
        tlsFingerprintSha256 = [string]$tlsStatus.serverCertFingerprintSha256
        revokedEntryCount = [int]$tlsStatus.revokedEntryCount
        mtlsRejectedWithoutClientCert = $mtlsRejected
    } | ConvertTo-Json
} finally {
    Pop-Location
}

