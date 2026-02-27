Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $ProjectRoot

try {
    & mvn -q -Dtest=RelayMeshRuntimeSecurityTest test
    if ($LASTEXITCODE -ne 0) {
        throw "gossip signing smoke test failed"
    }
    [ordered]@{
        smoke = "v2_gossip_signing"
        result = "passed"
        test = "RelayMeshRuntimeSecurityTest"
    } | ConvertTo-Json
} finally {
    Pop-Location
}

