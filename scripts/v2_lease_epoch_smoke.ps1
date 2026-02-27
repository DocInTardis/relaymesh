Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $ProjectRoot

try {
    & mvn -q -Dtest=TaskStoreLeaseEpochTest test
    if ($LASTEXITCODE -ne 0) {
        throw "lease epoch smoke test failed"
    }
    [ordered]@{
        smoke = "v2_lease_epoch"
        result = "passed"
        test = "TaskStoreLeaseEpochTest"
    } | ConvertTo-Json
} finally {
    Pop-Location
}

