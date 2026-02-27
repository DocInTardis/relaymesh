Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Push-Location $ProjectRoot

try {
    & mvn -q -Dtest=FileBusFairnessTest test
    if ($LASTEXITCODE -ne 0) {
        throw "Fairness smoke test failed"
    }

    [ordered]@{
        smoke = "v2_fairness"
        result = "passed"
        test = "FileBusFairnessTest"
    } | ConvertTo-Json
} finally {
    Pop-Location
}

