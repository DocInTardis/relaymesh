param(
    [string]$Root = "",
    [string]$DefaultNamespace = "default",
    [int]$WebPort = 18080,
    [int]$MetricsPort = 19090,
    [int]$AutoWorkers = 1,
    [string]$AutoTopology = "",
    [switch]$NoWeb,
    [switch]$NoMetrics,
    [switch]$NoInteractive,
    [switch]$KeepRunningOnExit,
    [string[]]$RunCommands
)

$ErrorActionPreference = "Stop"

$RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
if ([string]::IsNullOrWhiteSpace($Root)) {
    $Root = Join-Path $RepoRoot ("tmp\agent-hub-root-" + (Get-Date -Format "yyyyMMdd-HHmmss"))
}
$Root = [System.IO.Path]::GetFullPath($Root)
New-Item -ItemType Directory -Path $Root -Force | Out-Null

$LogsRoot = Join-Path $Root "hub-logs"
New-Item -ItemType Directory -Path $LogsRoot -Force | Out-Null

$NamespacesDir = Join-Path $Root "namespaces"
New-Item -ItemType Directory -Path $NamespacesDir -Force | Out-Null

$MvnCmd = Get-Command "mvn.cmd" -ErrorAction SilentlyContinue
if ($null -eq $MvnCmd) {
    $MvnCmd = Get-Command "mvn" -ErrorAction SilentlyContinue
}
if ($null -eq $MvnCmd) {
    throw "Maven not found. Install Maven and ensure `mvn` is in PATH."
}
$MvnExecutable = $MvnCmd.Source

$state = [ordered]@{
    Root = $Root
    ActiveNamespace = $DefaultNamespace
    Workers = @{}
    Services = @{}
}

$TopologyPresets = [ordered]@{
    "team-a" = @(
        @{ Name = "a-echo-1"; Namespace = "project-a"; AgentHint = "echo" },
        @{ Name = "a-fail-1"; Namespace = "project-a"; AgentHint = "fail" }
    )
    "team-b" = @(
        @{ Name = "b-echo-1"; Namespace = "project-b"; AgentHint = "echo" },
        @{ Name = "b-fail-1"; Namespace = "project-b"; AgentHint = "fail" }
    )
    "one-project" = @(
        @{ Name = "p-echo-1"; Namespace = "project-main"; AgentHint = "echo" },
        @{ Name = "p-echo-2"; Namespace = "project-main"; AgentHint = "echo" },
        @{ Name = "p-fail-1"; Namespace = "project-main"; AgentHint = "fail" }
    )
    "dual" = @(
        @{ Name = "a-echo-1"; Namespace = "project-a"; AgentHint = "echo" },
        @{ Name = "a-fail-1"; Namespace = "project-a"; AgentHint = "fail" },
        @{ Name = "b-echo-1"; Namespace = "project-b"; AgentHint = "echo" },
        @{ Name = "b-fail-1"; Namespace = "project-b"; AgentHint = "fail" }
    )
}

function Invoke-RelayMesh {
    param(
        [Parameter(Mandatory = $true)][string]$RelayArgs
    )
    & $script:MvnExecutable -q exec:java "-Dexec.args=$RelayArgs"
}

function Start-RelayBackground {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$RelayArgs,
        [Parameter(Mandatory = $true)][ValidateSet("worker", "web", "metrics")][string]$Kind
    )
    $outLog = Join-Path $script:LogsRoot "$Name.out.log"
    $errLog = Join-Path $script:LogsRoot "$Name.err.log"
    if (Test-Path $outLog) { Remove-Item $outLog -Force }
    if (Test-Path $errLog) { Remove-Item $errLog -Force }

    $prop = "-Dexec.args=`"$RelayArgs`""
    $proc = Start-Process -FilePath $script:MvnExecutable `
        -ArgumentList @("-q", "exec:java", $prop) `
        -WorkingDirectory $script:RepoRoot `
        -RedirectStandardOutput $outLog `
        -RedirectStandardError $errLog `
        -PassThru

    return [PSCustomObject]@{
        Name = $Name
        Kind = $Kind
        Pid = $proc.Id
        RelayArgs = $RelayArgs
        OutLog = $outLog
        ErrLog = $errLog
        StartTime = Get-Date
    }
}

function Stop-Entry {
    param([Parameter(Mandatory = $true)]$Entry)
    try {
        Stop-Process -Id $Entry.Pid -Force -ErrorAction Stop
    } catch {
        # Process may already be stopped.
    }
}

function Ensure-Project {
    param([Parameter(Mandatory = $true)][string]$Namespace)
    $safe = $Namespace.Trim()
    if ([string]::IsNullOrWhiteSpace($safe)) {
        throw "Namespace cannot be empty."
    }
    $path = Join-Path $script:NamespacesDir $safe
    New-Item -ItemType Directory -Path $path -Force | Out-Null
}

function Start-Worker {
    param(
        [Parameter(Mandatory = $true)][string]$Name,
        [Parameter(Mandatory = $true)][string]$Namespace,
        [string]$AgentHint = "",
        [string]$Topology = ""
    )
    if ($script:state.Workers.ContainsKey($Name)) {
        throw "Worker '$Name' already exists."
    }
    Ensure-Project -Namespace $Namespace
    $workerId = $Name
    $nodeId = "node-$Name"
    $args = "--root $($script:state.Root) --namespace $Namespace worker --worker-id $workerId --node-id $nodeId --interval-ms 1000 --maintenance"
    $entry = Start-RelayBackground -Name "worker-$Name" -RelayArgs $args -Kind "worker"
    $entry | Add-Member -NotePropertyName Namespace -NotePropertyValue $Namespace
    $entry | Add-Member -NotePropertyName WorkerId -NotePropertyValue $workerId
    $entry | Add-Member -NotePropertyName NodeId -NotePropertyValue $nodeId
    $entry | Add-Member -NotePropertyName AgentHint -NotePropertyValue $AgentHint
    $entry | Add-Member -NotePropertyName Topology -NotePropertyValue $Topology
    $script:state.Workers[$Name] = $entry
    $hintOut = if ([string]::IsNullOrWhiteSpace($AgentHint)) { "-" } else { $AgentHint }
    $topologyOut = if ([string]::IsNullOrWhiteSpace($Topology)) { "-" } else { $Topology }
    Write-Host "[worker] started name=$Name pid=$($entry.Pid) namespace=$Namespace hint=$hintOut topology=$topologyOut"
}

function Stop-Worker {
    param([Parameter(Mandatory = $true)][string]$Name)
    if ($Name -eq "all") {
        foreach ($kv in @($script:state.Workers.GetEnumerator())) {
            Stop-Entry -Entry $kv.Value
            $script:state.Workers.Remove($kv.Key)
        }
        Write-Host "[worker] all workers stopped"
        return
    }
    if (-not $script:state.Workers.ContainsKey($Name)) {
        throw "Worker '$Name' not found."
    }
    $entry = $script:state.Workers[$Name]
    Stop-Entry -Entry $entry
    $script:state.Workers.Remove($Name)
    Write-Host "[worker] stopped name=$Name pid=$($entry.Pid)"
}

function Start-ServiceEntry {
    param(
        [Parameter(Mandatory = $true)][ValidateSet("web", "metrics")][string]$ServiceName,
        [Parameter(Mandatory = $true)][int]$Port
    )
    if ($script:state.Services.ContainsKey($ServiceName)) {
        throw "Service '$ServiceName' already running."
    }
    if ($ServiceName -eq "web") {
        $args = "--root $($script:state.Root) serve-web --port $Port --ro-token relay_ro --rw-token relay_rw --write-rate-limit-per-min 120"
        $entry = Start-RelayBackground -Name "web" -RelayArgs $args -Kind "web"
        $entry | Add-Member -NotePropertyName Url -NotePropertyValue ("http://127.0.0.1:{0}/?token=relay_ro" -f $Port)
    } else {
        $args = "--root $($script:state.Root) serve-metrics --port $Port"
        $entry = Start-RelayBackground -Name "metrics" -RelayArgs $args -Kind "metrics"
        $entry | Add-Member -NotePropertyName Url -NotePropertyValue ("http://127.0.0.1:{0}/metrics" -f $Port)
    }
    $entry | Add-Member -NotePropertyName Port -NotePropertyValue $Port
    $script:state.Services[$ServiceName] = $entry
    Write-Host "[$ServiceName] started pid=$($entry.Pid) url=$($entry.Url)"
}

function Stop-ServiceEntry {
    param([Parameter(Mandatory = $true)][string]$ServiceName)
    if ($ServiceName -eq "all") {
        foreach ($kv in @($script:state.Services.GetEnumerator())) {
            Stop-Entry -Entry $kv.Value
            $script:state.Services.Remove($kv.Key)
        }
        Write-Host "[service] all stopped"
        return
    }
    if (-not $script:state.Services.ContainsKey($ServiceName)) {
        throw "Service '$ServiceName' not found."
    }
    $entry = $script:state.Services[$ServiceName]
    Stop-Entry -Entry $entry
    $script:state.Services.Remove($ServiceName)
    Write-Host "[$ServiceName] stopped pid=$($entry.Pid)"
}

function Show-Status {
    Write-Host ""
    Write-Host "Root: $($script:state.Root)"
    Write-Host "Active namespace: $($script:state.ActiveNamespace)"
    Write-Host ""
    Write-Host "Services:"
    if ($script:state.Services.Count -eq 0) {
        Write-Host "  (none)"
    } else {
        foreach ($svc in $script:state.Services.Values) {
            Write-Host ("  - {0}: pid={1}, url={2}" -f $svc.Name, $svc.Pid, $svc.Url)
        }
    }
    Write-Host ""
    Write-Host "Workers:"
    if ($script:state.Workers.Count -eq 0) {
        Write-Host "  (none)"
    } else {
        foreach ($w in $script:state.Workers.Values) {
            $hintOut = if ([string]::IsNullOrWhiteSpace($w.AgentHint)) { "-" } else { $w.AgentHint }
            $topologyOut = if ([string]::IsNullOrWhiteSpace($w.Topology)) { "-" } else { $w.Topology }
            Write-Host ("  - {0}: pid={1}, ns={2}, node={3}, hint={4}, topology={5}" -f $w.WorkerId, $w.Pid, $w.Namespace, $w.NodeId, $hintOut, $topologyOut)
        }
    }
    Write-Host ""
}

function Start-Topology {
    param([Parameter(Mandatory = $true)][string]$Preset)
    $key = $Preset.Trim().ToLowerInvariant()
    if (-not $script:TopologyPresets.Contains($key)) {
        $available = ($script:TopologyPresets.Keys -join ", ")
        throw "Unknown topology preset '$Preset'. Available: $available"
    }
    $started = 0
    $skipped = 0
    foreach ($spec in $script:TopologyPresets[$key]) {
        $name = [string]$spec.Name
        $ns = [string]$spec.Namespace
        $hint = [string]$spec.AgentHint
        if ($script:state.Workers.ContainsKey($name)) {
            $skipped++
            Write-Host "[topology] skip existing worker '$name'"
            continue
        }
        Start-Worker -Name $name -Namespace $ns -AgentHint $hint -Topology $key
        $started++
    }
    Write-Host "[topology] preset=$key started=$started skipped=$skipped"
}

function Stop-Topology {
    param([Parameter(Mandatory = $true)][string]$Preset)
    $key = $Preset.Trim().ToLowerInvariant()
    if ($key -ne "all" -and -not $script:TopologyPresets.Contains($key)) {
        $available = ($script:TopologyPresets.Keys -join ", ")
        throw "Unknown topology preset '$Preset'. Available: $available"
    }
    $targets = @()
    if ($key -eq "all") {
        $targets = @($script:state.Workers.Values | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Topology) })
    } else {
        $presetNames = @($script:TopologyPresets[$key] | ForEach-Object { [string]$_.Name })
        $targets = @(
            $script:state.Workers.Values | Where-Object {
                ($_.Topology -eq $key) -or ($presetNames -contains $_.WorkerId)
            }
        )
    }
    if ($targets.Count -eq 0) {
        Write-Host "[topology] no running workers for preset '$key'"
        return
    }
    foreach ($w in $targets) {
        Stop-Worker -Name $w.WorkerId
    }
    Write-Host "[topology] stopped workers count=$($targets.Count) preset=$key"
}

function Show-Topologies {
    Write-Host "Topology presets:"
    foreach ($keyObj in $script:TopologyPresets.Keys) {
        $key = [string]$keyObj
        $specs = $script:TopologyPresets[$key]
        $presetNames = @($specs | ForEach-Object { [string]$_.Name })
        $running = @($script:state.Workers.Values | Where-Object { $presetNames -contains $_.WorkerId }).Count
        Write-Host ("  - {0}: workers={1}, running={2}" -f $key, $specs.Count, $running)
    }
}

function Show-Help {
@"
Agent Hub Commands
==================
help
status
exit

project use <namespace>
project list
project create <namespace>

worker start <name> [namespace] [agent-hint]
worker stop <name|all>
worker list

topology up <team-a|team-b|one-project|dual>
topology down <preset|all>
topology list

service start web [port]
service start metrics [port]
service stop <web|metrics|all>
service list
open <web|metrics>

submit <agent> <priority> <text>
submitns <namespace> <agent> <priority> <text>
submith <agent-hint> <priority> <text>
workflow <file>
workflowns <namespace> <file>
tasks [status] [limit]
tasksns <namespace> [status] [limit]
task <taskId>
taskns <namespace> <taskId>
replay <taskId>
replayns <namespace> <taskId>

tail <worker:<name>|service:web|service:metrics> [out|err] [lines]
run <raw relaymesh args>

Examples
--------
worker start w1
worker start w2 project-a echo
topology up dual
submit echo high build-index-job
submitns project-a fail low smoke-failure
submith echo normal from-hinted-worker
tasks SUCCESS 20
run --namespace project-a lease-conflicts --since-hours 24 --limit 50
"@ | Write-Host
}

function Split-Args {
    param([string]$Text)
    if ([string]::IsNullOrWhiteSpace($Text)) { return @() }
    # Always return an array; a single token must not collapse into a scalar string.
    return ,@($Text -split '\s+' | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
}

function Invoke-HubCommand {
    param([Parameter(Mandatory = $true)][string]$Line)
    $raw = $Line.Trim()
    if ([string]::IsNullOrWhiteSpace($raw)) { return $true }

    $firstSplit = $raw.Split(" ", 2, [System.StringSplitOptions]::RemoveEmptyEntries)
    $cmd = $firstSplit[0].ToLowerInvariant()
    $rest = if ($firstSplit.Length -gt 1) { $firstSplit[1] } else { "" }

    try {
        switch ($cmd) {
            "help" {
                Show-Help
            }
            "status" {
                Show-Status
            }
            "project" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) { throw "Usage: project use <namespace> | project list | project create <namespace>" }
                switch ($parts[0].ToLowerInvariant()) {
                    "use" {
                        if ($parts.Count -lt 2) { throw "Usage: project use <namespace>" }
                        Ensure-Project -Namespace $parts[1]
                        $script:state.ActiveNamespace = $parts[1]
                        Write-Host "[project] active namespace set to '$($script:state.ActiveNamespace)'"
                    }
                    "list" {
                        $dirs = Get-ChildItem -Path $script:NamespacesDir -Directory -ErrorAction SilentlyContinue
                        if ($null -eq $dirs -or $dirs.Count -eq 0) {
                            Write-Host "(no namespaces yet)"
                        } else {
                            foreach ($d in $dirs) { Write-Host $d.Name }
                        }
                    }
                    "create" {
                        if ($parts.Count -lt 2) { throw "Usage: project create <namespace>" }
                        Ensure-Project -Namespace $parts[1]
                        Write-Host "[project] created '$($parts[1])'"
                    }
                    default {
                        throw "Usage: project use <namespace> | project list | project create <namespace>"
                    }
                }
            }
            "worker" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) { throw "Usage: worker start <name> [namespace] [agent-hint] | worker stop <name|all> | worker list" }
                switch ($parts[0].ToLowerInvariant()) {
                    "start" {
                        if ($parts.Count -lt 2) { throw "Usage: worker start <name> [namespace] [agent-hint]" }
                        $ns = if ($parts.Count -ge 3) { $parts[2] } else { $script:state.ActiveNamespace }
                        $hint = if ($parts.Count -ge 4) { $parts[3] } else { "" }
                        Start-Worker -Name $parts[1] -Namespace $ns -AgentHint $hint
                    }
                    "stop" {
                        if ($parts.Count -lt 2) { throw "Usage: worker stop <name|all>" }
                        Stop-Worker -Name $parts[1]
                    }
                    "list" {
                        if ($script:state.Workers.Count -eq 0) {
                            Write-Host "(no workers)"
                        } else {
                            foreach ($w in $script:state.Workers.Values) {
                                $hintOut = if ([string]::IsNullOrWhiteSpace($w.AgentHint)) { "-" } else { $w.AgentHint }
                                $topologyOut = if ([string]::IsNullOrWhiteSpace($w.Topology)) { "-" } else { $w.Topology }
                                Write-Host ("{0}`tpid={1}`tns={2}`thint={3}`ttopology={4}" -f $w.WorkerId, $w.Pid, $w.Namespace, $hintOut, $topologyOut)
                            }
                        }
                    }
                    default {
                        throw "Usage: worker start <name> [namespace] [agent-hint] | worker stop <name|all> | worker list"
                    }
                }
            }
            "topology" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) { throw "Usage: topology up <preset> | topology down <preset|all> | topology list" }
                switch ($parts[0].ToLowerInvariant()) {
                    "up" {
                        if ($parts.Count -lt 2) { throw "Usage: topology up <preset>" }
                        Start-Topology -Preset $parts[1]
                    }
                    "down" {
                        if ($parts.Count -lt 2) { throw "Usage: topology down <preset|all>" }
                        Stop-Topology -Preset $parts[1]
                    }
                    "list" {
                        Show-Topologies
                    }
                    default {
                        throw "Usage: topology up <preset> | topology down <preset|all> | topology list"
                    }
                }
            }
            "service" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) { throw "Usage: service start web [port] | service start metrics [port] | service stop <web|metrics|all> | service list" }
                switch ($parts[0].ToLowerInvariant()) {
                    "start" {
                        if ($parts.Count -lt 2) { throw "Usage: service start web [port] | service start metrics [port]" }
                        $svc = $parts[1].ToLowerInvariant()
                        if ($svc -eq "web") {
                            $port = if ($parts.Count -ge 3) { [int]$parts[2] } else { $WebPort }
                            Start-ServiceEntry -ServiceName "web" -Port $port
                        } elseif ($svc -eq "metrics") {
                            $port = if ($parts.Count -ge 3) { [int]$parts[2] } else { $MetricsPort }
                            Start-ServiceEntry -ServiceName "metrics" -Port $port
                        } else {
                            throw "Usage: service start web [port] | service start metrics [port]"
                        }
                    }
                    "stop" {
                        if ($parts.Count -lt 2) { throw "Usage: service stop <web|metrics|all>" }
                        Stop-ServiceEntry -ServiceName $parts[1].ToLowerInvariant()
                    }
                    "list" {
                        if ($script:state.Services.Count -eq 0) {
                            Write-Host "(no services)"
                        } else {
                            foreach ($svc in $script:state.Services.Values) {
                                Write-Host ("{0}`tpid={1}`turl={2}" -f $svc.Name, $svc.Pid, $svc.Url)
                            }
                        }
                    }
                    default {
                        throw "Usage: service start web [port] | service start metrics [port] | service stop <web|metrics|all> | service list"
                    }
                }
            }
            "open" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) { throw "Usage: open <web|metrics>" }
                $target = $parts[0].ToLowerInvariant()
                if (-not $script:state.Services.ContainsKey($target)) {
                    throw "Service '$target' is not running."
                }
                Start-Process $script:state.Services[$target].Url | Out-Null
            }
            "submit" {
                if ($rest -notmatch '^(?<agent>\S+)\s+(?<priority>high|normal|low)\s+(?<input>.+)$') {
                    throw "Usage: submit <agent> <priority> <text>"
                }
                $safeInput = $Matches["input"].Replace('"', '\"')
                Invoke-RelayMesh "--root $($script:state.Root) --namespace $($script:state.ActiveNamespace) submit --agent $($Matches['agent']) --priority $($Matches['priority']) --input `"$safeInput`""
            }
            "submitns" {
                if ($rest -notmatch '^(?<ns>\S+)\s+(?<agent>\S+)\s+(?<priority>high|normal|low)\s+(?<input>.+)$') {
                    throw "Usage: submitns <namespace> <agent> <priority> <text>"
                }
                Ensure-Project -Namespace $Matches["ns"]
                $safeInput = $Matches["input"].Replace('"', '\"')
                Invoke-RelayMesh "--root $($script:state.Root) --namespace $($Matches['ns']) submit --agent $($Matches['agent']) --priority $($Matches['priority']) --input `"$safeInput`""
            }
            "submith" {
                if ($rest -notmatch '^(?<hint>\S+)\s+(?<priority>high|normal|low)\s+(?<input>.+)$') {
                    throw "Usage: submith <agent-hint> <priority> <text>"
                }
                $hint = $Matches["hint"]
                $target = @($script:state.Workers.Values | Where-Object { $_.AgentHint -eq $hint } | Select-Object -First 1)
                if ($target.Count -eq 0) {
                    throw "No worker found with agent-hint '$hint'. Start one via: worker start <name> [namespace] $hint"
                }
                $safeInput = $Matches["input"].Replace('"', '\"')
                $targetNs = $target[0].Namespace
                Invoke-RelayMesh "--root $($script:state.Root) --namespace $targetNs submit --agent $hint --priority $($Matches['priority']) --input `"$safeInput`""
            }
            "workflow" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) { throw "Usage: workflow <json-file>" }
                Invoke-RelayMesh "--root $($script:state.Root) --namespace $($script:state.ActiveNamespace) submit-workflow --file $($parts[0])"
            }
            "workflowns" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 2) { throw "Usage: workflowns <namespace> <json-file>" }
                Ensure-Project -Namespace $parts[0]
                Invoke-RelayMesh "--root $($script:state.Root) --namespace $($parts[0]) submit-workflow --file $($parts[1])"
            }
            "tasks" {
                $parts = Split-Args $rest
                $status = if ($parts.Count -ge 1) { $parts[0] } else { "" }
                $limit = if ($parts.Count -ge 2) { $parts[1] } else { "20" }
                $statusArg = if ([string]::IsNullOrWhiteSpace($status)) { "" } else { " --status $status" }
                Invoke-RelayMesh "--root $($script:state.Root) --namespace $($script:state.ActiveNamespace) tasks --limit $limit$statusArg"
            }
            "tasksns" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) { throw "Usage: tasksns <namespace> [status] [limit]" }
                $ns = $parts[0]
                Ensure-Project -Namespace $ns
                $status = if ($parts.Count -ge 2) { $parts[1] } else { "" }
                $limit = if ($parts.Count -ge 3) { $parts[2] } else { "20" }
                $statusArg = if ([string]::IsNullOrWhiteSpace($status)) { "" } else { " --status $status" }
                Invoke-RelayMesh "--root $($script:state.Root) --namespace $ns tasks --limit $limit$statusArg"
            }
            "task" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) { throw "Usage: task <taskId>" }
                Invoke-RelayMesh "--root $($script:state.Root) --namespace $($script:state.ActiveNamespace) task $($parts[0])"
            }
            "taskns" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 2) { throw "Usage: taskns <namespace> <taskId>" }
                Ensure-Project -Namespace $parts[0]
                Invoke-RelayMesh "--root $($script:state.Root) --namespace $($parts[0]) task $($parts[1])"
            }
            "replay" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) { throw "Usage: replay <taskId>" }
                Invoke-RelayMesh "--root $($script:state.Root) --namespace $($script:state.ActiveNamespace) replay $($parts[0])"
            }
            "replayns" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 2) { throw "Usage: replayns <namespace> <taskId>" }
                Ensure-Project -Namespace $parts[0]
                Invoke-RelayMesh "--root $($script:state.Root) --namespace $($parts[0]) replay $($parts[1])"
            }
            "tail" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) {
                    throw "Usage: tail <worker:<name>|service:web|service:metrics> [out|err] [lines]"
                }
                $target = $parts[0]
                $stream = if ($parts.Count -ge 2) { $parts[1].ToLowerInvariant() } else { "out" }
                $lines = if ($parts.Count -ge 3) { [int]$parts[2] } else { 60 }
                if ($stream -ne "out" -and $stream -ne "err") {
                    throw "stream must be out or err"
                }
                if ($target.StartsWith("worker:")) {
                    $name = $target.Substring("worker:".Length)
                    if (-not $script:state.Workers.ContainsKey($name)) {
                        throw "Worker '$name' not found."
                    }
                    $entry = $script:state.Workers[$name]
                    $file = if ($stream -eq "out") { $entry.OutLog } else { $entry.ErrLog }
                    Get-Content $file -Tail $lines -Wait
                } elseif ($target.StartsWith("service:")) {
                    $svc = $target.Substring("service:".Length).ToLowerInvariant()
                    if (-not $script:state.Services.ContainsKey($svc)) {
                        throw "Service '$svc' not found."
                    }
                    $entry = $script:state.Services[$svc]
                    $file = if ($stream -eq "out") { $entry.OutLog } else { $entry.ErrLog }
                    Get-Content $file -Tail $lines -Wait
                } else {
                    throw "Usage: tail <worker:<name>|service:web|service:metrics> [out|err] [lines]"
                }
            }
            "run" {
                if ([string]::IsNullOrWhiteSpace($rest)) {
                    throw "Usage: run <raw relaymesh args>"
                }
                if ($rest.Contains("--root ")) {
                    Invoke-RelayMesh $rest
                } else {
                    Invoke-RelayMesh "--root $($script:state.Root) --namespace $($script:state.ActiveNamespace) $rest"
                }
            }
            "exit" {
                return $false
            }
            default {
                Write-Host "Unknown command: $cmd"
                Write-Host "Type 'help' for available commands."
            }
        }
    } catch {
        Write-Host "ERROR: $($_.Exception.Message)"
    }
    return $true
}

Write-Host ""
Write-Host "RelayMesh Agent Hub"
Write-Host "Repo: $RepoRoot"
Write-Host "Root: $Root"
Write-Host ""

Write-Host "[init] creating runtime root..."
Invoke-RelayMesh "--root $Root init" | Out-Host

Ensure-Project -Namespace $state.ActiveNamespace

if (-not $NoWeb) {
    Start-ServiceEntry -ServiceName "web" -Port $WebPort
}
if (-not $NoMetrics) {
    Start-ServiceEntry -ServiceName "metrics" -Port $MetricsPort
}

for ($i = 1; $i -le [Math]::Max(0, $AutoWorkers); $i++) {
    Start-Worker -Name ("w{0}" -f $i) -Namespace $state.ActiveNamespace
}
if (-not [string]::IsNullOrWhiteSpace($AutoTopology)) {
    Start-Topology -Preset $AutoTopology
}

Show-Status
Show-Help

if ($RunCommands -ne $null -and $RunCommands.Count -gt 0) {
    foreach ($cmd in $RunCommands) {
        Write-Host ""
        Write-Host ("hub[{0}]> {1}" -f $state.ActiveNamespace, $cmd)
        $keep = Invoke-HubCommand -Line $cmd
        if (-not $keep) { break }
    }
}

if (-not $NoInteractive) {
    while ($true) {
        $line = Read-Host ("hub[{0}]" -f $state.ActiveNamespace)
        $keep = Invoke-HubCommand -Line $line
        if (-not $keep) { break }
    }
}

if (-not $KeepRunningOnExit) {
    foreach ($kv in @($state.Workers.GetEnumerator())) {
        Stop-Entry -Entry $kv.Value
    }
    foreach ($kv in @($state.Services.GetEnumerator())) {
        Stop-Entry -Entry $kv.Value
    }
    Write-Host "[shutdown] stopped all workers/services started by this hub session."
} else {
    Write-Host "[shutdown] keeping processes alive (KeepRunningOnExit=true)."
}
