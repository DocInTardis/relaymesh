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
    [string[]]$RunCommands,
    [switch]$NoAutoRestore,
    [switch]$ResetSession,
    [switch]$ShowFullHelpOnStart
)

$ErrorActionPreference = "Stop"

$HubScriptPath = $PSCommandPath
$RepoRoot = [System.IO.Path]::GetFullPath((Join-Path $PSScriptRoot ".."))
$SessionFile = Join-Path $RepoRoot "tmp\agent-hub-session.json"
$WorkspaceFile = Join-Path $RepoRoot "tmp\agent-hub-workspaces.json"
$RestoredSession = $null
$RestoredWorkerSpecs = @()
$RestoredAliases = @{}
$RestoredTemplates = @{}

if ($ResetSession -and (Test-Path $SessionFile)) {
    Remove-Item -Path $SessionFile -Force
    Write-Host "[session] cleared saved session file: $SessionFile"
}

if ([string]::IsNullOrWhiteSpace($Root) -and -not $NoAutoRestore -and (Test-Path $SessionFile)) {
    try {
        $rawSession = Get-Content -Path $SessionFile -Raw -ErrorAction Stop
        if (-not [string]::IsNullOrWhiteSpace($rawSession)) {
            $RestoredSession = $rawSession | ConvertFrom-Json
        }
    } catch {
        Write-Host "[session] warning: failed to read session file; starting fresh."
    }
}

if ($null -ne $RestoredSession) {
    $savedRoot = [string]$RestoredSession.root
    if (-not [string]::IsNullOrWhiteSpace($savedRoot)) {
        $Root = $savedRoot
    }
    if (-not $PSBoundParameters.ContainsKey("DefaultNamespace")) {
        $savedNs = [string]$RestoredSession.activeNamespace
        if (-not [string]::IsNullOrWhiteSpace($savedNs)) {
            $DefaultNamespace = $savedNs
        }
    }
    if (-not $PSBoundParameters.ContainsKey("AutoWorkers")) {
        $AutoWorkers = 0
    }
    if (-not $PSBoundParameters.ContainsKey("AutoTopology")) {
        $savedTopology = [string]$RestoredSession.autoTopology
        if (-not [string]::IsNullOrWhiteSpace($savedTopology)) {
            $AutoTopology = $savedTopology
        } else {
            $RestoredWorkerSpecs = @($RestoredSession.workerSpecs)
        }
    }
    if ($null -ne $RestoredSession.aliases) {
        foreach ($prop in $RestoredSession.aliases.PSObject.Properties) {
            $name = [string]$prop.Name
            $expansion = [string]$prop.Value
            if (-not [string]::IsNullOrWhiteSpace($name) -and -not [string]::IsNullOrWhiteSpace($expansion)) {
                $RestoredAliases[$name.ToLowerInvariant()] = $expansion
            }
        }
    }
    if ($null -ne $RestoredSession.templates) {
        foreach ($prop in $RestoredSession.templates.PSObject.Properties) {
            $name = [string]$prop.Name
            $value = $prop.Value
            if ([string]::IsNullOrWhiteSpace($name) -or $null -eq $value) {
                continue
            }
            $RestoredTemplates[$name.ToLowerInvariant()] = [ordered]@{
                agent = [string]$value.agent
                priority = [string]$value.priority
                input = [string]$value.input
            }
        }
    }
}

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
    LastTopology = ""
    SessionFile = $SessionFile
    WorkspaceFile = $WorkspaceFile
    SessionRestored = ($null -ne $RestoredSession)
    SessionEnabled = $true
    Aliases = [ordered]@{
        "q" = "exit"
        "h" = "help"
        "p" = "panel"
        "st" = "status"
        "tl" = "topology list"
        "wl" = "worker list"
        "ts" = "tasks"
    }
    Templates = [ordered]@{
        "echo-quick" = [ordered]@{ agent = "echo"; priority = "normal"; input = "hello-from-template" }
        "fail-smoke" = [ordered]@{ agent = "fail"; priority = "low"; input = "smoke-failure-case" }
    }
}

if ($null -ne $RestoredSession) {
    $savedTopology = [string]$RestoredSession.autoTopology
    if (-not [string]::IsNullOrWhiteSpace($savedTopology)) {
        $state.LastTopology = $savedTopology.Trim().ToLowerInvariant()
    }
}

foreach ($kv in $RestoredAliases.GetEnumerator()) {
    $state.Aliases[$kv.Key] = [string]$kv.Value
}
foreach ($kv in $RestoredTemplates.GetEnumerator()) {
    $state.Templates[$kv.Key] = [ordered]@{
        agent = [string]$kv.Value.agent
        priority = [string]$kv.Value.priority
        input = [string]$kv.Value.input
    }
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
        -WindowStyle Hidden `
        -RedirectStandardOutput $outLog `
        -RedirectStandardError $errLog `
        -PassThru

    # `mvn.cmd` may spawn a separate long-running java process.
    # Track that child PID so stop operations can reliably terminate workers/services.
    $trackedPid = $proc.Id
    $resolvedPid = $trackedPid
    $deadline = (Get-Date).AddSeconds(5)
    while ((Get-Date) -lt $deadline) {
        $child = Get-CimInstance Win32_Process -Filter ("ParentProcessId = {0} AND Name = 'java.exe'" -f $trackedPid) -ErrorAction SilentlyContinue |
            Select-Object -First 1
        if ($null -ne $child) {
            $resolvedPid = [int]$child.ProcessId
            break
        }
        Start-Sleep -Milliseconds 120
    }

    return [PSCustomObject]@{
        Name = $Name
        Kind = $Kind
        Pid = $resolvedPid
        LauncherPid = $trackedPid
        RelayArgs = $RelayArgs
        OutLog = $outLog
        ErrLog = $errLog
        StartTime = Get-Date
    }
}

function Stop-Entry {
    param([Parameter(Mandatory = $true)]$Entry)
    $pids = @()
    if ($null -ne $Entry.Pid) { $pids += [int]$Entry.Pid }
    if ($null -ne $Entry.LauncherPid) { $pids += [int]$Entry.LauncherPid }
    $pids = @($pids | Select-Object -Unique)

    foreach ($procId in $pids) {
        try {
            Stop-Process -Id $procId -Force -ErrorAction Stop
        } catch {
            # Process may already be stopped.
        }
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
    $script:state.LastTopology = $key
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
    $remainingTopoWorkers = @($script:state.Workers.Values | Where-Object { -not [string]::IsNullOrWhiteSpace($_.Topology) }).Count
    if ($remainingTopoWorkers -eq 0) {
        $script:state.LastTopology = ""
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

function Get-HubPaletteItems {
    return @(
        [ordered]@{ cmd = "panel"; summary = "Show compact quick actions" }
        [ordered]@{ cmd = "help"; summary = "Show full command reference" }
        [ordered]@{ cmd = "status"; summary = "Show services/workers status" }
        [ordered]@{ cmd = "project use <namespace>"; summary = "Switch active project namespace" }
        [ordered]@{ cmd = "worker start <name> [namespace] [agent-hint]"; summary = "Start worker agent process" }
        [ordered]@{ cmd = "worker list"; summary = "List active workers" }
        [ordered]@{ cmd = "topology up dual"; summary = "Start dual-project team topology" }
        [ordered]@{ cmd = "topology down all"; summary = "Stop all topology workers" }
        [ordered]@{ cmd = "submit <agent> <priority> <text>"; summary = "Submit task to active namespace" }
        [ordered]@{ cmd = "submith <agent-hint> <priority> <text>"; summary = "Submit task via worker hint routing" }
        [ordered]@{ cmd = "template list"; summary = "List reusable submit templates" }
        [ordered]@{ cmd = "template run <name> [namespace]"; summary = "Run a submit template" }
        [ordered]@{ cmd = "alias list"; summary = "List command aliases" }
        [ordered]@{ cmd = "workspace list"; summary = "List saved workspace launch profiles" }
        [ordered]@{ cmd = "workspace save <name> [topology]"; summary = "Save current launch profile" }
        [ordered]@{ cmd = "workspace launch <name>"; summary = "Open new terminal with saved profile" }
        [ordered]@{ cmd = "monitor"; summary = "Show one-shot task status monitor for active namespace" }
        [ordered]@{ cmd = "monitor watch [namespace|all] [intervalSec] [iterations]"; summary = "Watch task status changes over time" }
        [ordered]@{ cmd = "session show"; summary = "Show saved session snapshot" }
        [ordered]@{ cmd = "session clear"; summary = "Clear auto-restore snapshot" }
        [ordered]@{ cmd = "exit"; summary = "Exit hub and stop managed processes" }
    )
}

function Show-Palette {
    param([string]$Query = "")
    $items = Get-HubPaletteItems
    $q = $Query.Trim().ToLowerInvariant()
    $filtered = if ([string]::IsNullOrWhiteSpace($q)) {
        $items
    } else {
        @($items | Where-Object {
            ([string]$_.cmd).ToLowerInvariant().Contains($q) -or ([string]$_.summary).ToLowerInvariant().Contains($q)
        })
    }

    if ($filtered.Count -eq 0) {
        Write-Host ("[palette] no command matched '{0}'" -f $Query)
        return @()
    }

    Write-Host ("Palette results ({0})" -f $filtered.Count)
    $idx = 1
    foreach ($it in $filtered) {
        Write-Host ("  {0}. {1}" -f $idx, $it.cmd)
        Write-Host ("     {0}" -f $it.summary)
        $idx++
    }
    return $filtered
}

function Resolve-HubAlias {
    param([Parameter(Mandatory = $true)][string]$RawLine)
    $line = $RawLine
    for ($i = 0; $i -lt 4; $i++) {
        $split = $line.Trim().Split(" ", 2, [System.StringSplitOptions]::RemoveEmptyEntries)
        if ($split.Length -eq 0) {
            return $line
        }
        $head = $split[0].ToLowerInvariant()
        if (-not $script:state.Aliases.Contains($head)) {
            return $line
        }
        $mapped = [string]$script:state.Aliases[$head]
        if ([string]::IsNullOrWhiteSpace($mapped)) {
            return $line
        }
        $tail = if ($split.Length -gt 1) { $split[1] } else { "" }
        $line = if ([string]::IsNullOrWhiteSpace($tail)) { $mapped } else { "$mapped $tail" }
    }
    return $line
}

function Normalize-Priority {
    param([string]$Priority)
    $p = $Priority.Trim().ToLowerInvariant()
    if ($p -ne "high" -and $p -ne "normal" -and $p -ne "low") {
        throw "priority must be high|normal|low"
    }
    return $p
}

function Show-Templates {
    if ($script:state.Templates.Count -eq 0) {
        Write-Host "(no templates)"
        return
    }
    foreach ($name in $script:state.Templates.Keys) {
        $t = $script:state.Templates[$name]
        Write-Host ("{0}`tagent={1}`tpriority={2}`tinput={3}" -f $name, $t.agent, $t.priority, $t.input)
    }
}

function Submit-FromTemplate {
    param(
        [Parameter(Mandatory = $true)][string]$TemplateName,
        [string]$Namespace = ""
    )
    $key = $TemplateName.Trim().ToLowerInvariant()
    if (-not $script:state.Templates.Contains($key)) {
        throw "Template '$TemplateName' not found."
    }
    $tpl = $script:state.Templates[$key]
    $agent = [string]$tpl.agent
    $priority = Normalize-Priority ([string]$tpl.priority)
    $input = [string]$tpl.input
    $safeInput = $input.Replace('"', '\"')
    $ns = if ([string]::IsNullOrWhiteSpace($Namespace)) { $script:state.ActiveNamespace } else { $Namespace.Trim() }
    Ensure-Project -Namespace $ns
    Invoke-RelayMesh "--root $($script:state.Root) --namespace $ns submit --agent $agent --priority $priority --input `"$safeInput`""
}

function Get-WorkspaceProfiles {
    if (-not (Test-Path $script:state.WorkspaceFile)) {
        return [ordered]@{}
    }
    try {
        $raw = Get-Content -Path $script:state.WorkspaceFile -Raw -ErrorAction Stop
        if ([string]::IsNullOrWhiteSpace($raw)) {
            return [ordered]@{}
        }
        $node = $raw | ConvertFrom-Json
        $out = [ordered]@{}
        foreach ($prop in $node.PSObject.Properties) {
            $name = [string]$prop.Name
            $v = $prop.Value
            $out[$name.ToLowerInvariant()] = [ordered]@{
                root = [string]$v.root
                defaultNamespace = [string]$v.defaultNamespace
                autoTopology = [string]$v.autoTopology
                noWeb = [bool]$v.noWeb
                noMetrics = [bool]$v.noMetrics
                createdAt = [string]$v.createdAt
            }
        }
        return $out
    } catch {
        Write-Host "[workspace] warning: failed to parse workspace file."
        return [ordered]@{}
    }
}

function Save-WorkspaceProfiles {
    param([Parameter(Mandatory = $true)]$Profiles)
    $dir = Split-Path -Path $script:state.WorkspaceFile -Parent
    if (-not [string]::IsNullOrWhiteSpace($dir)) {
        New-Item -Path $dir -ItemType Directory -Force | Out-Null
    }
    $Profiles | ConvertTo-Json -Depth 8 | Set-Content -Path $script:state.WorkspaceFile -Encoding UTF8
}

function Show-WorkspaceProfiles {
    $profiles = Get-WorkspaceProfiles
    if ($profiles.Count -eq 0) {
        Write-Host "(no workspace profiles)"
        return
    }
    foreach ($name in $profiles.Keys) {
        $p = $profiles[$name]
        Write-Host ("{0}`troot={1}`tns={2}`ttopology={3}" -f $name, $p.root, $p.defaultNamespace, $p.autoTopology)
    }
}

function Show-WorkspaceProfile {
    param([Parameter(Mandatory = $true)][string]$Name)
    $profiles = Get-WorkspaceProfiles
    $key = $Name.Trim().ToLowerInvariant()
    if (-not $profiles.Contains($key)) {
        throw "Workspace '$Name' not found."
    }
    $p = $profiles[$key]
    Write-Host ("[workspace] {0}" -f $key)
    Write-Host ("  root={0}" -f $p.root)
    Write-Host ("  defaultNamespace={0}" -f $p.defaultNamespace)
    Write-Host ("  autoTopology={0}" -f $p.autoTopology)
    Write-Host ("  noWeb={0}, noMetrics={1}" -f $p.noWeb, $p.noMetrics)
    Write-Host ("  createdAt={0}" -f $p.createdAt)
    $preview = "wt.exe -w new -p `"RelayMesh Studio`" powershell " + ((Build-WorkspaceScriptArgs -Profile $p) -join " ")
    Write-Host ("  launch={0}" -f $preview)
}

function Build-WorkspaceScriptArgs {
    param([Parameter(Mandatory = $true)]$Profile)
    $args = @("-NoProfile", "-ExecutionPolicy", "Bypass", "-NoExit", "-File", $script:HubScriptPath)
    if (-not [string]::IsNullOrWhiteSpace([string]$Profile.root)) {
        $args += @("-Root", [string]$Profile.root)
    }
    if (-not [string]::IsNullOrWhiteSpace([string]$Profile.defaultNamespace)) {
        $args += @("-DefaultNamespace", [string]$Profile.defaultNamespace)
    }
    $args += @("-AutoWorkers", "0")
    if (-not [string]::IsNullOrWhiteSpace([string]$Profile.autoTopology)) {
        $args += @("-AutoTopology", [string]$Profile.autoTopology)
    }
    if ([bool]$Profile.noWeb) { $args += "-NoWeb" }
    if ([bool]$Profile.noMetrics) { $args += "-NoMetrics" }
    return ,$args
}

function Launch-WorkspaceProfile {
    param([Parameter(Mandatory = $true)][string]$Name)
    $profiles = Get-WorkspaceProfiles
    $key = $Name.Trim().ToLowerInvariant()
    if (-not $profiles.Contains($key)) {
        throw "Workspace '$Name' not found."
    }
    $profile = $profiles[$key]
    $psArgs = Build-WorkspaceScriptArgs -Profile $profile
    $wt = Get-Command "wt.exe" -ErrorAction SilentlyContinue
    if ($null -ne $wt) {
        $wtArgs = @("-w", "new", "-p", "RelayMesh Studio", "powershell") + $psArgs
        Start-Process -FilePath $wt.Source -ArgumentList $wtArgs | Out-Null
        Write-Host ("[workspace] launched '{0}' in Windows Terminal." -f $key)
    } else {
        Start-Process -FilePath "powershell.exe" -ArgumentList $psArgs | Out-Null
        Write-Host ("[workspace] launched '{0}' in PowerShell." -f $key)
    }
}

function Get-MonitorNamespaces {
    param([string]$NamespaceArg = "")
    if ([string]::IsNullOrWhiteSpace($NamespaceArg)) {
        return @($script:state.ActiveNamespace)
    }
    $arg = $NamespaceArg.Trim().ToLowerInvariant()
    if ($arg -ne "all") {
        return @($NamespaceArg.Trim())
    }
    $dirs = Get-ChildItem -Path $script:NamespacesDir -Directory -ErrorAction SilentlyContinue
    if ($null -eq $dirs -or $dirs.Count -eq 0) {
        return @($script:state.ActiveNamespace)
    }
    return @($dirs | ForEach-Object { $_.Name } | Sort-Object -Unique)
}

function Get-TaskSummary {
    param([Parameter(Mandatory = $true)][string]$Namespace)
    Ensure-Project -Namespace $Namespace
    $raw = (Invoke-RelayMesh "--root $($script:state.Root) --namespace $Namespace tasks --limit 200" | Out-String)
    $rows = @()
    if (-not [string]::IsNullOrWhiteSpace($raw)) {
        try {
            $parsed = $raw | ConvertFrom-Json
            $rows = @($parsed)
        } catch {
            $rows = @()
        }
    }
    $counts = @{}
    foreach ($row in $rows) {
        $status = [string]$row.status
        if ([string]::IsNullOrWhiteSpace($status)) { $status = "UNKNOWN" }
        if (-not $counts.ContainsKey($status)) { $counts[$status] = 0 }
        $counts[$status] = [int]$counts[$status] + 1
    }
    return [ordered]@{
        namespace = $Namespace
        total = $rows.Count
        counts = $counts
    }
}

function Show-MonitorSnapshot {
    param([string]$NamespaceArg = "")
    $namespaces = Get-MonitorNamespaces -NamespaceArg $NamespaceArg
    Write-Host ("[monitor] {0}  namespaces={1}" -f (Get-Date).ToString("yyyy-MM-dd HH:mm:ss"), ($namespaces -join ", "))
    foreach ($ns in $namespaces) {
        $sum = Get-TaskSummary -Namespace $ns
        $parts = @()
        foreach ($k in @("SUCCESS", "RUNNING", "RETRYING", "DEAD_LETTER", "PENDING")) {
            if ($sum.counts.ContainsKey($k)) {
                $parts += ("{0}={1}" -f $k, $sum.counts[$k])
            }
        }
        foreach ($k in $sum.counts.Keys) {
            if (@("SUCCESS", "RUNNING", "RETRYING", "DEAD_LETTER", "PENDING") -notcontains $k) {
                $parts += ("{0}={1}" -f $k, $sum.counts[$k])
            }
        }
        $detail = if ($parts.Count -eq 0) { "none" } else { $parts -join ", " }
        Write-Host ("  - {0}: total={1}; {2}" -f $sum.namespace, $sum.total, $detail)
    }
    Write-Host ("  workers={0}, services={1}" -f $script:state.Workers.Count, $script:state.Services.Count)
}

function Watch-MonitorSnapshot {
    param(
        [string]$NamespaceArg = "",
        [int]$IntervalSec = 2,
        [int]$Iterations = 10
    )
    $sleepMs = [Math]::Max(1, $IntervalSec) * 1000
    $count = if ($Iterations -lt 0) { 0 } else { $Iterations }
    $i = 0
    while ($true) {
        Clear-Host
        Show-MonitorSnapshot -NamespaceArg $NamespaceArg
        $i++
        if ($count -gt 0 -and $i -ge $count) {
            break
        }
        Start-Sleep -Milliseconds $sleepMs
    }
}

function Save-SessionSnapshot {
    param([string]$Reason = "")
    if (-not $script:state.SessionEnabled) {
        return
    }
    try {
        $workers = @()
        foreach ($w in $script:state.Workers.Values) {
            $workers += [ordered]@{
                name = [string]$w.WorkerId
                namespace = [string]$w.Namespace
                agentHint = [string]$w.AgentHint
                topology = [string]$w.Topology
            }
        }
        $aliases = [ordered]@{}
        foreach ($k in $script:state.Aliases.Keys) {
            $aliases[$k] = [string]$script:state.Aliases[$k]
        }
        $templates = [ordered]@{}
        foreach ($k in $script:state.Templates.Keys) {
            $t = $script:state.Templates[$k]
            $templates[$k] = [ordered]@{
                agent = [string]$t.agent
                priority = [string]$t.priority
                input = [string]$t.input
            }
        }
        $snapshot = [ordered]@{
            savedAt = (Get-Date).ToString("o")
            reason = $Reason
            root = [string]$script:state.Root
            activeNamespace = [string]$script:state.ActiveNamespace
            autoTopology = [string]$script:state.LastTopology
            workerSpecs = $workers
            aliases = $aliases
            templates = $templates
            noWeb = (-not $script:state.Services.ContainsKey("web"))
            noMetrics = (-not $script:state.Services.ContainsKey("metrics"))
        }
        $sessionDir = Split-Path -Path $script:state.SessionFile -Parent
        if (-not [string]::IsNullOrWhiteSpace($sessionDir)) {
            New-Item -Path $sessionDir -ItemType Directory -Force | Out-Null
        }
        $snapshot | ConvertTo-Json -Depth 8 | Set-Content -Path $script:state.SessionFile -Encoding UTF8
    } catch {
        Write-Host "[session] warning: failed to save session snapshot."
    }
}

function Show-SessionSnapshot {
    if (-not (Test-Path $script:state.SessionFile)) {
        Write-Host "[session] no snapshot found."
        return
    }
    try {
        $raw = Get-Content -Path $script:state.SessionFile -Raw -ErrorAction Stop
        $node = $raw | ConvertFrom-Json
        Write-Host ("[session] file={0}" -f $script:state.SessionFile)
        Write-Host ("  savedAt={0}" -f $node.savedAt)
        Write-Host ("  root={0}" -f $node.root)
        Write-Host ("  activeNamespace={0}" -f $node.activeNamespace)
        Write-Host ("  autoTopology={0}" -f $node.autoTopology)
        $count = @($node.workerSpecs).Count
        Write-Host ("  workers={0}" -f $count)
        $aliasCount = if ($null -eq $node.aliases) { 0 } else { @($node.aliases.PSObject.Properties).Count }
        $templateCount = if ($null -eq $node.templates) { 0 } else { @($node.templates.PSObject.Properties).Count }
        Write-Host ("  aliases={0}, templates={1}" -f $aliasCount, $templateCount)
    } catch {
        Write-Host "[session] failed to parse snapshot."
    }
}

function Show-Welcome {
    Write-Host ""
    Write-Host "RelayMesh Studio Hub"
    Write-Host "One Terminal. Multi-Agent. Multi-Project."
    Write-Host ("Repo: {0}" -f $script:RepoRoot)
    Write-Host ("Root: {0}" -f $script:state.Root)
    if ($script:state.SessionRestored) {
        Write-Host ("[session] restored from {0}" -f $script:state.SessionFile)
    }
    Write-Host ""
}

function Show-QuickPanel {
@"
Quick Panel
===========
Core:
  panel | palette [query] | help | status | exit

Fast Start:
  topology up dual
  submith echo normal hello
  tasksns project-a
  tasksns project-b

Session:
  session show
  session save
  session clear

Productivity:
  alias list
  template list
  template run echo-quick
  monitor
  monitor watch all 2 10

Workspaces:
  workspace save dev dual
  workspace list
  workspace launch dev

Hotkeys (Windows Terminal):
  Ctrl+Shift+F  find
  Alt+Shift+D   split pane
"@ | Write-Host
}

function Show-Help {
@"
Agent Hub Commands
==================
panel
palette [query]
palette run <index> [query]
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

monitor [namespace|all]
monitor watch [namespace|all] [intervalSec] [iterations]

alias list
alias set <name> <expansion>
alias unset <name>

template list
template add <name> <agent> <priority> <text>
template run <name> [namespace]
template remove <name>

workspace list
workspace save <name> [topology]
workspace show <name>
workspace delete <name>
workspace launch <name>

session show
session save
session clear

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
    param([AllowEmptyString()][string]$Line = "")
    $raw = $Line.Trim()
    if ([string]::IsNullOrWhiteSpace($raw)) { return $true }
    $raw = Resolve-HubAlias -RawLine $raw

    $firstSplit = $raw.Split(" ", 2, [System.StringSplitOptions]::RemoveEmptyEntries)
    $cmd = $firstSplit[0].ToLowerInvariant()
    $rest = if ($firstSplit.Length -gt 1) { $firstSplit[1] } else { "" }

    try {
        switch ($cmd) {
            "panel" {
                Show-QuickPanel
            }
            "palette" {
                $parts = Split-Args $rest
                if ($parts.Count -ge 1 -and $parts[0].ToLowerInvariant() -eq "run") {
                    if ($parts.Count -lt 2) { throw "Usage: palette run <index> [query]" }
                    $index = [int]$parts[1]
                    if ($index -lt 1) { throw "index must be >= 1" }
                    $query = if ($parts.Count -ge 3) { ($parts[2..($parts.Count - 1)] -join " ") } else { "" }
                    $matches = Show-Palette -Query $query
                    if ($matches.Count -lt $index) { throw "palette index out of range" }
                    $selected = [string]$matches[$index - 1].cmd
                    Write-Host ("[palette] run: {0}" -f $selected)
                    $keep = Invoke-HubCommand -Line $selected
                    if (-not $keep) { return $false }
                } else {
                    Show-Palette -Query $rest.Trim() | Out-Null
                }
            }
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
            "monitor" {
                $parts = Split-Args $rest
                if ($parts.Count -ge 1 -and $parts[0].ToLowerInvariant() -eq "watch") {
                    $ns = if ($parts.Count -ge 2) { $parts[1] } else { $script:state.ActiveNamespace }
                    $interval = if ($parts.Count -ge 3) { [int]$parts[2] } else { 2 }
                    $iterations = if ($parts.Count -ge 4) { [int]$parts[3] } else { 10 }
                    Watch-MonitorSnapshot -NamespaceArg $ns -IntervalSec $interval -Iterations $iterations
                } else {
                    $ns = if ($parts.Count -ge 1) { $parts[0] } else { $script:state.ActiveNamespace }
                    Show-MonitorSnapshot -NamespaceArg $ns
                }
            }
            "alias" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) { throw "Usage: alias list | alias set <name> <expansion> | alias unset <name>" }
                switch ($parts[0].ToLowerInvariant()) {
                    "list" {
                        if ($script:state.Aliases.Count -eq 0) {
                            Write-Host "(no aliases)"
                        } else {
                            foreach ($k in $script:state.Aliases.Keys) {
                                Write-Host ("{0}`t=> {1}" -f $k, $script:state.Aliases[$k])
                            }
                        }
                    }
                    "set" {
                        if ($parts.Count -lt 3) { throw "Usage: alias set <name> <expansion>" }
                        $name = $parts[1].Trim().ToLowerInvariant()
                        if ([string]::IsNullOrWhiteSpace($name)) { throw "alias name cannot be empty" }
                        $expansion = ($parts[2..($parts.Count - 1)] -join " ").Trim()
                        if ([string]::IsNullOrWhiteSpace($expansion)) { throw "alias expansion cannot be empty" }
                        $script:state.Aliases[$name] = $expansion
                        Write-Host ("[alias] set {0} => {1}" -f $name, $expansion)
                    }
                    "unset" {
                        if ($parts.Count -lt 2) { throw "Usage: alias unset <name>" }
                        $name = $parts[1].Trim().ToLowerInvariant()
                        if ($script:state.Aliases.Contains($name)) {
                            $script:state.Aliases.Remove($name) | Out-Null
                            Write-Host ("[alias] removed {0}" -f $name)
                        } else {
                            Write-Host ("[alias] '{0}' not found" -f $name)
                        }
                    }
                    default {
                        throw "Usage: alias list | alias set <name> <expansion> | alias unset <name>"
                    }
                }
            }
            "template" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) {
                    throw "Usage: template list | template add <name> <agent> <priority> <text> | template run <name> [namespace] | template remove <name>"
                }
                switch ($parts[0].ToLowerInvariant()) {
                    "list" {
                        Show-Templates
                    }
                    "add" {
                        if ($rest -notmatch '^(?i:add)\s+(?<name>\S+)\s+(?<agent>\S+)\s+(?<priority>high|normal|low)\s+(?<input>.+)$') {
                            throw "Usage: template add <name> <agent> <priority> <text>"
                        }
                        $name = $Matches["name"].Trim().ToLowerInvariant()
                        $agent = $Matches["agent"].Trim()
                        $priority = Normalize-Priority $Matches["priority"]
                        $input = $Matches["input"].Trim()
                        $script:state.Templates[$name] = [ordered]@{ agent = $agent; priority = $priority; input = $input }
                        Write-Host ("[template] saved '{0}'" -f $name)
                    }
                    "run" {
                        if ($parts.Count -lt 2) { throw "Usage: template run <name> [namespace]" }
                        $name = $parts[1]
                        $ns = if ($parts.Count -ge 3) { $parts[2] } else { "" }
                        Submit-FromTemplate -TemplateName $name -Namespace $ns
                    }
                    "remove" {
                        if ($parts.Count -lt 2) { throw "Usage: template remove <name>" }
                        $name = $parts[1].Trim().ToLowerInvariant()
                        if ($script:state.Templates.Contains($name)) {
                            $script:state.Templates.Remove($name) | Out-Null
                            Write-Host ("[template] removed '{0}'" -f $name)
                        } else {
                            Write-Host ("[template] '{0}' not found" -f $name)
                        }
                    }
                    default {
                        throw "Usage: template list | template add <name> <agent> <priority> <text> | template run <name> [namespace] | template remove <name>"
                    }
                }
            }
            "workspace" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) {
                    throw "Usage: workspace list | workspace save <name> [topology] | workspace show <name> | workspace delete <name> | workspace launch <name>"
                }
                switch ($parts[0].ToLowerInvariant()) {
                    "list" {
                        Show-WorkspaceProfiles
                    }
                    "save" {
                        if ($parts.Count -lt 2) { throw "Usage: workspace save <name> [topology]" }
                        $name = $parts[1].Trim().ToLowerInvariant()
                        $topology = if ($parts.Count -ge 3) { $parts[2].Trim().ToLowerInvariant() } else { $script:state.LastTopology }
                        $profiles = Get-WorkspaceProfiles
                        $profiles[$name] = [ordered]@{
                            root = [string]$script:state.Root
                            defaultNamespace = [string]$script:state.ActiveNamespace
                            autoTopology = [string]$topology
                            noWeb = (-not $script:state.Services.ContainsKey("web"))
                            noMetrics = (-not $script:state.Services.ContainsKey("metrics"))
                            createdAt = (Get-Date).ToString("o")
                        }
                        Save-WorkspaceProfiles -Profiles $profiles
                        Write-Host ("[workspace] saved '{0}'" -f $name)
                    }
                    "show" {
                        if ($parts.Count -lt 2) { throw "Usage: workspace show <name>" }
                        Show-WorkspaceProfile -Name $parts[1]
                    }
                    "delete" {
                        if ($parts.Count -lt 2) { throw "Usage: workspace delete <name>" }
                        $name = $parts[1].Trim().ToLowerInvariant()
                        $profiles = Get-WorkspaceProfiles
                        if ($profiles.Contains($name)) {
                            $profiles.Remove($name)
                            Save-WorkspaceProfiles -Profiles $profiles
                            Write-Host ("[workspace] removed '{0}'" -f $name)
                        } else {
                            Write-Host ("[workspace] '{0}' not found" -f $name)
                        }
                    }
                    "launch" {
                        if ($parts.Count -lt 2) { throw "Usage: workspace launch <name>" }
                        Launch-WorkspaceProfile -Name $parts[1]
                    }
                    default {
                        throw "Usage: workspace list | workspace save <name> [topology] | workspace show <name> | workspace delete <name> | workspace launch <name>"
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
            "session" {
                $parts = Split-Args $rest
                if ($parts.Count -lt 1) {
                    throw "Usage: session show | session save | session clear"
                }
                switch ($parts[0].ToLowerInvariant()) {
                    "show" {
                        Show-SessionSnapshot
                    }
                    "save" {
                        $script:state.SessionEnabled = $true
                        Save-SessionSnapshot -Reason "manual"
                        Write-Host "[session] snapshot saved."
                    }
                    "clear" {
                        $script:state.SessionEnabled = $false
                        if (Test-Path $script:state.SessionFile) {
                            Remove-Item -Path $script:state.SessionFile -Force
                            Write-Host "[session] snapshot cleared."
                        } else {
                            Write-Host "[session] no snapshot to clear."
                        }
                    }
                    default {
                        throw "Usage: session show | session save | session clear"
                    }
                }
            }
            "exit" {
                Save-SessionSnapshot -Reason "exit"
                return $false
            }
            default {
                Write-Host "Unknown command: $cmd"
                Write-Host "Type 'panel' for quick actions or 'help' for full commands."
            }
        }
    } catch {
        Write-Host "ERROR: $($_.Exception.Message)"
    }
    return $true
}

Show-Welcome

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
if ($RestoredWorkerSpecs -ne $null -and $RestoredWorkerSpecs.Count -gt 0) {
    foreach ($spec in $RestoredWorkerSpecs) {
        $name = [string]$spec.name
        $ns = [string]$spec.namespace
        $hint = [string]$spec.agentHint
        $topology = [string]$spec.topology
        if ([string]::IsNullOrWhiteSpace($name) -or [string]::IsNullOrWhiteSpace($ns)) {
            continue
        }
        if ($script:state.Workers.ContainsKey($name)) {
            continue
        }
        try {
            Start-Worker -Name $name -Namespace $ns -AgentHint $hint -Topology $topology
        } catch {
            Write-Host ("[session] skip worker '{0}': {1}" -f $name, $_.Exception.Message)
        }
    }
}

Show-Status
if ($ShowFullHelpOnStart) {
    Show-Help
} else {
    Show-QuickPanel
}
Save-SessionSnapshot -Reason "startup"

if ($RunCommands -ne $null -and $RunCommands.Count -gt 0) {
    foreach ($cmd in $RunCommands) {
        Write-Host ""
        Write-Host ("hub[{0}]> {1}" -f $state.ActiveNamespace, $cmd)
        $keep = Invoke-HubCommand -Line $cmd
        if ($keep) {
            Save-SessionSnapshot -Reason "run-command"
        }
        if (-not $keep) { break }
    }
}

if (-not $NoInteractive) {
    while ($true) {
        $line = Read-Host ("hub[{0}]" -f $state.ActiveNamespace)
        if ($null -eq $line) { $line = "" }
        $keep = Invoke-HubCommand -Line $line
        if ($keep) {
            Save-SessionSnapshot -Reason "interactive"
        }
        if (-not $keep) { break }
    }
}

Save-SessionSnapshot -Reason "shutdown"
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
