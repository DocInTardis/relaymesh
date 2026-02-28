# Agent Hub Quickstart

`scripts/agent_hub.ps1` gives you a single-terminal control plane for RelayMesh:

- one-click startup
- launch/stop multiple workers (agent executors)
- isolate by project (`namespace`)
- label workers with `agent-hint` for quick routing
- bring up/down preset topologies for common team layouts
- command palette + aliases for faster operator workflows
- reusable task templates and live monitor snapshots
- workspace profiles that open new terminal sessions in one command
- submit tasks and workflows without leaving the same terminal
- inspect status and tail logs
- open Control Room (`/control-room`) for multi-pane live observability

## Start Hub

From repository root:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/agent_hub.ps1
```

This boots:

- runtime root under `tmp/agent-hub-root-<timestamp>`
- web console at `http://127.0.0.1:18080/?token=relay_ro`
- metrics endpoint at `http://127.0.0.1:19090/metrics`
- one default worker (`w1`)

Control Room URL (same auth token rules as classic web console):

- `http://127.0.0.1:18080/control-room?token=relay_ro`

Control Room enhancements:

- switch transport mode between `sse` (live stream) and `poll`
- save and restore pane layouts in browser local storage
- apply preset layouts (`ops` / `incident` / `throughput` / `audit`)

Fast preset startup (no default workers, directly build a dual-project topology):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/agent_hub.ps1 -AutoWorkers 0 -AutoTopology dual
```

Session restore behavior (default):

- if no `-Root` is provided, hub auto-resumes last snapshot from `tmp/agent-hub-session.json`
- resume includes previous `root`, active namespace, and topology/worker layout

Disable/clear restore if needed:

```powershell
# start fresh without using saved snapshot
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/agent_hub.ps1 -NoAutoRestore

# clear saved snapshot before startup
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/agent_hub.ps1 -ResetSession
```

## Core Commands

Inside the hub prompt:

```text
panel
palette [query]
palette run <index> [query]
project use <namespace>
worker start <name> [namespace] [agent-hint]
worker stop <name|all>
topology up <team-a|team-b|one-project|dual>
topology down <preset|all>
topology list
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
submit <agent> <priority> <text>
submitns <namespace> <agent> <priority> <text>
submith <agent-hint> <priority> <text>
session show
session save
session clear
tasks [status] [limit]
tasksns <namespace> [status] [limit]
status
help
exit
```

## Example: Multiple Agents, Multiple Projects

Option A (recommended): start from preset topology.

```text
topology up dual
submith echo high A-or-B-routed-by-hint
tasksns project-a
tasksns project-b
```

Option B: manual workers with explicit namespaces and hints.

```text
project create project-a
project create project-b

worker start a1 project-a echo
worker start a2 project-a fail
worker start b1 project-b echo

submitns project-a echo high A-task-1
submitns project-a fail low A-task-2
submitns project-b echo normal B-task-1

tasksns project-a
tasksns project-b
```

## Example: Multiple Agents on One Project

```text
project use project-a
worker start a3 project-a echo
worker start a4 project-a fail
submit echo normal one-project-load-1
submit echo high one-project-load-2
submit fail low one-project-failure-case
tasks
```

## Example: Productivity Features

```text
palette topo
palette run 1 topo

alias set ls status
ls

template add triage echo high investigate-failure
template run triage project-a

monitor
monitor watch all 2 5
```

## Example: Workspace Profiles (Terminal-in-Terminal)

```text
workspace save prod dual
workspace show prod
workspace launch prod
```

`workspace launch` opens a new Windows Terminal window/tab with the saved root, namespace, and topology options.

## Log Tailing

```text
tail worker:a1 out 80
tail service:web out 80
tail service:metrics out 80
```

## Notes

- In RelayMesh, workers consume tasks from the same namespace queue. Running more workers increases parallel processing.
- `submith` resolves namespace from a running worker's `agent-hint`, then submits with that hint as `--agent`.
- Agent selection is controlled by submit payload (`--agent ...`), while worker count controls throughput/concurrency.
- Use namespaces as project boundaries when you want one terminal to orchestrate multiple projects simultaneously.
- `topology down all` stops all workers created from topology presets.
- default startup shows a compact `panel`; use `help` for full command reference.
- alias/template/workspace state is captured in the session snapshot for next launch restore.
