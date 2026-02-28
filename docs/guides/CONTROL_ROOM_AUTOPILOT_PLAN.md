# RelayMesh Control Room Autopilot Plan

Updated: 2026-02-28

This document is the execution contract for "autopilot mode". It records the target state, phase-by-phase tasks, and acceptance checks.

## 1. Target State

After one-click startup, operators should be able to:

- Observe multiple panes on one screen (control-room style)
- Keep all panes bound to one shared backend data source
- Switch pane focus quickly with keyboard shortcuts
- Mix multi-project and single-project views via namespace controls
- Keep compatibility with existing CLI and Hub workflows

## 2. Execution Principles

- Preserve compatibility first (`serve-web`, `agent_hub.ps1`, core CLI)
- Ship incremental upgrades, not risky one-shot rewrites
- Keep each phase compilable and runnable
- Update documentation with every behavior change

## 3. Phases and Acceptance

## 3.1 Phase A - Backend Route Expansion

Delivered:

- `GET /control-room`
- `GET /api/namespaces`
- `GET /api/control-room/snapshot`

Acceptance:

- `/` and `/control-room` both respond
- snapshot API returns structured JSON payload
- namespace auth constraints are enforced

## 3.2 Phase B - Control Room MVP UI

Delivered:

- 4-pane layout with per-pane namespace/view/limit/status controls
- keyboard switching (`Alt+1..9`, `Tab`, `Shift+Tab`, `Ctrl+R`)
- shared snapshot fetch and pane-level rendering

Acceptance:

- keyboard-only pane switching works
- no full-page crash when switching panes and filters

## 3.3 Phase C - Docs and Reading Path

Delivered:

- system manual with control-room operations
- reading guide path from UI -> API -> runtime
- hub quickstart references control-room usage

Acceptance:

- new operator can complete first run from docs

## 3.4 Phase D - Validation and Delivery

Delivered:

- `mvn -q -DskipTests compile`
- smoke checks for `/`, `/control-room`, snapshot API
- commit and push to `origin/main`

Acceptance:

- no compile errors
- key paths validated by local smoke tests

## 3.5 Phase E - Realtime + Persistence Upgrade

Delivered:

- `GET /events/control-room` SSE stream endpoint
- transport mode switch in UI (`sse` vs `poll`)
- local layout persistence (browser localStorage)
- pane preset templates (`ops`, `incident`, `throughput`, `audit`)
- share-link export for layout state (token excluded)

Acceptance:

- control room runs in streaming mode without manual refresh loop
- layout survives browser reload on same machine/profile
- preset switch updates panes immediately

## 3.6 Phase F - Operator Action Bridge + Dynamic Panes

Delivered:

- `POST /api/control-room/action` write-action endpoint
- control-room action buttons for `cancel`, `replay`, `replay_batch`
- dynamic pane count (2..9) with add/remove controls
- keyboard pane-size controls (`Ctrl++`, `Ctrl+-`)
- command bar in UI for operator command execution (`Ctrl+Enter`)

Acceptance:

- operators can execute write actions from control-room without leaving page
- action endpoint enforces namespace auth and write permission
- pane count changes take effect immediately and persist in layout state
- command execution path works for `cancel`, `replay`, `replay-batch`, `preset`, `pane`, and `refresh`

## 4. Next Iteration Backlog

Planned for future autopilot rounds:

- persistent operator profiles in backend (not only browser local storage)
- richer workflow pane with dependency graph
- desktop shell packaging (JavaFX/Tauri/Electron) with native shortcuts
- multi-screen docking layouts and monitor-wall mode (cross-monitor placement)
