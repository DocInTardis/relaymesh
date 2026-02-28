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

## 3.7 Phase G - Command Backend + Workflow View

Delivered:

- `GET /api/control-room/workflow` for namespace-scoped workflow fetch
- `POST /api/control-room/command` for command bridge execution
- workflow pane mode in control-room UI with `focus task id`
- command bar upgraded to call backend command endpoint

Acceptance:

- workflow data can be fetched by both direct API and command bridge
- command bridge supports read + write ops with auth-aware behavior
- control-room page can inspect task DAG and execute commands without page switch

## 3.8 Phase H - Backend Layout Profiles

Delivered:

- `GET /api/control-room/layouts` and `GET /api/control-room/layouts?name=<profile>`
- `POST /api/control-room/layouts/save` and `POST /api/control-room/layouts/delete`
- profile controls in UI (save/load/delete/refresh)
- profile commands in command bar (`profile list|save|load|delete`)

Acceptance:

- layout profiles can survive browser changes because they are persisted server-side
- profile APIs are auth-aware (read vs write permission)
- UI can switch between saved profiles without leaving control-room

## 3.9 Phase I - Maintainability Refactor

Delivered:

- extracted control-room frontend page to `src/main/resources/web/control-room.html`
- added `ControlRoomPage` loader class for embedded static asset loading
- extracted command parser helpers to `ControlRoomCommandParser`
- extracted profile persistence helpers to `ControlRoomLayoutStore`
- added focused tests for page loading, parser behavior, and layout store round-trip

Acceptance:

- `RelayMeshCommand.java` reduced in size without route/behavior changes
- control-room APIs and UI entry still compile and run through existing tests
- control-room support logic is independently testable in dedicated classes

## 3.10 Phase J - Maintainability Refactor Round 2

Delivered:

- extracted namespace/runtime snapshot helpers to `ControlRoomRuntimeSupport`
- migrated control-room route handlers to call `ControlRoomRuntimeSupport` methods
- removed duplicated namespace/snapshot/workflow edge helper code from `RelayMeshCommand`
- added dedicated tests for namespace resolution/discovery, workflow edge generation, and snapshot payload shape

Acceptance:

- `RelayMeshCommand.java` complexity reduced further while preserving API behavior
- compile and test suite pass after helper migration
- control-room runtime support logic is reusable and directly testable

## 3.11 Phase K - API Smoke Coverage

Delivered:

- added `scripts/control_room_api_smoke.ps1` for control-room API integration smoke checks
- smoke flow covers:
  - control-room page + snapshot read paths
  - command bridge read path and write permission denial for RO token
  - profile list/save/get/delete paths
  - action endpoint write permission behavior for RO vs RW token

Acceptance:

- script runs end-to-end and reports success on healthy local environment
- control-room auth boundaries are validated through script assertions
- operators can run one command to verify core control-room APIs

## 3.12 Phase L - Route Registration Modularization

Delivered:

- extracted control-room route setup from `ServeWebCommand.call()` into `registerControlRoomRoutes(...)`
- kept route handlers and auth/write-limit checks unchanged
- re-validated with compile, full test suite, and control-room API smoke script

Acceptance:

- serve-web main flow is shorter and easier to reason about
- control-room registration is isolated for future class-level extraction
- no behavioral regression observed in automated validation runs

## 4. Next Iteration Backlog

Planned for future autopilot rounds:

- richer workflow pane with dependency graph and click-through node detail
- desktop shell packaging (JavaFX/Tauri/Electron) with native shortcuts
- multi-screen docking layouts and monitor-wall mode (cross-monitor placement)
