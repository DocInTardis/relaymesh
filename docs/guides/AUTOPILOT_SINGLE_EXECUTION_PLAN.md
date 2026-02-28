# RelayMesh Single Autopilot Execution Plan

Updated: 2026-02-28
Mode: Full Autopilot (no pause, execute in order)

This document is the single source of truth for ongoing engineering execution.
It consolidates architecture direction, implementation tasks, acceptance criteria, and execution records.

## 1. End-State Definition

After one-click startup, operators can:

- enter one control terminal
- start and stop multiple agents/workers freely
- run multiple projects in parallel by namespace
- run multiple workers on one project concurrently
- monitor all activity in control-room panes with shared backend data
- switch panes with keyboard-only workflows

## 2. Non-Negotiables

- Keep Java mainline active and continuously shippable.
- Preserve route/API compatibility while refactoring internals.
- Every refactor phase must finish with compile + test pass.
- Every phase updates this document with execution evidence.

## 3. Phase Matrix

- [x] Phase A-H: Control-room feature delivery (APIs, UI, SSE, command bridge, profile persistence).
- [x] Phase I: Maintainability refactor round 1 (page resource extraction, parser/store extraction).
- [x] Phase J: Maintainability refactor round 2 (namespace/snapshot/workflow helpers extraction).
- [x] Phase K: Control-room API integration test coverage expansion.
- [x] Phase L: serve-web route registration modularization.
- [ ] Phase M: desktop shell and packaging path.

## 4. Current Execution Batch (Phase J)

### 4.1 Tasks

- [x] J1: Extract namespace/runtime/snapshot/workflow helper logic from `RelayMeshCommand`.
- [x] J2: Wire all control-room call sites to new helper module.
- [x] J3: Add unit tests for extracted helper behaviors.
- [x] J4: Run `mvn -q -DskipTests compile` and `mvn -q test`.
- [x] J5: Commit and push to `origin/main`.

### 4.2 Acceptance

- `RelayMeshCommand.java` keeps shrinking without behavior changes.
- Control-room APIs still compile and pass test suite.
- Extracted helper module is directly testable.

## 5. Execution Log

- 2026-02-28: document created as unified autopilot contract.
- 2026-02-28: extracted `ControlRoomRuntimeSupport` and migrated namespace/snapshot/workflow helper call sites.
- 2026-02-28: added `ControlRoomRuntimeSupportTest` and passed compile + full test run.
- 2026-02-28: committed and pushed Phase J as `b4bf325` to `origin/main`.
- 2026-02-28: executed `scripts/control_room_api_smoke.ps1` and validated key control-room API paths (all checks passed).
- 2026-02-28: committed and pushed Phase K as `507778c` to `origin/main`.
- 2026-02-28: extracted control-room route registration from `serve-web` main flow into `registerControlRoomRoutes(...)`.
- 2026-02-28: re-validated with compile + full test + control-room API smoke script.

## 6. Current Execution Batch (Phase K)

### 6.1 Tasks

- [x] K1: Add control-room API smoke script to cover read/write endpoints.
- [x] K2: Cover profile APIs (list/save/get/delete) in smoke flow.
- [x] K3: Cover command/action auth behavior (ro denied, rw allowed) in smoke flow.
- [x] K4: Execute script and verify all checks pass.
- [x] K5: Commit and push to `origin/main`.

### 6.2 Acceptance

- Control-room key APIs are validated by a reproducible script.
- Script verifies both success and permission-denied paths.
- Script can be run by operators without modifying source code.

## 7. Current Execution Batch (Phase L)

### 7.1 Tasks

- [x] L1: Extract control-room route registration out of `ServeWebCommand.call()`.
- [x] L2: Keep all control-room routes and auth behavior unchanged.
- [x] L3: Re-run compile and full tests after modularization.
- [x] L4: Re-run control-room API smoke script for behavior regression check.
- [ ] L5: Commit and push to `origin/main`.

### 7.2 Acceptance

- `ServeWebCommand.call()` is shorter and clearer.
- Control-room route wiring lives in a dedicated helper method.
- Existing control-room API behavior remains intact after refactor.
