# Contributing

## Prerequisites

- Java 17
- Maven 3.9+
- PowerShell (for smoke scripts)

## Local Development

```powershell
mvn -q -DskipTests package
mvn -q test
```

## Project Conventions

- Keep runtime artifacts under `tmp/` (never in repository root).
- Keep example inputs under `examples/`.
- Keep architecture and rollout docs under `docs/`.
- Smoke/ops scripts live in `scripts/`.

## Validation Before PR

Run at least:

```powershell
mvn -q test
powershell -ExecutionPolicy Bypass -File scripts/v2_node_rpc_tls_smoke.ps1
powershell -ExecutionPolicy Bypass -File scripts/v2_replication_sync_smoke.ps1
```

For full validation, run the complete smoke matrix used in CI.

## Community Workflow

- Open issues using `.github/ISSUE_TEMPLATE/`.
- Use the PR checklist in `.github/pull_request_template.md`.
- Update `.github/CODEOWNERS` with real maintainers before enforcing required reviews.
