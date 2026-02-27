# Project Structure

RelayMesh follows a standard single-module Maven layout:

```text
relaymesh/
  .github/workflows/        # CI workflows
  docs/                     # architecture and rollout docs
  examples/                 # sample workflow inputs
  scripts/                  # smoke, benchmark, chaos scripts
  src/main/java/            # runtime implementation
  src/test/java/            # unit/integration tests
  tmp/                      # local runtime outputs (ignored)
  pom.xml                   # build definition
  README.md                 # project overview
  CONTRIBUTING.md           # contributor guide
  CODE_OF_CONDUCT.md        # community collaboration baseline
  SECURITY.md               # security disclosure policy
```

## Important Rules

- Do not commit runtime output directories.
- Use `tmp/...` as default root in scripts and local runs.
- Keep sample JSON under `examples/workflows/`.
