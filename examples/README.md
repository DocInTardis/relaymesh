# Examples

Workflow input files for local testing are stored under `examples/workflows/`.

Use with:

```powershell
mvn -q exec:java "-Dexec.args=--root=tmp/local submit-workflow --file examples/workflows/simple.json"
```
