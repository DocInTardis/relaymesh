# Node RPC Security (TLS, mTLS, Rotation, Revocation)

This document describes the RelayMesh `serve-node-rpc` security baseline and Phase C workflow.

## Features

- HTTPS transport for node RPC endpoints.
- Optional mTLS (`--require-client-auth=true`) with truststore validation.
- Optional revocation file (`--revocation-file`) enforced by trust manager.
- Hot reload of keystore/truststore/revocation file (`--reload-interval-ms`).
- Runtime TLS status endpoint: `GET /node/tls/status`.

## Commands

Start node RPC with TLS:

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-data serve-node-rpc --bind 127.0.0.1 --port 18443 --keystore demo-data/tls/node.p12 --keystore-pass changeit --keystore-type PKCS12"
```

Start with mTLS + revocation:

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-data serve-node-rpc --bind 127.0.0.1 --port 18443 --keystore demo-data/tls/node.p12 --keystore-pass changeit --keystore-type PKCS12 --truststore demo-data/tls/trust.p12 --truststore-pass changeit --truststore-type PKCS12 --require-client-auth=true --revocation-file demo-data/tls/revocations.txt --reload-interval-ms 1000"
```

Generate revocation template:

```powershell
mvn -q exec:java "-Dexec.args=--root=demo-data node-rpc-revocation-template --out demo-data/tls/revocations.txt"
```

## Revocation File Format

Line-oriented format:

```text
# comments are allowed
serial:1A2B3C
sha256:0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF
```

JSON format is also supported:

```json
{
  "serialNumbers": ["1A2B3C"],
  "sha256Fingerprints": ["0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF"]
}
```

Notes:

- Fingerprints must be SHA-256 hex (`64` hex chars).
- Serial numbers are normalized as hex and compared case-insensitively.
- UTF-8 and UTF-16 BOM inputs are supported.

## Rotation Workflow

1. Start `serve-node-rpc` with `--reload-interval-ms`.
2. Replace keystore file atomically (same path).
3. Optionally update revocation file.
4. Check `/node/tls/status` until `serverCertFingerprintSha256` changes and `revokedEntryCount` matches expected value.

Example status query:

```powershell
curl.exe -k https://127.0.0.1:18443/node/tls/status
```

## Validation

Smoke scripts:

- `scripts/v2_node_rpc_tls_smoke.ps1`
- `scripts/v2_node_rpc_rotation_smoke.ps1`

Unit tests:

- `src/test/java/io/relaymesh/security/NodeRpcTlsTest.java`
