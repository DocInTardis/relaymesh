# Security Policy

## Reporting

If you discover a security issue, do not open a public issue immediately.

Please report details to project maintainers with:

- affected component
- reproduction steps
- impact assessment
- suggested mitigation (if available)

## Response Targets

- Acknowledge report: within 72 hours
- Initial triage: within 7 days
- Patch or mitigation plan: as soon as practical based on severity

## Scope

Security-sensitive surfaces include:

- Node RPC TLS/mTLS and certificate handling
- Gossip packet signing and verification
- Replication import/export endpoints
- Web write APIs and auth/rate-limiting paths
