---
title: SolanaCDN (Integrated)
sidebar_position: 50
sidebar_label: SolanaCDN
pagination_label: SolanaCDN (Integrated)
---

This fork includes an **integrated SolanaCDN client** in the validator (no `solanacdn-agent` sidecar).

This feature is experimental/pre-release. For production today, the recommended path remains the
`solanacdn-agent` sidecar (so you can upgrade/rollback independently of validator binaries).

## What it does

When enabled, the validator:

- publishes inbound TVU shreds to a SolanaCDN POP (QUIC; optional UDP data plane)
- subscribes to POP shreds and injects them into local TVU/gossip
- optionally requests POP direct injection of raw shred UDP payloads to validator ports (lowest latency)
- tunnels UDP vote packets via the POP mesh (best-effort)
- posts per-run counters to the Pipe API ingest endpoint (so the Pipe console can show agent-style metrics)

## Enable (Pipe-managed POP discovery)

If you have a Pipe API key, you can enable SolanaCDN without specifying POP endpoints:

- Set `PIPE_API_KEY` (or `SOLANACDN_AGENT_API_TOKEN`) in the environment.
- Run the validator with your normal args, plus:
  - `--solanacdn-api-token <TOKEN>` (optional if using env)
  - (optional) `--solanacdn-only` to prefer SolanaCDN shreds when connected

The validator verifies the API key via `POST /v1/solanacdn-agent/verify` and uses the returned POP list.

## Enable (explicit POP list / private CA)

If you self-host POPs (or want to point at a specific POP directly), configure:

- `--solanacdn-pop <IP:PORT>` (repeatable)
- `--solanacdn-server-name <SNI>` (must match the POP certificate SAN)
- `--solanacdn-tls-ca-cert-path <FILE>` if using a private CA

Dev-only escape hatch:

- `--solanacdn-tls-insecure-skip-verify`

## TLS behavior

- By default, the validator verifies POP TLS certificates using the system/WebPKI trust roots.
- If you provide `--solanacdn-tls-ca-cert-path`, only that CA bundle is trusted for POP verification.
- If `/etc/solanacdn/tls/ca.crt` exists, it is used automatically as the POP/control CA bundle.

## Notes / limitations

- Vote tunneling is UDP-only. If validator QUIC votes are enabled, SolanaCDN vote tunneling is disabled automatically.
