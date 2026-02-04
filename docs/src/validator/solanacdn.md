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
- optionally enables fair transaction ordering for transactions submitted via SolanaCDN
- optionally audits/enforces fair ordering using leader-signed commits and ledger audit

## Enable (Pipe-managed POP discovery)

If you have a Pipe API key, you can enable SolanaCDN without specifying POP endpoints:

- Set `PIPE_API_KEY` (or `SOLANACDN_AGENT_API_TOKEN`) in the environment.
- Run the validator with your normal args, plus:
  - `--solanacdn-api-token <TOKEN>` (optional if using env)
  - (optional) `--solanacdn-only` to ingest only SolanaCDN-sourced shreds when connected (fallback to P2P when disconnected)
  - (optional) `--solanacdn-hybrid` to prefer SolanaCDN shreds when healthy, but fall back to P2P if SolanaCDN stalls while connected (`--solanacdn-hybrid-stale-ms` controls the stall threshold)

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

## Fair transaction ordering (experimental)

Enable fair transaction ordering for SolanaCDN-submitted flow:

- `--fair`

This affects only the SolanaCDN fair-batch path (it does not change how P2P-gossip transactions are prioritized).

When enabled, the validator advertises `tx_fair_ordering` to the POP and **expects transactions via `FairBatch`** (batch-of-1 is fine). Legacy `RelayTransaction` submissions are dropped to avoid bypassing the fair-ordering domain.

## Fair slashing (experimental)

Audit-only mode (records evidence/counters, does not change voting):

- `--fair-slashing`

Enforcement mode (implies `--fair-slashing`; withholds votes when a fair ordering violation is observed):

- `--fair-slashing-enforce`

In enforcement mode, vote withholding triggers on either:

- ledger audit failure, or
- commit equivocation (conflicting leader-signed fair ordering commits)

### Enforcement kill switch (admin RPC)

To disable enforcement immediately (no restart), call the admin RPC:

- `solanaCdnSetFairSlashingEnforce(false)` (forces enforcement off)

To restore the startup configuration:

- `solanaCdnClearFairSlashingEnforceOverride()` (returns to `--fair-slashing-enforce` / default)

## Observability

- Metrics + status: `--solanacdn-metrics-addr HOST:PORT` exposes Prometheus at `/metrics` and JSON status at `/solanacdn/status`.
- Admin RPC: `solanaCdnStatus` returns the same `SolanaCdnStatus` JSON (includes fair/slashing counters and enable flags).
- In `--solanacdn-hybrid` mode, `tvu_shred_stale` / `tvu_shred_stale_for_ms` reflect time since the last shred accepted into the validator pipeline (compare with `last_shred_*` to diagnose delivery vs discard).
- Race metrics (SolanaCDN vs gossip): enabled by default; disable with `--solanacdn-race=false`. Tune via `--solanacdn-race-sample-bits` and `--solanacdn-race-window-ms` (compatible with `--solanacdn-only` / `--solanacdn-hybrid`; does not change shred ingest mode).
- Fair Prometheus counters include fair-batch tx receive/inject totals (`solanacdn_tx_fair_batch_received_total`, `solanacdn_tx_fair_batch_injected_total`) and fair-priority lookups/hits (`solanacdn_fair_priority_lookups_total`, `solanacdn_fair_priority_hits_total`).
- Transaction hygiene counters include dedup drops (`solanacdn_tx_deduped_packets_total`) and relay drops in fair mode (`solanacdn_tx_relay_dropped_fair_mode_total`).
- Fair/slashing Prometheus counters include commits, audit failures, and vote withholding (`solanacdn_fair_votes_withheld_total`).

## Notes / limitations

- Vote tunneling is UDP-only. If validator QUIC votes are enabled, SolanaCDN vote tunneling is disabled automatically.
- `--fair-slashing-enforce` can reduce vote participation if violations are detected; use with care.
