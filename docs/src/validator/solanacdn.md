---
title: SolanaCDN (Integrated)
sidebar_position: 50
sidebar_label: SolanaCDN
pagination_label: SolanaCDN (Integrated)
---

This fork includes an **integrated SolanaCDN client** in the validator (no `solanacdn-agent` sidecar).

This is the recommended production path. The `solanacdn-agent` sidecar is deprecated and kept only
for legacy deployments.

## What it does

When enabled, the validator:

- publishes inbound TVU shreds to a SolanaCDN POP (QUIC; optional UDP data plane)
- subscribes to POP shreds and injects them into local TVU/gossip
- optionally requests POP direct injection of raw shred UDP payloads to validator ports (lowest latency)
- tunnels UDP vote packets via the POP mesh (best-effort) and injects POP vote-tunnel responses into
  the validator vote socket when enabled
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
- In the open-source build, POP TLS CA bootstrap via the Pipe API is opt-in only.
  Enable with `--solanacdn-api-tls-bootstrap`.

## Vote tunnel dedup tuning

- Vote-tunnel responses are deduplicated by default (TTL 2000ms, 200000 entries).
- Tune with `--solanacdn-vote-dedup-ttl-ms <MILLISECONDS>` (set `0` to disable).
- Tune with `--solanacdn-vote-dedup-max-entries <COUNT>` (set `0` to disable).

## Fair transaction ordering (experimental)

Enable fair transaction ordering for SolanaCDN-submitted flow:

- `--fair`

This affects only the SolanaCDN fair-batch path (it does not change how P2P-gossip transactions are prioritized).

When enabled, the validator advertises `tx_fair_ordering` to the POP and **expects transactions via `FairBatch`** (batch-of-1 is fine). Legacy `RelayTransaction` submissions are dropped to avoid bypassing the fair-ordering domain.

## Fair slashing (experimental)

Audit-only mode (records evidence/counters, does not change voting):

- `--fair-slashing`

Strict audit rules (implies `--fair-slashing`; treats missing committed transactions/commit chunks
as violations and requires the committed fair list to appear as a prefix, allowing only vote
transactions and commit-memo transactions ahead of it):

- `--fair-slashing-strict`

Witness mode (implies `--fair-slashing`; subscribes to leader-signed batch ACKs and POP-signed
batch witnesses so auditors can punish “leader ACKed but never committed it” and detect
ACK↔witness mismatches (and enforce strict audit on ACKed slots); requires POP support, protocol v7+):

- `--fair-slashing-witness`

Account-fence mode (implies `--fair-slashing`; treats any same-slot non-fair transaction that writes
to a non-signer account written by a committed fair transaction as a violation):

- `--fair-slashing-fence`

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
- Vote-tunnel counters include `solanacdn_rx_vote_packets_total` and `solanacdn_dropped_vote_datagrams_total`.
- Fair/slashing Prometheus counters include commits, audit failures, and vote withholding (`solanacdn_fair_votes_withheld_total`).

## Local POP stub (fair smoke test)

For a repeatable local-cluster `--fair` smoke test, use the lightweight POP stub binary included in this repo.

1. Start a local cluster with fair ordering enabled and SolanaCDN pointed at the stub:

   ```bash
   SOLANA_RUN_SH_VALIDATOR_ARGS="--fair --solanacdn-pop 127.0.0.1:9002 --solanacdn-tls-insecure-skip-verify --solanacdn-metrics-addr 127.0.0.1:9100" \\
     scripts/run.sh
   ```

   Or run the helper script (starts validator + stub and checks metrics):

   ```bash
   scripts/solanacdn-fair-smoke.sh
   ```

   The script asserts metric thresholds by default. Override via env:
   `WAIT_FOR_METRICS_SECS`, `EXPECT_FAIR_RECEIVED_MIN`, `EXPECT_FAIR_INJECTED_MIN`,
   `EXPECT_FAIR_COMMITS_RX_MIN`, `EXPECT_FAIR_LEDGER_COMMITS_SEEN_MIN`,
   `EXPECT_FAIR_LEDGER_AUDIT_CHECKED_MIN`, `STRICT_SLASHING_METRICS` (set to `1` to require
   ledger commit/audit counters > 0 when slashing is enabled).

2. In a second terminal, run the stub (sends one fair batch by default):

   ```bash
   cargo run -p solana-core --bin solanacdn-pop-stub -- --listen 127.0.0.1:9002
   ```

   To emit repeated batches:

   ```bash
   cargo run -p solana-core --bin solanacdn-pop-stub -- \\
     --listen 127.0.0.1:9002 --batches 0 --interval-ms 200 --exit-after-ms 0
   ```

3. Verify fair-batch counters moved:

   ```bash
   curl -s http://127.0.0.1:9100/metrics | rg 'solanacdn_tx_fair_batch_(received|injected)_total'
   ```

4. Fair-slashing smoke test (optional):

   - Start the validator with `--fair-slashing` or `--fair-slashing-enforce`.
   - Run the stub with a target slot and RPC URL so commit memos use a recent blockhash:

   ```bash
   cargo run -p solana-core --bin solanacdn-pop-stub -- \\
     --listen 127.0.0.1:9002 \\
     --target-slot 1 \\
     --rpc-url http://127.0.0.1:8899 \\
     --echo-commits
   ```

   Then watch the fair-slashing counters:

   ```bash
   curl -s http://127.0.0.1:9100/metrics | rg 'solanacdn_fair_(commits_rx_total|ledger_commits_seen_total|ledger_audit_checked_total)'
   ```

## Notes / limitations

- Vote tunneling is UDP-only. If validator QUIC votes are enabled, SolanaCDN vote tunneling is disabled automatically.
- `--solanacdn-no-repair` disables repair shreds; this can stall catch-up if SolanaCDN misses shreds. Use with care.
- `--fair-slashing-enforce` can reduce vote participation if violations are detected; use with care.
