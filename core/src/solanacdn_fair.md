# SolanaCDN `--fair` transaction ordering (validator-side FIFO)

This document describes the fairness contract implemented by the Agave validator integration when
SolanaCDN fair ordering is enabled (CLI `--fair`, `--fair-slashing`, `--fair-slashing-strict`,
`--fair-slashing-witness`, or `--fair-slashing-enforce`).

## Goal (retail user outcome)

Provide a stable, replayable **relative order** for transactions delivered via SolanaCDN to a given
leader by enforcing **non-overtake** based on the leader’s verified receipt order.

This does **not** attempt to define a cluster-wide total order across leaders.

## Core contract (what is “fair”)

When `--fair` is enabled, the validator assigns each verified SolanaCDN-delivered transaction a
monotonically increasing ordering index `order_ix`. Earlier `order_ix` always gets a higher
priority override than later `order_ix`.

`order_ix` is defined by the validator’s own processing order:

- Only **verified** transactions (deserialize + sanitize + strict signature verification) consume
  ordering space.
- Duplicate transactions (by signature) are ignored for ordering.
- Invalid transactions do not “burn” ordering indices.

## Validator behavior (FairBatch ingestion)

SolanaCDN delivers transactions in `PopToAgent::FairBatch` messages containing `FairTx { sig,
payload }` where `payload` is the wire transaction bytes.

For each batch, the validator:

1. **Applies cost bounds** (tx count cap + total payload bytes cap) to prevent CPU/memory abuse.
2. **Validates each candidate tx**:
   - The first signature parsed from `payload` must exist and match `FairTx.sig`.
   - The wire transaction must deserialize, sanitize, and pass strict ed25519 signature
     verification.
3. **Deduplicates** recent tx signatures using a short TTL cache (to avoid repeated injection).
4. **Allocates ordering indices** for the remaining verified txs (`order_start..order_start+N`).
5. **Overrides scheduler priority** for each tx signature so that earlier `order_ix` is scheduled
   ahead of later `order_ix` within the validator’s banking stage.

The validator then injects the verified wire transactions into TPU.

## Optional: ledger commit + audit (`--fair-slashing`)

When `--fair-slashing` is enabled and the batch includes a `target_slot`:

- The leader writes **memo-program** transaction(s) committing to `(batch_id, order_start, tx
  signatures...)` for that slot. These commits are chunked and self-validating.
- The leader also sends a signed `FairBatchCommit` back to the POP (and may attach a receipt commit
  containing `tx_count` and a Merkle root of signatures).

The validator audits slots it produced by reading entries from blockstore and checking for
**overtakes**: a transaction with a higher committed `order_ix` must not appear ahead of one with a
lower committed `order_ix`.

Missing tails are tolerated (e.g. if a committed transaction never landed), as long as no overtake
is observed.

### Strict mode (`--fair-slashing-strict`)

When strict mode is enabled, the leader’s ledger commit becomes a stronger contract:

- **No insertion ahead of the fair prefix:** except for vote transactions and the fair-commit memo
  transactions themselves, the committed fair transaction list must appear as a prefix in the
  target slot. Inserting other non-vote transactions “in front” of the committed fair list is a
  violation.
- **No committed drops:** every committed fair transaction signature must land in the target slot
  (missing tail is a violation).
- **Missing commit chunks are violations:** partial/fragmented ledger commits do not pass audit.

### Witness mode (`--fair-slashing-witness`)

The ledger alone cannot prove that a leader *received* a fair batch (a leader can always omit
commits and claim non-receipt). Witness mode adds an **external witness stream**:

- POPs broadcast a POP-signed `FairBatchWitness` containing an attestation payload
  (`batch_id`, `tx_count`, Merkle root of signatures, `target_slot`) and the intended
  `leader_pubkey`.
- Auditors subscribe to this witness stream and, during ledger audit, treat it as a violation if a
  witnessed batch has **no matching on-chain commit** (or the commit's signature list does not
  match the witnessed `tx_count`/Merkle root).

This mode requires POP support (protocol v6+) and shifts trust to the POP witness signer: a
malicious POP can falsely accuse a leader.

## Optional: enforcement (`--fair-slashing-enforce`)

When `--fair-slashing-enforce` is enabled, fair-ordering violations (ledger audit failure or commit
equivocation) trigger vote withholding for the violating leader/slot.

## Limitations / non-goals

- **Not cluster-wide total ordering:** fairness is enforced based on what a specific leader
  verified and accepted.
- **Not a global mempool:** this does not guarantee ordering for transactions that never reach the
  leader (or reach it only after expiration).
- **Only affects transactions with a fair priority override:** non-SolanaCDN traffic is outside the
  scope of the fairness contract.

## Observability

Key metrics (Prometheus) include:

- `solanacdn_tx_fair_batch_received_total`
- `solanacdn_tx_fair_batch_injected_total`
- `solanacdn_fair_batch_dropped_*`
- `solanacdn_fair_ledger_audit_*`
