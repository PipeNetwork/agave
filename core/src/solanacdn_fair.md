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

`order_ix` is defined by the POP-signed `FairBatch` attestation:

- `order_ix = tx_seq_start + leaf_index` where `tx_seq_start` is a POP-provided monotonic sequence
  per `(origin_pop_id, flow_id)` and `leaf_index` is the transaction’s 0-based position in the
  attested batch.
- In fair mode, the leader must **accept or reject the entire attested batch**. Partial filtering
  (dropping/rewriting/reordering the tx list) is treated as a contract violation when paired with
  the appropriate evidence streams (see `--fair-slashing-witness`).

## Validator behavior (FairBatch ingestion)

SolanaCDN delivers transactions in `PopToAgent::FairBatch` messages containing `FairTx { sig,
payload }` where `payload` is the wire transaction bytes.

For each batch, the validator:

1. **Applies cost bounds** (tx count cap + total payload bytes cap) to prevent CPU/memory abuse.
2. **Verifies the POP attestation** signature and checks that the attested `(tx_count, Merkle
   root)` matches the ordered `tx.sig` list.
3. **Validates each candidate tx**:
   - The first signature parsed from `payload` must exist and match `FairTx.sig`.
   - The wire transaction must deserialize, sanitize, and pass strict ed25519 signature
     verification.
4. **Rejects the entire batch** if any tx fails validation (no partial accept/filtering).
5. **Allocates ordering indices** using the POP-provided sequence (`tx_seq_start..tx_seq_start+N`).
6. **Overrides scheduler priority** for each tx signature so that earlier `order_ix` is scheduled
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

### Account fence (`--fair-slashing-fence`)

When account-fence mode is enabled, the audit additionally enforces a same-slot “account fence”:

- For a slot with committed fair batches, **transactions not in the committed fair list** must not
  write-lock any **non-signer account** written by a committed fair transaction in that slot.

This aims to prevent same-slot sandwich/back-run transactions that touch the same writable DEX
state/vault accounts as the fair flow.

### Witness mode (`--fair-slashing-witness`)

The ledger alone cannot prove that a leader *received* a fair batch (a leader can always omit
commits and claim non-receipt). Witness mode adds:

- A **leader-signed ACK stream** (`FairBatchAck`, protocol v7+) containing a compact receipt commit
  (`tx_count`, Merkle root, `target_slot`, `order_start`).
- A **POP-signed witness stream** (`FairBatchWitness`, protocol v6+) binding the leader identity to
  the POP attestation payload.
- Optional **on-chain witness memos** (`SCDNWITN`) that allow third parties to publish POP witness
  receipts to the ledger for replayable audits.
  - Validators can optionally publish these memos when receiving witness receipts via
    `--fair-slashing-publish-witness-memos`.

Auditors subscribe to both and treat it as a violation if:

- A leader **ACKs** a batch for a target slot but that slot has **no matching on-chain commit**
  (or the on-chain commit’s signature list does not match the ACK’s `tx_count`/Merkle root).
- A leader ACK and POP witness **disagree** on `tx_count`/Merkle root for the same batch (immediate
  violation; prevents leader-side insertion/dropping/rewrite of the attested list).
- When an ACK is present for a leader+slot, auditors apply **strict** ledger audit rules for that
  slot (no non-vote insertion ahead of the committed fair prefix; no committed drops).

This mode shifts trust assumptions: slashing requires leader-signed ACK evidence, so a malicious
POP witness alone cannot frame a leader, but witnesses remain necessary for cross-checking ACKs.

### Non-response mode (`--fair-slashing-nonresponse`)

If you also want to punish “POP witnessed delivery but leader never committed nor rejected”, enable
`--fair-slashing-nonresponse` (protocol v8+). This relies on POP-signed witness receipts as
external evidence of delivery; the ledger alone cannot prove non-receipt.

To reduce the risk of false positives from a single POP, use `--fair-slashing-witness-quorum N` to
require at least `N` distinct witnesses before using witness evidence for non-response slashing.

## Optional: enforcement (`--fair-slashing-enforce`)

When `--fair-slashing-enforce` is enabled, fair-ordering violations (ledger audit failure or commit
equivocation) trigger vote withholding for the violating leader/slot.

## Recommended: maximum protection vs malicious leaders

If your goal is “best possible outcome for retail” (minimize same-slot front-run/back-run around
fair flow), run auditors with:

- `--fair-slashing --fair-slashing-enforce`
- `--fair-slashing-strict` (no insertion ahead of the fair prefix; no committed drops)
- `--fair-slashing-witness --fair-slashing-nonresponse` (ACK-required slashing + witnessed
  non-receipt)
- `--fair-slashing-witness-quorum N` (recommended `N>=2` when multiple POP witnesses exist)
- `--fair-slashing-fence --fair-slashing-fence-reads` (same-slot account fence)
- `--fair-slashing-publish-witness-memos` (optional replayable witness evidence on-chain)

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
