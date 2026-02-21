# SolanaCDN `--fair` transaction ordering (validator-side FIFO)

This document describes the fairness contract implemented by the Agave validator integration when
SolanaCDN fair ordering is enabled (CLI `--fair`, `--fair-slashing`, `--fair-slashing-strict`,
`--fair-slashing-witness`, `--fair-slashing-enforce`, or `--fair-max-protection`).

In this fork, `--fair` enables **maximum protection** against malicious leaders (equivalent to
`--fair-max-protection`).

## In this fork: `--fair` defaults

`--fair` enables fair ordering **and** turns on the full set of protections:

- `--fair-require-target-slot`
- `--fair-slashing` (implied by the options below)
- `--fair-slashing-enforce`
- `--fair-slashing-strict`
- `--fair-slashing-witness`
- `--fair-slashing-nonresponse`
- `--fair-slashing-fence-reads` (implies `--fair-slashing-fence`)
- `--fair-slashing-publish-witness-memos`
- `--fair-slashing-witness-quorum` defaults to `2` (override explicitly if desired)

Enforcement is implemented as **vote withholding** on detected violations (not stake slashing).

## Goal (retail user outcome)

Provide a stable, replayable **relative order** for transactions delivered via SolanaCDN to a given
leader by enforcing **non-overtake** based on the leader’s verified receipt order.

This does **not** attempt to define a cluster-wide total order across leaders.

## Actors

- **Retail client**: produces signed wire transactions (not POP-attested by default).
- **POP**: receives transactions, builds an attested `FairBatch` (POP signature over ordered tx
  signatures + batch metadata), and forwards the batch to the leader.
- **Leader**: the validator currently scheduled as leader for `target_slot`. It verifies the POP
  attestation, assigns `order_ix`, injects transactions, and emits receipt evidence (on-chain and
  off-chain).
- **Auditors**: validators running `--fair` that consume evidence (ledger + off-chain streams) and
  enforce by withholding votes for violating leaders/slots.
- **Observers**: optional third parties that ingest the same evidence streams and/or replay the
  ledger to independently check audits.

## End-to-end workflow (happy path)

Under `--fair` in this fork, the fair flow is **slot-bound** (`target_slot` is required), so every
batch has replayable audit evidence.

```text
Retail client        POP                         Leader (slot S)                Ledger/Blockstore        Auditors
    |                 |                                |                              |                   |
    | submit txs      |                                |                              |                   |
    |---------------->| build POP-signed FairBatch     |                              |                   |
    |                 |  (ordered sig list + Merkle    |                              |                   |
    |                 |   root + tx_seq_start + S)     |                              |                   |
    |                 |------------------------------->| verify attestation + wire txs|                   |
    |                 |                                | assign order_ix (seq + idx)  |                   |
    |                 |                                | inject metadata first:       |                   |
    |                 |                                |  - on-chain ACK memo         |----> memos ------>|
    |                 |                                |  - on-chain COMMIT chunks    |----> memos ------>|
    |                 |                                | inject wire txs (fair-prio)  |----> tx order --->|
    |                 |<-------------------------------| off-chain ACK stream          |                   |
    |                 |<-------------------------------| off-chain COMMIT stream       |                   |
    |                 | produce POP-signed Witness(es) |                              |                   |
    |                 |------------------------------->|/auditors: Witness stream      |                   |
    |                 |                                |                              | audit slot S and  |
    |                 |                                |                              | enforce (votes)   |
```

If the leader rejects the batch, it emits a leader-signed `REJECT` (and may also publish an
on-chain reject memo) instead of committing/injecting the batch.

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

With `--fair-require-target-slot` (implied by `--fair` in this fork), the leader rejects any batch
missing `target_slot` so that accepted fair flow is always auditable/slashable.

## Evidence model (what can be proven)

Fair ordering can only be *enforced* when auditors have enough evidence to prove a leader violated
the contract. Evidence comes from two places:

- **On-chain (replayable):** memo-program transactions (`SCDNFAIR` commit chunks, plus optional
  `SCDNACKD`/`SCDNRJCT`/`SCDNWITN`).
- **Off-chain (low-latency):** signed control-plane streams (`FairBatchCommit`, `FairBatchAck`,
  `FairBatchReject`, `FairBatchWitness`).

### What the ledger alone can prove

If (and only if) the leader published **on-chain commit chunks** for a slot/batch, the ledger can
prove:

- **Non-overtake:** within that committed signature list, later `order_ix` must not appear ahead of
  earlier `order_ix` in the same slot.
- In **strict** mode: the committed fair list must appear as a **prefix**, and **no committed drops**
  are allowed (with vote/fair-memo exemptions).
- In **fence** mode: non-fair transactions must not write-lock fenced accounts touched by the fair
  flow in that slot.

The ledger alone cannot prove that a leader *received* a batch that it never committed.

### What requires external witnesses (non-receipt / non-response)

Punishing “leader received a fair batch but never committed nor rejected it” requires an **external
witness** because the ledger can’t prove non-receipt. In this system, the external witness is the
POP-signed `FairBatchWitness` stream (optionally mirrored to on-chain `SCDNWITN` memos).

To reduce framing risk, non-response slashing requires `--fair-slashing-witness-quorum N` distinct
witness POP keys agreeing on the same batch metadata.

### ACK-gated vs witness-gated slashing

There are two “acceptance” signals that can trigger slashing:

- **ACK-gated (witness mode):** the leader’s signed `ACK` (`FairBatchAck` stream and/or `SCDNACKD`
  memo). This is used to punish “leader agreed it received a specific batch” but then failed to
  commit the matching list, or tried to rewrite it.
- **Witness-gated (non-response mode):** POP-signed witness receipts (with quorum). This is used to
  punish “leader was witnessed receiving a batch” but never committed it nor rejected it.

Under `--fair` in this fork, both are enabled by default (max protection), so leaders cannot escape
accountability by omitting one evidence stream (non-response enforcement still depends on meeting
the configured witness quorum).

### When audits become strict (and what is exempt)

Auditors treat a slot as **strict** if any of the following are true:

- `--fair-slashing-strict` is enabled, or
- the auditor has leader `ACK` evidence for the slot, or
- `--fair-slashing-nonresponse` is enabled and the auditor has quorum witness evidence for the slot.

In strict mode, **vote-only** transactions and **valid fair metadata memo-only** transactions are
treated as exempt (they may appear “ahead” of the fair prefix without triggering insertion
violations). Valid fair metadata memos are exempt even if they land in a different slot than the
one they reference, to avoid strict-mode false positives from mis-slotted memos.

## Optional: ledger commit + audit (`--fair-slashing`)

When `--fair-slashing` is enabled and the batch includes a `target_slot`:

- The leader writes **on-chain commit chunks** (`SCDNFAIR`) committing to the ordered signature list
  for `(slot, batch_id, order_start, tx_sigs...)`. Commits are chunked (12 signatures/chunk) and
  leader-signed.
- The leader writes a leader-signed **ACK memo** (`SCDNACKD`) for accepted batches (binding
  `tx_count` and `tx_merkle_root`), and a leader-signed **REJECT memo** (`SCDNRJCT`) for rejected
  batches.
- Auditors may additionally publish POP witness receipts as **witness memos** (`SCDNWITN`) (see
  `--fair-slashing-publish-witness-memos`).
- The leader sends signed off-chain streams back to the POP: `FairBatchCommit` (full signature
  list) and, when `target_slot` is set, `FairBatchAck` (receipt commit).

Implementation note: metadata memo transactions are injected before the fair wire transactions and
are fee/priority-boosted to land early under load.

If you want all fair flow to be *slot-bound* (so it is always auditable/slashable), enable
`--fair-require-target-slot` (implied by `--fair` / `--fair-max-protection` in this fork).

The validator audits slots it produced by reading entries from blockstore and checking for
**overtakes**: a transaction with a higher committed `order_ix` must not appear ahead of one with a
lower committed `order_ix`.

Missing tails are tolerated (e.g. if a committed transaction never landed), as long as no overtake
is observed.

### Strict mode (`--fair-slashing-strict`)

When strict mode is enabled, the leader’s ledger commit becomes a stronger contract:

- **No insertion ahead of the fair prefix:** except for vote transactions and the fair-commit memo
  transactions themselves (and other **valid fair metadata memos** like ACK/REJECT/WITNESS), the
  committed fair transaction list must appear as a prefix in the target slot. Inserting other
  non-vote transactions “in front” of the committed fair list is a violation.
- **No committed drops:** every committed fair transaction signature must land in the target slot
  (missing tail is a violation).
- **Missing commit chunks are violations:** partial/fragmented ledger commits do not pass audit.

Note: valid fair metadata memo transactions are treated as exempt even if they land in a different
slot than the one they reference, to avoid strict-mode false positives from “mis-slotted” memos.

Auditors also treat a slot as strict when they have **receipt evidence** for it (leader ACKs, and/or
POP witnesses when non-response mode is enabled), even if `--fair-slashing-strict` is not set.

### Account fence (`--fair-slashing-fence`)

When account-fence mode is enabled, the audit additionally enforces a same-slot “account fence”:

- For a slot with committed fair batches, **transactions not in the committed fair list** must not
  write-lock any **non-signer account** written by a committed fair transaction in that slot.

This aims to prevent same-slot sandwich/back-run transactions that touch the same writable DEX
state/vault accounts as the fair flow.

With `--fair-slashing-fence-reads`, the set of fenced accounts is expanded to also include
**non-signer accounts read by** a committed fair transaction (violations are still detected when a
non-fair transaction writes a fenced account).

### Witness mode (`--fair-slashing-witness`)

The ledger alone cannot prove that a leader *received* a fair batch (a leader can always omit
commits and claim non-receipt). Witness mode adds:

- A **leader-signed ACK stream** (`FairBatchAck`, protocol v7+) containing a compact receipt commit
  (`tx_count`, Merkle root, `target_slot`, `order_start`).
- A **POP-signed witness stream** (`FairBatchWitness`, protocol v6+) binding the leader identity to
  the POP attestation payload.
- **On-chain ACK memos** (`SCDNACKD`) and **REJECT memos** (`SCDNRJCT`) that make receipt evidence
  replayable from the ledger.
- **On-chain witness memos** (`SCDNWITN`) that allow third parties to publish POP witness receipts
  to the ledger for replayable audits (enabled by default under `--fair` in this fork).
  - Validators can optionally publish these memos when receiving witness receipts via
    `--fair-slashing-publish-witness-memos` (implied by `--fair` in this fork).

Auditors subscribe to both and treat it as a violation if:

- A leader **ACKs** a batch for a target slot but that slot has **no matching on-chain commit**
  (or the on-chain commit’s signature list does not match the ACK’s `tx_count`/Merkle root).
- A leader ACK and POP witness **disagree** on `tx_count`/Merkle root for the same batch (immediate
  violation; prevents leader-side insertion/dropping/rewrite of the attested list).
- When an ACK is present for a leader+slot, auditors apply **strict** ledger audit rules for that
  slot (no non-vote insertion ahead of the committed fair prefix; no committed drops).
- A leader must not both **ACK and REJECT** the same batch ID for a given `(leader, slot)`.

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

Commit equivocation includes signing conflicting fair order receipts for the same `(leader, slot,
order_ix)` mapping (off-chain `FairBatchCommit` stream), and/or emitting conflicting on-chain commit
chunks for the same batch/chunk.

## Violation / evidence matrix

All enforcement below is **vote withholding** for the offending `(leader, slot)` when
`--fair-slashing-enforce` is enabled (it is implied by `--fair` in this fork).

| Violation (leader misbehavior) | Evidence required | Auditor check | Notes |
| --- | --- | --- | --- |
| **Overtake / reorder** within committed fair list | On-chain `SCDNFAIR` commit chunks + slot tx order | Later committed `order_ix` appears before an earlier one | The baseline “FIFO” rule. |
| **Insertion ahead** of fair prefix (front-run by inserting own txs) | Strict slot + on-chain commits + slot tx order | First non-exempt non-fair tx appears before the fair prefix completes | Answers “what stops a validator inserting its own txs?”: strict prefix + audit. |
| **Committed drops** (agree, then drop) | Strict slot + on-chain commits + slot tx order | Any committed signature missing from the target slot | This is the “no drops” protection. |
| **Missing commit chunks** / fragmented commit | Strict slot + on-chain commits | Any chunk index missing for a batch | Non-strict mode treats this as inconclusive; strict makes it slashable. |
| **ACK without commit** | Leader-signed `ACK` + ledger scan | ACK exists for `(slot,batch_id)` but no matching on-chain commit | Prevents “agree then don’t commit.” |
| **ACK ↔ commit mismatch** | Leader ACK + on-chain commits | Commit `(tx_count, merkle_root)` doesn’t match ACK | Detects rewrite/insertion/dropping of the attested list. |
| **ACK ↔ witness mismatch** | Leader ACK + quorum POP witnesses | Witness metadata disagrees with ACK | Immediate slashable equivocation (framing mitigated via quorum). |
| **ACK + REJECT equivocation** | Leader ACK + leader REJECT | Both exist for same `(leader, slot, batch_id)` | Either leader equivocation or POP/infra bug; treated as slashable. |
| **REJECT but also commit** | Leader REJECT + on-chain commits | Rejected batch_id has an on-chain commit in that slot | Prevents “reject” excuses when committed anyway. |
| **Non-response** (witnessed delivery, no commit/reject) | Quorum POP witnesses + ledger scan | Witness exists but neither commit nor matching reject exists | Requires `--fair-slashing-nonresponse` and quorum. |
| **Commit equivocation (off-chain)** | 2+ leader-signed `FairBatchCommit` streams | Conflicting `(leader,slot,order_ix)->tx_sig` mapping observed | Works even if ledger commits are missing. |
| **Commit equivocation (on-chain)** | On-chain `SCDNFAIR` commits | Conflicting chunks (same `(batch_id,chunk_index)` but different leader sig/payload) | Also covers inconsistent `order_start`/`chunk_total`. |
| **Account-fence violation** (same-slot sandwich/back-run on shared accounts) | Fence enabled + bank tx decoding + on-chain commits | Non-fair tx writes to a fenced non-signer account touched by fair flow | `--fair` implies `--fair-slashing-fence-reads` in this fork. |

## Recommended: maximum protection vs malicious leaders

If your goal is “best possible outcome for retail” (minimize same-slot front-run/back-run around
fair flow), run auditors with:

- `--fair`

Equivalent manual configuration:

- `--fair-require-target-slot` (make all fair batches slot-bound/auditable)
- `--fair-slashing --fair-slashing-enforce`
- `--fair-slashing-strict` (no insertion ahead of the fair prefix; no committed drops)
- `--fair-slashing-witness --fair-slashing-nonresponse` (ACK-required slashing + witnessed
  non-receipt)
- `--fair-slashing-witness-quorum N` (recommended `N>=2` when multiple POP witnesses exist)
- `--fair-slashing-fence --fair-slashing-fence-reads` (same-slot account fence)
- `--fair-slashing-publish-witness-memos` (replayable witness evidence on-chain; adds load/fees)

## Operations / deployment notes

- **Fund the payer(s):** leaders pay for `SCDNFAIR`/`SCDNACKD`/`SCDNRJCT` memo tx fees using their
  `identity_keypair`; auditors publishing `SCDNWITN` pay those fees using their own identity.
- **Expect extra transactions:** per fair batch, the leader emits ~`1 + ceil(tx_count/12)` on-chain
  memo transactions (ACK + commit chunks). With `tx_count=512` this is up to `44` memo txs per
  batch, plus the fair wire transactions themselves.
- **Witness quorum is a tradeoff:** `--fair` defaults `--fair-slashing-witness-quorum=2`. This
  reduces framing risk, but if your deployment only has 1 witness, non-response enforcement will
  never trigger unless you explicitly set quorum to `1`.
- **Strict mode is unforgiving:** if a leader emits an ACK but the matching on-chain commit does not
  land (fee starvation, blockhash issues, TPU injection failure), auditors will treat that as a
  slashable violation. Ensure commit metadata has enough fee/priority to reliably land.
- **Enforcement is local policy:** vote withholding only has teeth if enough stake runs `--fair`.
  For meaningful deterrence, deploy `--fair` on a significant auditor set (ideally across multiple
  operators).

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

## Appendix: fair ledger memo formats

SolanaCDN fair slashing metadata is carried as **memo-program** transactions using program id:

- `Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo`

These memos are **not** UTF-8 strings. The memo instruction `data` is a `bincode`-serialized Rust
struct whose payload begins with an 8-byte ASCII `magic` and a 1-byte `version`:

- Commit chunk: `magic="SCDNFAIR"`, `version=1`
- ACK memo: `magic="SCDNACKD"`, `version=1`
- REJECT memo: `magic="SCDNRJCT"`, `version=1`
- WITNESS memo: `magic="SCDNWITN"`, `version=1`

### `SCDNFAIR` (commit chunks)

- Encoded type: `FairLedgerCommitChunk { payload, signature }`
- `signature` is an ed25519 signature by `payload.leader_pubkey` over `bincode(payload)`
- `payload` fields:
  - `slot: u64`
  - `batch_id: u128`
  - `order_start: u64`
  - `chunk_index: u16`, `chunk_total: u16`
  - `tx_sigs: Vec<[u8; 64]>` (max 12 signatures per chunk)
  - `leader_pubkey: [u8; 32]`
  - `leader_time_ms: u64`

Auditors reconstruct the committed signature list by concatenating chunks in increasing
`chunk_index` order for each `batch_id`.

### `SCDNACKD` (leader ACK)

- Encoded type: `FairLedgerAckMemo { payload, signature }`
- `signature` is an ed25519 signature by `payload.leader_pubkey` over `bincode(payload)`
- `payload` fields:
  - `slot: u64`
  - `origin_pop_id_hash: [u8; 32]` (`sha256(origin_pop_id)` from the off-chain streams)
  - `flow_id: u128`
  - `batch_id: u128`
  - `order_start: u64`
  - `tx_count: u32`
  - `tx_merkle_root: [u8; 32]`
  - `leader_pubkey: [u8; 32]`
  - `leader_time_ms: u64`

### `SCDNRJCT` (leader REJECT)

- Encoded type: `FairLedgerRejectMemo { payload, signature }`
- `signature` is an ed25519 signature by `payload.leader_pubkey` over `bincode(payload)`
- `payload` fields:
  - `slot: u64`
  - `origin_pop_id_hash: [u8; 32]`
  - `flow_id: u128`
  - `batch_id: u128`
  - `order_start: u64`
  - `reason: FairBatchRejectReason`
  - `leader_pubkey: [u8; 32]`
  - `leader_time_ms: u64`

### `SCDNWITN` (POP witness receipt)

- Encoded type: `FairLedgerWitnessMemo { payload }`
- `payload` fields:
  - `witness_pop_pubkey: [u8; 32]`
  - `witness: FairBatchWitness` (contains POP-signed evidence binding leader identity to an
    attestation and delivery time; verified via `witness.verify(witness_pop_pubkey)`)
