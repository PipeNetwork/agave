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
- `--solanacdn-pop-pubkey-pinning enforce` unless explicitly overridden (when Pipe discovery provides expected POP pubkeys)
- SolanaCDN transaction injection (fair batches + on-chain receipt memos) injects to the local TPU via **QUIC** when enabled; if TPU QUIC is disabled (`--tpu-disable-quic`), `--fair` falls back to enabling TPU UDP (deprecated) so fair ordering remains functional

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

## End-to-end workflows

Under `--fair` in this fork, the fair flow is **slot-bound** (`target_slot` is required), so every
batch has replayable audit evidence.

### Accept path (commit)

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

### Reject path

```text
Retail client        POP                         Leader (slot S)                Ledger/Blockstore        Auditors
    |                 |                                |                              |                   |
    | submit txs      |                                |                              |                   |
    |---------------->| build POP-signed FairBatch     |                              |                   |
    |                 |------------------------------->| verify attestation + wire txs|                   |
    |                 |                                | decide REJECT (batch-level)  |                   |
    |                 |<-------------------------------| off-chain REJECT stream       |                   |
    |                 |                                | inject on-chain REJECT memo   |----> memo ------->|
    |                 |                                | (no COMMIT chunks, no txs)    |                   |
    |                 |                                |                              | audit: rejected   |
    |                 |                                |                              | batch_id must not |
    |                 |                                |                              | have on-chain commit |
```

A leader must not both **ACK and REJECT** the same `(leader, slot, batch_id)`, and must not
REJECT a batch that it also committed on-chain.

### Non-response path (witnessed delivery, no commit/reject)

```text
Retail client        POP                         Leader (slot S)                Ledger/Blockstore        Auditors
    |                 |                                |                              |                   |
    | submit txs      |                                |                              |                   |
    |---------------->| build POP-signed FairBatch     |                              |                   |
    |                 |------------------------------->| (delivery attempt)            |                   |
    |                 |------------------------------->|/auditors: POP Witness stream  |                   |
    |                 |                                | (leader stays silent)         |                   |
    |                 |                                | no ACK/REJECT/COMMIT          |                   |
    |                 |                                |                              | audit slot S: if  |
    |                 |                                |                              | quorum witness AND|
    |                 |                                |                              | no commit/reject =>|
    |                 |                                |                              | vote withhold     |
```

This is the “ledger can’t prove non-receipt” case: auditors need quorum POP witness receipts to
slash non-response.

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

The validator then injects the verified wire transactions into TPU. In this fork, SolanaCDN
injects SolanaCDN-delivered transactions and receipt metadata to the local TPU via **QUIC** when
enabled (default). If TPU QUIC is disabled, `--fair` enables TPU UDP (deprecated) as a fallback.

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

In this fork, quorum checks also require best-effort **witness diversity**: when POP discovery
metadata includes `region` and/or `asn`, witnessers must span at least two distinct “network
domains” (region/ASN) when `N >= 2`. If metadata is missing, auditors fall back to unique POP
pubkeys.

To avoid framing via arbitrary third-party keys, auditors only count witness receipts from POP
pubkeys they recognize (via Pipe discovery signer metadata and/or POP pubkey pinning data). If you
connect to only a subset of POP endpoints, discovery should still provide the full POP signer
registry so forwarded witnesses can be verified.

In particular, on-chain witness memos (`SCDNWITN`) are permissionless: anyone can post a memo that
contains a “witness” signed by an arbitrary keypair. Auditors therefore only treat witness memos as
evidence when the `witness_pop_pubkey` is known/allowlisted.

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

- A **leader-signed ACK stream** (`FairBatchAck`, protocol v9+) containing a compact receipt commit
  (`tx_count`, Merkle root, `target_slot`, `order_start`).
- A **POP-signed witness stream** (`FairBatchWitness`, protocol v9+) binding the leader identity to
  the POP attestation payload.
  - For deployments that forward/gossip witness receipts across the POP mesh, protocol v9 adds
    `FairBatchWitnessV2 { witness_pop_pubkey, witness }` so observers can verify forwarded witness
    receipts without relying on session-level POP pubkey context.
- **On-chain ACK memos** (`SCDNACKD`) and **REJECT memos** (`SCDNRJCT`) that make receipt evidence
  replayable from the ledger.
- **On-chain witness memos** (`SCDNWITN`) that allow third parties to publish POP witness receipts
  to the ledger for replayable audits (enabled by default under `--fair` in this fork).
  - Validators can optionally publish these memos when receiving witness receipts via
    `--fair-slashing-publish-witness-memos` (implied by `--fair` in this fork).
  - To bound load/fees, validators cap witness memo publication to **at most N memos per
    (leader,slot,batch_id)** where `N = --fair-slashing-witness-quorum` (default `2` under
    `--fair`).

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
`--fair-slashing-nonresponse` (protocol v9+). This relies on POP-signed witness receipts as
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
- **Deliver near the target slot:** to keep slot-bound receipt evidence stable, POPs should deliver
  `FairBatch` messages **right before** the intended `target_slot` (e.g., in `target_slot - 1`).
  Delivering too early increases the chance another leader includes the transactions/memos in a
  different slot than the one referenced by the receipts; delivering too late increases the chance
  receipt memos land in a later slot.
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

The SolanaCDN metrics server also exposes a read-only JSON snapshot of recent fair evidence at
`/solanacdn/fair-evidence` (recent ACK/witness/reject keys and in-memory slashing state).

## Local validation

Recommended local smoke tests in this repo:

- Single validator: `scripts/solanacdn-fair-smoke.sh` (uses `scripts/run.sh` + `solanacdn-pop-stub`)
- Multinode + load: `scripts/solanacdn-fair-multinode-smoke.sh` (uses `multinode-demo/*` to run
  validators as separate OS processes)

Note: the SolanaCDN integration currently uses process-global state, so multi-validator
in-process harnesses like `solana-local-cluster` are not suitable for testing SolanaCDN fair mode.

## Auditor runbook (alerts + triage)

### Suggested alerts (PromQL examples)

- **Any fair audit failures (page):**
  - `increase(solanacdn_fair_ledger_audit_failed_total[5m]) > 0`
- **Fair audit failure ratio (page):**
  - `increase(solanacdn_fair_ledger_audit_failed_total[10m]) / clamp_min(increase(solanacdn_fair_ledger_audit_checked_total[10m]), 1) > 0`
- **Blockstore read failures during audit (page):**
  - `increase(solanacdn_fair_ledger_audit_get_slot_entries_failed_total[5m]) > 0`
- **Inconclusive audits (should be ~0 under `--fair` because strict is always on in this fork) (ticket):**
  - `increase(solanacdn_fair_ledger_audit_inconclusive_total[30m]) > 0`

### Triage checklist

- Confirm flags are actually enabled on the auditor:
  - `solanacdn_tx_fair_ordering_enabled == 1`
  - `solanacdn_tx_fair_slashing_enabled == 1`
  - `solanacdn_tx_fair_slashing_enforce_enabled == 1`
- If `solanacdn_fair_ledger_audit_get_slot_entries_failed_total` is incrementing, treat it as a
  node health issue (blockstore read failures) rather than leader malice.
- If `solanacdn_fair_ledger_audit_failed_total` increments:
  - expect vote withholding (see `solanacdn_fair_votes_withheld_total`) and logs like
    `solanacdn: detected fair ordering violation; withholding votes ...`
  - the failure is either (a) real leader misbehavior, or (b) a reliability issue causing missing
    required metadata/txs under strict rules (e.g., ACK exists but commit chunks didn’t land).

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

### Test vectors (memo instruction `data` hex)

These are **example memo instruction `data` bytes** (the memo instruction payload), encoded as
`bincode`. They are intended for decoder/interoperability testing.

Parameters used:

- leader signing key: `09` repeated 32 bytes (ed25519), `leader_time_ms=1234567890`
- witness POP signing key: `07` repeated 32 bytes (ed25519), `created_at_ms=111`, `pop_time_ms=222`
- `slot=42`, `flow_id=0`, `batch_id=7`, `order_start=0`, `origin_pop_id="pop-test-1"`
- `tx_sigs=[[01..01] (64 bytes), [02..02] (64 bytes)]`, `tx_count=2`

```text
SCDNFAIR(commit_chunk_v1)=5343444e46414952012a00000000000000070000000000000000000000000000000000000000000000000001000200000000000000400000000000000001010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101010101400000000000000002020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202020202fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618d20296490000000040000000000000003beaa6b94d7792c3c49b3730f82787560db22f967bbb764c4b10eebc8ad9daf6123c0a00db63ef369e77983ee59fe8bbc6947b15c747dd5f1716627637357f04
SCDNACKD(ack_memo_v1)=5343444e41434b44012a000000000000008f486e5f857b7da6b54eed59c8cc8baae0925800b73fba5aaa1aa29bf1d091e90000000000000000000000000000000007000000000000000000000000000000000000000000000002000000df7699a77a088690c7c4c50792be798e1a9f28028b31bf0b9398037733e0d201fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618d20296490000000040000000000000006ef83e8df22ae6d971ac9a6e880f418d15774f8701de2e4ad09e6bf24681377fe472b5e38d567e8168ddf9acbecd19c2b2f212f9b1a437cf63ead036cbb8b405
SCDNRJCT(reject_memo_v1)=5343444e524a4354012a000000000000008f486e5f857b7da6b54eed59c8cc8baae0925800b73fba5aaa1aa29bf1d091e90000000000000000000000000000000007000000000000000000000000000000000000000000000008000000fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618d202964900000000400000000000000003b767ed173c2a8239533f1adc6837028b17dfcd3e38450494941b9fcf2d022e7c284ad68426805c92531ae032d8347540e9ff644ab1a725434b95baf7b3cd0c
SCDNWITN(witness_memo_v1)=5343444e5749544e01ea4a6c63e29c520abef5507b132ec5f9954776aebebe7b92421eea691446d22c0a00000000000000706f702d746573742d310000000000000000000000000000000007000000000000000000000000000000000000000000000002000000df7699a77a088690c7c4c50792be798e1a9f28028b31bf0b9398037733e0d2016f000000000000000000012a00000000000000fd1724385aa0c75b64fb78cd602fa1d991fdebf76b13c58ed702eac835e9f618de000000000000004000000000000000039a824e9e7518516f31c9f70dd62894aa204097e8eb915af9bf5b81b29a30c73eaac2aa2f7921568adbac190a1a8a0446350e58c33f7a7b828bcb528032a80d
```
