use {
    arc_swap::ArcSwapOption,
    bincode,
    dashmap::DashMap,
    ed25519_dalek_v2::{Signer as DalekSigner, SigningKey, VerifyingKey},
    log::warn,
    serde::{Deserialize, Serialize},
    solana_compute_budget_interface::ComputeBudgetInstruction,
    solana_compute_budget_interface::ID as COMPUTE_BUDGET_PROGRAM_ID,
    solana_entry::entry::Entry,
    solana_hash::Hash,
    solana_instruction::Instruction,
    solana_keypair::Keypair,
    solana_message::Message,
    solana_message::VersionedMessage,
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_sha256_hasher as sha256_hasher,
    solana_signer::Signer,
    solana_transaction::Transaction,
    solana_transaction::versioned::VersionedTransaction,
    std::collections::{HashMap, HashSet, VecDeque},
    std::sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
        Mutex,
    },
    std::time::{SystemTime, UNIX_EPOCH},
    thiserror::Error,
};

pub type LaneId = u8;

pub type McpHash32 = [u8; 32];

pub type TxBlobId = [u8; 32];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct McpMemoIxMetaV1 {
    pub slot: u64,
    pub lane_id: LaneId,
    pub leader_pubkey: [u8; 32],
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum McpMemoIxParseV1 {
    Valid(McpMemoIxMetaV1),
    InvalidSignature,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct McpMemoChunkPayloadV1 {
    pub kind: McpLedgerMemoKindV1,
    pub slot: u64,
    pub lane_id: LaneId,
    pub object_id: [u8; 32],
    pub chunk_index: u16,
    pub chunk_total: u16,
    pub object_chunk: Vec<u8>,
    pub leader_pubkey: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum McpMemoChunkParseV1 {
    Valid(McpMemoChunkPayloadV1),
    InvalidSignature,
}

pub(crate) fn parse_mcp_memo_chunk_payload_v1(
    program_id: &Pubkey,
    data: &[u8],
) -> Option<McpMemoChunkParseV1> {
    if program_id != &MCP_LEDGER_MEMO_PROGRAM_ID {
        return None;
    }
    if data.len() < MCP_LEDGER_MEMO_MAGIC.len()
        || &data[..MCP_LEDGER_MEMO_MAGIC.len()] != MCP_LEDGER_MEMO_MAGIC
    {
        return None;
    }
    let Ok(chunk) = bincode::deserialize::<McpLedgerMemoChunkV1>(data) else {
        return None;
    };
    if !chunk.verify() {
        return Some(McpMemoChunkParseV1::InvalidSignature);
    }
    let p = chunk.payload;
    Some(McpMemoChunkParseV1::Valid(McpMemoChunkPayloadV1 {
        kind: p.kind,
        slot: p.slot,
        lane_id: p.lane_id,
        object_id: p.object_id,
        chunk_index: p.chunk_index,
        chunk_total: p.chunk_total,
        object_chunk: p.object_chunk,
        leader_pubkey: p.leader_pubkey,
    }))
}

pub(crate) fn parse_mcp_memo_ix_data_v1(
    program_id: &Pubkey,
    data: &[u8],
) -> Option<McpMemoIxParseV1> {
    match parse_mcp_memo_chunk_payload_v1(program_id, data)? {
        McpMemoChunkParseV1::InvalidSignature => Some(McpMemoIxParseV1::InvalidSignature),
        McpMemoChunkParseV1::Valid(p) => Some(McpMemoIxParseV1::Valid(McpMemoIxMetaV1 {
            slot: p.slot,
            lane_id: p.lane_id,
            leader_pubkey: p.leader_pubkey,
        })),
    }
}

const MCP_MICRO_LAMPORTS_PER_LAMPORT: u64 = 1_000_000;

const MCP_SLASHED_TTL_MS: u64 = 10 * 60_000;
const MCP_SLASHED_MAX_ENTRIES: usize = 200_000;

const MCP_HASH_DOMAIN_BLOB_ID_V1: &[u8; 8] = b"MCPBLOB1";
const MCP_HASH_DOMAIN_MB_REFS_ROOT_V1: &[u8; 8] = b"MCPRREF1";
const MCP_HASH_DOMAIN_MB_ID_V1: &[u8; 8] = b"MCPMBID1";
const MCP_HASH_DOMAIN_MB_SIG_V1: &[u8; 8] = b"MCPMBSG1";
const MCP_HASH_DOMAIN_POH_INIT_V1: &[u8; 8] = b"MCPPOH01";
const MCP_HASH_DOMAIN_POH_MIXIN_V1: &[u8; 8] = b"MCPPOHM1";
const MCP_HASH_DOMAIN_POH_STEP_V1: &[u8; 8] = b"MCPPOH21";
const MCP_HASH_DOMAIN_LANE_SELECT_V1: &[u8; 8] = b"MCPLAN01";
const MCP_HASH_DOMAIN_TX_LANE_SELECT_V1: &[u8; 8] = b"MCPTXL01";
const MCP_HASH_DOMAIN_CKPT_ID_V1: &[u8; 8] = b"MCPCKID1";
const MCP_HASH_DOMAIN_CKPT_ROOT_V1: &[u8; 8] = b"MCPCKRT1";
const MCP_HASH_DOMAIN_CKPT_SIG_V1: &[u8; 8] = b"MCPCKSG1";
const MCP_HASH_DOMAIN_DA_ATTEST_ID_V1: &[u8; 8] = b"MCPDAID1";
const MCP_HASH_DOMAIN_DA_ATTEST_SIG_V1: &[u8; 8] = b"MCPDASG1";
const MCP_HASH_DOMAIN_DA_CERT_ID_V1: &[u8; 8] = b"MCPDCID1";

const MCP_MAX_POH_HASHES_AFTER_MIXIN_V1: u32 = 4096;
const MCP_SCHEDULED_SCAN_SLOTS_V1: u64 = 256;

const MCP_LEDGER_MEMO_PROGRAM_ID: Pubkey =
    solana_pubkey::pubkey!("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");

const MCP_LEDGER_MEMO_MAGIC: [u8; 8] = *b"SCDNMCP\0";
const MCP_LEDGER_MEMO_VERSION: u8 = 1;

const MCP_LEDGER_MEMO_DEFAULT_MAX_CHUNK_BYTES: usize = 512;
const MCP_LEDGER_MEMO_MAX_CHUNKS_PER_OBJECT: usize = 8 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum McpLeaderMode {
    /// Only accept MCP objects signed by the slot leader (current Solana model).
    SlotLeaderOnly,
    /// Accept MCP objects signed by any validator, with per-lane leader selection
    /// performed during audit.
    AnyLeader,
    /// Accept MCP objects signed by a deterministic scheduled K-set derived from the
    /// leader schedule (still aggregated by the slot leader in the legacy block format).
    ScheduledLeaderSchedule,
}

impl Default for McpLeaderMode {
    fn default() -> Self {
        Self::SlotLeaderOnly
    }
}

#[derive(Clone, Debug)]
pub struct McpConfig {
    /// Concurrent proposer set size per slot (K).
    pub lanes_per_slot: u8,
    /// Data-availability threshold in basis points of total stake (e.g. 6667 ~= 2/3).
    pub da_threshold_bps: u16,
    /// Soft cap used by proposers / validators when forming microblocks.
    pub microblock_max_refs: u16,
    /// When enabled, withhold votes for leaders that violate the MCP ledger audit.
    pub enforce_vote_withholding: bool,
    pub leader_mode: McpLeaderMode,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self {
            lanes_per_slot: 2,
            da_threshold_bps: 6667,
            microblock_max_refs: 64,
            enforce_vote_withholding: false,
            leader_mode: McpLeaderMode::SlotLeaderOnly,
        }
    }
}

impl McpConfig {
    pub fn validate(&self) -> Result<(), McpError> {
        if self.lanes_per_slot == 0 {
            return Err(McpError::InvalidValue);
        }
        if !(1..=10_000).contains(&self.da_threshold_bps) {
            return Err(McpError::InvalidValue);
        }
        if self.microblock_max_refs == 0 {
            return Err(McpError::InvalidValue);
        }
        Ok(())
    }
}

static GLOBAL: ArcSwapOption<McpHandle> = ArcSwapOption::const_empty();

pub fn configure(cfg: Option<McpConfig>) {
    let Some(cfg) = cfg else {
        GLOBAL.store(None);
        return;
    };
    if cfg.validate().is_err() {
        GLOBAL.store(None);
        return;
    }
    GLOBAL.store(Some(Arc::new(McpHandle::new(cfg))));
}

pub fn global() -> Option<Arc<McpHandle>> {
    GLOBAL.load_full()
}

pub fn audit_votable_bank(
    blockstore: &Blockstore,
    bank: &Bank,
    leader_schedule_cache: &LeaderScheduleCache,
) {
    let Some(handle) = global() else {
        return;
    };
    handle.audit_slot(blockstore, bank, leader_schedule_cache);
}

pub fn mcp_slashing_is_slashed_leader(leader: &Pubkey, bank: &Bank) -> bool {
    let Some(handle) = global() else {
        return false;
    };
    let enforcement_feature_active = bank
        .feature_set
        .is_active(&agave_feature_set::mcp_vote_withholding::ID);
    handle.mcp_slashing_is_slashed_leader(leader, bank.slot(), enforcement_feature_active)
}

pub fn mcp_slashing_note_vote_withheld(leader: &Pubkey, slot: u64) {
    let Some(handle) = global() else {
        return;
    };
    handle.note_mcp_vote_withheld(leader, slot);
}

pub struct McpHandle {
    cfg: McpConfig,
    audit_checked: AtomicU64,
    audit_failed: AtomicU64,
    ledger_order_mismatch: AtomicU64,
    slashed_leaders: DashMap<McpSlashedKey, McpSlashedEntry>,
    votes_withheld: AtomicU64,
    memo_chunks_seen: AtomicU64,
    memo_chunks_invalid: AtomicU64,
    audited_slots: DashMap<u64, bool>,
    vote_withholding_feature_active: AtomicBool,
    lane_builders: Mutex<HashMap<(u64, LaneId), LaneBuilderStateV1>>,
    ordering: McpOrderingV1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
struct McpSlashedKey {
    leader: [u8; 32],
    slot: u64,
}

#[derive(Clone, Copy, Debug)]
struct McpSlashedEntry {
    expires_at_ms: u64,
}

#[derive(Clone, Copy, Debug)]
struct LaneBuilderStateV1 {
    next_seq: u32,
    next_checkpoint_ix: u16,
    prev_microblock_id: McpHash32,
    prev_poh: McpHash32,
    last_update_ms: u64,
}

struct McpOrderingV1 {
    cfg: McpConfig,
    priorities: DashMap<(u64, TxBlobId), u64>,
    inner: Mutex<McpOrderingInnerV1>,
}

struct McpOrderingInnerV1 {
    slots: HashMap<u64, McpOrderingSlotStateV1>,
    order: VecDeque<u64>,
}

#[derive(Default)]
struct McpOrderingSlotStateV1 {
    objects: HashMap<(LaneId, [u8; 32]), McpOrderingObjectChunksV1>,
    seen_microblock_ids: HashSet<[u8; 32]>,
    microblocks_by_lane: HashMap<LaneId, Vec<McpMicroblockV1>>,
    priority_blob_ids: Vec<TxBlobId>,
}

struct McpOrderingObjectChunksV1 {
    leader_pubkey: [u8; 32],
    chunk_total: u16,
    chunks: Vec<Option<Vec<u8>>>,
    seen: u16,
}

impl McpOrderingObjectChunksV1 {
    fn new(leader_pubkey: [u8; 32], chunk_total: u16) -> Option<Self> {
        if chunk_total == 0 || (chunk_total as usize) > MCP_LEDGER_MEMO_MAX_CHUNKS_PER_OBJECT {
            return None;
        }
        Some(Self {
            leader_pubkey,
            chunk_total,
            chunks: vec![None; chunk_total as usize],
            seen: 0,
        })
    }

    fn insert(&mut self, chunk_index: u16, object_chunk: Vec<u8>) -> bool {
        let idx = chunk_index as usize;
        let Some(slot) = self.chunks.get_mut(idx) else {
            return false;
        };
        if let Some(existing) = slot.as_ref() {
            return existing == &object_chunk;
        }
        *slot = Some(object_chunk);
        self.seen = self.seen.saturating_add(1);
        true
    }

    fn is_complete(&self) -> bool {
        self.seen == self.chunk_total
    }

    fn reassemble(&self) -> Option<Vec<u8>> {
        if !self.is_complete() {
            return None;
        }
        let mut out: Vec<u8> = Vec::new();
        for c in &self.chunks {
            out.extend_from_slice(c.as_ref()?);
        }
        Some(out)
    }
}

impl McpOrderingV1 {
    const MAX_SLOTS: usize = 16;
    const MAX_MICROBLOCKS_PER_LANE: usize = 256;

    fn new(cfg: McpConfig) -> Self {
        Self {
            cfg,
            priorities: DashMap::new(),
            inner: Mutex::new(McpOrderingInnerV1 {
                slots: HashMap::new(),
                order: VecDeque::new(),
            }),
        }
    }

    fn priority_for_slot_blob_id_v1(&self, slot: u64, blob_id: TxBlobId) -> Option<u64> {
        self.priorities.get(&(slot, blob_id)).map(|v| *v.value())
    }

    fn ingest_memo_chunk_payload_v1(&self, p: McpMemoChunkPayloadV1) -> bool {
        // Avoid allowing arbitrary validators to inject priority overrides.
        if self.cfg.leader_mode == McpLeaderMode::AnyLeader {
            return false;
        }
        if p.kind != McpLedgerMemoKindV1::Microblock {
            return false;
        }

        let lanes_per_slot = self.cfg.lanes_per_slot.max(1);
        if p.lane_id >= lanes_per_slot {
            return false;
        }
        if p.chunk_total == 0 || (p.chunk_total as usize) > MCP_LEDGER_MEMO_MAX_CHUNKS_PER_OBJECT {
            return false;
        }

        let mut inner = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if !inner.slots.contains_key(&p.slot) {
            if inner.slots.len() >= Self::MAX_SLOTS {
                if let Some(evict_slot) = inner.order.pop_front() {
                    if let Some(mut evicted) = inner.slots.remove(&evict_slot) {
                        for blob_id in evicted.priority_blob_ids.drain(..) {
                            self.priorities.remove(&(evict_slot, blob_id));
                        }
                    }
                }
            }
            inner.order.push_back(p.slot);
            inner.slots.insert(p.slot, McpOrderingSlotStateV1::default());
        }

        let Some(slot_state) = inner.slots.get_mut(&p.slot) else {
            return false;
        };

        let key = (p.lane_id, p.object_id);
        let obj = slot_state.objects.entry(key).or_insert_with(|| {
            McpOrderingObjectChunksV1::new(p.leader_pubkey, p.chunk_total)
                .expect("chunk_total validated")
        });
        if obj.chunk_total != p.chunk_total || obj.leader_pubkey != p.leader_pubkey {
            return false;
        }
        if !obj.insert(p.chunk_index, p.object_chunk) {
            return false;
        }
        if !obj.is_complete() {
            return false;
        }

        let Some(bytes) = obj.reassemble() else {
            return false;
        };
        slot_state.objects.remove(&key);

        let Ok(mb) = McpMicroblockV1::decode_v1(&bytes) else {
            return false;
        };
        if mb.header.slot != p.slot || mb.header.lane_id != p.lane_id {
            return false;
        }
        if mb.header.microblock_id_v1() != p.object_id {
            return false;
        }
        if mb.header.leader_pubkey.to_bytes() != p.leader_pubkey {
            return false;
        }
        if !slot_state.seen_microblock_ids.insert(p.object_id) {
            return false;
        }

        let lane_mbs = slot_state.microblocks_by_lane.entry(p.lane_id).or_default();
        if lane_mbs.len() >= Self::MAX_MICROBLOCKS_PER_LANE {
            return false;
        }
        lane_mbs.push(mb);

        // Recompute priorities for this slot.
        for blob_id in slot_state.priority_blob_ids.drain(..) {
            self.priorities.remove(&(p.slot, blob_id));
        }

        let mut lane_microblocks: HashMap<LaneId, Vec<McpMicroblockV1>> = HashMap::new();
        for (&lane_id, microblocks) in slot_state.microblocks_by_lane.iter() {
            let Some(first) = microblocks.first() else {
                continue;
            };
            let expected_leader = first.header.leader_pubkey.to_bytes();
            let Ok(ordered) =
                verify_lane_microblocks_v1(p.slot, lane_id, lanes_per_slot, microblocks.clone(), expected_leader)
            else {
                continue;
            };
            lane_microblocks.insert(lane_id, ordered);
        }

        let merged_refs = merge_lanes_by_bid_v1(&lane_microblocks);
        slot_state
            .priority_blob_ids
            .reserve(merged_refs.len().min(8192));
        for (idx, r) in merged_refs.into_iter().enumerate() {
            let priority = u64::MAX.wrapping_sub(idx as u64);
            self.priorities.insert((p.slot, r.blob_id), priority);
            slot_state.priority_blob_ids.push(r.blob_id);
        }

        true
    }
}

#[derive(Clone, Debug)]
pub struct McpStatus {
    pub cfg: McpConfig,
    pub vote_withholding_feature_active: bool,
    pub vote_withholding_enabled: bool,
    pub audit_checked_total: u64,
    pub audit_failed_total: u64,
    pub ledger_order_mismatch_total: u64,
    pub slashed_leaders_len: u64,
    pub votes_withheld_total: u64,
    pub memo_chunks_seen_total: u64,
    pub memo_chunks_invalid_total: u64,
    pub audited_slots_len: u64,
}

pub fn status_snapshot() -> Option<McpStatus> {
    global().map(|h| h.status_snapshot())
}

impl McpHandle {
    fn new(cfg: McpConfig) -> Self {
        Self {
            ordering: McpOrderingV1::new(cfg.clone()),
            cfg,
            audit_checked: AtomicU64::new(0),
            audit_failed: AtomicU64::new(0),
            ledger_order_mismatch: AtomicU64::new(0),
            slashed_leaders: DashMap::new(),
            votes_withheld: AtomicU64::new(0),
            memo_chunks_seen: AtomicU64::new(0),
            memo_chunks_invalid: AtomicU64::new(0),
            audited_slots: DashMap::new(),
            vote_withholding_feature_active: AtomicBool::new(false),
            lane_builders: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn ingest_memo_chunk_for_ordering_v1(&self, p: McpMemoChunkPayloadV1) -> bool {
        self.ordering.ingest_memo_chunk_payload_v1(p)
    }

    pub(crate) fn ordering_priority_for_slot_blob_id_v1(
        &self,
        slot: u64,
        blob_id: TxBlobId,
    ) -> Option<u64> {
        self.ordering.priority_for_slot_blob_id_v1(slot, blob_id)
    }

    pub fn select_lane_for_slot_v1(&self, slot: u64, leader_pubkey: Pubkey) -> LaneId {
        let lanes = self.cfg.lanes_per_slot.max(1);
        let slot_bytes = slot.to_le_bytes();
        let leader_bytes = leader_pubkey.to_bytes();
        let digest = sha256_domain(MCP_HASH_DOMAIN_LANE_SELECT_V1, &[&slot_bytes, &leader_bytes]);
        digest[0] % lanes
    }

    pub(crate) fn scheduled_lane_leaders_from_leader_schedule_v1(
        &self,
        slot: u64,
        bank: &Bank,
        leader_schedule_cache: &LeaderScheduleCache,
    ) -> HashMap<LaneId, [u8; 32]> {
        let lanes_target: usize = self.cfg.lanes_per_slot.max(1) as usize;
        let mut out: HashMap<LaneId, [u8; 32]> = HashMap::with_capacity(lanes_target);

        for offset in 0..MCP_SCHEDULED_SCAN_SLOTS_V1 {
            if out.len() >= lanes_target {
                break;
            }
            let candidate_slot = slot.saturating_add(offset);
            let Some(leader) = leader_schedule_cache.slot_leader_at(candidate_slot, Some(bank)) else {
                continue;
            };
            let lane_id = self.select_lane_for_slot_v1(slot, leader);
            if out.contains_key(&lane_id) {
                continue;
            }
            out.insert(lane_id, leader.to_bytes());
        }

        out
    }

    pub fn bid_sorted_refs_from_payloads_v1(&self, tx_payloads: &[&[u8]]) -> Vec<McpTxRefV1> {
        let mut refs: Vec<McpTxRefV1> = Vec::with_capacity(tx_payloads.len());
        for payload in tx_payloads {
            refs.push(McpTxRefV1 {
                blob_id: tx_blob_id_v1(payload),
                bid_hint: bid_hint_from_wire_tx_v1(payload).unwrap_or(BidHintV1 {
                    cu_price: 0,
                    cu_limit: 0,
                    sig_count: 0,
                }),
            });
        }
        refs.sort_by(|a, b| {
            bid_key_from_hint_v1(a.blob_id, a.bid_hint).cmp(&bid_key_from_hint_v1(b.blob_id, b.bid_hint))
        });
        refs
    }

    pub fn build_lane_microblock_and_checkpoint_memo_txs_from_payloads_v1(
        &self,
        identity_keypair: &Keypair,
        signing_key: &SigningKey,
        recent_blockhash: Hash,
        slot: u64,
        lane_id: LaneId,
        tx_payloads: &[&[u8]],
    ) -> Vec<Vec<u8>> {
        let refs = self.bid_sorted_refs_from_payloads_v1(tx_payloads);
        self.build_lane_microblock_and_checkpoint_memo_txs_from_refs_v1(
            identity_keypair,
            signing_key,
            recent_blockhash,
            slot,
            lane_id,
            refs,
        )
    }

    fn build_lane_microblocks_and_checkpoint_v1(
        &self,
        identity_keypair: &Keypair,
        signing_key: &SigningKey,
        recent_blockhash: Hash,
        slot: u64,
        lane_id: LaneId,
        mut refs: Vec<McpTxRefV1>,
    ) -> (Vec<Vec<u8>>, Option<McpCheckpointV1>) {
        if slot == 0 {
            return (Vec::new(), None);
        }
        if lane_id >= self.cfg.lanes_per_slot {
            return (Vec::new(), None);
        }

        // Deterministic tx->lane assignment: each transaction belongs to exactly one lane per slot.
        refs.retain(|r| select_lane_for_blob_id_v1(slot, r.blob_id, self.cfg.lanes_per_slot) == lane_id);
        if refs.is_empty() {
            return (Vec::new(), None);
        }

        refs.sort_by(|a, b| {
            bid_key_from_hint_v1(a.blob_id, a.bid_hint).cmp(&bid_key_from_hint_v1(b.blob_id, b.bid_hint))
        });

        let max_refs = usize::from(self.cfg.microblock_max_refs).max(1);

        let now = now_ms();
        let mut lane_builders = self.lane_builders.lock().unwrap_or_else(|p| p.into_inner());
        let min_slot = slot.saturating_sub(32);
        lane_builders.retain(|(s, _), st| *s >= min_slot && now.saturating_sub(st.last_update_ms) <= 120_000);

        let state = lane_builders
            .entry((slot, lane_id))
            .or_insert_with(|| LaneBuilderStateV1 {
                next_seq: 0,
                next_checkpoint_ix: 0,
                prev_microblock_id: [0u8; 32],
                prev_poh: lane_poh_init_v1(slot, lane_id),
                last_update_ms: now,
            });

        let leader_pubkey = identity_keypair.pubkey();
        let mut out: Vec<Vec<u8>> = Vec::new();
        let mut built: Vec<McpMicroblockV1> = Vec::new();
        for refs_chunk in refs.chunks(max_refs) {
            let mut mb = McpMicroblockV1::build_unsigned(
                slot,
                lane_id,
                state.next_seq,
                state.prev_microblock_id,
                state.prev_poh,
                refs_chunk.to_vec(),
                0,
                leader_pubkey,
            );
            mb.sign_v1(signing_key);

            let microblock_id = mb.header.microblock_id_v1();
            state.prev_microblock_id = microblock_id;
            state.prev_poh = mb.header.poh_hash;
            state.next_seq = state.next_seq.saturating_add(1);
            state.last_update_ms = now_ms();

            out.extend(build_mcp_microblock_memo_txs_v1(
                identity_keypair,
                signing_key,
                recent_blockhash,
                &mb,
            ));
            built.push(mb);
        }

        if built.is_empty() {
            return (out, None);
        }

        let checkpoint_ix = state.next_checkpoint_ix;
        state.next_checkpoint_ix = state.next_checkpoint_ix.saturating_add(1);
        drop(lane_builders);

        let first_seq_no = built.first().map(|mb| mb.header.seq_no).unwrap_or(0);
        let last_seq_no = built.last().map(|mb| mb.header.seq_no).unwrap_or(first_seq_no);
        let microblock_ids: Vec<McpHash32> = built
            .iter()
            .map(|mb| mb.header.microblock_id_v1())
            .collect();
        let microblock_root = microblock_root_v1(microblock_ids.as_slice());
        let tx_ref_count: u32 = built.iter().map(|mb| mb.refs.len() as u32).sum();

        let mut ckpt = McpCheckpointV1 {
            header: McpCheckpointHeaderV1 {
                slot,
                lane_id,
                checkpoint_ix,
                first_seq_no,
                last_seq_no,
                microblock_root,
                tx_ref_count,
                leader_pubkey,
            },
            leader_sig: [0u8; 64],
        };
        ckpt.sign_v1(signing_key);
        out.extend(build_mcp_checkpoint_memo_txs_v1(
            identity_keypair,
            signing_key,
            recent_blockhash,
            &ckpt,
        ));

        (out, Some(ckpt))
    }

    pub fn build_lane_microblocks_and_checkpoint_memos_with_checkpoint_v1(
        &self,
        identity_keypair: &Keypair,
        signing_key: &SigningKey,
        recent_blockhash: Hash,
        slot: u64,
        lane_id: LaneId,
        refs: Vec<McpTxRefV1>,
    ) -> (Vec<Vec<u8>>, Option<McpCheckpointV1>) {
        self.build_lane_microblocks_and_checkpoint_v1(
            identity_keypair,
            signing_key,
            recent_blockhash,
            slot,
            lane_id,
            refs,
        )
    }

    pub fn build_lane_microblock_and_checkpoint_memo_txs_from_refs_v1(
        &self,
        identity_keypair: &Keypair,
        signing_key: &SigningKey,
        recent_blockhash: Hash,
        slot: u64,
        lane_id: LaneId,
        refs: Vec<McpTxRefV1>,
    ) -> Vec<Vec<u8>> {
        self.build_lane_microblocks_and_checkpoint_v1(
            identity_keypair,
            signing_key,
            recent_blockhash,
            slot,
            lane_id,
            refs,
        )
        .0
    }

    pub fn build_lane_microblock_checkpoint_and_da_cert_memo_txs_from_refs_v1(
        &self,
        identity_keypair: &Keypair,
        signing_key: &SigningKey,
        bank: &Bank,
        recent_blockhash: Hash,
        slot: u64,
        lane_id: LaneId,
        refs: Vec<McpTxRefV1>,
    ) -> Vec<Vec<u8>> {
        let (mut out, checkpoint) = self.build_lane_microblocks_and_checkpoint_v1(
            identity_keypair,
            signing_key,
            recent_blockhash,
            slot,
            lane_id,
            refs,
        );
        let Some(checkpoint) = checkpoint else {
            return out;
        };

        let epoch: u64 = bank.epoch_schedule().get_epoch(slot);
        let stakes = bank
            .epoch_staked_nodes(epoch)
            .unwrap_or_else(|| bank.current_epoch_staked_nodes());

        let mut attest = DaAttestV1 {
            header: DaAttestHeaderV1 {
                epoch,
                slot,
                lane_id,
                checkpoint_ix: checkpoint.header.checkpoint_ix,
                checkpoint_id: checkpoint.header.checkpoint_id_v1(),
                validator_pubkey: identity_keypair.pubkey(),
            },
            sig: [0u8; 64],
        };
        attest.sign_v1(signing_key);

        let cert = DaCertV1 {
            epoch,
            slot,
            lane_id,
            checkpoint_ix: checkpoint.header.checkpoint_ix,
            checkpoint_id: checkpoint.header.checkpoint_id_v1(),
            stake_threshold_bps: self.cfg.da_threshold_bps,
            sigs: vec![(identity_keypair.pubkey().to_bytes(), attest.sig)],
        };

        // Only emit a DA certificate if it meets the configured stake threshold.
        if cert.verify_with_stakes_v1(stakes.as_ref()).is_ok() {
            out.extend(build_mcp_da_cert_memo_txs_v1(
                identity_keypair,
                signing_key,
                recent_blockhash,
                &cert,
            ));
        }

        out
    }

    pub fn build_lane_microblock_memo_txs_from_payloads_v1(
        &self,
        identity_keypair: &Keypair,
        signing_key: &SigningKey,
        recent_blockhash: Hash,
        slot: u64,
        lane_id: LaneId,
        tx_payloads: &[&[u8]],
    ) -> Vec<Vec<u8>> {
        if slot == 0 || tx_payloads.is_empty() {
            return Vec::new();
        }
        if lane_id >= self.cfg.lanes_per_slot {
            return Vec::new();
        }

        let mut refs: Vec<McpTxRefV1> = Vec::with_capacity(tx_payloads.len());
        for payload in tx_payloads {
            let blob_id = tx_blob_id_v1(payload);
            if select_lane_for_blob_id_v1(slot, blob_id, self.cfg.lanes_per_slot) != lane_id {
                continue;
            }
            refs.push(McpTxRefV1 {
                blob_id,
                bid_hint: bid_hint_from_wire_tx_v1(payload).unwrap_or(BidHintV1 {
                    cu_price: 0,
                    cu_limit: 0,
                    sig_count: 0,
                }),
            });
        }
        if refs.is_empty() {
            return Vec::new();
        }
        refs.sort_by(|a, b| {
            bid_key_from_hint_v1(a.blob_id, a.bid_hint).cmp(&bid_key_from_hint_v1(b.blob_id, b.bid_hint))
        });

        let max_refs = usize::from(self.cfg.microblock_max_refs).max(1);

        let now = now_ms();
        let mut lane_builders = self.lane_builders.lock().unwrap_or_else(|p| p.into_inner());
        let min_slot = slot.saturating_sub(32);
        lane_builders.retain(|(s, _), st| *s >= min_slot && now.saturating_sub(st.last_update_ms) <= 120_000);

        let state = lane_builders.entry((slot, lane_id)).or_insert_with(|| LaneBuilderStateV1 {
            next_seq: 0,
            next_checkpoint_ix: 0,
            prev_microblock_id: [0u8; 32],
            prev_poh: lane_poh_init_v1(slot, lane_id),
            last_update_ms: now,
        });

        let leader_pubkey = identity_keypair.pubkey();
        let mut out: Vec<Vec<u8>> = Vec::new();
        for refs_chunk in refs.chunks(max_refs) {
            let mut mb = McpMicroblockV1::build_unsigned(
                slot,
                lane_id,
                state.next_seq,
                state.prev_microblock_id,
                state.prev_poh,
                refs_chunk.to_vec(),
                0,
                leader_pubkey,
            );
            mb.sign_v1(signing_key);

            let microblock_id = mb.header.microblock_id_v1();
            state.prev_microblock_id = microblock_id;
            state.prev_poh = mb.header.poh_hash;
            state.next_seq = state.next_seq.saturating_add(1);
            state.last_update_ms = now_ms();

            out.extend(build_mcp_microblock_memo_txs_v1(
                identity_keypair,
                signing_key,
                recent_blockhash,
                &mb,
            ));
        }

        out
    }

    pub fn status_snapshot(&self) -> McpStatus {
        let vote_withholding_feature_active =
            self.vote_withholding_feature_active.load(Ordering::Relaxed);
        McpStatus {
            cfg: self.cfg.clone(),
            vote_withholding_feature_active,
            vote_withholding_enabled: self.mcp_slashing_enforce_enabled(vote_withholding_feature_active),
            audit_checked_total: self.audit_checked.load(Ordering::Relaxed),
            audit_failed_total: self.audit_failed.load(Ordering::Relaxed),
            ledger_order_mismatch_total: self.ledger_order_mismatch.load(Ordering::Relaxed),
            slashed_leaders_len: self.slashed_leaders.len() as u64,
            votes_withheld_total: self.votes_withheld.load(Ordering::Relaxed),
            memo_chunks_seen_total: self.memo_chunks_seen.load(Ordering::Relaxed),
            memo_chunks_invalid_total: self.memo_chunks_invalid.load(Ordering::Relaxed),
            audited_slots_len: self.audited_slots.len() as u64,
        }
    }

    fn note_mcp_vote_withheld(&self, _leader: &Pubkey, _slot: u64) {
        self.votes_withheld.fetch_add(1, Ordering::Relaxed);
    }

    fn mcp_slashing_enforce_enabled(&self, vote_withholding_feature_active: bool) -> bool {
        self.cfg.enforce_vote_withholding && vote_withholding_feature_active
    }

    pub fn mcp_slashing_is_slashed_leader(
        &self,
        leader: &Pubkey,
        slot: u64,
        vote_withholding_feature_active: bool,
    ) -> bool {
        if !self.mcp_slashing_enforce_enabled(vote_withholding_feature_active) {
            return false;
        }
        let key = McpSlashedKey {
            leader: leader.to_bytes(),
            slot,
        };
        self.mcp_slashing_is_slashed_key(&key, now_ms())
    }

    fn mcp_slashing_is_slashed_key(&self, key: &McpSlashedKey, now: u64) -> bool {
        let Some(entry) = self.slashed_leaders.get(key) else {
            return false;
        };
        if entry.expires_at_ms < now {
            drop(entry);
            self.slashed_leaders.remove(key);
            return false;
        }
        true
    }

    fn mark_mcp_slashed(
        &self,
        leader: [u8; 32],
        slot: u64,
        now: u64,
        reason: &'static str,
        vote_withholding_feature_active: bool,
    ) {
        let key = McpSlashedKey { leader, slot };
        let already = self.mcp_slashing_is_slashed_key(&key, now);
        let entry = McpSlashedEntry {
            expires_at_ms: now.saturating_add(MCP_SLASHED_TTL_MS),
        };
        if self.slashed_leaders.len() > MCP_SLASHED_MAX_ENTRIES {
            self.slashed_leaders.clear();
        }
        self.slashed_leaders.insert(key, entry);

        if !already {
            let leader = Pubkey::new_from_array(leader);
            if self.mcp_slashing_enforce_enabled(vote_withholding_feature_active) {
                warn!(
                    "mcp: detected audit violation; withholding votes for leader={} slot={} reason={}",
                    leader,
                    slot,
                    reason
                );
            } else if self.cfg.enforce_vote_withholding {
                warn!(
                    "mcp: detected audit violation (vote withholding feature inactive); leader={} slot={} reason={}",
                    leader,
                    slot,
                    reason
                );
            } else {
                warn!(
                    "mcp: detected audit violation (enforcement disabled); leader={} slot={} reason={}",
                    leader,
                    slot,
                    reason
                );
            }
        }
    }

    fn audit_slot(
        &self,
        blockstore: &Blockstore,
        bank: &Bank,
        leader_schedule_cache: &LeaderScheduleCache,
    ) {
        let slot = bank.slot();
        let vote_withholding_feature_active = bank
            .feature_set
            .is_active(&agave_feature_set::mcp_vote_withholding::ID);
        self.vote_withholding_feature_active
            .store(vote_withholding_feature_active, Ordering::Relaxed);

        if self.audited_slots.contains_key(&slot) {
            return;
        }

        self.audit_checked.fetch_add(1, Ordering::Relaxed);

        let ok = match blockstore.get_slot_entries(slot, 0) {
            Ok(entries) => self.audit_mcp_memos_in_entries(&entries, bank, leader_schedule_cache),
            Err(_) => true,
        };

        if self.audited_slots.len() > 100_000 {
            self.audited_slots.clear();
        }
        self.audited_slots.insert(slot, ok);
        if !ok {
            self.audit_failed.fetch_add(1, Ordering::Relaxed);
            warn!("mcp: slot {slot} ledger audit failed");
            self.mark_mcp_slashed(
                bank.collector_id().to_bytes(),
                slot,
                now_ms(),
                "audit_failed",
                vote_withholding_feature_active,
            );
        }
    }

    fn audit_mcp_memos_in_entries(
        &self,
        entries: &[Entry],
        bank: &Bank,
        leader_schedule_cache: &LeaderScheduleCache,
    ) -> bool {
        if entries.is_empty() {
            return true;
        }

        let slot = bank.slot();
        let vote_withholding_feature_active = bank
            .feature_set
            .is_active(&agave_feature_set::mcp_vote_withholding::ID);
        let expected_leader = bank.collector_id().to_bytes();
        let leader_mode = self.cfg.leader_mode;
        let scheduled_lane_leaders = (leader_mode == McpLeaderMode::ScheduledLeaderSchedule).then(|| {
            self.scheduled_lane_leaders_from_leader_schedule_v1(slot, bank, leader_schedule_cache)
        });

        let memo_program_id = MCP_LEDGER_MEMO_PROGRAM_ID;

        #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
        struct ObjectKeyV1 {
            kind: McpLedgerMemoKindV1,
            lane_id: LaneId,
            object_id: [u8; 32],
        }

        #[derive(Clone, Debug)]
        struct ObjectChunksV1 {
            leader_pubkey: [u8; 32],
            chunk_total: u16,
            chunks: Vec<Option<Vec<u8>>>,
            seen: u16,
        }

        impl ObjectChunksV1 {
            fn new(leader_pubkey: [u8; 32], chunk_total: u16) -> Option<Self> {
                if chunk_total == 0 || (chunk_total as usize) > MCP_LEDGER_MEMO_MAX_CHUNKS_PER_OBJECT {
                    return None;
                }
                Some(Self {
                    leader_pubkey,
                    chunk_total,
                    chunks: vec![None; chunk_total as usize],
                    seen: 0,
                })
            }

            fn insert(&mut self, chunk_index: u16, object_chunk: Vec<u8>) -> bool {
                let idx = chunk_index as usize;
                let Some(slot) = self.chunks.get_mut(idx) else {
                    return false;
                };
                if let Some(existing) = slot.as_ref() {
                    return existing == &object_chunk;
                }
                *slot = Some(object_chunk);
                self.seen = self.seen.saturating_add(1);
                true
            }

            fn is_complete(&self) -> bool {
                self.seen == self.chunk_total
            }

            fn reassemble(&self) -> Option<Vec<u8>> {
                if !self.is_complete() {
                    return None;
                }
                let mut out: Vec<u8> = Vec::new();
                for c in &self.chunks {
                    out.extend_from_slice(c.as_ref()?);
                }
                Some(out)
            }
        }

        let mut objects: HashMap<ObjectKeyV1, ObjectChunksV1> = HashMap::new();
        let mut ok = true;

        for entry in entries {
            for tx in entry.transactions.iter() {
                let (account_keys, instructions) = match &tx.message {
                    VersionedMessage::Legacy(msg) => {
                        (msg.account_keys.as_slice(), msg.instructions.as_slice())
                    }
                    VersionedMessage::V0(msg) => (msg.account_keys.as_slice(), msg.instructions.as_slice()),
                };

                for ix in instructions {
                    let Some(program_id) = account_keys.get(ix.program_id_index as usize) else {
                        continue;
                    };
                    if program_id != &memo_program_id {
                        continue;
                    }
                    let data = ix.data.as_slice();
                    if data.len() < MCP_LEDGER_MEMO_MAGIC.len()
                        || &data[..MCP_LEDGER_MEMO_MAGIC.len()] != MCP_LEDGER_MEMO_MAGIC
                    {
                        continue;
                    }

                    let Ok(chunk) = bincode::deserialize::<McpLedgerMemoChunkV1>(data) else {
                        continue;
                    };

                    self.memo_chunks_seen.fetch_add(1, Ordering::Relaxed);

                    if !chunk.verify() {
                        self.memo_chunks_invalid.fetch_add(1, Ordering::Relaxed);
                        ok = false;
                        continue;
                    }

                    let p = chunk.payload;
                    if p.slot != slot {
                        continue;
                    }

                    match leader_mode {
                        McpLeaderMode::SlotLeaderOnly => {
                            // Default (safe): only accept objects signed by the slot leader.
                            if p.leader_pubkey != expected_leader {
                                continue;
                            }
                        }
                        McpLeaderMode::AnyLeader => {}
                        McpLeaderMode::ScheduledLeaderSchedule => {
                            let Some(scheduled_lane_leaders) = scheduled_lane_leaders.as_ref() else {
                                ok = false;
                                continue;
                            };
                            let Some(&lane_leader) = scheduled_lane_leaders.get(&p.lane_id) else {
                                ok = false;
                                continue;
                            };
                            if p.leader_pubkey != lane_leader {
                                ok = false;
                                continue;
                            }
                        }
                    }
                    if p.lane_id >= self.cfg.lanes_per_slot {
                        ok = false;
                        continue;
                    }

                    let key = ObjectKeyV1 {
                        kind: p.kind,
                        lane_id: p.lane_id,
                        object_id: p.object_id,
                    };

                    if p.chunk_total == 0
                        || (p.chunk_total as usize) > MCP_LEDGER_MEMO_MAX_CHUNKS_PER_OBJECT
                    {
                        ok = false;
                        continue;
                    }

                    let obj = objects.entry(key).or_insert_with(|| {
                        ObjectChunksV1::new(p.leader_pubkey, p.chunk_total)
                            .expect("chunk_total validated")
                    });
                    if obj.chunk_total != p.chunk_total || obj.leader_pubkey != p.leader_pubkey {
                        ok = false;
                        continue;
                    }
                    if !obj.insert(p.chunk_index, p.object_chunk) {
                        ok = false;
                        continue;
                    }
                }
            }
        }

        let mut microblocks_by_lane_leader: HashMap<LaneId, HashMap<[u8; 32], Vec<McpMicroblockV1>>> =
            HashMap::new();
        let mut checkpoints: Vec<([u8; 32], [u8; 32], McpCheckpointV1)> = Vec::new();
        let mut certs: Vec<([u8; 32], [u8; 32], DaCertV1)> = Vec::new();

        for (key, obj) in objects.into_iter() {
            let Some(bytes) = obj.reassemble() else {
                ok = false;
                continue;
            };

            let object_leader_pubkey = obj.leader_pubkey;
            match key.kind {
                McpLedgerMemoKindV1::Microblock => {
                    let Ok(mb) = McpMicroblockV1::decode_v1(&bytes) else {
                        ok = false;
                        continue;
                    };
                    if mb.header.slot != slot || mb.header.lane_id != key.lane_id {
                        ok = false;
                        continue;
                    }
                    if mb.header.microblock_id_v1() != key.object_id {
                        ok = false;
                        continue;
                    }
                    if mb.header.leader_pubkey.to_bytes() != object_leader_pubkey {
                        ok = false;
                        continue;
                    }
                    microblocks_by_lane_leader
                        .entry(key.lane_id)
                        .or_default()
                        .entry(object_leader_pubkey)
                        .or_default()
                        .push(mb);
                }
                McpLedgerMemoKindV1::Checkpoint => {
                    let Ok(ckpt) = McpCheckpointV1::decode_v1(&bytes) else {
                        ok = false;
                        continue;
                    };
                    if ckpt.header.slot != slot || ckpt.header.lane_id != key.lane_id {
                        ok = false;
                        continue;
                    }
                    if ckpt.header.checkpoint_id_v1() != key.object_id {
                        ok = false;
                        continue;
                    }
                    if ckpt.header.leader_pubkey.to_bytes() != object_leader_pubkey {
                        ok = false;
                        continue;
                    }
                    checkpoints.push((object_leader_pubkey, key.object_id, ckpt));
                }
                McpLedgerMemoKindV1::DaCert => {
                    let Ok(cert) = DaCertV1::decode_v1(&bytes) else {
                        ok = false;
                        continue;
                    };
                    if cert.slot != slot || cert.lane_id != key.lane_id {
                        ok = false;
                        continue;
                    }
                    if cert.cert_id_v1() != key.object_id {
                        ok = false;
                        continue;
                    }
                    certs.push((object_leader_pubkey, key.object_id, cert));
                }
            }
        }

        // Verify lane microblock chains + ordering.
        let mut lane_microblocks: HashMap<LaneId, Vec<McpMicroblockV1>> = HashMap::new();
        let mut lane_leaders: HashMap<LaneId, [u8; 32]> = HashMap::new();
        let staked_nodes = bank.current_epoch_staked_nodes();
        for (lane_id, by_leader) in microblocks_by_lane_leader {
            if leader_mode == McpLeaderMode::ScheduledLeaderSchedule {
                let Some(scheduled_lane_leaders) = scheduled_lane_leaders.as_ref() else {
                    ok = false;
                    continue;
                };
                let Some(&scheduled_leader) = scheduled_lane_leaders.get(&lane_id) else {
                    ok = false;
                    continue;
                };

                let mut selected_mbs: Option<Vec<McpMicroblockV1>> = None;
                for (leader_bytes, mbs) in by_leader {
                    if leader_bytes == scheduled_leader {
                        selected_mbs = Some(mbs);
                        break;
                    }
                }
                let Some(mbs) = selected_mbs else {
                    ok = false;
                    continue;
                };

                match verify_lane_microblocks_v1(
                    slot,
                    lane_id,
                    self.cfg.lanes_per_slot,
                    mbs,
                    scheduled_leader,
                ) {
                    Ok(ordered) => {
                        lane_leaders.insert(lane_id, scheduled_leader);
                        lane_microblocks.insert(lane_id, ordered);
                    }
                    Err(_) => {
                        ok = false;
                    }
                }
                continue;
            }

            let mut candidates: Vec<(u64, [u8; 32], Vec<McpMicroblockV1>)> =
                Vec::with_capacity(by_leader.len());
            for (leader_bytes, mbs) in by_leader {
                if self.cfg.leader_mode == McpLeaderMode::SlotLeaderOnly
                    && leader_bytes != expected_leader
                {
                    continue;
                }
                let stake = staked_nodes
                    .get(&Pubkey::new_from_array(leader_bytes))
                    .copied()
                    .unwrap_or(0);
                candidates.push((stake, leader_bytes, mbs));
            }

            candidates.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

            let mut selected: Option<([u8; 32], Vec<McpMicroblockV1>)> = None;
            for (_stake, leader_bytes, mbs) in candidates {
                match verify_lane_microblocks_v1(
                    slot,
                    lane_id,
                    self.cfg.lanes_per_slot,
                    mbs,
                    leader_bytes,
                ) {
                    Ok(ordered) => {
                        selected = Some((leader_bytes, ordered));
                        break;
                    }
                    Err(_) => {}
                }
            }

            if let Some((leader_bytes, ordered)) = selected {
                lane_leaders.insert(lane_id, leader_bytes);
                lane_microblocks.insert(lane_id, ordered);
            } else {
                ok = false;
            }
        }

        // Optional: compare on-ledger transaction order against the merged MCP order
        // (subsequence check, ignores missing/extra transactions).
        let merged_refs = merge_lanes_by_bid_v1(&lane_microblocks);
        if !merged_refs.is_empty() {
            let mut expected_pos: HashMap<TxBlobId, usize> = HashMap::with_capacity(merged_refs.len());
            for (idx, r) in merged_refs.iter().enumerate() {
                expected_pos.entry(r.blob_id).or_insert(idx);
            }

            let mut last_seen_pos: Option<usize> = None;
            let mut mismatch = false;
            for entry in entries {
                for tx in entry.transactions.iter() {
                    let Ok(tx_bytes) = bincode::serialize(tx) else {
                        continue;
                    };
                    let blob_id = tx_blob_id_v1(&tx_bytes);
                    let Some(&pos) = expected_pos.get(&blob_id) else {
                        continue;
                    };
                    if let Some(prev) = last_seen_pos {
                        if pos < prev {
                            mismatch = true;
                            break;
                        }
                    }
                    last_seen_pos = Some(pos);
                }
                if mismatch {
                    break;
                }
            }
            if mismatch {
                self.ledger_order_mismatch.fetch_add(1, Ordering::Relaxed);
                self.mark_mcp_slashed(
                    expected_leader,
                    slot,
                    now_ms(),
                    "ledger_order_mismatch",
                    vote_withholding_feature_active,
                );
            }
        }

        // Verify checkpoints against lane microblocks.
        let mut checkpoints_by_id: HashMap<[u8; 32], McpCheckpointV1> = HashMap::new();
        for (memo_leader, id, ckpt) in checkpoints {
            if ckpt.header.lane_id >= self.cfg.lanes_per_slot {
                ok = false;
                continue;
            }
            let Some(&lane_leader) = lane_leaders.get(&ckpt.header.lane_id) else {
                ok = false;
                continue;
            };
            if ckpt.header.leader_pubkey.to_bytes() != lane_leader || memo_leader != lane_leader {
                if self.cfg.leader_mode != McpLeaderMode::AnyLeader {
                    ok = false;
                }
                continue;
            }
            if ckpt.verify_v1().is_err() {
                ok = false;
                continue;
            }
            let Some(mbs) = lane_microblocks.get(&ckpt.header.lane_id) else {
                ok = false;
                continue;
            };
            let first = ckpt.header.first_seq_no as usize;
            let last = ckpt.header.last_seq_no as usize;
            if first > last || last >= mbs.len() {
                ok = false;
                continue;
            }
            let ids: Vec<McpHash32> = mbs[first..=last]
                .iter()
                .map(|mb| mb.header.microblock_id_v1())
                .collect();
            let got_root = microblock_root_v1(&ids);
            if got_root != ckpt.header.microblock_root {
                ok = false;
                continue;
            }
            let got_count: u32 = mbs[first..=last]
                .iter()
                .map(|mb| mb.refs.len() as u32)
                .sum();
            if got_count != ckpt.header.tx_ref_count {
                ok = false;
                continue;
            }
            checkpoints_by_id.insert(id, ckpt);
        }

        // Verify DA certs (structure, linkage, and stake threshold).
        for (memo_leader, _id, cert) in certs {
            if cert.lane_id >= self.cfg.lanes_per_slot {
                ok = false;
                continue;
            }
            let Some(&lane_leader) = lane_leaders.get(&cert.lane_id) else {
                ok = false;
                continue;
            };
            if memo_leader != lane_leader {
                if self.cfg.leader_mode != McpLeaderMode::AnyLeader {
                    ok = false;
                }
                continue;
            }
            if cert.stake_threshold_bps != self.cfg.da_threshold_bps {
                ok = false;
                continue;
            }
            let Some(ckpt) = checkpoints_by_id.get(&cert.checkpoint_id) else {
                ok = false;
                continue;
            };
            if ckpt.header.lane_id != cert.lane_id || ckpt.header.checkpoint_ix != cert.checkpoint_ix {
                ok = false;
                continue;
            }
            let stakes = bank
                .epoch_staked_nodes(cert.epoch)
                .unwrap_or_else(|| bank.current_epoch_staked_nodes());
            if cert.verify_with_stakes_v1(stakes.as_ref()).is_err() {
                ok = false;
                continue;
            }
        }

        ok
    }
}

fn verify_lane_microblocks_v1(
    slot: u64,
    lane_id: LaneId,
    lanes_per_slot: u8,
    microblocks: Vec<McpMicroblockV1>,
    expected_leader: [u8; 32],
) -> Result<Vec<McpMicroblockV1>, McpError> {
    if microblocks.is_empty() {
        return Ok(Vec::new());
    }
    if lane_id >= lanes_per_slot.max(1) {
        return Err(McpError::InvalidValue);
    }

    // Precompute IDs and parent->children map.
    let mut id_to_mb: HashMap<McpHash32, McpMicroblockV1> = HashMap::with_capacity(microblocks.len());
    let mut parent_to_children: HashMap<McpHash32, Vec<McpHash32>> = HashMap::new();
    let mut genesis: Vec<McpHash32> = Vec::new();

    for mb in microblocks {
        if mb.header.slot != slot || mb.header.lane_id != lane_id {
            return Err(McpError::InvalidValue);
        }
        if mb.header.leader_pubkey.to_bytes() != expected_leader {
            return Err(McpError::InvalidValue);
        }

        // Check tx ref ordering (by bid hints) is deterministic.
        let mut prev_key: Option<BidKeyV1> = None;
        for r in &mb.refs {
            if select_lane_for_blob_id_v1(slot, r.blob_id, lanes_per_slot) != lane_id {
                return Err(McpError::InvalidValue);
            }
            let key = bid_key_from_hint_v1(r.blob_id, r.bid_hint);
            if let Some(prev) = prev_key {
                if prev > key {
                    return Err(McpError::InvalidValue);
                }
            }
            prev_key = Some(key);
        }

        let id = mb.header.microblock_id_v1();
        if id_to_mb.insert(id, mb).is_some() {
            return Err(McpError::InvalidValue);
        }
    }

    for (id, mb) in id_to_mb.iter() {
        let parent = mb.header.prev_microblock_hash;
        if parent == [0u8; 32] {
            genesis.push(*id);
        } else {
            parent_to_children.entry(parent).or_default().push(*id);
        }
    }

    if genesis.len() != 1 {
        return Err(McpError::InvalidValue);
    }

    let mut ordered: Vec<McpMicroblockV1> = Vec::new();
    let mut prev_id: McpHash32 = [0u8; 32];
    let mut prev_poh: McpHash32 = lane_poh_init_v1(slot, lane_id);
    let mut next_id = genesis[0];
    let mut expected_seq: u32 = 0;

    loop {
        let Some(mb) = id_to_mb.remove(&next_id) else {
            return Err(McpError::InvalidValue);
        };
        if mb.header.seq_no != expected_seq {
            return Err(McpError::InvalidValue);
        }
        if mb.header.prev_microblock_hash != prev_id {
            return Err(McpError::InvalidValue);
        }
        mb.verify_v1(prev_poh)?;
        prev_poh = mb.header.poh_hash;
        prev_id = next_id;
        expected_seq = expected_seq.saturating_add(1);
        ordered.push(mb);

        match parent_to_children.get(&prev_id).map(|v| v.as_slice()) {
            None => break,
            Some([only]) => next_id = *only,
            Some(_) => return Err(McpError::InvalidValue),
        }
    }

    // Any remaining microblocks imply equivocation or disconnected chain.
    if !id_to_mb.is_empty() {
        return Err(McpError::InvalidValue);
    }

    Ok(ordered)
}

#[derive(Debug, Error)]
pub enum McpError {
    #[error("encode error")]
    Encode,
    #[error("decode error")]
    Decode,
    #[error("trailing bytes")]
    TrailingBytes,
    #[error("invalid value")]
    InvalidValue,
    #[error("signature verify failed")]
    BadSignature,
    #[error("poh verify failed")]
    BadPoh,
    #[error("stake threshold not met")]
    StakeThresholdNotMet,
}

fn sha256v_bytes(parts: &[&[u8]]) -> McpHash32 {
    let digest = sha256_hasher::hashv(parts);
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

fn sha256_domain(domain: &[u8; 8], parts: &[&[u8]]) -> McpHash32 {
    let mut all: Vec<&[u8]> = Vec::with_capacity(parts.len().saturating_add(1));
    all.push(domain);
    all.extend_from_slice(parts);
    sha256v_bytes(&all)
}

pub fn lane_poh_init_v1(slot: u64, lane_id: LaneId) -> McpHash32 {
    let slot_bytes = slot.to_le_bytes();
    let lane_bytes = [lane_id];
    sha256_domain(MCP_HASH_DOMAIN_POH_INIT_V1, &[&slot_bytes, &lane_bytes])
}

pub fn select_lane_for_blob_id_v1(slot: u64, blob_id: TxBlobId, lanes_per_slot: u8) -> LaneId {
    let lanes = lanes_per_slot.max(1);
    let slot_bytes = slot.to_le_bytes();
    let digest = sha256_domain(MCP_HASH_DOMAIN_TX_LANE_SELECT_V1, &[&slot_bytes, &blob_id]);
    digest[0] % lanes
}

pub fn tx_blob_id_v1(tx_bytes: &[u8]) -> TxBlobId {
    sha256_domain(MCP_HASH_DOMAIN_BLOB_ID_V1, &[tx_bytes])
}

fn microblock_root_v1(microblock_ids: &[McpHash32]) -> McpHash32 {
    let mut w = McpEncWriter::default();
    w.write_u32(u32::try_from(microblock_ids.len()).unwrap_or(u32::MAX));
    for id in microblock_ids {
        w.write_hash32(id);
    }
    let bytes = w.into_bytes();
    sha256_domain(MCP_HASH_DOMAIN_CKPT_ROOT_V1, &[&bytes])
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BidHintV1 {
    /// `ComputeBudgetInstruction::set_compute_unit_price` (micro-lamports / CU).
    pub cu_price: u64,
    pub cu_limit: u32,
    pub sig_count: u16,
}

pub(crate) fn bid_hint_from_wire_tx_v1(payload: &[u8]) -> Option<BidHintV1> {
    let tx: VersionedTransaction = bincode::deserialize(payload).ok()?;

    let sig_count = tx.signatures.len().min(u16::MAX as usize) as u16;

    let (account_keys, instructions) = match &tx.message {
        VersionedMessage::Legacy(msg) => (msg.account_keys.as_slice(), msg.instructions.as_slice()),
        VersionedMessage::V0(msg) => (msg.account_keys.as_slice(), msg.instructions.as_slice()),
    };

    let mut cu_limit: u32 = 0;
    let mut cu_price: u64 = 0;
    for ix in instructions {
        let Some(program_id) = account_keys.get(ix.program_id_index as usize) else {
            continue;
        };
        if *program_id != COMPUTE_BUDGET_PROGRAM_ID {
            continue;
        }
        if ix.data.is_empty() {
            continue;
        }

        // These match the borsh-serialized `ComputeBudgetInstruction` enum variant indices:
        // - 2 => SetComputeUnitLimit(u32)
        // - 3 => SetComputeUnitPrice(u64)
        match ix.data[0] {
            2 => {
                if ix.data.len() >= 1 + 4 {
                    let bytes: [u8; 4] = ix.data[1..5].try_into().ok()?;
                    cu_limit = u32::from_le_bytes(bytes);
                }
            }
            3 => {
                if ix.data.len() >= 1 + 8 {
                    let bytes: [u8; 8] = ix.data[1..9].try_into().ok()?;
                    cu_price = u64::from_le_bytes(bytes);
                }
            }
            _ => {}
        }
    }

    Some(BidHintV1 {
        cu_price,
        cu_limit,
        sig_count,
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BidKeyV1 {
    pub cu_price: u64,
    pub priority_fee: u64,
    pub blob_id: TxBlobId,
}

impl Ord for BidKeyV1 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Highest bid first: cu_price desc, then priority_fee desc.
        // Deterministic tie-break: blob_id asc.
        other
            .cu_price
            .cmp(&self.cu_price)
            .then_with(|| other.priority_fee.cmp(&self.priority_fee))
            .then_with(|| self.blob_id.cmp(&other.blob_id))
    }
}

impl PartialOrd for BidKeyV1 {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub fn bid_key_from_hint_v1(
    blob_id: TxBlobId,
    bid_hint: BidHintV1,
) -> BidKeyV1 {
    let priority_fee: u64 = {
        let micro_lamport_fee: u128 = (bid_hint.cu_price as u128)
            .saturating_mul(u128::from(bid_hint.cu_limit));
        micro_lamport_fee
            .saturating_add(u128::from(MCP_MICRO_LAMPORTS_PER_LAMPORT - 1))
            .checked_div(u128::from(MCP_MICRO_LAMPORTS_PER_LAMPORT))
            .and_then(|fee| u64::try_from(fee).ok())
            .unwrap_or(u64::MAX)
    };

    BidKeyV1 {
        cu_price: bid_hint.cu_price,
        priority_fee,
        blob_id,
    }
}

pub fn merge_lanes_by_bid_v1(
    lane_microblocks: &HashMap<LaneId, Vec<McpMicroblockV1>>,
) -> Vec<McpTxRefV1> {
    use std::cmp::Reverse;
    use std::collections::{BinaryHeap, HashSet};

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct CursorV1 {
        lane_id: LaneId,
        microblock_index: usize,
        ref_index: usize,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    struct HeapItemV1 {
        bid: BidKeyV1,
        cursor: CursorV1,
    }

    impl Ord for HeapItemV1 {
        fn cmp(&self, other: &Self) -> std::cmp::Ordering {
            Reverse(self.bid)
                .cmp(&Reverse(other.bid))
                .then_with(|| Reverse(self.cursor.lane_id).cmp(&Reverse(other.cursor.lane_id)))
                .then_with(|| {
                    Reverse(self.cursor.microblock_index).cmp(&Reverse(other.cursor.microblock_index))
                })
                .then_with(|| Reverse(self.cursor.ref_index).cmp(&Reverse(other.cursor.ref_index)))
        }
    }

    impl PartialOrd for HeapItemV1 {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(other))
        }
    }

    fn next_cursor_v1(
        microblocks: &[McpMicroblockV1],
        mut microblock_index: usize,
        mut ref_index: usize,
    ) -> Option<(usize, usize)> {
        loop {
            let mb = microblocks.get(microblock_index)?;
            if let Some(_r) = mb.refs.get(ref_index) {
                return Some((microblock_index, ref_index));
            }
            microblock_index = microblock_index.saturating_add(1);
            ref_index = 0;
        }
    }

    let mut lane_ids: Vec<LaneId> = lane_microblocks.keys().copied().collect();
    lane_ids.sort_unstable();

    let mut heap: BinaryHeap<HeapItemV1> = BinaryHeap::new();
    for lane_id in lane_ids.iter().copied() {
        let Some(microblocks) = lane_microblocks.get(&lane_id) else {
            continue;
        };
        let Some((mb_idx, ref_idx)) = next_cursor_v1(microblocks, 0, 0) else {
            continue;
        };
        let r = &microblocks[mb_idx].refs[ref_idx];
        heap.push(HeapItemV1 {
            bid: bid_key_from_hint_v1(r.blob_id, r.bid_hint),
            cursor: CursorV1 {
                lane_id,
                microblock_index: mb_idx,
                ref_index: ref_idx,
            },
        });
    }

    let mut seen: HashSet<TxBlobId> = HashSet::new();
    let mut out: Vec<McpTxRefV1> = Vec::new();

    while let Some(item) = heap.pop() {
        let lane_id = item.cursor.lane_id;
        let Some(microblocks) = lane_microblocks.get(&lane_id) else {
            continue;
        };
        let Some(mb) = microblocks.get(item.cursor.microblock_index) else {
            continue;
        };
        let Some(r) = mb.refs.get(item.cursor.ref_index) else {
            continue;
        };
        if seen.insert(r.blob_id) {
            out.push(r.clone());
        }

        let next_ref_index = item.cursor.ref_index.saturating_add(1);
        if let Some((mb_idx, ref_idx)) =
            next_cursor_v1(microblocks, item.cursor.microblock_index, next_ref_index)
        {
            let r = &microblocks[mb_idx].refs[ref_idx];
            heap.push(HeapItemV1 {
                bid: bid_key_from_hint_v1(r.blob_id, r.bid_hint),
                cursor: CursorV1 {
                    lane_id,
                    microblock_index: mb_idx,
                    ref_index: ref_idx,
                },
            });
        }
    }

    out
}

impl BidHintV1 {
    pub fn encode_v1(&self) -> Vec<u8> {
        let mut w = McpEncWriter::default();
        w.write_u64(self.cu_price);
        w.write_u32(self.cu_limit);
        w.write_u16(self.sig_count);
        w.into_bytes()
    }

    pub fn decode_v1(bytes: &[u8]) -> Result<Self, McpError> {
        let mut r = McpEncReader::new(bytes);
        let cu_price = r.read_u64()?;
        let cu_limit = r.read_u32()?;
        let sig_count = r.read_u16()?;
        r.finish()?;
        Ok(Self {
            cu_price,
            cu_limit,
            sig_count,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpTxRefV1 {
    pub blob_id: TxBlobId,
    pub bid_hint: BidHintV1,
}

impl McpTxRefV1 {
    pub fn encode_v1(&self) -> Vec<u8> {
        let mut w = McpEncWriter::default();
        w.write_hash32(&self.blob_id);
        w.write_bytes(&self.bid_hint.encode_v1());
        w.into_bytes()
    }

    pub fn decode_v1(bytes: &[u8]) -> Result<Self, McpError> {
        let mut r = McpEncReader::new(bytes);
        let blob_id = r.read_hash32()?;
        let bid_hint_bytes = r.read_bytes()?;
        let bid_hint = BidHintV1::decode_v1(&bid_hint_bytes)?;
        r.finish()?;
        Ok(Self { blob_id, bid_hint })
    }
}

fn refs_root_v1(refs: &[McpTxRefV1]) -> McpHash32 {
    let mut w = McpEncWriter::default();
    w.write_u32(u32::try_from(refs.len()).unwrap_or(u32::MAX));
    for r in refs {
        w.write_hash32(&r.blob_id);
        w.write_u64(r.bid_hint.cu_price);
        w.write_u32(r.bid_hint.cu_limit);
        w.write_u16(r.bid_hint.sig_count);
    }
    let bytes = w.into_bytes();
    sha256_domain(MCP_HASH_DOMAIN_MB_REFS_ROOT_V1, &[&bytes])
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpMicroblockHeaderV1 {
    pub slot: u64,
    pub lane_id: LaneId,
    pub seq_no: u32,
    pub prev_microblock_hash: McpHash32,
    pub refs_root: McpHash32,
    pub poh_hash: McpHash32,
    pub poh_num_hashes_after_mixin: u32,
    pub leader_pubkey: Pubkey,
}

impl McpMicroblockHeaderV1 {
    fn encode_v1(&self) -> Vec<u8> {
        let mut w = McpEncWriter::default();
        w.write_u64(self.slot);
        w.write_u8(self.lane_id);
        w.write_u32(self.seq_no);
        w.write_hash32(&self.prev_microblock_hash);
        w.write_hash32(&self.refs_root);
        w.write_hash32(&self.poh_hash);
        w.write_u32(self.poh_num_hashes_after_mixin);
        w.write_pubkey(&self.leader_pubkey);
        w.into_bytes()
    }

    pub fn microblock_id_v1(&self) -> McpHash32 {
        let bytes = self.encode_v1();
        sha256_domain(MCP_HASH_DOMAIN_MB_ID_V1, &[&bytes])
    }

    fn signing_digest_v1(&self) -> McpHash32 {
        let bytes = self.encode_v1();
        sha256_domain(MCP_HASH_DOMAIN_MB_SIG_V1, &[&bytes])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpMicroblockV1 {
    pub header: McpMicroblockHeaderV1,
    pub refs: Vec<McpTxRefV1>,
    pub leader_sig: [u8; 64],
}

impl McpMicroblockV1 {
    pub fn build_unsigned(
        slot: u64,
        lane_id: LaneId,
        seq_no: u32,
        prev_microblock_hash: McpHash32,
        prev_poh: McpHash32,
        refs: Vec<McpTxRefV1>,
        poh_num_hashes_after_mixin: u32,
        leader_pubkey: Pubkey,
    ) -> Self {
        let refs_root = refs_root_v1(&refs);
        let poh_hash = lane_poh_hash_v1(prev_poh, refs_root, poh_num_hashes_after_mixin);
        Self {
            header: McpMicroblockHeaderV1 {
                slot,
                lane_id,
                seq_no,
                prev_microblock_hash,
                refs_root,
                poh_hash,
                poh_num_hashes_after_mixin,
                leader_pubkey,
            },
            refs,
            leader_sig: [0u8; 64],
        }
    }

    pub fn sign_v1(&mut self, signing_key: &SigningKey) {
        let digest = self.header.signing_digest_v1();
        let signature = signing_key.sign(&digest);
        self.leader_sig = signature.to_bytes();
    }

    pub fn verify_v1(&self, prev_poh: McpHash32) -> Result<(), McpError> {
        if self.header.poh_num_hashes_after_mixin > MCP_MAX_POH_HASHES_AFTER_MIXIN_V1 {
            return Err(McpError::InvalidValue);
        }
        let expected_refs_root = refs_root_v1(&self.refs);
        if self.header.refs_root != expected_refs_root {
            return Err(McpError::InvalidValue);
        }
        let expected_poh = lane_poh_hash_v1(
            prev_poh,
            self.header.refs_root,
            self.header.poh_num_hashes_after_mixin,
        );
        if self.header.poh_hash != expected_poh {
            return Err(McpError::BadPoh);
        }

        let pk_bytes = self.header.leader_pubkey.to_bytes();
        let verifying_key =
            VerifyingKey::from_bytes(&pk_bytes).map_err(|_| McpError::BadSignature)?;
        verifying_key
            .verify_strict(
                &self.header.signing_digest_v1(),
                &ed25519_dalek_v2::Signature::from_bytes(&self.leader_sig),
            )
            .map_err(|_| McpError::BadSignature)?;
        Ok(())
    }

    pub fn encode_v1(&self) -> Vec<u8> {
        let mut w = McpEncWriter::default();
        w.write_u64(self.header.slot);
        w.write_u8(self.header.lane_id);
        w.write_u32(self.header.seq_no);
        w.write_hash32(&self.header.prev_microblock_hash);
        w.write_hash32(&self.header.refs_root);
        w.write_hash32(&self.header.poh_hash);
        w.write_u32(self.header.poh_num_hashes_after_mixin);
        w.write_pubkey(&self.header.leader_pubkey);
        w.write_u32(u32::try_from(self.refs.len()).unwrap_or(u32::MAX));
        for r in &self.refs {
            w.write_hash32(&r.blob_id);
            w.write_u64(r.bid_hint.cu_price);
            w.write_u32(r.bid_hint.cu_limit);
            w.write_u16(r.bid_hint.sig_count);
        }
        w.write_sig64(&self.leader_sig);
        w.into_bytes()
    }

    pub fn decode_v1(bytes: &[u8]) -> Result<Self, McpError> {
        let mut r = McpEncReader::new(bytes);
        let slot = r.read_u64()?;
        let lane_id = r.read_u8()?;
        let seq_no = r.read_u32()?;
        let prev_microblock_hash = r.read_hash32()?;
        let refs_root = r.read_hash32()?;
        let poh_hash = r.read_hash32()?;
        let poh_num_hashes_after_mixin = r.read_u32()?;
        let leader_pubkey = r.read_pubkey()?;
        let refs_len = r.read_u32()? as usize;
        let mut refs = Vec::with_capacity(refs_len.min(1_000_000));
        for _ in 0..refs_len {
            let blob_id = r.read_hash32()?;
            let cu_price = r.read_u64()?;
            let cu_limit = r.read_u32()?;
            let sig_count = r.read_u16()?;
            refs.push(McpTxRefV1 {
                blob_id,
                bid_hint: BidHintV1 {
                    cu_price,
                    cu_limit,
                    sig_count,
                },
            });
        }
        let leader_sig = r.read_sig64()?;
        r.finish()?;
        Ok(Self {
            header: McpMicroblockHeaderV1 {
                slot,
                lane_id,
                seq_no,
                prev_microblock_hash,
                refs_root,
                poh_hash,
                poh_num_hashes_after_mixin,
                leader_pubkey,
            },
            refs,
            leader_sig,
        })
    }
}

pub fn lane_poh_hash_v1(prev_poh: McpHash32, refs_root: McpHash32, num_hashes_after: u32) -> McpHash32 {
    let mut poh = sha256_domain(
        MCP_HASH_DOMAIN_POH_MIXIN_V1,
        &[&prev_poh, &refs_root],
    );
    for _ in 0..num_hashes_after {
        poh = sha256_domain(MCP_HASH_DOMAIN_POH_STEP_V1, &[&poh]);
    }
    poh
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpCheckpointHeaderV1 {
    pub slot: u64,
    pub lane_id: LaneId,
    pub checkpoint_ix: u16,
    pub first_seq_no: u32,
    pub last_seq_no: u32,
    pub microblock_root: McpHash32,
    pub tx_ref_count: u32,
    pub leader_pubkey: Pubkey,
}

impl McpCheckpointHeaderV1 {
    fn encode_v1(&self) -> Vec<u8> {
        let mut w = McpEncWriter::default();
        w.write_u64(self.slot);
        w.write_u8(self.lane_id);
        w.write_u16(self.checkpoint_ix);
        w.write_u32(self.first_seq_no);
        w.write_u32(self.last_seq_no);
        w.write_hash32(&self.microblock_root);
        w.write_u32(self.tx_ref_count);
        w.write_pubkey(&self.leader_pubkey);
        w.into_bytes()
    }

    pub fn checkpoint_id_v1(&self) -> McpHash32 {
        let bytes = self.encode_v1();
        sha256_domain(MCP_HASH_DOMAIN_CKPT_ID_V1, &[&bytes])
    }

    fn signing_digest_v1(&self) -> McpHash32 {
        let bytes = self.encode_v1();
        sha256_domain(MCP_HASH_DOMAIN_CKPT_SIG_V1, &[&bytes])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct McpCheckpointV1 {
    pub header: McpCheckpointHeaderV1,
    pub leader_sig: [u8; 64],
}

impl McpCheckpointV1 {
    pub fn sign_v1(&mut self, signing_key: &SigningKey) {
        let signature = signing_key.sign(&self.header.signing_digest_v1());
        self.leader_sig = signature.to_bytes();
    }

    pub fn verify_v1(&self) -> Result<(), McpError> {
        let pk_bytes = self.header.leader_pubkey.to_bytes();
        let verifying_key =
            VerifyingKey::from_bytes(&pk_bytes).map_err(|_| McpError::BadSignature)?;
        verifying_key
            .verify_strict(
                &self.header.signing_digest_v1(),
                &ed25519_dalek_v2::Signature::from_bytes(&self.leader_sig),
            )
            .map_err(|_| McpError::BadSignature)?;
        Ok(())
    }

    pub fn encode_v1(&self) -> Vec<u8> {
        let mut w = McpEncWriter::default();
        w.write_u64(self.header.slot);
        w.write_u8(self.header.lane_id);
        w.write_u16(self.header.checkpoint_ix);
        w.write_u32(self.header.first_seq_no);
        w.write_u32(self.header.last_seq_no);
        w.write_hash32(&self.header.microblock_root);
        w.write_u32(self.header.tx_ref_count);
        w.write_pubkey(&self.header.leader_pubkey);
        w.write_sig64(&self.leader_sig);
        w.into_bytes()
    }

    pub fn decode_v1(bytes: &[u8]) -> Result<Self, McpError> {
        let mut r = McpEncReader::new(bytes);
        let slot = r.read_u64()?;
        let lane_id = r.read_u8()?;
        let checkpoint_ix = r.read_u16()?;
        let first_seq_no = r.read_u32()?;
        let last_seq_no = r.read_u32()?;
        let microblock_root = r.read_hash32()?;
        let tx_ref_count = r.read_u32()?;
        let leader_pubkey = r.read_pubkey()?;
        let leader_sig = r.read_sig64()?;
        r.finish()?;
        Ok(Self {
            header: McpCheckpointHeaderV1 {
                slot,
                lane_id,
                checkpoint_ix,
                first_seq_no,
                last_seq_no,
                microblock_root,
                tx_ref_count,
                leader_pubkey,
            },
            leader_sig,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaAttestHeaderV1 {
    pub epoch: u64,
    pub slot: u64,
    pub lane_id: LaneId,
    pub checkpoint_ix: u16,
    pub checkpoint_id: McpHash32,
    pub validator_pubkey: Pubkey,
}

impl DaAttestHeaderV1 {
    fn encode_v1(&self) -> Vec<u8> {
        let mut w = McpEncWriter::default();
        w.write_u64(self.epoch);
        w.write_u64(self.slot);
        w.write_u8(self.lane_id);
        w.write_u16(self.checkpoint_ix);
        w.write_hash32(&self.checkpoint_id);
        w.write_pubkey(&self.validator_pubkey);
        w.into_bytes()
    }

    pub fn attest_id_v1(&self) -> McpHash32 {
        let bytes = self.encode_v1();
        sha256_domain(MCP_HASH_DOMAIN_DA_ATTEST_ID_V1, &[&bytes])
    }

    fn signing_digest_v1(&self) -> McpHash32 {
        let bytes = self.encode_v1();
        sha256_domain(MCP_HASH_DOMAIN_DA_ATTEST_SIG_V1, &[&bytes])
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaAttestV1 {
    pub header: DaAttestHeaderV1,
    pub sig: [u8; 64],
}

impl DaAttestV1 {
    pub fn sign_v1(&mut self, signing_key: &SigningKey) {
        let signature = signing_key.sign(&self.header.signing_digest_v1());
        self.sig = signature.to_bytes();
    }

    pub fn verify_v1(&self) -> Result<(), McpError> {
        let pk_bytes = self.header.validator_pubkey.to_bytes();
        let verifying_key =
            VerifyingKey::from_bytes(&pk_bytes).map_err(|_| McpError::BadSignature)?;
        verifying_key
            .verify_strict(
                &self.header.signing_digest_v1(),
                &ed25519_dalek_v2::Signature::from_bytes(&self.sig),
            )
            .map_err(|_| McpError::BadSignature)?;
        Ok(())
    }

    pub fn encode_v1(&self) -> Vec<u8> {
        let mut w = McpEncWriter::default();
        w.write_u64(self.header.epoch);
        w.write_u64(self.header.slot);
        w.write_u8(self.header.lane_id);
        w.write_u16(self.header.checkpoint_ix);
        w.write_hash32(&self.header.checkpoint_id);
        w.write_pubkey(&self.header.validator_pubkey);
        w.write_sig64(&self.sig);
        w.into_bytes()
    }

    pub fn decode_v1(bytes: &[u8]) -> Result<Self, McpError> {
        let mut r = McpEncReader::new(bytes);
        let epoch = r.read_u64()?;
        let slot = r.read_u64()?;
        let lane_id = r.read_u8()?;
        let checkpoint_ix = r.read_u16()?;
        let checkpoint_id = r.read_hash32()?;
        let validator_pubkey = r.read_pubkey()?;
        let sig = r.read_sig64()?;
        r.finish()?;
        Ok(Self {
            header: DaAttestHeaderV1 {
                epoch,
                slot,
                lane_id,
                checkpoint_ix,
                checkpoint_id,
                validator_pubkey,
            },
            sig,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DaCertV1 {
    pub epoch: u64,
    pub slot: u64,
    pub lane_id: LaneId,
    pub checkpoint_ix: u16,
    pub checkpoint_id: McpHash32,
    /// Threshold in basis points of total stake (e.g. 6667 ~= 2/3).
    pub stake_threshold_bps: u16,
    /// (validator_pubkey, signature over DaAttestHeaderV1 digest)
    pub sigs: Vec<([u8; 32], [u8; 64])>,
}

impl DaCertV1 {
    fn encode_header_v1(&self) -> Vec<u8> {
        let mut w = McpEncWriter::default();
        w.write_u64(self.epoch);
        w.write_u64(self.slot);
        w.write_u8(self.lane_id);
        w.write_u16(self.checkpoint_ix);
        w.write_hash32(&self.checkpoint_id);
        w.write_u16(self.stake_threshold_bps);
        w.into_bytes()
    }

    pub fn cert_id_v1(&self) -> McpHash32 {
        let bytes = self.encode_header_v1();
        sha256_domain(MCP_HASH_DOMAIN_DA_CERT_ID_V1, &[&bytes])
    }

    pub fn verify_with_stakes_v1(
        &self,
        stake_by_pubkey: &std::collections::HashMap<Pubkey, u64>,
    ) -> Result<(), McpError> {
        if self.stake_threshold_bps == 0 || self.stake_threshold_bps > 10_000 {
            return Err(McpError::InvalidValue);
        }

        // Ensure deterministic form: sorted, unique signers.
        {
            let mut last: Option<&[u8; 32]> = None;
            for (pk, _) in &self.sigs {
                if let Some(prev) = last {
                    if pk <= prev {
                        return Err(McpError::InvalidValue);
                    }
                }
                last = Some(pk);
            }
        }

        let total_stake: u128 = stake_by_pubkey.values().map(|v| u128::from(*v)).sum();
        if total_stake == 0 {
            return Err(McpError::InvalidValue);
        }
        let needed: u128 = total_stake
            .saturating_mul(u128::from(self.stake_threshold_bps))
            .saturating_add(9_999u128)
            / 10_000u128;

        let attest_header = DaAttestHeaderV1 {
            epoch: self.epoch,
            slot: self.slot,
            lane_id: self.lane_id,
            checkpoint_ix: self.checkpoint_ix,
            checkpoint_id: self.checkpoint_id,
            // NOTE: validator_pubkey is per-sig, not in header for cert-level reuse.
            validator_pubkey: Pubkey::default(),
        };

        let mut signed_stake: u128 = 0;
        for (pk_bytes, sig_bytes) in &self.sigs {
            let validator_pubkey = Pubkey::new_from_array(*pk_bytes);
            let Some(stake) = stake_by_pubkey.get(&validator_pubkey) else {
                continue;
            };

            let att = DaAttestV1 {
                header: DaAttestHeaderV1 {
                    validator_pubkey,
                    ..attest_header.clone()
                },
                sig: *sig_bytes,
            };
            att.verify_v1()?;
            signed_stake = signed_stake.saturating_add(u128::from(*stake));
        }

        if signed_stake < needed {
            return Err(McpError::StakeThresholdNotMet);
        }
        Ok(())
    }

    pub fn encode_v1(&self) -> Vec<u8> {
        let mut w = McpEncWriter::default();
        w.write_u64(self.epoch);
        w.write_u64(self.slot);
        w.write_u8(self.lane_id);
        w.write_u16(self.checkpoint_ix);
        w.write_hash32(&self.checkpoint_id);
        w.write_u16(self.stake_threshold_bps);
        w.write_u32(u32::try_from(self.sigs.len()).unwrap_or(u32::MAX));
        for (pk, sig) in &self.sigs {
            w.write_pubkey_bytes(pk);
            w.write_sig64(sig);
        }
        w.into_bytes()
    }

    pub fn decode_v1(bytes: &[u8]) -> Result<Self, McpError> {
        let mut r = McpEncReader::new(bytes);
        let epoch = r.read_u64()?;
        let slot = r.read_u64()?;
        let lane_id = r.read_u8()?;
        let checkpoint_ix = r.read_u16()?;
        let checkpoint_id = r.read_hash32()?;
        let stake_threshold_bps = r.read_u16()?;
        let sigs_len = r.read_u32()? as usize;
        let mut sigs = Vec::with_capacity(sigs_len.min(1_000_000));
        for _ in 0..sigs_len {
            let pk = r.read_pubkey_bytes()?;
            let sig = r.read_sig64()?;
            sigs.push((pk, sig));
        }
        r.finish()?;
        Ok(Self {
            epoch,
            slot,
            lane_id,
            checkpoint_ix,
            checkpoint_id,
            stake_threshold_bps,
            sigs,
        })
    }
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub enum McpLedgerMemoKindV1 {
    Microblock = 1,
    Checkpoint = 2,
    DaCert = 3,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct McpLedgerMemoChunkPayloadV1 {
    magic: [u8; 8],
    version: u8,
    kind: McpLedgerMemoKindV1,
    slot: u64,
    lane_id: LaneId,
    object_id: [u8; 32],
    chunk_index: u16,
    chunk_total: u16,
    object_chunk: Vec<u8>,
    leader_pubkey: [u8; 32],
    leader_time_ms: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct McpLedgerMemoChunkV1 {
    payload: McpLedgerMemoChunkPayloadV1,
    signature: Vec<u8>,
}

impl McpLedgerMemoChunkV1 {
    fn sign(payload: McpLedgerMemoChunkPayloadV1, signing_key: &SigningKey) -> Option<Self> {
        let bytes = bincode::serialize(&payload).ok()?;
        let signature = signing_key.sign(&bytes);
        Some(Self {
            payload,
            signature: signature.to_bytes().to_vec(),
        })
    }

    fn verify(&self) -> bool {
        if self.payload.magic != MCP_LEDGER_MEMO_MAGIC || self.payload.version != MCP_LEDGER_MEMO_VERSION {
            return false;
        }
        let bytes = match bincode::serialize(&self.payload) {
            Ok(v) => v,
            Err(_) => return false,
        };
        let verifying_key = match VerifyingKey::from_bytes(&self.payload.leader_pubkey) {
            Ok(v) => v,
            Err(_) => return false,
        };
        let sig_bytes: [u8; 64] = match self.signature.as_slice().try_into() {
            Ok(v) => v,
            Err(_) => return false,
        };
        verifying_key
            .verify_strict(
                &bytes,
                &ed25519_dalek_v2::Signature::from_bytes(&sig_bytes),
            )
            .is_ok()
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn build_mcp_ledger_memo_txs_v1(
    identity_keypair: &Keypair,
    signing_key: &SigningKey,
    recent_blockhash: Hash,
    kind: McpLedgerMemoKindV1,
    slot: u64,
    lane_id: LaneId,
    object_id: [u8; 32],
    object_bytes: &[u8],
    max_chunk_bytes: usize,
) -> Vec<Vec<u8>> {
    if slot == 0 || object_bytes.is_empty() {
        return Vec::new();
    }

    let max_chunk_bytes = max_chunk_bytes.max(1);
    let chunk_total = object_bytes
        .len()
        .div_ceil(max_chunk_bytes)
        .min(u16::MAX as usize) as u16;

    let leader_time_ms = now_ms();
    let leader_pubkey = signing_key.verifying_key().to_bytes();
    if identity_keypair.pubkey().to_bytes() != leader_pubkey {
        return Vec::new();
    }

    let mut out: Vec<Vec<u8>> = Vec::new();
    for (chunk_index, object_chunk) in object_bytes.chunks(max_chunk_bytes).enumerate() {
        let chunk_index: u16 = match chunk_index.try_into() {
            Ok(v) => v,
            Err(_) => break,
        };
        if chunk_index >= chunk_total {
            break;
        }

        let payload = McpLedgerMemoChunkPayloadV1 {
            magic: MCP_LEDGER_MEMO_MAGIC,
            version: MCP_LEDGER_MEMO_VERSION,
            kind,
            slot,
            lane_id,
            object_id,
            chunk_index,
            chunk_total,
            object_chunk: object_chunk.to_vec(),
            leader_pubkey,
            leader_time_ms,
        };
        let Some(chunk) = McpLedgerMemoChunkV1::sign(payload, signing_key) else {
            continue;
        };
        debug_assert!(chunk.verify());
        let Ok(memo_bytes) = bincode::serialize(&chunk) else {
            continue;
        };

        // Prioritize MCP metadata so it is likely to land before referenced TX blobs under load.
        let cu_price = ComputeBudgetInstruction::set_compute_unit_price(10_000);
        let memo_ix = Instruction {
            program_id: MCP_LEDGER_MEMO_PROGRAM_ID,
            accounts: Vec::new(),
            data: memo_bytes,
        };
        let message = Message::new(&[cu_price, memo_ix], Some(&identity_keypair.pubkey()));
        let signers = vec![identity_keypair as &dyn Signer];
        let tx = Transaction::new(&signers, message, recent_blockhash);
        if let Ok(tx_bytes) = bincode::serialize(&tx) {
            out.push(tx_bytes);
        }
    }
    out
}

pub fn build_mcp_microblock_memo_txs_v1(
    identity_keypair: &Keypair,
    signing_key: &SigningKey,
    recent_blockhash: Hash,
    microblock: &McpMicroblockV1,
) -> Vec<Vec<u8>> {
    build_mcp_ledger_memo_txs_v1(
        identity_keypair,
        signing_key,
        recent_blockhash,
        McpLedgerMemoKindV1::Microblock,
        microblock.header.slot,
        microblock.header.lane_id,
        microblock.header.microblock_id_v1(),
        &microblock.encode_v1(),
        MCP_LEDGER_MEMO_DEFAULT_MAX_CHUNK_BYTES,
    )
}

pub fn build_mcp_checkpoint_memo_txs_v1(
    identity_keypair: &Keypair,
    signing_key: &SigningKey,
    recent_blockhash: Hash,
    checkpoint: &McpCheckpointV1,
) -> Vec<Vec<u8>> {
    build_mcp_ledger_memo_txs_v1(
        identity_keypair,
        signing_key,
        recent_blockhash,
        McpLedgerMemoKindV1::Checkpoint,
        checkpoint.header.slot,
        checkpoint.header.lane_id,
        checkpoint.header.checkpoint_id_v1(),
        &checkpoint.encode_v1(),
        MCP_LEDGER_MEMO_DEFAULT_MAX_CHUNK_BYTES,
    )
}

pub fn build_mcp_da_cert_memo_txs_v1(
    identity_keypair: &Keypair,
    signing_key: &SigningKey,
    recent_blockhash: Hash,
    cert: &DaCertV1,
) -> Vec<Vec<u8>> {
    build_mcp_ledger_memo_txs_v1(
        identity_keypair,
        signing_key,
        recent_blockhash,
        McpLedgerMemoKindV1::DaCert,
        cert.slot,
        cert.lane_id,
        cert.cert_id_v1(),
        &cert.encode_v1(),
        MCP_LEDGER_MEMO_DEFAULT_MAX_CHUNK_BYTES,
    )
}

#[derive(Default)]
struct McpEncWriter {
    bytes: Vec<u8>,
}

impl McpEncWriter {
    fn write_u8(&mut self, v: u8) {
        self.bytes.push(v);
    }

    fn write_u16(&mut self, v: u16) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    fn write_u32(&mut self, v: u32) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    fn write_u64(&mut self, v: u64) {
        self.bytes.extend_from_slice(&v.to_le_bytes());
    }

    fn write_hash32(&mut self, v: &McpHash32) {
        self.bytes.extend_from_slice(v);
    }

    fn write_pubkey(&mut self, pk: &Pubkey) {
        self.bytes.extend_from_slice(pk.as_ref());
    }

    fn write_pubkey_bytes(&mut self, pk: &[u8; 32]) {
        self.bytes.extend_from_slice(pk);
    }

    fn write_sig64(&mut self, sig: &[u8; 64]) {
        self.bytes.extend_from_slice(sig);
    }

    fn write_bytes(&mut self, v: &[u8]) {
        let len = u32::try_from(v.len()).unwrap_or(u32::MAX);
        self.write_u32(len);
        self.bytes.extend_from_slice(v);
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

struct McpEncReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> McpEncReader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], McpError> {
        let end = self.offset.saturating_add(n);
        let slice = self.bytes.get(self.offset..end).ok_or(McpError::Decode)?;
        self.offset = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8, McpError> {
        Ok(self.take(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, McpError> {
        let v = self.take(2)?;
        Ok(u16::from_le_bytes([v[0], v[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, McpError> {
        let v = self.take(4)?;
        Ok(u32::from_le_bytes([v[0], v[1], v[2], v[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, McpError> {
        let v = self.take(8)?;
        Ok(u64::from_le_bytes([
            v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7],
        ]))
    }

    fn read_hash32(&mut self) -> Result<McpHash32, McpError> {
        let v = self.take(32)?;
        let mut out = [0u8; 32];
        out.copy_from_slice(v);
        Ok(out)
    }

    fn read_pubkey(&mut self) -> Result<Pubkey, McpError> {
        let v = self.take(32)?;
        let mut out = [0u8; 32];
        out.copy_from_slice(v);
        Ok(Pubkey::new_from_array(out))
    }

    fn read_pubkey_bytes(&mut self) -> Result<[u8; 32], McpError> {
        let v = self.take(32)?;
        let mut out = [0u8; 32];
        out.copy_from_slice(v);
        Ok(out)
    }

    fn read_sig64(&mut self) -> Result<[u8; 64], McpError> {
        let v = self.take(64)?;
        let mut out = [0u8; 64];
        out.copy_from_slice(v);
        Ok(out)
    }

    fn read_bytes(&mut self) -> Result<Vec<u8>, McpError> {
        let len = self.read_u32()? as usize;
        let v = self.take(len)?;
        Ok(v.to_vec())
    }

    fn finish(self) -> Result<(), McpError> {
        if self.offset != self.bytes.len() {
            return Err(McpError::TrailingBytes);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::StdRng, RngCore, SeedableRng};
    use solana_runtime::genesis_utils::{create_genesis_config_with_vote_accounts, ValidatorVoteKeypairs};

    fn random_keypair() -> SigningKey {
        let mut rng = StdRng::from_seed([42u8; 32]);
        let mut sk = [0u8; 32];
        rng.fill_bytes(&mut sk);
        SigningKey::from_bytes(&sk)
    }

    fn build_wire_tx_bytes_with_compute_budget(
        cu_price: u64,
        cu_limit: u32,
        recent_blockhash: Hash,
        payer_seed: u8,
    ) -> Vec<u8> {
        let payer = Keypair::new_from_array([payer_seed; 32]);
        let ix_limit = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
        let ix_price = ComputeBudgetInstruction::set_compute_unit_price(cu_price);
        let message = Message::new(&[ix_limit, ix_price], Some(&payer.pubkey()));
        let signers = vec![&payer as &dyn Signer];
        let tx = Transaction::new(&signers, message, recent_blockhash);
        let vtx: VersionedTransaction = tx.into();
        bincode::serialize(&vtx).unwrap()
    }

    fn decode_single_microblock_from_memo_tx(tx_bytes: &[u8]) -> McpMicroblockV1 {
        let tx: Transaction = bincode::deserialize(tx_bytes).unwrap();
        let memo_data = tx.message.instructions[1].data.clone();
        let chunk: McpLedgerMemoChunkV1 = bincode::deserialize(&memo_data).unwrap();
        assert!(chunk.verify());
        assert_eq!(chunk.payload.kind, McpLedgerMemoKindV1::Microblock);
        assert_eq!(chunk.payload.chunk_total, 1);
        McpMicroblockV1::decode_v1(&chunk.payload.object_chunk).unwrap()
    }

    fn memo_kinds_from_memo_txs(memo_txs: &[Vec<u8>]) -> Vec<McpLedgerMemoKindV1> {
        memo_txs
            .iter()
            .map(|tx_bytes| {
                let tx: Transaction = bincode::deserialize(tx_bytes).unwrap();
                let memo_data = tx.message.instructions[1].data.clone();
                let chunk: McpLedgerMemoChunkV1 = bincode::deserialize(&memo_data).unwrap();
                assert!(chunk.verify());
                chunk.payload.kind
            })
            .collect()
    }

    #[test]
    fn test_tx_blob_id_v1_stable() {
        let id1 = tx_blob_id_v1(b"hello");
        let id2 = tx_blob_id_v1(b"hello");
        let id3 = tx_blob_id_v1(b"world");
        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_bid_hint_from_wire_tx_v1_parses_compute_budget() {
        let recent_blockhash = Hash::new_unique();
        let payload = build_wire_tx_bytes_with_compute_budget(54321, 12345, recent_blockhash, 7);
        let hint = bid_hint_from_wire_tx_v1(&payload).unwrap();
        assert_eq!(hint.cu_price, 54321);
        assert_eq!(hint.cu_limit, 12345);
        assert_eq!(hint.sig_count, 1);
    }

    #[test]
    fn test_build_lane_microblock_memo_txs_from_payloads_v1_chains() {
        let cfg = McpConfig {
            lanes_per_slot: 2,
            da_threshold_bps: 6667,
            microblock_max_refs: 64,
            enforce_vote_withholding: false,
            leader_mode: McpLeaderMode::SlotLeaderOnly,
        };
        let lanes_per_slot = cfg.lanes_per_slot;
        let handle = McpHandle::new(cfg);

        let identity_keypair = Keypair::new();
        let seed: [u8; 32] = identity_keypair.to_bytes()[..32].try_into().unwrap();
        let signing_key = SigningKey::from_bytes(&seed);
        assert_eq!(
            identity_keypair.pubkey().to_bytes(),
            signing_key.verifying_key().to_bytes()
        );

        let recent_blockhash = Hash::new_unique();
        let slot = 123;
        let lane_id: LaneId = 0;

        fn find_tx_for_lane(
            cu_price: u64,
            recent_blockhash: Hash,
            slot: u64,
            lane_id: LaneId,
            lanes_per_slot: u8,
            next_seed: &mut u8,
        ) -> Vec<u8> {
            for _ in 0..512 {
                let seed = *next_seed;
                *next_seed = next_seed.wrapping_add(1);
                let tx = build_wire_tx_bytes_with_compute_budget(cu_price, 1_000, recent_blockhash, seed);
                if select_lane_for_blob_id_v1(slot, tx_blob_id_v1(&tx), lanes_per_slot) == lane_id {
                    return tx;
                }
            }
            panic!("failed to find tx for lane {lane_id} (K={lanes_per_slot})");
        }

        let mut seed: u8 = 1;
        let tx_a = find_tx_for_lane(100, recent_blockhash, slot, lane_id, lanes_per_slot, &mut seed);
        let tx_b = find_tx_for_lane(200, recent_blockhash, slot, lane_id, lanes_per_slot, &mut seed);
        let tx_c = find_tx_for_lane(150, recent_blockhash, slot, lane_id, lanes_per_slot, &mut seed);

        let payloads1: Vec<&[u8]> = vec![tx_a.as_slice(), tx_b.as_slice(), tx_c.as_slice()];
        let memos1 = handle.build_lane_microblock_memo_txs_from_payloads_v1(
            &identity_keypair,
            &signing_key,
            recent_blockhash,
            slot,
            lane_id,
            payloads1.as_slice(),
        );
        assert_eq!(memos1.len(), 1);
        let mb0 = decode_single_microblock_from_memo_tx(&memos1[0]);
        assert_eq!(mb0.header.seq_no, 0);
        assert_eq!(mb0.header.prev_microblock_hash, [0u8; 32]);
        assert_eq!(mb0.refs.len(), 3);
        assert_eq!(mb0.refs[0].bid_hint.cu_price, 200);
        assert_eq!(mb0.refs[1].bid_hint.cu_price, 150);
        assert_eq!(mb0.refs[2].bid_hint.cu_price, 100);
        mb0.verify_v1(lane_poh_init_v1(slot, lane_id)).unwrap();

        let tx_d = find_tx_for_lane(250, recent_blockhash, slot, lane_id, lanes_per_slot, &mut seed);
        let payloads2: Vec<&[u8]> = vec![tx_d.as_slice()];
        let memos2 = handle.build_lane_microblock_memo_txs_from_payloads_v1(
            &identity_keypair,
            &signing_key,
            recent_blockhash,
            slot,
            lane_id,
            payloads2.as_slice(),
        );
        assert_eq!(memos2.len(), 1);
        let mb1 = decode_single_microblock_from_memo_tx(&memos2[0]);
        assert_eq!(mb1.header.seq_no, 1);
        assert_eq!(mb1.header.prev_microblock_hash, mb0.header.microblock_id_v1());
        mb1.verify_v1(mb0.header.poh_hash).unwrap();
    }

    #[test]
    fn test_microblock_roundtrip_and_verify() {
        let leader_sk = random_keypair();
        let leader_pk = Pubkey::new_from_array(leader_sk.verifying_key().to_bytes());

        let prev_poh = [7u8; 32];
        let prev_mb = [9u8; 32];

        let refs = vec![
            McpTxRefV1 {
                blob_id: tx_blob_id_v1(b"tx1"),
                bid_hint: BidHintV1 {
                    cu_price: 10,
                    cu_limit: 1_000,
                    sig_count: 1,
                },
            },
            McpTxRefV1 {
                blob_id: tx_blob_id_v1(b"tx2"),
                bid_hint: BidHintV1 {
                    cu_price: 20,
                    cu_limit: 2_000,
                    sig_count: 2,
                },
            },
        ];

        let mut keys: Vec<_> = refs
            .iter()
            .map(|r| bid_key_from_hint_v1(r.blob_id, r.bid_hint))
            .collect();
        keys.sort();
        assert_eq!(keys[0].cu_price, 20);

        let mut mb = McpMicroblockV1::build_unsigned(
            123,
            1,
            0,
            prev_mb,
            prev_poh,
            refs,
            7,
            leader_pk,
        );
        mb.sign_v1(&leader_sk);
        mb.verify_v1(prev_poh).unwrap();

        let bytes = mb.encode_v1();
        let got = McpMicroblockV1::decode_v1(&bytes).unwrap();
        assert_eq!(mb, got);
        got.verify_v1(prev_poh).unwrap();
    }

    #[test]
    fn test_checkpoint_roundtrip_and_verify() {
        let leader_sk = random_keypair();
        let leader_pk = Pubkey::new_from_array(leader_sk.verifying_key().to_bytes());

        let mut ckpt = McpCheckpointV1 {
            header: McpCheckpointHeaderV1 {
                slot: 123,
                lane_id: 1,
                checkpoint_ix: 2,
                first_seq_no: 0,
                last_seq_no: 15,
                microblock_root: [3u8; 32],
                tx_ref_count: 999,
                leader_pubkey: leader_pk,
            },
            leader_sig: [0u8; 64],
        };
        ckpt.sign_v1(&leader_sk);
        ckpt.verify_v1().unwrap();

        let bytes = ckpt.encode_v1();
        let got = McpCheckpointV1::decode_v1(&bytes).unwrap();
        assert_eq!(ckpt, got);
        got.verify_v1().unwrap();
    }

    #[test]
    fn test_da_cert_verify_threshold() {
        let mut rng = StdRng::from_seed([1u8; 32]);

        let sk_a = {
            let mut b = [0u8; 32];
            rng.fill_bytes(&mut b);
            SigningKey::from_bytes(&b)
        };
        let sk_b = {
            let mut b = [0u8; 32];
            rng.fill_bytes(&mut b);
            SigningKey::from_bytes(&b)
        };
        let sk_c = {
            let mut b = [0u8; 32];
            rng.fill_bytes(&mut b);
            SigningKey::from_bytes(&b)
        };

        let pk_a = Pubkey::new_from_array(sk_a.verifying_key().to_bytes());
        let pk_b = Pubkey::new_from_array(sk_b.verifying_key().to_bytes());
        let pk_c = Pubkey::new_from_array(sk_c.verifying_key().to_bytes());

        let mut stakes = std::collections::HashMap::new();
        stakes.insert(pk_a, 40);
        stakes.insert(pk_b, 30);
        stakes.insert(pk_c, 30);

        let epoch = 7;
        let slot = 99;
        let lane_id = 0;
        let checkpoint_ix = 1;
        let checkpoint_id = [8u8; 32];

        let attest_a = {
            let mut a = DaAttestV1 {
                header: DaAttestHeaderV1 {
                    epoch,
                    slot,
                    lane_id,
                    checkpoint_ix,
                    checkpoint_id,
                    validator_pubkey: pk_a,
                },
                sig: [0u8; 64],
            };
            a.sign_v1(&sk_a);
            a
        };
        let attest_b = {
            let mut a = DaAttestV1 {
                header: DaAttestHeaderV1 {
                    epoch,
                    slot,
                    lane_id,
                    checkpoint_ix,
                    checkpoint_id,
                    validator_pubkey: pk_b,
                },
                sig: [0u8; 64],
            };
            a.sign_v1(&sk_b);
            a
        };

        // 2/3 threshold ~= 6667 bps.
        let mut sigs = vec![
            (pk_a.to_bytes(), attest_a.sig),
            (pk_b.to_bytes(), attest_b.sig),
        ];
        sigs.sort_by(|a, b| a.0.cmp(&b.0));

        let cert = DaCertV1 {
            epoch,
            slot,
            lane_id,
            checkpoint_ix,
            checkpoint_id,
            stake_threshold_bps: 6667,
            sigs,
        };

        cert.verify_with_stakes_v1(&stakes).unwrap();

        let cert_missing = DaCertV1 {
            sigs: vec![(pk_b.to_bytes(), attest_b.sig)],
            ..cert.clone()
        };
        assert!(matches!(
            cert_missing.verify_with_stakes_v1(&stakes),
            Err(McpError::StakeThresholdNotMet)
        ));

        let bytes = cert.encode_v1();
        let got = DaCertV1::decode_v1(&bytes).unwrap();
        assert_eq!(cert, got);
        got.verify_with_stakes_v1(&stakes).unwrap();
    }

    #[test]
    fn test_build_lane_microblock_checkpoint_and_da_cert_memo_txs_from_refs_v1_emits_based_on_threshold(
    ) {
        let identity_keypair = Keypair::new();
        let seed: [u8; 32] = identity_keypair.to_bytes()[..32].try_into().unwrap();
        let signing_key = SigningKey::from_bytes(&seed);
        assert_eq!(
            identity_keypair.pubkey().to_bytes(),
            signing_key.verifying_key().to_bytes()
        );

        let vote_keypairs = vec![
            ValidatorVoteKeypairs::new(
                identity_keypair.insecure_clone(),
                Keypair::new(),
                Keypair::new(),
            ),
            ValidatorVoteKeypairs::new_rand(),
        ];
        let stakes = vec![40_u64, 60_u64];
        let genesis_config_info =
            create_genesis_config_with_vote_accounts(1_000_000, &vote_keypairs, stakes);
        let bank = Bank::new_for_tests(&genesis_config_info.genesis_config);

        assert_eq!(
            bank.current_epoch_staked_nodes()
                .as_ref()
                .get(&identity_keypair.pubkey())
                .copied(),
            Some(40)
        );

        let slot = 123;
        let lane_id: LaneId = 0;
        let lanes_per_slot: u8 = 2;
        let blob_id_0 = (0u8..=255)
            .map(|i| tx_blob_id_v1(&[i]))
            .find(|blob_id| select_lane_for_blob_id_v1(slot, *blob_id, lanes_per_slot) == lane_id)
            .expect("find blob id for lane");
        let blob_id_1 = (0u8..=255)
            .map(|i| tx_blob_id_v1(&[i, 1]))
            .find(|blob_id| select_lane_for_blob_id_v1(slot, *blob_id, lanes_per_slot) == lane_id)
            .expect("find blob id for lane");

        let refs = vec![
            McpTxRefV1 {
                blob_id: blob_id_0,
                bid_hint: BidHintV1 {
                    cu_price: 123,
                    cu_limit: 0,
                    sig_count: 0,
                },
            },
            McpTxRefV1 {
                blob_id: blob_id_1,
                bid_hint: BidHintV1 {
                    cu_price: 456,
                    cu_limit: 0,
                    sig_count: 0,
                },
            },
        ];

        let recent_blockhash = Hash::new_unique();

        // Threshold not met: 40/100 stake < 2/3, so no DA cert memo should be emitted.
        let handle_no = McpHandle::new(McpConfig {
            lanes_per_slot,
            da_threshold_bps: 6667,
            microblock_max_refs: 64,
            enforce_vote_withholding: false,
            leader_mode: McpLeaderMode::SlotLeaderOnly,
        });
        let memos_no = handle_no.build_lane_microblock_checkpoint_and_da_cert_memo_txs_from_refs_v1(
            &identity_keypair,
            &signing_key,
            &bank,
            recent_blockhash,
            slot,
            lane_id,
            refs.clone(),
        );
        let kinds_no = memo_kinds_from_memo_txs(&memos_no);
        assert!(kinds_no.iter().any(|k| *k == McpLedgerMemoKindV1::Microblock));
        assert!(kinds_no.iter().any(|k| *k == McpLedgerMemoKindV1::Checkpoint));
        assert!(!kinds_no.iter().any(|k| *k == McpLedgerMemoKindV1::DaCert));

        // Threshold met: 40/100 stake >= 1 bps, so DA cert memo should be emitted.
        let handle_yes = McpHandle::new(McpConfig {
            lanes_per_slot,
            da_threshold_bps: 1,
            microblock_max_refs: 64,
            enforce_vote_withholding: false,
            leader_mode: McpLeaderMode::SlotLeaderOnly,
        });
        let memos_yes =
            handle_yes.build_lane_microblock_checkpoint_and_da_cert_memo_txs_from_refs_v1(
                &identity_keypair,
                &signing_key,
                &bank,
                recent_blockhash,
                slot,
                lane_id,
                refs,
            );
        let kinds_yes = memo_kinds_from_memo_txs(&memos_yes);
        assert!(kinds_yes.iter().any(|k| *k == McpLedgerMemoKindV1::Microblock));
        assert!(kinds_yes.iter().any(|k| *k == McpLedgerMemoKindV1::Checkpoint));
        assert!(kinds_yes.iter().any(|k| *k == McpLedgerMemoKindV1::DaCert));
    }

    #[test]
    fn test_mcp_vote_withholding_is_feature_gated() {
        let handle = McpHandle::new(McpConfig {
            enforce_vote_withholding: true,
            ..McpConfig::default()
        });

        let leader = Pubkey::new_unique();
        let slot = 123;
        handle.mark_mcp_slashed(
            leader.to_bytes(),
            slot,
            now_ms(),
            "test_slashed",
            false, // feature inactive at time of marking
        );

        // Even if an audit violation was observed, vote withholding must remain disabled
        // unless the on-chain feature is active.
        assert!(!handle.mcp_slashing_is_slashed_leader(&leader, slot, false));
        assert!(handle.mcp_slashing_is_slashed_leader(&leader, slot, true));
    }

    #[test]
    fn test_merge_lanes_by_bid_v1_dedups_and_orders() {
        let leader_sk = random_keypair();
        let leader_pk = Pubkey::new_from_array(leader_sk.verifying_key().to_bytes());

        let dup_blob = tx_blob_id_v1(b"dup");

        let lane0_refs = vec![
            McpTxRefV1 {
                blob_id: tx_blob_id_v1(b"l0_hi"),
                bid_hint: BidHintV1 {
                    cu_price: 30,
                    cu_limit: 1,
                    sig_count: 0,
                },
            },
            McpTxRefV1 {
                blob_id: dup_blob,
                bid_hint: BidHintV1 {
                    cu_price: 10,
                    cu_limit: 1,
                    sig_count: 0,
                },
            },
        ];
        let lane1_refs = vec![
            McpTxRefV1 {
                blob_id: tx_blob_id_v1(b"l1_mid"),
                bid_hint: BidHintV1 {
                    cu_price: 20,
                    cu_limit: 1,
                    sig_count: 0,
                },
            },
            McpTxRefV1 {
                blob_id: dup_blob,
                bid_hint: BidHintV1 {
                    cu_price: 5,
                    cu_limit: 1,
                    sig_count: 0,
                },
            },
        ];

        let mut lane0 = McpMicroblockV1::build_unsigned(
            123,
            0,
            0,
            [0u8; 32],
            lane_poh_init_v1(123, 0),
            lane0_refs,
            0,
            leader_pk,
        );
        lane0.sign_v1(&leader_sk);
        let mut lane1 = McpMicroblockV1::build_unsigned(
            123,
            1,
            0,
            [0u8; 32],
            lane_poh_init_v1(123, 1),
            lane1_refs,
            0,
            leader_pk,
        );
        lane1.sign_v1(&leader_sk);

        let lanes: HashMap<LaneId, Vec<McpMicroblockV1>> =
            [(0u8, vec![lane0]), (1u8, vec![lane1])].into_iter().collect();

        let merged = merge_lanes_by_bid_v1(&lanes);
        // Deduped: 3 unique blob IDs.
        assert_eq!(merged.len(), 3);
        // Highest cu_price first.
        assert_eq!(merged[0].bid_hint.cu_price, 30);
        assert_eq!(merged[1].bid_hint.cu_price, 20);
        assert_eq!(merged[2].bid_hint.cu_price, 10);
    }

    #[test]
    fn test_mcp_ledger_memo_microblock_chunking_and_verify() {
        let identity_keypair = Keypair::new();
        let seed: [u8; 32] = identity_keypair.to_bytes()[..32].try_into().unwrap();
        let signing_key = SigningKey::from_bytes(&seed);
        assert_eq!(
            identity_keypair.pubkey().to_bytes(),
            signing_key.verifying_key().to_bytes()
        );

        let leader_pk = Pubkey::new_from_array(signing_key.verifying_key().to_bytes());

        let prev_poh = [1u8; 32];
        let prev_mb = [2u8; 32];
        let refs: Vec<McpTxRefV1> = (0..100)
            .map(|i| McpTxRefV1 {
                blob_id: tx_blob_id_v1(&[i as u8]),
                bid_hint: BidHintV1 {
                    cu_price: i as u64,
                    cu_limit: 1_000,
                    sig_count: 1,
                },
            })
            .collect();

        let mut mb = McpMicroblockV1::build_unsigned(
            123,
            0,
            0,
            prev_mb,
            prev_poh,
            refs,
            3,
            leader_pk,
        );
        mb.sign_v1(&signing_key);

        let recent_blockhash = Hash::new_unique();
        let txs = build_mcp_microblock_memo_txs_v1(
            &identity_keypair,
            &signing_key,
            recent_blockhash,
            &mb,
        );
        assert!(txs.len() > 1);

        let mut chunks: Vec<_> = txs
            .iter()
            .map(|tx_bytes| {
                let tx: Transaction = bincode::deserialize(tx_bytes).unwrap();
                let memo_data = tx.message.instructions[1].data.clone();
                let chunk: McpLedgerMemoChunkV1 = bincode::deserialize(&memo_data).unwrap();
                assert!(chunk.verify());
                chunk
            })
            .collect();
        chunks.sort_by_key(|c| c.payload.chunk_index);

        assert_eq!(chunks[0].payload.chunk_total as usize, chunks.len());
        assert_eq!(
            chunks[0].payload.object_id,
            mb.header.microblock_id_v1()
        );

        let mut reassembled: Vec<u8> = Vec::new();
        for c in chunks {
            reassembled.extend_from_slice(&c.payload.object_chunk);
        }
        assert_eq!(reassembled, mb.encode_v1());
    }

    #[test]
    fn test_ordering_priority_overrides_from_microblock_memos() {
        let cfg = McpConfig {
            lanes_per_slot: 2,
            da_threshold_bps: 6667,
            microblock_max_refs: 64,
            enforce_vote_withholding: false,
            leader_mode: McpLeaderMode::SlotLeaderOnly,
        };
        let lanes_per_slot = cfg.lanes_per_slot;
        let handle = McpHandle::new(cfg);

        let identity_keypair = Keypair::new();
        let seed: [u8; 32] = identity_keypair.to_bytes()[..32].try_into().unwrap();
        let signing_key = SigningKey::from_bytes(&seed);
        assert_eq!(
            identity_keypair.pubkey().to_bytes(),
            signing_key.verifying_key().to_bytes()
        );

        let recent_blockhash = Hash::new_unique();
        let slot = 123;
        let lane_id: LaneId = 0;

        fn find_tx_for_lane(
            cu_price: u64,
            recent_blockhash: Hash,
            slot: u64,
            lane_id: LaneId,
            lanes_per_slot: u8,
            next_seed: &mut u8,
        ) -> Vec<u8> {
            for _ in 0..512 {
                let seed = *next_seed;
                *next_seed = next_seed.wrapping_add(1);
                let tx = build_wire_tx_bytes_with_compute_budget(
                    cu_price,
                    1_000,
                    recent_blockhash,
                    seed,
                );
                if select_lane_for_blob_id_v1(slot, tx_blob_id_v1(&tx), lanes_per_slot) == lane_id
                {
                    return tx;
                }
            }
            panic!("failed to find tx for lane {lane_id} (K={lanes_per_slot})");
        }

        let mut next_seed: u8 = 1;
        let tx_hi =
            find_tx_for_lane(2000, recent_blockhash, slot, lane_id, lanes_per_slot, &mut next_seed);
        let tx_lo =
            find_tx_for_lane(1000, recent_blockhash, slot, lane_id, lanes_per_slot, &mut next_seed);

        let tx_hi_blob_id = tx_blob_id_v1(&tx_hi);
        let tx_lo_blob_id = tx_blob_id_v1(&tx_lo);

        let payloads = vec![tx_hi.as_slice(), tx_lo.as_slice()];
        let microblock_memo_txs = handle.build_lane_microblock_memo_txs_from_payloads_v1(
            &identity_keypair,
            &signing_key,
            recent_blockhash,
            slot,
            lane_id,
            payloads.as_slice(),
        );
        assert!(!microblock_memo_txs.is_empty());

        for tx_bytes in microblock_memo_txs.iter() {
            let tx: Transaction = bincode::deserialize(tx_bytes).unwrap();
            let memo_ix = &tx.message.instructions[1];
            let program_id = &tx.message.account_keys[memo_ix.program_id_index as usize];
            let Some(parse) = parse_mcp_memo_chunk_payload_v1(program_id, &memo_ix.data) else {
                panic!("expected mcp memo chunk payload");
            };
            let McpMemoChunkParseV1::Valid(p) = parse else {
                panic!("expected valid mcp memo chunk payload");
            };
            handle.ingest_memo_chunk_for_ordering_v1(p);
        }

        assert_eq!(
            handle.ordering_priority_for_slot_blob_id_v1(slot, tx_hi_blob_id),
            Some(u64::MAX)
        );
        assert_eq!(
            handle.ordering_priority_for_slot_blob_id_v1(slot, tx_lo_blob_id),
            Some(u64::MAX - 1)
        );
    }
}
