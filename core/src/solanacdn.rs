use std::collections::{hash_map::DefaultHasher, HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::sync::Once;
use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use dashmap::{mapref::entry::Entry, DashMap, DashSet};
use ed25519_dalek_v2::{Signer as DalekSigner, SigningKey, VerifyingKey};
use quinn::Endpoint;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::{mpsc, watch};

use solana_compute_budget_interface::ComputeBudgetInstruction;
use solana_instruction::Instruction;
use solana_keypair::Keypair;
use solana_ledger::blockstore::Blockstore;
use solana_ledger::shred::ShredId as LedgerShredId;
use solana_message::{Message, VersionedMessage};
use solana_packet::PACKET_DATA_SIZE;
use solana_pubkey::Pubkey;
use solana_runtime::bank::Bank;
use solana_sha256_hasher as sha256_hasher;
use solana_signer::Signer;
use solana_svm_transaction::svm_message::SVMMessage;
use solana_transaction::{
    versioned::VersionedTransaction, Transaction, TransactionVerificationMode,
};

use solanacdn_protocol::crypto::{random_nonce_16, PubkeyBytes, SignatureBytes};
use solanacdn_protocol::frame::{
    decode_envelope, encode_envelope, FrameError, DEFAULT_MAX_FRAME_BYTES,
};
use solanacdn_protocol::messages::{
    AgentCapabilities, AgentToPop, AuthRefresh, AuthRequest, AuthRequestPayload,
    AuthWithSessionToken, ControlRequest, ControlResponse, FairBatchCommit, FairBatchCommitPayload,
    FairBatchReceiptCommit, FairBatchReceiptCommitPayload, FairBatchReject, FairBatchRejectPayload,
    FairBatchRejectReason, Heartbeat, HeartbeatStats, PopToAgent, Shred, ShredBatch, ShredId,
    ShredKind, StreamKind, VoteDatagram,
};

static GLOBAL: ArcSwapOption<SolanaCdnHandle> = ArcSwapOption::const_empty();

// FNV-1a 128-bit for stable, dependency-free IDs (helps dedupe mirrored packets).
const FNV1A_128_OFFSET_BASIS: u128 = 0x6c62_272e_07bb_0142_62b8_2175_6295_c58d;
const FNV1A_128_PRIME: u128 = 0x0000_0000_0100_0000_0000_0000_0000_013b;

const FAIR_PRIORITY_TTL_MS: u64 = 30_000;
const FAIR_PRIORITY_MAX_ENTRIES: usize = 500_000;
const FAIR_PRIORITY_PRUNE_LIMIT: usize = 50_000;
const FAIR_PRIORITY_PRUNE_INTERVAL_MS: u64 = 1_000;

// Defense-in-depth: bound per-message FairBatch processing costs (CPU for tx verification and
// memory for buffering wire payloads). POPs can split batches if needed.
const FAIR_BATCH_MAX_TXS: usize = 512;
const FAIR_BATCH_MAX_TOTAL_BYTES: usize = 256 * PACKET_DATA_SIZE;

const TX_DEDUP_TTL_MS: u64 = 2_000;
const TX_SIG_DEDUP_MAX_ENTRIES: usize = 300_000;
const DEFAULT_VOTE_DEDUP_TTL_MS: u64 = 2_000;
const DEFAULT_VOTE_DEDUP_MAX_ENTRIES: usize = 200_000;
const VOTE_TUNNEL_ALLOWED_DST_TTL_MS: u64 = 60_000;
const VOTE_TUNNEL_ALLOWED_DST_MAX_ENTRIES: usize = 4_096;

const FAIR_MERKLE_LEAF_DOMAIN: &[u8] = b"SCDNFAIRLEAFv1";
const FAIR_MERKLE_NODE_DOMAIN: &[u8] = b"SCDNFAIRNODEv1";

const FAIR_LEDGER_COMMIT_MEMO_PROGRAM_ID: Pubkey =
    solana_pubkey::pubkey!("Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo");

const FAIR_LEDGER_COMMIT_MAGIC: [u8; 8] = *b"SCDNFAIR";
const FAIR_LEDGER_COMMIT_VERSION: u8 = 1;
const FAIR_LEDGER_COMMIT_MAX_SIGS_PER_CHUNK: usize = 12;
const FAIR_LEDGER_ACK_MAGIC: [u8; 8] = *b"SCDNACKD";
const FAIR_LEDGER_ACK_VERSION: u8 = 1;
const FAIR_LEDGER_REJECT_MAGIC: [u8; 8] = *b"SCDNRJCT";
const FAIR_LEDGER_REJECT_VERSION: u8 = 1;
const FAIR_LEDGER_WITNESS_MAGIC: [u8; 8] = *b"SCDNWITN";
const FAIR_LEDGER_WITNESS_VERSION: u8 = 1;

const FAIR_SLASH_WITNESS_TTL_MS: u64 = 60_000;
const FAIR_SLASH_WITNESS_MAX_ENTRIES: usize = 1_000_000;
const FAIR_SLASHED_TTL_MS: u64 = 10 * 60_000;
const FAIR_SLASHED_MAX_ENTRIES: usize = 20_000;
const FAIR_BATCH_WITNESS_TTL_MS: u64 = 10 * 60_000;
const FAIR_BATCH_WITNESS_MAX_SLOTS: usize = 50_000;
const FAIR_BATCH_WITNESS_MAX_BATCHES_PER_SLOT: usize = 8_192;
const FAIR_BATCH_WITNESS_MAX_WITNESSERS_PER_BATCH: usize = 8;
const FAIR_BATCH_ACK_TTL_MS: u64 = 10 * 60_000;
const FAIR_BATCH_ACK_MAX_SLOTS: usize = 50_000;
const FAIR_BATCH_ACK_MAX_BATCHES_PER_SLOT: usize = 8_192;
const FAIR_BATCH_REJECT_TTL_MS: u64 = 10 * 60_000;
const FAIR_BATCH_REJECT_MAX_SLOTS: usize = 50_000;
const FAIR_BATCH_REJECT_MAX_BATCHES_PER_SLOT: usize = 8_192;
const FAIR_RECENT_BLOCKHASH_TTL_MS: u64 = 60_000;
const POP_EGRESS_IP_TTL_MS: u64 = 10 * 60_000;
const POP_EGRESS_IP_MAX_ENTRIES: usize = 50_000;

// Tighten framing limits beyond the protocol default (16MiB). These are chosen to be generous for
// expected message sizes while capping per-frame allocations if a POP misbehaves.
const CTRL_MAX_FRAME_BYTES: usize = 1 * 1024 * 1024;
const SHREDS_MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const VOTES_MAX_FRAME_BYTES: usize = 256 * 1024;

// Bound CPU/memory for decoding large multi-shred batches.
const PUSH_SHRED_BATCH_MAX_SHREDS: usize = 4_096;

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TxFairSlashingEnforceOverride {
    Inherit = 0,
    ForceOff = 1,
    ForceOn = 2,
}

impl TxFairSlashingEnforceOverride {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::ForceOff,
            2 => Self::ForceOn,
            _ => Self::Inherit,
        }
    }

    fn as_option_bool(self) -> Option<bool> {
        match self {
            Self::Inherit => None,
            Self::ForceOff => Some(false),
            Self::ForceOn => Some(true),
        }
    }

    fn from_option_bool(value: Option<bool>) -> Self {
        match value {
            None => Self::Inherit,
            Some(false) => Self::ForceOff,
            Some(true) => Self::ForceOn,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Inherit => "inherit",
            Self::ForceOff => "force_off",
            Self::ForceOn => "force_on",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct FairPriorityEntry {
    pub(crate) priority: u64,
    pub(crate) expires_at_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FairLedgerCommitChunkPayload {
    magic: [u8; 8],
    version: u8,
    slot: u64,
    batch_id: u128,
    order_start: u64,
    chunk_index: u16,
    chunk_total: u16,
    tx_sigs: Vec<SignatureBytes>,
    leader_pubkey: PubkeyBytes,
    leader_time_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FairLedgerCommitChunk {
    payload: FairLedgerCommitChunkPayload,
    signature: SignatureBytes,
}

impl FairLedgerCommitChunk {
    fn sign(payload: FairLedgerCommitChunkPayload, signing_key: &SigningKey) -> Option<Self> {
        let bytes = bincode::serialize(&payload).ok()?;
        let signature = signing_key.sign(&bytes);
        Some(Self {
            payload,
            signature: SignatureBytes(signature.to_bytes()),
        })
    }

    fn verify(&self) -> bool {
        if self.payload.magic != FAIR_LEDGER_COMMIT_MAGIC
            || self.payload.version != FAIR_LEDGER_COMMIT_VERSION
        {
            return false;
        }
        let bytes = match bincode::serialize(&self.payload) {
            Ok(v) => v,
            Err(_) => return false,
        };
        let verifying_key = match VerifyingKey::from_bytes(&self.payload.leader_pubkey.0) {
            Ok(v) => v,
            Err(_) => return false,
        };
        verifying_key
            .verify_strict(
                &bytes,
                &ed25519_dalek_v2::Signature::from_bytes(&self.signature.0),
            )
            .is_ok()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FairLedgerRejectMemoPayload {
    magic: [u8; 8],
    version: u8,
    slot: u64,
    origin_pop_id_hash: [u8; 32],
    flow_id: u128,
    batch_id: u128,
    order_start: u64,
    reason: FairBatchRejectReason,
    leader_pubkey: PubkeyBytes,
    leader_time_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FairLedgerRejectMemo {
    payload: FairLedgerRejectMemoPayload,
    signature: SignatureBytes,
}

impl FairLedgerRejectMemo {
    fn sign(payload: FairLedgerRejectMemoPayload, signing_key: &SigningKey) -> Option<Self> {
        let bytes = bincode::serialize(&payload).ok()?;
        let signature = signing_key.sign(&bytes);
        Some(Self {
            payload,
            signature: SignatureBytes(signature.to_bytes()),
        })
    }

    fn verify(&self) -> bool {
        if self.payload.magic != FAIR_LEDGER_REJECT_MAGIC
            || self.payload.version != FAIR_LEDGER_REJECT_VERSION
        {
            return false;
        }
        let bytes = match bincode::serialize(&self.payload) {
            Ok(v) => v,
            Err(_) => return false,
        };
        let verifying_key = match VerifyingKey::from_bytes(&self.payload.leader_pubkey.0) {
            Ok(v) => v,
            Err(_) => return false,
        };
        verifying_key
            .verify_strict(
                &bytes,
                &ed25519_dalek_v2::Signature::from_bytes(&self.signature.0),
            )
            .is_ok()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FairLedgerAckMemoPayload {
    magic: [u8; 8],
    version: u8,
    slot: u64,
    origin_pop_id_hash: [u8; 32],
    flow_id: u128,
    batch_id: u128,
    order_start: u64,
    tx_count: u32,
    tx_merkle_root: [u8; 32],
    leader_pubkey: PubkeyBytes,
    leader_time_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FairLedgerAckMemo {
    payload: FairLedgerAckMemoPayload,
    signature: SignatureBytes,
}

impl FairLedgerAckMemo {
    fn sign(payload: FairLedgerAckMemoPayload, signing_key: &SigningKey) -> Option<Self> {
        let bytes = bincode::serialize(&payload).ok()?;
        let signature = signing_key.sign(&bytes);
        Some(Self {
            payload,
            signature: SignatureBytes(signature.to_bytes()),
        })
    }

    fn verify(&self) -> bool {
        if self.payload.magic != FAIR_LEDGER_ACK_MAGIC
            || self.payload.version != FAIR_LEDGER_ACK_VERSION
        {
            return false;
        }
        let bytes = match bincode::serialize(&self.payload) {
            Ok(v) => v,
            Err(_) => return false,
        };
        let verifying_key = match VerifyingKey::from_bytes(&self.payload.leader_pubkey.0) {
            Ok(v) => v,
            Err(_) => return false,
        };
        verifying_key
            .verify_strict(
                &bytes,
                &ed25519_dalek_v2::Signature::from_bytes(&self.signature.0),
            )
            .is_ok()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FairLedgerWitnessMemoPayload {
    magic: [u8; 8],
    version: u8,
    witness_pop_pubkey: PubkeyBytes,
    witness: solanacdn_protocol::messages::FairBatchWitness,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct FairLedgerWitnessMemo {
    payload: FairLedgerWitnessMemoPayload,
}

impl FairLedgerWitnessMemo {
    fn verify(&self) -> bool {
        if self.payload.magic != FAIR_LEDGER_WITNESS_MAGIC
            || self.payload.version != FAIR_LEDGER_WITNESS_VERSION
        {
            return false;
        }
        self.payload
            .witness
            .verify(self.payload.witness_pop_pubkey)
            .is_ok()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct FairOrderWitnessKey {
    leader: PubkeyBytes,
    slot: u64,
    order_ix: u64,
}

#[derive(Clone, Copy, Debug)]
struct FairOrderWitnessEntry {
    tx_sig: [u8; 64],
    expires_at_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct FairSlashedKey {
    leader: PubkeyBytes,
    slot: u64,
}

#[derive(Clone, Copy, Debug)]
struct FairSlashedEntry {
    expires_at_ms: u64,
}

#[derive(Clone, Debug)]
struct FairWitnessBatch {
    origin_pop_id_hash: [u8; 32],
    flow_id: u128,
    tx_count: u32,
    tx_merkle_root: [u8; 32],
    order_start: u64,
    pop_time_ms: u64,
    witnessers: Vec<PubkeyBytes>,
}

#[derive(Clone, Debug)]
struct FairWitnessSlotState {
    gen: u64,
    expires_at_ms: u64,
    batches: HashMap<u128, FairWitnessBatch>,
}

#[derive(Clone, Copy, Debug)]
struct FairAckBatch {
    origin_pop_id_hash: [u8; 32],
    flow_id: u128,
    tx_count: u32,
    tx_merkle_root: [u8; 32],
    order_start: u64,
    leader_time_ms: u64,
}

#[derive(Clone, Debug)]
struct FairAckSlotState {
    gen: u64,
    expires_at_ms: u64,
    batches: HashMap<u128, FairAckBatch>,
}

#[derive(Clone, Copy, Debug)]
struct FairRejectBatch {
    origin_pop_id_hash: [u8; 32],
    flow_id: u128,
    order_start: u64,
    reason: FairBatchRejectReason,
    leader_time_ms: u64,
}

#[derive(Clone, Debug)]
struct FairRejectSlotState {
    gen: u64,
    expires_at_ms: u64,
    batches: HashMap<u128, FairRejectBatch>,
}

#[derive(Clone, Copy, Debug)]
struct FairLedgerAuditSlotEntry {
    ok: bool,
    ack_gen: u64,
    witness_gen: u64,
    reject_gen: u64,
}

static FAIR_PRIORITIES: OnceLock<DashMap<[u8; 64], FairPriorityEntry>> = OnceLock::new();
static FAIR_PRIORITY_LOOKUPS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FAIR_PRIORITY_HITS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FAIR_PRIORITY_LAST_PRUNE_MS: AtomicU64 = AtomicU64::new(0);
static FAIR_BATCH_DROPPED_SIG_MISMATCH_TOTAL: AtomicU64 = AtomicU64::new(0);
static FAIR_BATCH_DROPPED_DUP_SIG_TOTAL: AtomicU64 = AtomicU64::new(0);
static FAIR_BATCH_DROPPED_PAYLOAD_TOO_LARGE_TOTAL: AtomicU64 = AtomicU64::new(0);
static FAIR_BATCH_DROPPED_INVALID_WIRE_TX_TOTAL: AtomicU64 = AtomicU64::new(0);
static FAIR_BATCH_DROPPED_TOO_MANY_TXS_TOTAL: AtomicU64 = AtomicU64::new(0);
static FAIR_BATCH_DROPPED_TOTAL_BYTES_EXCEEDED_TOTAL: AtomicU64 = AtomicU64::new(0);

pub(crate) fn fair_priorities() -> &'static DashMap<[u8; 64], FairPriorityEntry> {
    FAIR_PRIORITIES.get_or_init(DashMap::new)
}

fn prune_expired_fair_priorities(map: &DashMap<[u8; 64], FairPriorityEntry>, now: u64) {
    let mut expired: Vec<[u8; 64]> = Vec::new();
    for entry in map.iter().take(FAIR_PRIORITY_PRUNE_LIMIT) {
        if entry.expires_at_ms < now {
            expired.push(*entry.key());
        }
    }
    for key in expired {
        map.remove(&key);
    }
}

pub fn fair_priority_for_tx_signature(sig: &[u8; 64]) -> Option<u64> {
    FAIR_PRIORITY_LOOKUPS_TOTAL.fetch_add(1, Ordering::Relaxed);
    let now = now_ms();
    let map = fair_priorities();
    let entry = map.get(sig)?;
    if entry.expires_at_ms < now {
        drop(entry);
        map.remove(sig);
        return None;
    }
    FAIR_PRIORITY_HITS_TOTAL.fetch_add(1, Ordering::Relaxed);
    Some(entry.priority)
}

pub(crate) fn insert_fair_priority(sig: [u8; 64], priority: u64) {
    let map = fair_priorities();
    if map.len() > FAIR_PRIORITY_MAX_ENTRIES {
        let now = now_ms();
        let last = FAIR_PRIORITY_LAST_PRUNE_MS.load(Ordering::Relaxed);
        if now.saturating_sub(last) >= FAIR_PRIORITY_PRUNE_INTERVAL_MS
            && FAIR_PRIORITY_LAST_PRUNE_MS
                .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            prune_expired_fair_priorities(map, now);
        }
        if map.len() > FAIR_PRIORITY_MAX_ENTRIES {
            map.clear();
        }
    }
    map.insert(
        sig,
        FairPriorityEntry {
            priority,
            expires_at_ms: now_ms().saturating_add(FAIR_PRIORITY_TTL_MS),
        },
    );
}

fn verified_recent_blockhash_from_wire_tx(payload: &[u8]) -> Option<solana_hash::Hash> {
    let tx: VersionedTransaction = bincode::deserialize(payload).ok()?;
    tx.sanitize().ok()?;

    let message_bytes = tx.message.serialize();
    for (sig, pubkey) in tx
        .signatures
        .iter()
        .zip(tx.message.static_account_keys().iter())
    {
        let verifying_key = VerifyingKey::try_from(pubkey.as_ref()).ok()?;
        let signature = ed25519_dalek_v2::Signature::try_from(sig.as_ref()).ok()?;
        verifying_key
            .verify_strict(&message_bytes, &signature)
            .ok()?;
    }

    Some(*tx.message.recent_blockhash())
}

fn try_first_signature_bytes_from_wire_tx(payload: &[u8]) -> Option<[u8; 64]> {
    let (sig_count, consumed) = parse_shortvec_len(payload)?;
    if sig_count == 0 {
        return None;
    }
    let start = consumed;
    let end = start.checked_add(64)?;
    if payload.len() < end {
        return None;
    }
    let mut sig = [0u8; 64];
    sig.copy_from_slice(&payload[start..end]);
    Some(sig)
}

fn parse_shortvec_len(input: &[u8]) -> Option<(usize, usize)> {
    // Solana shortvec: 7-bit groups, MSB is continuation.
    let mut value: usize = 0;
    let mut shift: u32 = 0;
    for (idx, byte) in input.iter().copied().take(3).enumerate() {
        value |= usize::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some((value, idx + 1));
        }
        shift = shift.saturating_add(7);
    }
    None
}

fn wire_tx_has_program_id(payload: &[u8], program_id: &[u8; 32]) -> bool {
    // Wire tx format:
    // - signatures: shortvec len + 64-byte signatures
    // - message (legacy) OR versioned message (v0) with version prefix byte.
    let (sig_count, consumed) = match parse_shortvec_len(payload) {
        Some(v) => v,
        None => return false,
    };
    if sig_count == 0 {
        return false;
    }
    let sig_bytes = match sig_count.checked_mul(64) {
        Some(v) => v,
        None => return false,
    };
    let msg_start = match consumed.checked_add(sig_bytes) {
        Some(v) => v,
        None => return false,
    };
    let msg = match payload.get(msg_start..) {
        Some(v) => v,
        None => return false,
    };
    if msg.is_empty() {
        return false;
    }

    // Message header: legacy starts immediately; v0 starts with 0x80|version.
    let mut cursor: usize = 0;
    if msg[0] & 0x80 != 0 {
        let version = msg[0] & 0x7f;
        if version != 0 {
            return false;
        }
        cursor = 1;
    }

    cursor = match cursor.checked_add(3) {
        Some(v) => v,
        None => return false,
    };
    if msg.len() < cursor {
        return false;
    }

    let (key_count, consumed) = match parse_shortvec_len(msg.get(cursor..).unwrap_or(&[])) {
        Some(v) => v,
        None => return false,
    };
    cursor = match cursor.checked_add(consumed) {
        Some(v) => v,
        None => return false,
    };

    let keys_start = cursor;
    let keys_bytes = match key_count.checked_mul(32) {
        Some(v) => v,
        None => return false,
    };
    cursor = match keys_start.checked_add(keys_bytes) {
        Some(v) => v,
        None => return false,
    };
    if msg.len() < cursor {
        return false;
    }

    // recent_blockhash
    cursor = match cursor.checked_add(32) {
        Some(v) => v,
        None => return false,
    };
    if msg.len() < cursor {
        return false;
    }

    let (ix_count, consumed) = match parse_shortvec_len(msg.get(cursor..).unwrap_or(&[])) {
        Some(v) => v,
        None => return false,
    };
    cursor = match cursor.checked_add(consumed) {
        Some(v) => v,
        None => return false,
    };
    if msg.len() < cursor {
        return false;
    }

    for _ in 0..ix_count {
        let program_id_index = match msg.get(cursor) {
            Some(v) => *v as usize,
            None => return false,
        };
        cursor = match cursor.checked_add(1) {
            Some(v) => v,
            None => return false,
        };

        let (account_count, consumed) = match parse_shortvec_len(msg.get(cursor..).unwrap_or(&[])) {
            Some(v) => v,
            None => return false,
        };
        cursor = match cursor.checked_add(consumed) {
            Some(v) => v,
            None => return false,
        };
        cursor = match cursor.checked_add(account_count) {
            Some(v) => v,
            None => return false,
        };
        if cursor > msg.len() {
            return false;
        }

        let (data_len, consumed) = match parse_shortvec_len(msg.get(cursor..).unwrap_or(&[])) {
            Some(v) => v,
            None => return false,
        };
        cursor = match cursor.checked_add(consumed) {
            Some(v) => v,
            None => return false,
        };
        cursor = match cursor.checked_add(data_len) {
            Some(v) => v,
            None => return false,
        };
        if cursor > msg.len() {
            return false;
        }

        if program_id_index < key_count {
            let start = match keys_start.checked_add(program_id_index.saturating_mul(32)) {
                Some(v) => v,
                None => return false,
            };
            let end = match start.checked_add(32) {
                Some(v) => v,
                None => return false,
            };
            let key_bytes = match msg.get(start..end) {
                Some(v) => v,
                None => return false,
            };
            if key_bytes == program_id.as_slice() {
                return true;
            }
        }
    }

    false
}

fn build_fair_ledger_commit_memo_txs(
    auth: &AuthContext,
    recent_blockhash: solana_hash::Hash,
    slot: u64,
    batch_id: u128,
    order_start: u64,
    tx_sigs: &[[u8; 64]],
) -> Vec<Vec<u8>> {
    if slot == 0 || tx_sigs.is_empty() {
        return Vec::new();
    }

    let chunk_size = FAIR_LEDGER_COMMIT_MAX_SIGS_PER_CHUNK.max(1);
    let chunk_total = tx_sigs.len().div_ceil(chunk_size).min(u16::MAX as usize) as u16;

    let leader_time_ms = now_ms();
    let leader_pubkey = auth.validator_pubkey;

    let mut out: Vec<Vec<u8>> = Vec::new();
    for (chunk_index, sigs_chunk) in tx_sigs.chunks(chunk_size).enumerate() {
        let chunk_index: u16 = match chunk_index.try_into() {
            Ok(v) => v,
            Err(_) => break,
        };
        if chunk_index >= chunk_total {
            break;
        }

        let payload = FairLedgerCommitChunkPayload {
            magic: FAIR_LEDGER_COMMIT_MAGIC,
            version: FAIR_LEDGER_COMMIT_VERSION,
            slot,
            batch_id,
            order_start,
            chunk_index,
            chunk_total,
            tx_sigs: sigs_chunk.iter().copied().map(SignatureBytes).collect(),
            leader_pubkey,
            leader_time_ms,
        };
        let Some(chunk) = FairLedgerCommitChunk::sign(payload, &auth.signing_key) else {
            continue;
        };
        let Ok(memo_bytes) = bincode::serialize(&chunk) else {
            continue;
        };

        // Prioritize commit metadata so it is likely to land before the committed TXs under load.
        let cu_price = ComputeBudgetInstruction::set_compute_unit_price(10_000);
        let memo_ix = Instruction {
            program_id: FAIR_LEDGER_COMMIT_MEMO_PROGRAM_ID,
            accounts: Vec::new(),
            data: memo_bytes,
        };
        let message = Message::new(&[cu_price, memo_ix], Some(&auth.identity_keypair.pubkey()));
        let signers = vec![auth.identity_keypair.as_ref()];
        let tx = Transaction::new(&signers, message, recent_blockhash);
        if let Some(sig) = tx
            .signatures
            .get(0)
            .and_then(|s| s.as_ref().try_into().ok())
        {
            insert_fair_priority(sig, u64::MAX);
        }
        if let Ok(tx_bytes) = bincode::serialize(&tx) {
            out.push(tx_bytes);
        }
    }
    out
}

fn build_fair_ledger_ack_memo_tx(
    auth: &AuthContext,
    recent_blockhash: solana_hash::Hash,
    slot: u64,
    origin_pop_id: &str,
    flow_id: u128,
    batch_id: u128,
    order_start: u64,
    tx_count: u32,
    tx_merkle_root: [u8; 32],
) -> Option<Vec<u8>> {
    if slot == 0 || tx_count == 0 {
        return None;
    }

    let leader_time_ms = now_ms();
    let leader_pubkey = auth.validator_pubkey;
    let origin_pop_id_hash = sha256_bytes(origin_pop_id.as_bytes());

    let payload = FairLedgerAckMemoPayload {
        magic: FAIR_LEDGER_ACK_MAGIC,
        version: FAIR_LEDGER_ACK_VERSION,
        slot,
        origin_pop_id_hash,
        flow_id,
        batch_id,
        order_start,
        tx_count,
        tx_merkle_root,
        leader_pubkey,
        leader_time_ms,
    };
    let memo = FairLedgerAckMemo::sign(payload, &auth.signing_key)?;
    let memo_bytes = bincode::serialize(&memo).ok()?;

    let cu_price = ComputeBudgetInstruction::set_compute_unit_price(10_000);
    let memo_ix = Instruction {
        program_id: FAIR_LEDGER_COMMIT_MEMO_PROGRAM_ID,
        accounts: Vec::new(),
        data: memo_bytes,
    };
    let message = Message::new(&[cu_price, memo_ix], Some(&auth.identity_keypair.pubkey()));
    let signers = vec![auth.identity_keypair.as_ref()];
    let tx = Transaction::new(&signers, message, recent_blockhash);
    if let Some(sig) = tx
        .signatures
        .get(0)
        .and_then(|s| s.as_ref().try_into().ok())
    {
        insert_fair_priority(sig, u64::MAX);
    }
    bincode::serialize(&tx).ok()
}

fn build_fair_ledger_reject_memo_tx(
    auth: &AuthContext,
    recent_blockhash: solana_hash::Hash,
    slot: u64,
    origin_pop_id: &str,
    flow_id: u128,
    batch_id: u128,
    order_start: u64,
    reason: FairBatchRejectReason,
) -> Option<Vec<u8>> {
    if slot == 0 {
        return None;
    }

    let leader_time_ms = now_ms();
    let leader_pubkey = auth.validator_pubkey;
    let origin_pop_id_hash = sha256_bytes(origin_pop_id.as_bytes());

    let payload = FairLedgerRejectMemoPayload {
        magic: FAIR_LEDGER_REJECT_MAGIC,
        version: FAIR_LEDGER_REJECT_VERSION,
        slot,
        origin_pop_id_hash,
        flow_id,
        batch_id,
        order_start,
        reason,
        leader_pubkey,
        leader_time_ms,
    };
    let memo = FairLedgerRejectMemo::sign(payload, &auth.signing_key)?;
    let memo_bytes = bincode::serialize(&memo).ok()?;

    let cu_price = ComputeBudgetInstruction::set_compute_unit_price(10_000);
    let memo_ix = Instruction {
        program_id: FAIR_LEDGER_COMMIT_MEMO_PROGRAM_ID,
        accounts: Vec::new(),
        data: memo_bytes,
    };
    let message = Message::new(&[cu_price, memo_ix], Some(&auth.identity_keypair.pubkey()));
    let signers = vec![auth.identity_keypair.as_ref()];
    let tx = Transaction::new(&signers, message, recent_blockhash);
    if let Some(sig) = tx
        .signatures
        .get(0)
        .and_then(|s| s.as_ref().try_into().ok())
    {
        insert_fair_priority(sig, u64::MAX);
    }
    bincode::serialize(&tx).ok()
}

fn build_fair_ledger_witness_memo_tx(
    payer: &Keypair,
    recent_blockhash: solana_hash::Hash,
    witness_pop_pubkey: PubkeyBytes,
    witness: &solanacdn_protocol::messages::FairBatchWitness,
) -> Option<Vec<u8>> {
    // Only treat verifiable POP witnesses as ledger-exempt metadata.
    if witness.verify(witness_pop_pubkey).is_err() {
        return None;
    }

    let payload = FairLedgerWitnessMemoPayload {
        magic: FAIR_LEDGER_WITNESS_MAGIC,
        version: FAIR_LEDGER_WITNESS_VERSION,
        witness_pop_pubkey,
        witness: witness.clone(),
    };
    let memo = FairLedgerWitnessMemo { payload };
    let memo_bytes = bincode::serialize(&memo).ok()?;

    let cu_price = ComputeBudgetInstruction::set_compute_unit_price(10_000);
    let memo_ix = Instruction {
        program_id: FAIR_LEDGER_COMMIT_MEMO_PROGRAM_ID,
        accounts: Vec::new(),
        data: memo_bytes,
    };
    let message = Message::new(&[cu_price, memo_ix], Some(&payer.pubkey()));
    let signers = vec![payer];
    let tx = Transaction::new(&signers, message, recent_blockhash);
    if let Some(sig) = tx
        .signatures
        .get(0)
        .and_then(|s| s.as_ref().try_into().ok())
    {
        insert_fair_priority(sig, u64::MAX);
    }
    bincode::serialize(&tx).ok()
}

async fn try_send_fair_batch_reject(
    ctrl_out_tx: &mpsc::Sender<AgentToPop>,
    auth: &AuthContext,
    origin_pop_id: &str,
    flow_id: u128,
    batch_id: u128,
    order_start: u64,
    target_slot: Option<u64>,
    reason: FairBatchRejectReason,
) {
    let leader_time_ms = now_ms();
    let payload = FairBatchRejectPayload {
        origin_pop_id: origin_pop_id.to_string(),
        flow_id,
        batch_id,
        order_start,
        target_slot,
        reason,
        leader_pubkey: auth.validator_pubkey,
        leader_time_ms,
    };
    let reject = match FairBatchReject::sign(payload, &auth.signing_key) {
        Ok(v) => v,
        Err(e) => {
            debug!("solanacdn: failed to sign fair batch reject: {e}");
            return;
        }
    };
    let _ = ctrl_out_tx.send(AgentToPop::FairBatchReject(reject)).await;
}

async fn try_inject_fair_batch_reject_memo(
    udp_inject_tpu: &UdpSocket,
    auth: &AuthContext,
    recent_blockhash: Option<solana_hash::Hash>,
    origin_pop_id: &str,
    flow_id: u128,
    batch_id: u128,
    order_start: u64,
    target_slot: Option<u64>,
    reason: FairBatchRejectReason,
) {
    let (Some(slot), Some(recent_blockhash)) = (target_slot, recent_blockhash) else {
        return;
    };
    let Some(tx_bytes) = build_fair_ledger_reject_memo_tx(
        auth,
        recent_blockhash,
        slot,
        origin_pop_id,
        flow_id,
        batch_id,
        order_start,
        reason,
    ) else {
        return;
    };
    let _ = udp_inject_tpu.send(&tx_bytes).await;
}

async fn try_inject_fair_ledger_witness_memo(
    udp_inject_tpu: &UdpSocket,
    auth: &AuthContext,
    recent_blockhash: Option<solana_hash::Hash>,
    witness_pop_pubkey: PubkeyBytes,
    witness: &solanacdn_protocol::messages::FairBatchWitness,
) {
    let Some(recent_blockhash) = recent_blockhash else {
        return;
    };
    let Some(tx_bytes) = build_fair_ledger_witness_memo_tx(
        auth.identity_keypair.as_ref(),
        recent_blockhash,
        witness_pop_pubkey,
        witness,
    ) else {
        return;
    };
    let _ = udp_inject_tpu.send(&tx_bytes).await;
}

fn fair_merkle_leaf_hash(index: u32, sig: &[u8; 64]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(FAIR_MERKLE_LEAF_DOMAIN.len() + 4 + 64);
    buf.extend_from_slice(FAIR_MERKLE_LEAF_DOMAIN);
    buf.extend_from_slice(&index.to_le_bytes());
    buf.extend_from_slice(sig);
    sha256_bytes(&buf)
}

fn fair_merkle_node_hash(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut buf = Vec::with_capacity(FAIR_MERKLE_NODE_DOMAIN.len() + 32 + 32);
    buf.extend_from_slice(FAIR_MERKLE_NODE_DOMAIN);
    buf.extend_from_slice(left);
    buf.extend_from_slice(right);
    sha256_bytes(&buf)
}

fn fair_merkle_root(sigs: &[[u8; 64]]) -> [u8; 32] {
    if sigs.is_empty() {
        return [0u8; 32];
    }
    let mut level: Vec<[u8; 32]> = sigs
        .iter()
        .enumerate()
        .map(|(idx, sig)| fair_merkle_leaf_hash(idx as u32, sig))
        .collect();
    while level.len() > 1 {
        let mut next: Vec<[u8; 32]> = Vec::with_capacity(level.len().div_ceil(2));
        let mut i = 0usize;
        while i < level.len() {
            let left = level[i];
            let right = if i + 1 < level.len() {
                level[i + 1]
            } else {
                left
            };
            next.push(fair_merkle_node_hash(&left, &right));
            i = i.saturating_add(2);
        }
        level = next;
    }
    level[0]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DataPlaneMode {
    Off,
    Auto,
    Always,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TvuShredIngestMode {
    /// Always ingest turbine TVU shreds from any peer (normal validator behavior).
    All,
    /// When connected to SolanaCDN, only ingest shreds sourced from POPs or local reinjection.
    /// When disconnected, fall back to normal P2P ingest.
    SolanaCdnOnly,
    /// Prefer SolanaCDN shreds when healthy, but allow P2P turbine shreds if SolanaCDN is
    /// connected-but-stalled.
    SolanaCdnPreferred,
}

impl Default for TvuShredIngestMode {
    fn default() -> Self {
        Self::All
    }
}

#[derive(Clone, Debug)]
pub struct SolanaCdnConfig {
    pub pop_endpoints: Vec<SocketAddr>,

    pub control_endpoint: Option<SocketAddr>,
    pub control_server_name: String,
    pub control_tls_insecure_skip_verify: bool,
    pub control_tls_ca_cert_path: Option<PathBuf>,
    pub control_refresh_ms: u64,

    pub server_name: String,
    pub tls_insecure_skip_verify: bool,
    pub tls_ca_cert_path: Option<PathBuf>,

    pub udp_mode: DataPlaneMode,

    pub pipe_api_base_url: String,
    pub pipe_api_token: Option<String>,
    pub pipe_api_timeout_ms: u64,
    pub pipe_api_tls_insecure_skip_verify: bool,
    pub pipe_api_tls_ca_cert_path: Option<PathBuf>,
    pub pipe_api_tls_bootstrap: bool,

    /// Optional Prometheus/HTTP status listener for SolanaCDN integration.
    pub metrics_listen_addr: Option<SocketAddr>,

    /// If enabled, measure “race” outcomes between SolanaCDN-delivered shreds and gossip shreds.
    /// This requires ingesting shreds from both paths (do not use `--solanacdn-only`/hybrid).
    pub race_enabled: bool,
    /// Deterministic sampling: track 1/(2^sample_bits) shreds. 0 means “track all” (not
    /// recommended).
    pub race_sample_bits: u8,
    /// Max age (ms) to wait for the other source before dropping a sampled shred from the race
    /// tracker.
    pub race_window_ms: u64,

    pub publish_shreds: bool,
    pub publish_discarded_shreds: bool,
    pub subscribe_shreds: bool,
    pub inject_shreds: bool,
    /// If disabled, drop repair shreds (risk: can stall if shreds are missing).
    pub repair_shreds: bool,
    /// How to gate turbine TVU shreds when SolanaCDN is enabled.
    pub tvu_shred_ingest_mode: TvuShredIngestMode,
    /// In `SolanaCdnPreferred` mode, treat SolanaCDN as stalled when no POP-delivered shreds have
    /// been observed for this many milliseconds.
    pub tvu_shred_hybrid_stale_ms: u64,
    pub direct_shreds_from_pop: bool,
    pub vote_tunnel: bool,
    pub vote_dedup_ttl_ms: u64,
    pub vote_dedup_max_entries: usize,
    /// If enabled, accept POP fair micro-batches and enforce receipt-based ordering.
    pub tx_fair_ordering: bool,
    /// If enabled (in addition to `tx_fair_ordering`), reject fair batches that omit a
    /// `target_slot`.
    ///
    /// Slashing/auditing relies on `target_slot` to bind receipts to a specific leader slot.
    pub tx_fair_require_target_slot: bool,
    /// If enabled, subscribe to leader-signed fair ordering commits and audit the ledger for fair
    /// ordering violations (records evidence/counters only).
    pub tx_fair_slashing: bool,
    /// If enabled (in addition to `tx_fair_slashing`), treat missing ledger commit chunks, missing
    /// committed fair transactions, and non-exempt transaction insertion ahead of the committed
    /// fair prefix as fair-ordering violations.
    pub tx_fair_slashing_strict: bool,
    /// If enabled (in addition to `tx_fair_slashing`), treat “leader ACKed but not committed
    /// on-chain” as a fair-ordering violation.
    ///
    /// Slashing requires a leader-signed ACK (`FairBatchAck` / `FairBatchReceiptCommit`).
    /// POP-signed witnesses are used to detect ACK↔witness mismatches; witness-only slashing for
    /// non-response is controlled by `tx_fair_slashing_nonresponse`.
    pub tx_fair_slashing_witness: bool,
    /// If enabled (in addition to `tx_fair_slashing`), treat “POP witnessed delivery but leader
    /// never committed nor rejected” as a fair-ordering violation.
    ///
    /// This relies on POP-signed witnesses as external evidence of delivery (the ledger alone
    /// cannot prove non-receipt). Use with care: false positives are possible if POPs misbehave.
    pub tx_fair_slashing_nonresponse: bool,
    /// If enabled (in addition to `tx_fair_slashing_witness` and/or `tx_fair_slashing_nonresponse`),
    /// publish POP witness receipts as on-chain memo transactions for replayable audits.
    ///
    /// This increases transaction load and pays fees from the validator identity keypair.
    pub tx_fair_slashing_publish_witness_memos: bool,
    /// Minimum number of distinct POP witnesses required before using POP witness evidence for
    /// slashing decisions (ACK↔witness mismatch and non-response slashing).
    ///
    /// Values less than 1 are treated as 1.
    pub tx_fair_slashing_witness_quorum: u8,
    /// If enabled (in addition to `tx_fair_slashing`), enforce a same-slot “account fence”:
    /// transactions not in the committed fair list must not write-lock any non-signer account
    /// written by a committed fair transaction in that slot.
    pub tx_fair_slashing_fence: bool,
    /// If enabled (in addition to `tx_fair_slashing_fence`), extend the account fence to also
    /// include non-signer read-only accounts accessed by committed fair transactions.
    pub tx_fair_slashing_fence_reads: bool,
    /// If enabled (in addition to `tx_fair_slashing`), enforce fair ordering non-equivocation via
    /// vote withholding when a fair ordering violation is observed (ledger audit failure or
    /// commit equivocation).
    pub tx_fair_slashing_enforce: bool,

    pub shreds_queue_len: usize,
    pub votes_queue_len: usize,
}

impl SolanaCdnConfig {
    pub fn new(pop_addr: SocketAddr) -> Self {
        Self {
            pop_endpoints: vec![pop_addr],
            control_endpoint: None,
            control_server_name: "solanacdn-control".to_string(),
            control_tls_insecure_skip_verify: false,
            control_tls_ca_cert_path: None,
            control_refresh_ms: 1_000,
            server_name: "solanacdn-pop".to_string(),
            tls_insecure_skip_verify: false,
            tls_ca_cert_path: None,
            udp_mode: DataPlaneMode::Auto,
            pipe_api_base_url: "https://api.pipedev.network".to_string(),
            pipe_api_token: None,
            pipe_api_timeout_ms: 2_000,
            pipe_api_tls_insecure_skip_verify: false,
            pipe_api_tls_ca_cert_path: None,
            pipe_api_tls_bootstrap: false,
            metrics_listen_addr: None,
            race_enabled: true,
            race_sample_bits: 12,
            race_window_ms: 5_000,
            publish_shreds: true,
            publish_discarded_shreds: true,
            subscribe_shreds: true,
            inject_shreds: true,
            repair_shreds: true,
            tvu_shred_ingest_mode: TvuShredIngestMode::All,
            tvu_shred_hybrid_stale_ms: 2_000,
            direct_shreds_from_pop: true,
            vote_tunnel: true,
            vote_dedup_ttl_ms: DEFAULT_VOTE_DEDUP_TTL_MS,
            vote_dedup_max_entries: DEFAULT_VOTE_DEDUP_MAX_ENTRIES,
            tx_fair_ordering: false,
            tx_fair_require_target_slot: false,
            tx_fair_slashing: false,
            tx_fair_slashing_strict: false,
            tx_fair_slashing_witness: false,
            tx_fair_slashing_nonresponse: false,
            tx_fair_slashing_publish_witness_memos: false,
            tx_fair_slashing_witness_quorum: 1,
            tx_fair_slashing_fence: false,
            tx_fair_slashing_fence_reads: false,
            tx_fair_slashing_enforce: false,
            shreds_queue_len: 8192,
            votes_queue_len: 1024,
        }
    }
}

impl Default for SolanaCdnConfig {
    fn default() -> Self {
        Self {
            pop_endpoints: Vec::new(),
            control_endpoint: None,
            control_server_name: "solanacdn-control".to_string(),
            control_tls_insecure_skip_verify: false,
            control_tls_ca_cert_path: None,
            control_refresh_ms: 1_000,
            server_name: "solanacdn-pop".to_string(),
            tls_insecure_skip_verify: false,
            tls_ca_cert_path: None,
            udp_mode: DataPlaneMode::Auto,
            pipe_api_base_url: "https://api.pipedev.network".to_string(),
            pipe_api_token: None,
            pipe_api_timeout_ms: 2_000,
            pipe_api_tls_insecure_skip_verify: false,
            pipe_api_tls_ca_cert_path: None,
            pipe_api_tls_bootstrap: false,
            metrics_listen_addr: None,
            race_enabled: true,
            race_sample_bits: 12,
            race_window_ms: 5_000,
            publish_shreds: true,
            publish_discarded_shreds: true,
            subscribe_shreds: true,
            inject_shreds: true,
            repair_shreds: true,
            tvu_shred_ingest_mode: TvuShredIngestMode::All,
            tvu_shred_hybrid_stale_ms: 2_000,
            direct_shreds_from_pop: true,
            vote_tunnel: true,
            vote_dedup_ttl_ms: DEFAULT_VOTE_DEDUP_TTL_MS,
            vote_dedup_max_entries: DEFAULT_VOTE_DEDUP_MAX_ENTRIES,
            tx_fair_ordering: false,
            tx_fair_require_target_slot: false,
            tx_fair_slashing: false,
            tx_fair_slashing_strict: false,
            tx_fair_slashing_witness: false,
            tx_fair_slashing_nonresponse: false,
            tx_fair_slashing_publish_witness_memos: false,
            tx_fair_slashing_witness_quorum: 1,
            tx_fair_slashing_fence: false,
            tx_fair_slashing_fence_reads: false,
            tx_fair_slashing_enforce: false,
            shreds_queue_len: 8192,
            votes_queue_len: 1024,
        }
    }
}

#[derive(Clone, Debug)]
struct ShredPublish {
    kind: ShredKind,
    payload: Bytes,
}

#[derive(Clone, Debug)]
struct VotePublish {
    dst: SocketAddr,
    payload: Bytes,
}

#[derive(Debug)]
struct VoteInjectSockets {
    v4: UdpSocket,
    v6: Option<UdpSocket>,
}

impl VoteInjectSockets {
    async fn bind() -> std::io::Result<Self> {
        let v4 = UdpSocket::bind("0.0.0.0:0").await?;
        let v6 = match std::net::UdpSocket::bind("[::]:0") {
            Ok(sock) => {
                if let Err(e) = sock.set_nonblocking(true) {
                    debug!("solanacdn: failed to set nonblocking IPv6 vote socket: {e}");
                    None
                } else {
                    match UdpSocket::from_std(sock) {
                        Ok(sock) => Some(sock),
                        Err(e) => {
                            debug!("solanacdn: failed to wrap IPv6 vote socket: {e}");
                            None
                        }
                    }
                }
            }
            Err(e) => {
                debug!("solanacdn: failed to bind IPv6 vote socket: {e}");
                None
            }
        };
        Ok(Self { v4, v6 })
    }

    async fn send_to(&self, payload: &[u8], dst: SocketAddr) -> std::io::Result<usize> {
        match dst {
            SocketAddr::V4(_) => self.v4.send_to(payload, dst).await,
            SocketAddr::V6(_) => {
                if let Some(sock) = &self.v6 {
                    sock.send_to(payload, dst).await
                } else {
                    Err(std::io::Error::new(
                        std::io::ErrorKind::AddrNotAvailable,
                        "no IPv6 vote socket available",
                    ))
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
enum UplinkMsg {
    Shred(ShredPublish),
    Vote(VotePublish),
}

#[derive(Clone, Debug)]
struct SessionUplink {
    tx: mpsc::Sender<UplinkMsg>,
}

#[derive(Clone, Copy, Debug, Default)]
struct RateSample {
    at_ms: u64,
    rx_shred_payloads: u64,
    tunneled_vote_packets: u64,
}

#[derive(Debug, Default)]
struct RateState {
    last: Option<RateSample>,
    rx_shred_payloads_per_sec: f64,
    tunneled_vote_packets_per_sec: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RaceSource {
    SolanaCdn,
    Gossip,
}

impl RaceSource {
    fn as_str(&self) -> &'static str {
        match self {
            Self::SolanaCdn => "solanacdn",
            Self::Gossip => "gossip",
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct RaceEntry {
    solanacdn_first_ms: Option<u64>,
    solanacdn_endpoint: Option<SocketAddr>,
    gossip_first_ms: Option<u64>,
    gossip_src_ip: Option<IpAddr>,
    first_seen_ms: u64,
}

#[derive(Clone, Copy, Debug)]
struct RaceSample {
    gossip_src_ip: IpAddr,
    delta_ms: i64,
}

const RACE_LEAD_BUCKETS_MS: [u64; 12] = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000, 2_000, 5_000];
const RACE_DELTA_BUCKETS_MS: [i64; 25] = [
    -5_000, -2_000, -1_000, -500, -200, -100, -50, -20, -10, -5, -2, -1, 0, 1, 2, 5, 10, 20, 50,
    100, 200, 500, 1_000, 2_000, 5_000,
];

#[derive(Clone, Debug)]
struct RaceHistogramSnapshot {
    lead_bucket_counts_by_winner: [(RaceSource, [u64; RACE_LEAD_BUCKETS_MS.len()]); 2],
    lead_sum_ms_by_winner: [(RaceSource, u64); 2],
    lead_count_by_winner: [(RaceSource, u64); 2],
    delta_bucket_counts: [u64; RACE_DELTA_BUCKETS_MS.len()],
    delta_sum_ms: i64,
    delta_count: u64,
    delta_by_pop_endpoint: Vec<(SocketAddr, RaceDeltaHistogramSnapshot)>,
    delta_by_hour_utc: [RaceDeltaHistogramSnapshot; 24],
}

#[derive(Clone, Debug, Default)]
struct RaceMetricsSnapshot {
    enabled: bool,
    sample_bits: u8,
    window_ms: u64,
    inflight: usize,
    pairs_total: u64,
    wins_solanacdn_total: u64,
    wins_gossip_total: u64,
    ties_total: u64,
    last_winner: Option<RaceSource>,
    last_lead_ms: Option<u64>,
    last_shred_slot: Option<u64>,
    histogram: Option<RaceHistogramSnapshot>,
}

#[derive(Clone, Copy, Debug, Default, Serialize)]
struct RaceDeltaHistogramSnapshot {
    bucket_counts: [u64; RACE_DELTA_BUCKETS_MS.len()],
    sum_ms: i64,
    count: u64,
}

#[derive(Clone, Debug)]
struct RaceHistogram {
    solanacdn_bucket_counts: [u64; RACE_LEAD_BUCKETS_MS.len()],
    gossip_bucket_counts: [u64; RACE_LEAD_BUCKETS_MS.len()],
    solanacdn_sum_ms: u64,
    gossip_sum_ms: u64,
    solanacdn_count: u64,
    gossip_count: u64,
    delta: RaceDeltaHistogram,
    delta_by_pop_endpoint: HashMap<SocketAddr, RaceDeltaHistogram>,
    delta_by_hour_utc: [RaceDeltaHistogram; 24],
}

impl RaceHistogram {
    fn new() -> Self {
        let delta_by_hour_utc = std::array::from_fn(|_| RaceDeltaHistogram::new());
        Self {
            solanacdn_bucket_counts: [0u64; RACE_LEAD_BUCKETS_MS.len()],
            gossip_bucket_counts: [0u64; RACE_LEAD_BUCKETS_MS.len()],
            solanacdn_sum_ms: 0,
            gossip_sum_ms: 0,
            solanacdn_count: 0,
            gossip_count: 0,
            delta: RaceDeltaHistogram::new(),
            delta_by_pop_endpoint: HashMap::new(),
            delta_by_hour_utc,
        }
    }

    fn observe(&mut self, winner: RaceSource, lead_ms: u64) {
        let (buckets, sum_ms, count) = match winner {
            RaceSource::SolanaCdn => (
                &mut self.solanacdn_bucket_counts,
                &mut self.solanacdn_sum_ms,
                &mut self.solanacdn_count,
            ),
            RaceSource::Gossip => (
                &mut self.gossip_bucket_counts,
                &mut self.gossip_sum_ms,
                &mut self.gossip_count,
            ),
        };
        *sum_ms = sum_ms.saturating_add(lead_ms);
        *count = count.saturating_add(1);
        for (i, bound_ms) in RACE_LEAD_BUCKETS_MS.iter().enumerate() {
            if lead_ms <= *bound_ms {
                buckets[i] = buckets[i].saturating_add(1);
            }
        }
    }

    fn observe_delta(
        &mut self,
        delta_ms: i64,
        solanacdn_endpoint: Option<SocketAddr>,
        event_ms: u64,
    ) {
        self.delta.observe(delta_ms);

        let hour_utc = ((event_ms / 1000) / 3600) % 24;
        if let Some(hist) = self.delta_by_hour_utc.get_mut(hour_utc as usize) {
            hist.observe(delta_ms);
        }

        const MAX_POP_SEGMENTS: usize = 64;
        if let Some(endpoint) = solanacdn_endpoint {
            if self.delta_by_pop_endpoint.len() < MAX_POP_SEGMENTS
                || self.delta_by_pop_endpoint.contains_key(&endpoint)
            {
                self.delta_by_pop_endpoint
                    .entry(endpoint)
                    .or_insert_with(RaceDeltaHistogram::new)
                    .observe(delta_ms);
            }
        }
    }

    fn snapshot(&self) -> RaceHistogramSnapshot {
        let mut delta_by_pop_endpoint: Vec<(SocketAddr, RaceDeltaHistogramSnapshot)> = self
            .delta_by_pop_endpoint
            .iter()
            .map(|(ep, hist)| (*ep, hist.snapshot()))
            .collect();
        delta_by_pop_endpoint.sort_by_key(|(ep, _)| *ep);

        RaceHistogramSnapshot {
            lead_bucket_counts_by_winner: [
                (RaceSource::SolanaCdn, self.solanacdn_bucket_counts),
                (RaceSource::Gossip, self.gossip_bucket_counts),
            ],
            lead_sum_ms_by_winner: [
                (RaceSource::SolanaCdn, self.solanacdn_sum_ms),
                (RaceSource::Gossip, self.gossip_sum_ms),
            ],
            lead_count_by_winner: [
                (RaceSource::SolanaCdn, self.solanacdn_count),
                (RaceSource::Gossip, self.gossip_count),
            ],
            delta_bucket_counts: self.delta.bucket_counts,
            delta_sum_ms: self.delta.sum_ms,
            delta_count: self.delta.count,
            delta_by_pop_endpoint,
            delta_by_hour_utc: std::array::from_fn(|i| self.delta_by_hour_utc[i].snapshot()),
        }
    }
}

#[derive(Clone, Debug)]
struct RaceDeltaHistogram {
    bucket_counts: [u64; RACE_DELTA_BUCKETS_MS.len()],
    sum_ms: i64,
    count: u64,
}

impl RaceDeltaHistogram {
    fn new() -> Self {
        Self {
            bucket_counts: [0u64; RACE_DELTA_BUCKETS_MS.len()],
            sum_ms: 0,
            count: 0,
        }
    }

    fn observe(&mut self, delta_ms: i64) {
        self.sum_ms = self.sum_ms.saturating_add(delta_ms);
        self.count = self.count.saturating_add(1);
        for (i, bound_ms) in RACE_DELTA_BUCKETS_MS.iter().enumerate() {
            if delta_ms <= *bound_ms {
                self.bucket_counts[i] = self.bucket_counts[i].saturating_add(1);
            }
        }
    }

    fn snapshot(&self) -> RaceDeltaHistogramSnapshot {
        RaceDeltaHistogramSnapshot {
            bucket_counts: self.bucket_counts,
            sum_ms: self.sum_ms,
            count: self.count,
        }
    }
}

#[derive(Clone, Debug)]
struct RaceTracker {
    inflight: HashMap<LedgerShredId, RaceEntry>,
    last_cleanup_ms: u64,
    pairs_total: u64,
    wins_solanacdn_total: u64,
    wins_gossip_total: u64,
    ties_total: u64,
    last_winner: Option<RaceSource>,
    last_lead_ms: Option<u64>,
    last_shred_slot: Option<u64>,
    histogram: RaceHistogram,
    samples: VecDeque<RaceSample>,
}

impl RaceTracker {
    fn new() -> Self {
        Self {
            inflight: HashMap::new(),
            last_cleanup_ms: 0,
            pairs_total: 0,
            wins_solanacdn_total: 0,
            wins_gossip_total: 0,
            ties_total: 0,
            last_winner: None,
            last_lead_ms: None,
            last_shred_slot: None,
            histogram: RaceHistogram::new(),
            samples: VecDeque::new(),
        }
    }

    fn push_sample(&mut self, sample: RaceSample) {
        const MAX_SAMPLES: usize = 4096;
        if self.samples.len() >= MAX_SAMPLES {
            self.samples.pop_front();
        }
        self.samples.push_back(sample);
    }

    fn peek_samples(&self, max: usize) -> Vec<RaceSample> {
        self.samples.iter().take(max).copied().collect()
    }

    fn consume_samples(&mut self, n: usize) {
        for _ in 0..n.min(self.samples.len()) {
            self.samples.pop_front();
        }
    }

    fn cleanup(&mut self, now_ms: u64, window_ms: u64) {
        if self.inflight.is_empty() {
            self.last_cleanup_ms = now_ms;
            return;
        }
        if now_ms.saturating_sub(self.last_cleanup_ms) < 1_000 {
            return;
        }
        self.last_cleanup_ms = now_ms;
        let expire_before = now_ms.saturating_sub(window_ms.max(250));
        self.inflight
            .retain(|_, entry| entry.first_seen_ms >= expire_before);
    }

    fn observe(
        &mut self,
        shred_id: LedgerShredId,
        source: RaceSource,
        now_ms: u64,
        window_ms: u64,
        pop_endpoint: Option<SocketAddr>,
        gossip_src_ip: Option<IpAddr>,
    ) {
        self.cleanup(now_ms, window_ms);

        const RACE_MAX_INFLIGHT: usize = 100_000;
        if !self.inflight.contains_key(&shred_id) && self.inflight.len() >= RACE_MAX_INFLIGHT {
            return;
        }

        let (solanacdn_ms, gossip_ms, solanacdn_endpoint, observed_gossip_src_ip) = {
            let entry = self.inflight.entry(shred_id).or_insert(RaceEntry {
                solanacdn_first_ms: None,
                solanacdn_endpoint: None,
                gossip_first_ms: None,
                gossip_src_ip: None,
                first_seen_ms: now_ms,
            });
            entry.first_seen_ms = entry.first_seen_ms.min(now_ms);
            match source {
                RaceSource::SolanaCdn => {
                    if entry.solanacdn_first_ms.is_none()
                        || entry.solanacdn_first_ms.is_some_and(|t| now_ms < t)
                    {
                        entry.solanacdn_first_ms = Some(now_ms);
                        if pop_endpoint.is_some() {
                            entry.solanacdn_endpoint = pop_endpoint;
                        }
                    } else if entry.solanacdn_endpoint.is_none() && pop_endpoint.is_some() {
                        entry.solanacdn_endpoint = pop_endpoint;
                    }
                }
                RaceSource::Gossip => {
                    if entry.gossip_first_ms.is_none()
                        || entry.gossip_first_ms.is_some_and(|t| now_ms < t)
                    {
                        entry.gossip_first_ms = Some(now_ms);
                        if gossip_src_ip.is_some() {
                            entry.gossip_src_ip = gossip_src_ip;
                        }
                    } else if entry.gossip_src_ip.is_none() && gossip_src_ip.is_some() {
                        entry.gossip_src_ip = gossip_src_ip;
                    }
                }
            }
            (
                entry.solanacdn_first_ms,
                entry.gossip_first_ms,
                entry.solanacdn_endpoint,
                entry.gossip_src_ip,
            )
        };

        let (Some(solanacdn_ms), Some(gossip_ms)) = (solanacdn_ms, gossip_ms) else {
            return;
        };

        self.pairs_total = self.pairs_total.saturating_add(1);
        self.inflight.remove(&shred_id);

        let event_ms = solanacdn_ms.min(gossip_ms);
        let delta_ms: i64 = (solanacdn_ms as i64).saturating_sub(gossip_ms as i64);
        if let Some(ip) = observed_gossip_src_ip {
            self.push_sample(RaceSample {
                gossip_src_ip: ip,
                delta_ms,
            });
        }
        let winner = if delta_ms < 0 {
            Some(RaceSource::SolanaCdn)
        } else if delta_ms > 0 {
            Some(RaceSource::Gossip)
        } else {
            self.ties_total = self.ties_total.saturating_add(1);
            self.last_winner = None;
            self.last_lead_ms = Some(0);
            self.last_shred_slot = Some(shred_id.slot());

            self.histogram
                .observe_delta(delta_ms, solanacdn_endpoint, event_ms);
            return;
        };

        let (winner, lead_ms) = match winner.expect("delta nonzero implies winner") {
            RaceSource::SolanaCdn => {
                self.wins_solanacdn_total = self.wins_solanacdn_total.saturating_add(1);
                (RaceSource::SolanaCdn, delta_ms.unsigned_abs())
            }
            RaceSource::Gossip => {
                self.wins_gossip_total = self.wins_gossip_total.saturating_add(1);
                (RaceSource::Gossip, delta_ms.unsigned_abs())
            }
        };
        self.last_winner = Some(winner);
        self.last_lead_ms = Some(lead_ms);
        self.last_shred_slot = Some(shred_id.slot());
        self.histogram.observe(winner, lead_ms);
        self.histogram
            .observe_delta(delta_ms, solanacdn_endpoint, event_ms);
    }

    fn snapshot(&self, cfg: &SolanaCdnConfig) -> RaceMetricsSnapshot {
        let histogram = cfg.race_enabled.then(|| self.histogram.snapshot());
        RaceMetricsSnapshot {
            enabled: cfg.race_enabled,
            sample_bits: cfg.race_sample_bits,
            window_ms: cfg.race_window_ms,
            inflight: self.inflight.len(),
            pairs_total: self.pairs_total,
            wins_solanacdn_total: self.wins_solanacdn_total,
            wins_gossip_total: self.wins_gossip_total,
            ties_total: self.ties_total,
            last_winner: self.last_winner,
            last_lead_ms: self.last_lead_ms,
            last_shred_slot: self.last_shred_slot.map(|v| v as u64),
            histogram,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolanaCdnStatus {
    pub connected: bool,
    pub publisher: Option<String>,
    pub connected_pops: Vec<String>,
    pub publisher_switches_total: u64,
    pub tx_fair_ordering: bool,
    pub tx_fair_require_target_slot: bool,
    pub tx_fair_slashing: bool,
    pub tx_fair_slashing_strict: bool,
    pub tx_fair_slashing_witness: bool,
    pub tx_fair_slashing_nonresponse: bool,
    pub tx_fair_slashing_publish_witness_memos: bool,
    pub tx_fair_slashing_fence: bool,
    pub tx_fair_slashing_enforce: bool,
    pub tx_fair_slashing_enforce_configured: bool,
    pub tx_fair_slashing_enforce_override: Option<bool>,
    pub tx_fair_batch_received_total: u64,
    pub tx_fair_batch_injected_total: u64,
    pub tx_fair_batch_inject_failed_total: u64,
    pub tx_deduped_packets_total: u64,
    pub tx_relay_dropped_fair_mode_total: u64,
    pub fair_priority_lookups_total: u64,
    pub fair_priority_hits_total: u64,
    pub fair_commits_rx_total: u64,
    pub fair_commits_invalid_total: u64,
    pub fair_equivocations_total: u64,
    pub fair_votes_withheld_total: u64,
    pub fair_ledger_audit_checked_total: u64,
    pub fair_ledger_audit_failed_total: u64,
    pub fair_ledger_audit_inconclusive_total: u64,
    pub fair_ledger_audit_get_slot_entries_failed_total: u64,
    pub fair_ledger_commits_seen_total: u64,
    pub fair_ledger_commits_invalid_total: u64,
    pub fair_order_witnesses_len: u64,
    pub fair_slashed_leaders_len: u64,
    pub fair_ledger_audited_slots_len: u64,
    pub rx_shred_bytes_total: u64,
    pub rx_shred_payloads_total: u64,
    pub dropped_shred_batches_oversized_total: u64,
    pub rx_shred_payloads_per_sec: f64,
    pub tunneled_vote_packets_total: u64,
    pub tunneled_vote_packets_per_sec: f64,
    pub rx_vote_packets_total: u64,
    pub dropped_vote_datagrams_total: u64,
    pub dropped_vote_datagrams_oversized_payload_total: u64,
    pub dropped_vote_datagrams_invalid_payload_total: u64,
    pub dropped_vote_datagrams_unexpected_dst_total: u64,
    pub dropped_quic_shreds_unexpected_msg_total: u64,
    pub dropped_quic_votes_unexpected_msg_total: u64,
    pub dropped_udp_shreds_unexpected_peer_total: u64,
    pub dropped_udp_shreds_unexpected_msg_total: u64,
    pub dropped_udp_votes_unexpected_peer_total: u64,
    pub dropped_udp_votes_unexpected_msg_total: u64,
    pub vote_tunnel_allowed_dsts_len: u64,
    pub last_shred_slot: Option<u64>,
    pub last_shred_timestamp_ms: Option<u64>,
    pub last_shred_age_ms: Option<u64>,
    pub last_accepted_shred_slot: Option<u64>,
    pub last_accepted_shred_timestamp_ms: Option<u64>,
    pub last_accepted_shred_age_ms: Option<u64>,
    pub tvu_shred_ingest_mode: TvuShredIngestMode,
    pub tvu_shred_stale: Option<bool>,
    pub tvu_shred_stale_for_ms: Option<u64>,
    pub race_enabled: bool,
    pub race_sample_bits: u8,
    pub race_window_ms: u64,
    pub race_inflight: u64,
    pub race_pairs_total: u64,
    pub race_wins_solanacdn_total: u64,
    pub race_wins_gossip_total: u64,
    pub race_ties_total: u64,
    pub race_last_winner: Option<String>,
    pub race_last_lead_ms: Option<u64>,
    pub race_last_shred_slot: Option<u64>,
}

#[derive(Debug)]
pub struct SolanaCdnHandle {
    cfg: SolanaCdnConfig,
    connected: AtomicBool,
    published_shred_batches: AtomicU64,
    pushed_shred_batches: AtomicU64,
    rx_shred_bytes: AtomicU64,
    rx_shred_payloads: AtomicU64,
    last_solanacdn_shred_rx_ms: AtomicU64,
    last_solanacdn_shred_slot: AtomicU64,
    last_solanacdn_shred_slot_valid: AtomicBool,
    last_solanacdn_shred_accepted_ms: AtomicU64,
    last_solanacdn_shred_accepted_slot: AtomicU64,
    last_solanacdn_shred_accepted_slot_valid: AtomicBool,
    tunneled_vote_packets: AtomicU64,
    rx_vote_packets: AtomicU64,
    rx_tx_packets: AtomicU64,
    tx_injected_packets: AtomicU64,
    tx_deduped_packets: AtomicU64,
    tx_inject_failed: AtomicU64,
    tx_relay_dropped_fair_mode: AtomicU64,
    tx_fair_batch_received: AtomicU64,
    tx_fair_batch_injected: AtomicU64,
    tx_fair_batch_inject_failed: AtomicU64,
    fair_commits_rx: AtomicU64,
    fair_commits_invalid: AtomicU64,
    fair_equivocations: AtomicU64,
    fair_votes_withheld: AtomicU64,
    tx_fair_slashing_enforce_override: AtomicU8,
    fair_ledger_audit_checked: AtomicU64,
    fair_ledger_audit_failed: AtomicU64,
    fair_ledger_audit_inconclusive: AtomicU64,
    fair_ledger_audit_get_slot_entries_failed: AtomicU64,
    fair_ledger_commits_seen: AtomicU64,
    fair_ledger_commits_invalid: AtomicU64,
    fair_ledger_audited_slots: DashMap<u64, FairLedgerAuditSlotEntry>,
    fair_order_witnesses: DashMap<FairOrderWitnessKey, FairOrderWitnessEntry>,
    fair_slashed_leaders: DashMap<FairSlashedKey, FairSlashedEntry>,
    fair_batch_witness_rx: AtomicU64,
    fair_batch_witness_invalid: AtomicU64,
    fair_batch_witness_slots: DashMap<FairSlashedKey, FairWitnessSlotState>,
    fair_batch_ack_slots: DashMap<FairSlashedKey, FairAckSlotState>,
    fair_batch_reject_slots: DashMap<FairSlashedKey, FairRejectSlotState>,
    dropped_shred_payloads: AtomicU64,
    dropped_shred_batches_oversized: AtomicU64,
    dropped_vote_datagrams: AtomicU64,
    dropped_vote_datagrams_oversized_payload: AtomicU64,
    dropped_vote_datagrams_invalid_payload: AtomicU64,
    dropped_vote_datagrams_unexpected_dst: AtomicU64,
    dropped_quic_shreds_unexpected_msg: AtomicU64,
    dropped_quic_votes_unexpected_msg: AtomicU64,
    dropped_udp_shreds_unexpected_peer: AtomicU64,
    dropped_udp_shreds_unexpected_msg: AtomicU64,
    dropped_udp_votes_unexpected_peer: AtomicU64,
    dropped_udp_votes_unexpected_msg: AtomicU64,
    uplink_broadcast_lagged: AtomicU64,
    pop_endpoint_ips: DashSet<IpAddr>,
    pop_egress_ips: DashMap<IpAddr, u64>,
    connected_pops: DashSet<SocketAddr>,
    publisher_endpoint: ArcSwapOption<String>,
    publisher_switches_total: AtomicU64,
    heartbeat_schema_version: AtomicU32,
    publisher_uplink: ArcSwapOption<SessionUplink>,
    rate_state: std::sync::Mutex<RateState>,
    race_state: std::sync::Mutex<RaceTracker>,
    fair_recent_blockhash: std::sync::Mutex<Option<(solana_hash::Hash, u64)>>,

    recent_tx_sigs: DashMap<[u8; 64], u64>,
    recent_vote_payloads: DashMap<u128, u64>,
    vote_tunnel_allowed_dsts: DashMap<SocketAddr, u64>,
}

impl SolanaCdnHandle {
    fn new(cfg: SolanaCdnConfig) -> Self {
        Self {
            cfg,
            connected: AtomicBool::new(false),
            published_shred_batches: AtomicU64::new(0),
            pushed_shred_batches: AtomicU64::new(0),
            rx_shred_bytes: AtomicU64::new(0),
            rx_shred_payloads: AtomicU64::new(0),
            last_solanacdn_shred_rx_ms: AtomicU64::new(0),
            last_solanacdn_shred_slot: AtomicU64::new(0),
            last_solanacdn_shred_slot_valid: AtomicBool::new(false),
            last_solanacdn_shred_accepted_ms: AtomicU64::new(0),
            last_solanacdn_shred_accepted_slot: AtomicU64::new(0),
            last_solanacdn_shred_accepted_slot_valid: AtomicBool::new(false),
            tunneled_vote_packets: AtomicU64::new(0),
            rx_vote_packets: AtomicU64::new(0),
            rx_tx_packets: AtomicU64::new(0),
            tx_injected_packets: AtomicU64::new(0),
            tx_deduped_packets: AtomicU64::new(0),
            tx_inject_failed: AtomicU64::new(0),
            tx_relay_dropped_fair_mode: AtomicU64::new(0),
            tx_fair_batch_received: AtomicU64::new(0),
            tx_fair_batch_injected: AtomicU64::new(0),
            tx_fair_batch_inject_failed: AtomicU64::new(0),
            fair_commits_rx: AtomicU64::new(0),
            fair_commits_invalid: AtomicU64::new(0),
            fair_equivocations: AtomicU64::new(0),
            fair_votes_withheld: AtomicU64::new(0),
            tx_fair_slashing_enforce_override: AtomicU8::new(
                TxFairSlashingEnforceOverride::Inherit as u8,
            ),
            fair_ledger_audit_checked: AtomicU64::new(0),
            fair_ledger_audit_failed: AtomicU64::new(0),
            fair_ledger_audit_inconclusive: AtomicU64::new(0),
            fair_ledger_audit_get_slot_entries_failed: AtomicU64::new(0),
            fair_ledger_commits_seen: AtomicU64::new(0),
            fair_ledger_commits_invalid: AtomicU64::new(0),
            fair_ledger_audited_slots: DashMap::new(),
            fair_order_witnesses: DashMap::new(),
            fair_slashed_leaders: DashMap::new(),
            fair_batch_witness_rx: AtomicU64::new(0),
            fair_batch_witness_invalid: AtomicU64::new(0),
            fair_batch_witness_slots: DashMap::new(),
            fair_batch_ack_slots: DashMap::new(),
            fair_batch_reject_slots: DashMap::new(),
            dropped_shred_payloads: AtomicU64::new(0),
            dropped_shred_batches_oversized: AtomicU64::new(0),
            dropped_vote_datagrams: AtomicU64::new(0),
            dropped_vote_datagrams_oversized_payload: AtomicU64::new(0),
            dropped_vote_datagrams_invalid_payload: AtomicU64::new(0),
            dropped_vote_datagrams_unexpected_dst: AtomicU64::new(0),
            dropped_quic_shreds_unexpected_msg: AtomicU64::new(0),
            dropped_quic_votes_unexpected_msg: AtomicU64::new(0),
            dropped_udp_shreds_unexpected_peer: AtomicU64::new(0),
            dropped_udp_shreds_unexpected_msg: AtomicU64::new(0),
            dropped_udp_votes_unexpected_peer: AtomicU64::new(0),
            dropped_udp_votes_unexpected_msg: AtomicU64::new(0),
            uplink_broadcast_lagged: AtomicU64::new(0),
            pop_endpoint_ips: DashSet::new(),
            pop_egress_ips: DashMap::new(),
            connected_pops: DashSet::new(),
            publisher_endpoint: ArcSwapOption::const_empty(),
            publisher_switches_total: AtomicU64::new(0),
            heartbeat_schema_version: AtomicU32::new(0),
            publisher_uplink: ArcSwapOption::const_empty(),
            rate_state: std::sync::Mutex::new(RateState::default()),
            race_state: std::sync::Mutex::new(RaceTracker::new()),
            fair_recent_blockhash: std::sync::Mutex::new(None),
            recent_tx_sigs: DashMap::new(),
            recent_vote_payloads: DashMap::new(),
            vote_tunnel_allowed_dsts: DashMap::new(),
        }
    }

    fn note_fair_recent_blockhash(&self, blockhash: solana_hash::Hash) {
        if !(self.cfg.tx_fair_ordering || self.cfg.tx_fair_slashing) {
            return;
        }
        let now = now_ms();
        let mut state = match self.fair_recent_blockhash.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        *state = Some((blockhash, now));
    }

    fn fair_recent_blockhash(&self) -> Option<solana_hash::Hash> {
        if !(self.cfg.tx_fair_ordering || self.cfg.tx_fair_slashing) {
            return None;
        }
        let now = now_ms();
        let state = match self.fair_recent_blockhash.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        let (hash, updated_at_ms) = state.as_ref().copied()?;
        (now.saturating_sub(updated_at_ms) <= FAIR_RECENT_BLOCKHASH_TTL_MS).then_some(hash)
    }

    fn should_dedup_tx_sig(&self, sig: [u8; 64], now: u64) -> bool {
        self.prune_recent_tx_sigs_if_needed(now);
        if let Some(entry) = self.recent_tx_sigs.get(&sig) {
            let expired = *entry < now;
            drop(entry);
            if !expired {
                return true;
            }
            self.recent_tx_sigs.remove(&sig);
        }
        false
    }

    fn note_dedup_tx_sig(&self, sig: [u8; 64], now: u64) {
        self.prune_recent_tx_sigs_if_needed(now);
        self.recent_tx_sigs
            .insert(sig, now.saturating_add(TX_DEDUP_TTL_MS));
    }

    fn prune_recent_tx_sigs_if_needed(&self, now: u64) {
        if self.recent_tx_sigs.len() <= TX_SIG_DEDUP_MAX_ENTRIES {
            return;
        }
        let mut expired: Vec<[u8; 64]> = Vec::new();
        for entry in self.recent_tx_sigs.iter().take(4096) {
            if *entry.value() < now {
                expired.push(*entry.key());
            }
        }
        for key in expired {
            self.recent_tx_sigs.remove(&key);
        }
        if self.recent_tx_sigs.len() > TX_SIG_DEDUP_MAX_ENTRIES {
            self.recent_tx_sigs.clear();
        }
    }

    fn should_dedup_vote_payload(&self, dst: SocketAddr, payload: &[u8], now: u64) -> bool {
        let ttl_ms = self.cfg.vote_dedup_ttl_ms;
        let max_entries = self.cfg.vote_dedup_max_entries;
        if ttl_ms == 0 || max_entries == 0 {
            return false;
        }
        self.prune_recent_vote_payloads_if_needed(now, max_entries);
        let key = vote_dedup_key(&dst, payload);
        if let Some(entry) = self.recent_vote_payloads.get(&key) {
            let expired = *entry < now;
            drop(entry);
            if !expired {
                return true;
            }
            self.recent_vote_payloads.remove(&key);
        }
        self.recent_vote_payloads
            .insert(key, now.saturating_add(ttl_ms));
        false
    }

    fn prune_recent_vote_payloads_if_needed(&self, now: u64, max_entries: usize) {
        if self.recent_vote_payloads.len() <= max_entries {
            return;
        }
        let mut expired: Vec<u128> = Vec::new();
        for entry in self.recent_vote_payloads.iter().take(4096) {
            if *entry.value() < now {
                expired.push(*entry.key());
            }
        }
        for key in expired {
            self.recent_vote_payloads.remove(&key);
        }
        if self.recent_vote_payloads.len() > max_entries {
            self.recent_vote_payloads.clear();
        }
    }

    fn note_vote_tunnel_allowed_dst(&self, dst: SocketAddr, now: u64) {
        if !self.cfg.vote_tunnel {
            return;
        }
        if VOTE_TUNNEL_ALLOWED_DST_TTL_MS == 0 || VOTE_TUNNEL_ALLOWED_DST_MAX_ENTRIES == 0 {
            return;
        }
        self.prune_vote_tunnel_allowed_dsts_if_needed(now);
        self.vote_tunnel_allowed_dsts.insert(
            dst,
            now.saturating_add(VOTE_TUNNEL_ALLOWED_DST_TTL_MS),
        );
    }

    fn prune_vote_tunnel_allowed_dsts_if_needed(&self, now: u64) {
        if self.vote_tunnel_allowed_dsts.len() <= VOTE_TUNNEL_ALLOWED_DST_MAX_ENTRIES {
            return;
        }
        let mut expired: Vec<SocketAddr> = Vec::new();
        for entry in self.vote_tunnel_allowed_dsts.iter().take(1024) {
            if *entry.value() < now {
                expired.push(*entry.key());
            }
        }
        for key in expired {
            self.vote_tunnel_allowed_dsts.remove(&key);
        }
        if self.vote_tunnel_allowed_dsts.len() > VOTE_TUNNEL_ALLOWED_DST_MAX_ENTRIES {
            self.vote_tunnel_allowed_dsts.clear();
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub fn publish_shreds_enabled(&self) -> bool {
        self.cfg.publish_shreds
    }

    pub fn publish_discarded_shreds_enabled(&self) -> bool {
        self.cfg.publish_discarded_shreds
    }

    pub fn subscribe_shreds_enabled(&self) -> bool {
        self.cfg.subscribe_shreds
    }

    /// Count an incoming shred delivered from a POP (direct injection path).
    ///
    /// When POPs are configured for `direct_shreds_from_pop`, they send raw shreds directly to
    /// the validator's TVU socket, bypassing the `PopToAgent::PushShredBatch` stream. This helper
    /// lets the validator pipeline increment "Pushed" counters so operators can confirm traffic
    /// is flowing even in fully-direct mode.
    pub fn note_pop_delivered_shred(&self, bytes: usize) {
        self.note_pop_delivered_shred_with_slot(bytes, None);
    }

    pub fn note_pop_delivered_shred_with_slot(&self, bytes: usize, slot: Option<u64>) {
        self.pushed_shred_batches.fetch_add(1, Ordering::Relaxed);
        self.note_solanacdn_shreds_rx(bytes, 1, slot);
    }

    pub fn inject_shreds_enabled(&self) -> bool {
        self.cfg.inject_shreds
    }

    pub fn direct_shreds_from_pop_enabled(&self) -> bool {
        self.cfg.direct_shreds_from_pop
    }

    pub fn vote_tunnel_enabled(&self) -> bool {
        self.cfg.vote_tunnel
    }

    pub fn repair_shreds_enabled(&self) -> bool {
        self.cfg.repair_shreds
    }

    pub fn tx_fair_slashing_enabled(&self) -> bool {
        self.cfg.tx_fair_slashing
    }

    fn tx_fair_slashing_enforce_override_state(&self) -> TxFairSlashingEnforceOverride {
        TxFairSlashingEnforceOverride::from_u8(
            self.tx_fair_slashing_enforce_override
                .load(Ordering::Relaxed),
        )
    }

    pub fn set_tx_fair_slashing_enforce_override(&self, enforce: Option<bool>) {
        let state = TxFairSlashingEnforceOverride::from_option_bool(enforce);
        self.tx_fair_slashing_enforce_override
            .store(state as u8, Ordering::Relaxed);
    }

    pub fn tx_fair_slashing_enforce_override(&self) -> Option<bool> {
        self.tx_fair_slashing_enforce_override_state()
            .as_option_bool()
    }

    pub fn tx_fair_slashing_enforce_enabled(&self) -> bool {
        if !self.cfg.tx_fair_slashing {
            return false;
        }
        match self.tx_fair_slashing_enforce_override_state() {
            TxFairSlashingEnforceOverride::Inherit => self.cfg.tx_fair_slashing_enforce,
            TxFairSlashingEnforceOverride::ForceOff => false,
            TxFairSlashingEnforceOverride::ForceOn => true,
        }
    }

    fn note_fair_vote_withheld(&self, _leader: &Pubkey, _slot: u64) {
        self.fair_votes_withheld.fetch_add(1, Ordering::Relaxed);
    }

    pub fn fair_slashing_is_slashed_leader(&self, leader: &Pubkey, slot: u64) -> bool {
        if !self.tx_fair_slashing_enforce_enabled() {
            return false;
        }
        let key = FairSlashedKey {
            leader: PubkeyBytes(leader.to_bytes()),
            slot,
        };
        self.fair_slashing_is_slashed_key(&key, now_ms())
    }

    fn fair_slashing_is_slashed_key(&self, key: &FairSlashedKey, now: u64) -> bool {
        let Some(entry) = self.fair_slashed_leaders.get(key) else {
            return false;
        };
        if entry.expires_at_ms < now {
            drop(entry);
            self.fair_slashed_leaders.remove(key);
            return false;
        }
        true
    }

    fn mark_fair_slashed(&self, leader: PubkeyBytes, slot: u64, order_ix: u64, now: u64) {
        let key = FairSlashedKey { leader, slot };
        let already = self.fair_slashing_is_slashed_key(&key, now);
        let entry = FairSlashedEntry {
            expires_at_ms: now.saturating_add(FAIR_SLASHED_TTL_MS),
        };
        self.fair_slashed_leaders.insert(key, entry);
        if !already {
            self.fair_equivocations.fetch_add(1, Ordering::Relaxed);
            if self.tx_fair_slashing_enforce_enabled() {
                warn!(
                    "solanacdn: detected fair ordering violation; withholding votes for leader={} slot={} order_ix={}",
                    leader.to_base58(),
                    slot,
                    order_ix
                );
            } else {
                warn!(
                    "solanacdn: detected fair ordering violation (enforcement disabled); leader={} slot={} order_ix={}",
                    leader.to_base58(),
                    slot,
                    order_ix
                );
            }
        }
    }

    fn note_fair_commit_for_slashing(&self, commit: &FairBatchCommit) {
        if !self.cfg.tx_fair_slashing {
            return;
        }
        let Some(slot) = commit.payload.target_slot else {
            return;
        };

        if self.fair_order_witnesses.len() > FAIR_SLASH_WITNESS_MAX_ENTRIES {
            self.fair_order_witnesses.clear();
        }
        if self.fair_slashed_leaders.len() > FAIR_SLASHED_MAX_ENTRIES {
            self.fair_slashed_leaders.clear();
        }

        let now = now_ms();
        let leader = commit.payload.leader_pubkey;
        let order_start = commit.payload.order_start;

        for (idx, tx_sig) in commit.payload.tx_sigs.iter().enumerate() {
            let order_ix = order_start.wrapping_add(idx as u64);
            let key = FairOrderWitnessKey {
                leader,
                slot,
                order_ix,
            };

            if let Some(existing) = self.fair_order_witnesses.get(&key) {
                let expired = existing.expires_at_ms < now;
                let existing_tx_sig = existing.tx_sig;
                drop(existing);

                if expired {
                    self.fair_order_witnesses.remove(&key);
                } else if existing_tx_sig != tx_sig.0 {
                    self.mark_fair_slashed(leader, slot, order_ix, now);
                    // Once a leader/slot is slashed, extra bookkeeping isn't required.
                    break;
                }
            }

            self.fair_order_witnesses.insert(
                key,
                FairOrderWitnessEntry {
                    tx_sig: tx_sig.0,
                    expires_at_ms: now.saturating_add(FAIR_SLASH_WITNESS_TTL_MS),
                },
            );
        }
    }

    fn note_fair_batch_ack_for_slashing(&self, ack: &FairBatchReceiptCommit) {
        if !self.cfg.tx_fair_slashing || !self.cfg.tx_fair_slashing_witness {
            return;
        }
        let Some(slot) = ack.payload.target_slot else {
            return;
        };

        let now = now_ms();
        let leader = ack.payload.leader_pubkey;
        let batch_id = ack.payload.batch_id;
        let order_start = ack.payload.order_start;
        let leader_time_ms = ack.payload.leader_time_ms;
        let origin_pop_id_hash = sha256_bytes(ack.payload.origin_pop_id.as_bytes());
        let flow_id = ack.payload.flow_id;
        let tx_count = ack.payload.tx_count;
        if tx_count == 0 {
            return;
        }
        let tx_merkle_root = ack.payload.tx_merkle_root;

        if self.fair_batch_ack_slots.len() > FAIR_BATCH_ACK_MAX_SLOTS {
            self.fair_batch_ack_slots.clear();
        }

        let expires_at_ms = now.saturating_add(FAIR_BATCH_ACK_TTL_MS);
        let key = FairSlashedKey { leader, slot };
        let witness_quorum = (self.cfg.tx_fair_slashing_witness_quorum.max(1) as usize)
            .min(FAIR_BATCH_WITNESS_MAX_WITNESSERS_PER_BATCH);
        let ack = FairAckBatch {
            origin_pop_id_hash,
            flow_id,
            tx_count,
            tx_merkle_root,
            order_start,
            leader_time_ms,
        };

        match self.fair_batch_ack_slots.entry(key) {
            Entry::Occupied(mut occ) => {
                let state = occ.get_mut();
                if state.expires_at_ms < now {
                    state.gen = 0;
                    state.batches.clear();
                }
                state.expires_at_ms = expires_at_ms;

                if state.batches.len() > FAIR_BATCH_ACK_MAX_BATCHES_PER_SLOT {
                    state.batches.clear();
                }

                if let Some(existing) = state.batches.get(&batch_id) {
                    if existing.origin_pop_id_hash == ack.origin_pop_id_hash
                        && existing.flow_id == ack.flow_id
                        && existing.tx_count == ack.tx_count
                        && existing.tx_merkle_root == ack.tx_merkle_root
                        && existing.order_start == ack.order_start
                    {
                        return;
                    }
                    self.mark_fair_slashed(leader, slot, ack.order_start, now);
                    return;
                }

                state.gen = state.gen.wrapping_add(1);
                state.batches.insert(batch_id, ack);
            }
            Entry::Vacant(vac) => {
                let mut batches = HashMap::new();
                batches.insert(batch_id, ack);
                vac.insert(FairAckSlotState {
                    gen: 1,
                    expires_at_ms,
                    batches,
                });
            }
        }

        // If we have a POP witness for this batch, it must match the leader ACK.
        if let Some(entry) = self.fair_batch_witness_slots.get(&key) {
            if entry.expires_at_ms < now {
                drop(entry);
                self.fair_batch_witness_slots.remove(&key);
                return;
            }
            if let Some(witness) = entry.batches.get(&batch_id) {
                if witness.witnessers.len() >= witness_quorum {
                    if witness.origin_pop_id_hash != origin_pop_id_hash
                        || witness.flow_id != flow_id
                        || witness.order_start != order_start
                        || witness.tx_count != tx_count
                        || witness.tx_merkle_root != tx_merkle_root
                    {
                        self.mark_fair_slashed(leader, slot, order_start, now);
                    }
                }
            }
        }

        // Reject and ACK for the same leader+slot+batch_id is an equivocation (or a POP bug).
        if let Some(entry) = self.fair_batch_reject_slots.get(&key) {
            if entry.expires_at_ms < now {
                drop(entry);
                self.fair_batch_reject_slots.remove(&key);
                return;
            }
            if let Some(reject) = entry.batches.get(&batch_id) {
                if reject.origin_pop_id_hash == origin_pop_id_hash
                    && reject.flow_id == flow_id
                    && reject.order_start == order_start
                {
                    self.mark_fair_slashed(leader, slot, order_start, now);
                }
            }
        }
    }

    fn fair_batch_ack_gen(&self, key: &FairSlashedKey, now: u64) -> u64 {
        let Some(entry) = self.fair_batch_ack_slots.get(key) else {
            return 0;
        };
        if entry.expires_at_ms < now {
            drop(entry);
            self.fair_batch_ack_slots.remove(key);
            return 0;
        }
        entry.gen
    }

    fn fair_batch_witness_gen(&self, key: &FairSlashedKey, now: u64) -> u64 {
        let Some(entry) = self.fair_batch_witness_slots.get(key) else {
            return 0;
        };
        if entry.expires_at_ms < now {
            drop(entry);
            self.fair_batch_witness_slots.remove(key);
            return 0;
        }
        entry.gen
    }

    fn fair_batch_reject_gen(&self, key: &FairSlashedKey, now: u64) -> u64 {
        let Some(entry) = self.fair_batch_reject_slots.get(key) else {
            return 0;
        };
        if entry.expires_at_ms < now {
            drop(entry);
            self.fair_batch_reject_slots.remove(key);
            return 0;
        }
        entry.gen
    }

    fn note_fair_batch_witness_for_slashing(
        &self,
        witness_pop_pubkey: PubkeyBytes,
        witness: &solanacdn_protocol::messages::FairBatchWitness,
    ) -> bool {
        if !self.cfg.tx_fair_slashing
            || (!self.cfg.tx_fair_slashing_witness
                && !self.cfg.tx_fair_slashing_nonresponse
                && !self.cfg.tx_fair_slashing_publish_witness_memos)
        {
            return false;
        }
        let Some(slot) = witness.payload.attestation.target_slot else {
            return false;
        };

        if self.fair_batch_witness_slots.len() > FAIR_BATCH_WITNESS_MAX_SLOTS {
            self.fair_batch_witness_slots.clear();
        }

        let now = now_ms();
        let key = FairSlashedKey {
            leader: witness.payload.leader_pubkey,
            slot,
        };
        let expires_at_ms = now.saturating_add(FAIR_BATCH_WITNESS_TTL_MS);
        let witness_quorum = (self.cfg.tx_fair_slashing_witness_quorum.max(1) as usize)
            .min(FAIR_BATCH_WITNESS_MAX_WITNESSERS_PER_BATCH);
        let witnessers_len: usize;
        let mut witnesser_added = false;

        let batch_id = witness.payload.attestation.batch_id;
        let origin_pop_id_hash = sha256_bytes(witness.payload.attestation.origin_pop_id.as_bytes());
        let flow_id = witness.payload.attestation.flow_id;
        let batch = FairWitnessBatch {
            origin_pop_id_hash,
            flow_id,
            tx_count: witness.payload.attestation.tx_count,
            tx_merkle_root: witness.payload.attestation.tx_merkle_root,
            order_start: witness.payload.attestation.tx_seq_start,
            pop_time_ms: witness.payload.pop_time_ms,
            witnessers: vec![witness_pop_pubkey],
        };
        let witness_order_start = batch.order_start;
        let witness_tx_count = batch.tx_count;
        let witness_tx_merkle_root = batch.tx_merkle_root;

        match self.fair_batch_witness_slots.entry(key) {
            Entry::Occupied(mut occ) => {
                let state = occ.get_mut();
                if state.expires_at_ms < now {
                    state.gen = 0;
                    state.batches.clear();
                }
                state.expires_at_ms = expires_at_ms;

                if state.batches.len() > FAIR_BATCH_WITNESS_MAX_BATCHES_PER_SLOT {
                    state.batches.clear();
                }

                if let Some(existing) = state.batches.get_mut(&batch_id) {
                    if existing.origin_pop_id_hash != batch.origin_pop_id_hash
                        || existing.flow_id != batch.flow_id
                        || existing.order_start != batch.order_start
                        || existing.tx_count != batch.tx_count
                        || existing.tx_merkle_root != batch.tx_merkle_root
                    {
                        // Conflicting POP witness for the same batch_id: keep the first one to
                        // avoid accidental slashing due to inconsistent witness streams.
                        debug!(
                            "solanacdn: conflicting POP witness for batch_id={}; leader={} slot={} existing_pop_time_ms={} new_pop_time_ms={}",
                            batch_id,
                            key.leader.to_base58(),
                            slot,
                            existing.pop_time_ms,
                            batch.pop_time_ms,
                        );
                        self.fair_batch_witness_invalid
                            .fetch_add(1, Ordering::Relaxed);
                        return false;
                    }

                    existing.pop_time_ms = existing.pop_time_ms.min(batch.pop_time_ms);
                    if !existing.witnessers.contains(&witness_pop_pubkey)
                        && existing.witnessers.len() < FAIR_BATCH_WITNESS_MAX_WITNESSERS_PER_BATCH
                    {
                        existing.witnessers.push(witness_pop_pubkey);
                        witnesser_added = true;
                        state.gen = state.gen.wrapping_add(1);
                    }
                    witnessers_len = existing.witnessers.len();
                } else {
                    witnessers_len = batch.witnessers.len();
                    witnesser_added = true;
                    state.gen = state.gen.wrapping_add(1);
                    state.batches.insert(batch_id, batch);
                }
            }
            Entry::Vacant(vac) => {
                witnessers_len = batch.witnessers.len();
                witnesser_added = true;
                let mut batches = HashMap::new();
                batches.insert(batch_id, batch);
                vac.insert(FairWitnessSlotState {
                    gen: 1,
                    expires_at_ms,
                    batches,
                });
            }
        }

        // If we have a leader ACK for this batch, it must match the POP witness.
        if witnessers_len < witness_quorum {
            return witnesser_added;
        }
        if let Some(entry) = self.fair_batch_ack_slots.get(&key) {
            if entry.expires_at_ms < now {
                drop(entry);
                self.fair_batch_ack_slots.remove(&key);
                return witnesser_added;
            }
            if let Some(ack) = entry.batches.get(&batch_id) {
                if ack.origin_pop_id_hash != origin_pop_id_hash
                    || ack.flow_id != flow_id
                    || ack.order_start != witness_order_start
                    || ack.tx_count != witness_tx_count
                    || ack.tx_merkle_root != witness_tx_merkle_root
                {
                    self.mark_fair_slashed(key.leader, slot, ack.order_start, now);
                }
            }
        }

        witnesser_added
    }

    fn note_fair_batch_reject_for_slashing(&self, reject: &FairBatchReject) {
        if !self.cfg.tx_fair_slashing
            || (!self.cfg.tx_fair_slashing_nonresponse && !self.cfg.tx_fair_slashing_witness)
        {
            return;
        }
        let Some(slot) = reject.payload.target_slot else {
            return;
        };

        if self.fair_batch_reject_slots.len() > FAIR_BATCH_REJECT_MAX_SLOTS {
            self.fair_batch_reject_slots.clear();
        }

        let now = now_ms();
        let leader = reject.payload.leader_pubkey;
        let batch_id = reject.payload.batch_id;
        let order_start = reject.payload.order_start;
        let reason = reject.payload.reason;
        let leader_time_ms = reject.payload.leader_time_ms;
        let origin_pop_id_hash = sha256_bytes(reject.payload.origin_pop_id.as_bytes());
        let flow_id = reject.payload.flow_id;

        let key = FairSlashedKey { leader, slot };
        let expires_at_ms = now.saturating_add(FAIR_BATCH_REJECT_TTL_MS);
        let reject = FairRejectBatch {
            origin_pop_id_hash,
            flow_id,
            order_start,
            reason,
            leader_time_ms,
        };

        match self.fair_batch_reject_slots.entry(key) {
            Entry::Occupied(mut occ) => {
                let state = occ.get_mut();
                if state.expires_at_ms < now {
                    state.gen = 0;
                    state.batches.clear();
                }
                state.expires_at_ms = expires_at_ms;

                if state.batches.len() > FAIR_BATCH_REJECT_MAX_BATCHES_PER_SLOT {
                    state.batches.clear();
                }

                if let Some(existing) = state.batches.get(&batch_id) {
                    if existing.origin_pop_id_hash == reject.origin_pop_id_hash
                        && existing.flow_id == reject.flow_id
                        && existing.order_start == reject.order_start
                        && std::mem::discriminant(&existing.reason)
                            == std::mem::discriminant(&reject.reason)
                        && existing.leader_time_ms == reject.leader_time_ms
                    {
                        return;
                    }
                    self.mark_fair_slashed(leader, slot, reject.order_start, now);
                    return;
                }

                state.gen = state.gen.wrapping_add(1);
                state.batches.insert(batch_id, reject);
            }
            Entry::Vacant(vac) => {
                let mut batches = HashMap::new();
                batches.insert(batch_id, reject);
                vac.insert(FairRejectSlotState {
                    gen: 1,
                    expires_at_ms,
                    batches,
                });
            }
        }

        // Reject and ACK for the same leader+slot+batch_id is an equivocation (or a POP bug).
        if let Some(entry) = self.fair_batch_ack_slots.get(&key) {
            if entry.expires_at_ms < now {
                drop(entry);
                self.fair_batch_ack_slots.remove(&key);
                return;
            }
            if let Some(ack) = entry.batches.get(&batch_id) {
                if ack.origin_pop_id_hash == origin_pop_id_hash
                    && ack.flow_id == flow_id
                    && ack.order_start == order_start
                {
                    self.mark_fair_slashed(leader, slot, order_start, now);
                }
            }
        }
    }

    fn audit_fair_ledger_commits_for_slot(
        &self,
        blockstore: &Blockstore,
        leader: &Pubkey,
        slot: u64,
    ) {
        self.audit_fair_ledger_commits_for_slot_impl(blockstore, None, leader, slot);
    }

    fn audit_fair_ledger_commits_for_slot_with_bank(
        &self,
        blockstore: &Blockstore,
        bank: &Bank,
        leader: &Pubkey,
        slot: u64,
    ) {
        self.audit_fair_ledger_commits_for_slot_impl(blockstore, Some(bank), leader, slot);
    }

    fn audit_fair_ledger_commits_for_slot_impl(
        &self,
        blockstore: &Blockstore,
        bank: Option<&Bank>,
        leader: &Pubkey,
        slot: u64,
    ) {
        if !self.cfg.tx_fair_slashing {
            return;
        }

        let now = now_ms();
        let gen_key = FairSlashedKey {
            leader: PubkeyBytes(leader.to_bytes()),
            slot,
        };
        let ack_gen = self
            .cfg
            .tx_fair_slashing_witness
            .then(|| self.fair_batch_ack_gen(&gen_key, now))
            .unwrap_or(0);
        let witness_gen = self
            .cfg
            .tx_fair_slashing_nonresponse
            .then(|| self.fair_batch_witness_gen(&gen_key, now))
            .unwrap_or(0);
        let reject_gen = (self.cfg.tx_fair_slashing_nonresponse
            || self.cfg.tx_fair_slashing_witness)
            .then(|| self.fair_batch_reject_gen(&gen_key, now))
            .unwrap_or(0);

        if let Some(entry) = self.fair_ledger_audited_slots.get(&slot) {
            let ok = entry.ok;
            let prev_ack_gen = entry.ack_gen;
            let prev_witness_gen = entry.witness_gen;
            let prev_reject_gen = entry.reject_gen;
            drop(entry);

            if !ok {
                return;
            }
            if (!self.cfg.tx_fair_slashing_witness || ack_gen <= prev_ack_gen)
                && (!self.cfg.tx_fair_slashing_nonresponse
                    || (witness_gen <= prev_witness_gen && reject_gen <= prev_reject_gen))
            {
                return;
            }
        }

        self.fair_ledger_audit_checked
            .fetch_add(1, Ordering::Relaxed);

        let entries = match blockstore.get_slot_entries(slot, 0) {
            Ok(entries) => entries,
            Err(_) => {
                self.fair_ledger_audit_get_slot_entries_failed
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        let ok =
            self.audit_fair_ledger_commits_in_entries_impl(entries.as_slice(), bank, leader, slot);

        if self.fair_ledger_audited_slots.len() > 100_000 {
            self.fair_ledger_audited_slots.clear();
        }
        self.fair_ledger_audited_slots.insert(
            slot,
            FairLedgerAuditSlotEntry {
                ok,
                ack_gen,
                witness_gen,
                reject_gen,
            },
        );

        if !ok {
            self.fair_ledger_audit_failed
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    #[cfg(test)]
    fn audit_fair_ledger_commits_in_entries(
        &self,
        entries: &[solana_entry::entry::Entry],
        leader: &Pubkey,
        slot: u64,
    ) -> bool {
        self.audit_fair_ledger_commits_in_entries_impl(entries, None, leader, slot)
    }

    fn audit_fair_ledger_commits_in_entries_impl(
        &self,
        entries: &[solana_entry::entry::Entry],
        bank: Option<&Bank>,
        leader: &Pubkey,
        slot: u64,
    ) -> bool {
        if entries.is_empty() {
            return true;
        }

        #[derive(Clone, Copy, Debug)]
        struct SlotTx {
            sig0: [u8; 64],
            is_exempt: bool,
        }

        let now = now_ms();
        let expected_leader = PubkeyBytes(leader.to_bytes());
        let memo_program_id = FAIR_LEDGER_COMMIT_MEMO_PROGRAM_ID;
        let compute_budget_program_id = solana_compute_budget_interface::id();
        let vote_program_id = solana_vote_program::id();
        let witness_quorum = (self.cfg.tx_fair_slashing_witness_quorum.max(1) as usize)
            .min(FAIR_BATCH_WITNESS_MAX_WITNESSERS_PER_BATCH);
        let offchain_ack_batches: Option<HashMap<u128, FairAckBatch>> = self
            .cfg
            .tx_fair_slashing_witness
            .then(|| {
                let key = FairSlashedKey {
                    leader: expected_leader,
                    slot,
                };
                let Some(entry) = self.fair_batch_ack_slots.get(&key) else {
                    return None;
                };
                if entry.expires_at_ms < now {
                    drop(entry);
                    self.fair_batch_ack_slots.remove(&key);
                    return None;
                }
                if entry.batches.is_empty() {
                    return None;
                }
                Some(entry.batches.clone())
            })
            .flatten();
        let offchain_witness_batches: Option<HashMap<u128, FairWitnessBatch>> =
            (self.cfg.tx_fair_slashing_nonresponse || self.cfg.tx_fair_slashing_witness)
                .then(|| {
                    let key = FairSlashedKey {
                        leader: expected_leader,
                        slot,
                    };
                    let Some(entry) = self.fair_batch_witness_slots.get(&key) else {
                        return None;
                    };
                    if entry.expires_at_ms < now {
                        drop(entry);
                        self.fair_batch_witness_slots.remove(&key);
                        return None;
                    }
                    if entry.batches.is_empty() {
                        return None;
                    }
                    Some(entry.batches.clone())
                })
                .flatten();
        let offchain_reject_batches: Option<HashMap<u128, FairRejectBatch>> =
            (self.cfg.tx_fair_slashing_nonresponse || self.cfg.tx_fair_slashing_witness)
                .then(|| {
                    let key = FairSlashedKey {
                        leader: expected_leader,
                        slot,
                    };
                    let Some(entry) = self.fair_batch_reject_slots.get(&key) else {
                        return None;
                    };
                    if entry.expires_at_ms < now {
                        drop(entry);
                        self.fair_batch_reject_slots.remove(&key);
                        return None;
                    }
                    if entry.batches.is_empty() {
                        return None;
                    }
                    Some(entry.batches.clone())
                })
                .flatten();

        let mut slot_txs: Vec<SlotTx> = Vec::new();
        let mut batch_order_start: HashMap<u128, u64> = HashMap::new();
        let mut batch_chunk_total: HashMap<u128, u16> = HashMap::new();
        let mut chunk_sigs: HashMap<(u128, u16), Vec<[u8; 64]>> = HashMap::new();
        let mut chunk_commit_sigs: HashMap<(u128, u16), [u8; 64]> = HashMap::new();
        let mut onchain_ack_batches: HashMap<u128, FairAckBatch> = HashMap::new();
        let mut onchain_reject_batches: HashMap<u128, FairRejectBatch> = HashMap::new();
        let mut onchain_witness_batches: HashMap<u128, FairWitnessBatch> = HashMap::new();

        for entry in entries {
            for tx in entry.transactions.iter() {
                let Some(sig0) = tx
                    .signatures
                    .get(0)
                    .and_then(|s| s.as_ref().try_into().ok())
                else {
                    continue;
                };

                let (account_keys, instructions) = match &tx.message {
                    VersionedMessage::Legacy(msg) => {
                        (msg.account_keys.as_slice(), msg.instructions.as_slice())
                    }
                    VersionedMessage::V0(msg) => {
                        (msg.account_keys.as_slice(), msg.instructions.as_slice())
                    }
                };

                let mut has_non_compute_ix = false;
                let mut vote_only = true;
                let mut memo_only = true;
                let mut saw_vote = false;
                let mut saw_valid_fair_commit_chunk = false;
                let mut saw_valid_fair_ack_memo = false;
                let mut saw_valid_fair_reject_memo = false;
                let mut saw_valid_fair_witness_memo = false;

                for ix in instructions {
                    let Some(program_id) = account_keys.get(ix.program_id_index as usize) else {
                        continue;
                    };

                    if program_id == &compute_budget_program_id {
                        continue;
                    }

                    has_non_compute_ix = true;

                    if program_id == &vote_program_id {
                        saw_vote = true;
                        memo_only = false;
                        continue;
                    }

                    vote_only = false;

                    if program_id != &memo_program_id {
                        memo_only = false;
                        continue;
                    }
                    let data = ix.data.as_slice();
                    if data.len() >= FAIR_LEDGER_COMMIT_MAGIC.len()
                        && &data[..FAIR_LEDGER_COMMIT_MAGIC.len()] == FAIR_LEDGER_COMMIT_MAGIC
                    {
                        let Ok(chunk) = bincode::deserialize::<FairLedgerCommitChunk>(data) else {
                            memo_only = false;
                            continue;
                        };

                        self.fair_ledger_commits_seen
                            .fetch_add(1, Ordering::Relaxed);

                        if !chunk.verify() {
                            self.fair_ledger_commits_invalid
                                .fetch_add(1, Ordering::Relaxed);
                            memo_only = false;
                            continue;
                        }
                        saw_valid_fair_commit_chunk = true;
                        if chunk.payload.slot != slot
                            || chunk.payload.leader_pubkey != expected_leader
                        {
                            continue;
                        }

                        let batch_id = chunk.payload.batch_id;
                        if let Some(prev_order_start) =
                            batch_order_start.insert(batch_id, chunk.payload.order_start)
                        {
                            if prev_order_start != chunk.payload.order_start {
                                self.mark_fair_slashed(
                                    expected_leader,
                                    slot,
                                    chunk.payload.order_start,
                                    now_ms(),
                                );
                                return false;
                            }
                        }

                        if let Some(prev_total) =
                            batch_chunk_total.insert(batch_id, chunk.payload.chunk_total)
                        {
                            if prev_total != chunk.payload.chunk_total {
                                self.mark_fair_slashed(
                                    expected_leader,
                                    slot,
                                    chunk.payload.order_start,
                                    now_ms(),
                                );
                                return false;
                            }
                        }

                        let key = (batch_id, chunk.payload.chunk_index);
                        if let Some(existing_sig) = chunk_commit_sigs.get(&key) {
                            if existing_sig != &chunk.signature.0 {
                                self.mark_fair_slashed(
                                    expected_leader,
                                    slot,
                                    chunk.payload.order_start,
                                    now_ms(),
                                );
                                return false;
                            }
                        } else {
                            chunk_commit_sigs.insert(key, chunk.signature.0);
                            let sigs: Vec<[u8; 64]> =
                                chunk.payload.tx_sigs.into_iter().map(|sig| sig.0).collect();
                            chunk_sigs.insert(key, sigs);
                        }
                    } else if data.len() >= FAIR_LEDGER_ACK_MAGIC.len()
                        && &data[..FAIR_LEDGER_ACK_MAGIC.len()] == FAIR_LEDGER_ACK_MAGIC
                    {
                        let Ok(memo) = bincode::deserialize::<FairLedgerAckMemo>(data) else {
                            memo_only = false;
                            continue;
                        };

                        if !memo.verify() {
                            memo_only = false;
                            continue;
                        }
                        saw_valid_fair_ack_memo = true;
                        if memo.payload.slot != slot
                            || memo.payload.leader_pubkey != expected_leader
                        {
                            continue;
                        }

                        if self.cfg.tx_fair_slashing_witness {
                            let batch_id = memo.payload.batch_id;
                            let ack = FairAckBatch {
                                origin_pop_id_hash: memo.payload.origin_pop_id_hash,
                                flow_id: memo.payload.flow_id,
                                tx_count: memo.payload.tx_count,
                                tx_merkle_root: memo.payload.tx_merkle_root,
                                order_start: memo.payload.order_start,
                                leader_time_ms: memo.payload.leader_time_ms,
                            };
                            if let Some(existing) = onchain_ack_batches.get(&batch_id) {
                                if existing.origin_pop_id_hash != ack.origin_pop_id_hash
                                    || existing.flow_id != ack.flow_id
                                    || existing.order_start != ack.order_start
                                    || existing.tx_count != ack.tx_count
                                    || existing.tx_merkle_root != ack.tx_merkle_root
                                {
                                    self.mark_fair_slashed(
                                        expected_leader,
                                        slot,
                                        ack.order_start,
                                        now_ms(),
                                    );
                                    return false;
                                }
                            } else {
                                onchain_ack_batches.insert(batch_id, ack);
                            }
                        }
                    } else if data.len() >= FAIR_LEDGER_WITNESS_MAGIC.len()
                        && &data[..FAIR_LEDGER_WITNESS_MAGIC.len()] == FAIR_LEDGER_WITNESS_MAGIC
                    {
                        let Ok(memo) = bincode::deserialize::<FairLedgerWitnessMemo>(data) else {
                            memo_only = false;
                            continue;
                        };

                        if !memo.verify() {
                            memo_only = false;
                            continue;
                        }
                        saw_valid_fair_witness_memo = true;

                        if !self.cfg.tx_fair_slashing_witness
                            && !self.cfg.tx_fair_slashing_nonresponse
                        {
                            continue;
                        }

                        let witness = &memo.payload.witness;
                        let Some(target_slot) = witness.payload.attestation.target_slot else {
                            continue;
                        };
                        if target_slot != slot || witness.payload.leader_pubkey != expected_leader {
                            continue;
                        }

                        let batch_id = witness.payload.attestation.batch_id;
                        let origin_pop_id_hash =
                            sha256_bytes(witness.payload.attestation.origin_pop_id.as_bytes());
                        let flow_id = witness.payload.attestation.flow_id;
                        let batch = FairWitnessBatch {
                            origin_pop_id_hash,
                            flow_id,
                            tx_count: witness.payload.attestation.tx_count,
                            tx_merkle_root: witness.payload.attestation.tx_merkle_root,
                            order_start: witness.payload.attestation.tx_seq_start,
                            pop_time_ms: witness.payload.pop_time_ms,
                            witnessers: vec![memo.payload.witness_pop_pubkey],
                        };

                        if let Some(existing) = onchain_witness_batches.get_mut(&batch_id) {
                            if existing.origin_pop_id_hash != batch.origin_pop_id_hash
                                || existing.flow_id != batch.flow_id
                                || existing.order_start != batch.order_start
                                || existing.tx_count != batch.tx_count
                                || existing.tx_merkle_root != batch.tx_merkle_root
                            {
                                // Conflicting POP witness for the same batch_id: keep the first
                                // one to avoid accidental slashing due to inconsistent witness
                                // streams.
                                self.fair_batch_witness_invalid
                                    .fetch_add(1, Ordering::Relaxed);
                                continue;
                            }

                            existing.pop_time_ms = existing.pop_time_ms.min(batch.pop_time_ms);
                            let witness_pop_pubkey = memo.payload.witness_pop_pubkey;
                            if !existing.witnessers.contains(&witness_pop_pubkey)
                                && existing.witnessers.len()
                                    < FAIR_BATCH_WITNESS_MAX_WITNESSERS_PER_BATCH
                            {
                                existing.witnessers.push(witness_pop_pubkey);
                            }
                        } else {
                            onchain_witness_batches.insert(batch_id, batch);
                        }
                    } else if data.len() >= FAIR_LEDGER_REJECT_MAGIC.len()
                        && &data[..FAIR_LEDGER_REJECT_MAGIC.len()] == FAIR_LEDGER_REJECT_MAGIC
                    {
                        let Ok(memo) = bincode::deserialize::<FairLedgerRejectMemo>(data) else {
                            memo_only = false;
                            continue;
                        };

                        if !memo.verify() {
                            memo_only = false;
                            continue;
                        }
                        saw_valid_fair_reject_memo = true;
                        if memo.payload.slot != slot
                            || memo.payload.leader_pubkey != expected_leader
                        {
                            continue;
                        }
                        let batch_id = memo.payload.batch_id;
                        let reject = FairRejectBatch {
                            origin_pop_id_hash: memo.payload.origin_pop_id_hash,
                            flow_id: memo.payload.flow_id,
                            order_start: memo.payload.order_start,
                            reason: memo.payload.reason,
                            leader_time_ms: memo.payload.leader_time_ms,
                        };
                        if let Some(existing) = onchain_reject_batches.get(&batch_id) {
                            if existing.origin_pop_id_hash != reject.origin_pop_id_hash
                                || existing.flow_id != reject.flow_id
                                || existing.order_start != reject.order_start
                                || std::mem::discriminant(&existing.reason)
                                    != std::mem::discriminant(&reject.reason)
                            {
                                self.mark_fair_slashed(
                                    expected_leader,
                                    slot,
                                    reject.order_start,
                                    now_ms(),
                                );
                                return false;
                            }
                        } else {
                            onchain_reject_batches.insert(batch_id, reject);
                        }
                    } else {
                        memo_only = false;
                        continue;
                    }
                }

                let is_vote_exempt = has_non_compute_ix && vote_only && saw_vote;
                let is_fair_memo_exempt = has_non_compute_ix
                    && memo_only
                    && (saw_valid_fair_commit_chunk
                        || saw_valid_fair_ack_memo
                        || saw_valid_fair_reject_memo
                        || saw_valid_fair_witness_memo);

                slot_txs.push(SlotTx {
                    sig0,
                    is_exempt: is_vote_exempt || is_fair_memo_exempt,
                });
            }
        }

        let ack_batches: Option<HashMap<u128, FairAckBatch>> = if self.cfg.tx_fair_slashing_witness
        {
            let mut merged: HashMap<u128, FairAckBatch> = offchain_ack_batches.unwrap_or_default();
            for (batch_id, ack) in onchain_ack_batches.into_iter() {
                match merged.entry(batch_id) {
                    std::collections::hash_map::Entry::Occupied(occ) => {
                        let existing = occ.get();
                        if existing.origin_pop_id_hash != ack.origin_pop_id_hash
                            || existing.flow_id != ack.flow_id
                            || existing.order_start != ack.order_start
                            || existing.tx_count != ack.tx_count
                            || existing.tx_merkle_root != ack.tx_merkle_root
                        {
                            self.mark_fair_slashed(expected_leader, slot, ack.order_start, now);
                            return false;
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(vac) => {
                        vac.insert(ack);
                    }
                }
            }
            (!merged.is_empty()).then_some(merged)
        } else {
            None
        };

        let reject_batches: Option<HashMap<u128, FairRejectBatch>> = {
            let mut merged: HashMap<u128, FairRejectBatch> =
                offchain_reject_batches.unwrap_or_default();
            for (batch_id, reject) in onchain_reject_batches.into_iter() {
                match merged.entry(batch_id) {
                    std::collections::hash_map::Entry::Occupied(occ) => {
                        let existing = occ.get();
                        if existing.origin_pop_id_hash != reject.origin_pop_id_hash
                            || existing.flow_id != reject.flow_id
                            || existing.order_start != reject.order_start
                        {
                            self.mark_fair_slashed(expected_leader, slot, reject.order_start, now);
                            return false;
                        }
                    }
                    std::collections::hash_map::Entry::Vacant(vac) => {
                        vac.insert(reject);
                    }
                }
            }
            (!merged.is_empty()).then_some(merged)
        };

        let witness_batches: Option<HashMap<u128, FairWitnessBatch>> =
            (self.cfg.tx_fair_slashing_nonresponse || self.cfg.tx_fair_slashing_witness)
                .then(|| {
                    let mut merged: HashMap<u128, FairWitnessBatch> =
                        offchain_witness_batches.unwrap_or_default();
                    for (batch_id, witness) in onchain_witness_batches.iter() {
                        match merged.entry(*batch_id) {
                            std::collections::hash_map::Entry::Occupied(mut occ) => {
                                let existing = occ.get_mut();
                                if existing.origin_pop_id_hash != witness.origin_pop_id_hash
                                    || existing.flow_id != witness.flow_id
                                    || existing.order_start != witness.order_start
                                    || existing.tx_count != witness.tx_count
                                    || existing.tx_merkle_root != witness.tx_merkle_root
                                {
                                    // Conflicting POP witness for the same batch_id: keep the first
                                    // one to avoid accidental slashing due to inconsistent witness
                                    // streams.
                                    self.fair_batch_witness_invalid
                                        .fetch_add(1, Ordering::Relaxed);
                                    continue;
                                }

                                existing.pop_time_ms =
                                    existing.pop_time_ms.min(witness.pop_time_ms);
                                for witnesser in witness.witnessers.iter() {
                                    if existing.witnessers.len()
                                        >= FAIR_BATCH_WITNESS_MAX_WITNESSERS_PER_BATCH
                                    {
                                        break;
                                    }
                                    if !existing.witnessers.contains(witnesser) {
                                        existing.witnessers.push(*witnesser);
                                    }
                                }
                            }
                            std::collections::hash_map::Entry::Vacant(vac) => {
                                vac.insert(witness.clone());
                            }
                        }
                    }

                    let mut quorum_met: HashMap<u128, FairWitnessBatch> = HashMap::new();
                    for (batch_id, witness) in merged.into_iter() {
                        if witness.witnessers.len() >= witness_quorum {
                            quorum_met.insert(batch_id, witness);
                        }
                    }
                    (!quorum_met.is_empty()).then_some(quorum_met)
                })
                .flatten();

        if let (Some(acked), Some(rejected)) = (
            ack_batches.as_ref().filter(|m| !m.is_empty()),
            reject_batches.as_ref().filter(|m| !m.is_empty()),
        ) {
            for (batch_id, ack) in acked.iter() {
                if let Some(reject) = rejected.get(batch_id) {
                    if reject.origin_pop_id_hash == ack.origin_pop_id_hash
                        && reject.flow_id == ack.flow_id
                        && reject.order_start == ack.order_start
                    {
                        self.mark_fair_slashed(expected_leader, slot, ack.order_start, now);
                        return false;
                    }
                }
            }
        }

        if self.cfg.tx_fair_slashing_witness {
            // If we have both leader acks and POP witness receipts for this leader+slot, they must
            // agree on the attested batch metadata (prevents leader-side insertion/dropping/rewrite
            // of the POP attested signature list).
            if let (Some(acked), Some(witnessed)) = (
                ack_batches.as_ref().filter(|m| !m.is_empty()),
                witness_batches.as_ref().filter(|m| !m.is_empty()),
            ) {
                for (batch_id, ack) in acked.iter() {
                    if let Some(witness) = witnessed.get(batch_id) {
                        if witness.origin_pop_id_hash != ack.origin_pop_id_hash
                            || witness.flow_id != ack.flow_id
                            || witness.order_start != ack.order_start
                            || witness.tx_count != ack.tx_count
                            || witness.tx_merkle_root != ack.tx_merkle_root
                        {
                            self.mark_fair_slashed(expected_leader, slot, ack.order_start, now);
                            return false;
                        }
                    }
                }
            }
        }

        if let Some(acked) = ack_batches.as_ref().filter(|m| !m.is_empty()) {
            // If we have leader acks for this leader+slot, the slot must contain matching
            // on-chain fair commit metadata for each acked batch.
            if batch_order_start.is_empty() {
                let (batch_id, ack) = acked.iter().next().expect("non-empty");
                debug!(
                    "solanacdn: missing on-chain fair commits for acked slot; leader={} slot={} batch_id={} order_start={} leader_time_ms={}",
                    expected_leader.to_base58(),
                    slot,
                    batch_id,
                    ack.order_start,
                    ack.leader_time_ms
                );
                self.mark_fair_slashed(expected_leader, slot, ack.order_start, now);
                return false;
            }

            for (batch_id, ack) in acked.iter() {
                let Some(&order_start) = batch_order_start.get(batch_id) else {
                    debug!(
                        "solanacdn: missing on-chain fair commit for acked batch; leader={} slot={} batch_id={} order_start={} leader_time_ms={}",
                        expected_leader.to_base58(),
                        slot,
                        batch_id,
                        ack.order_start,
                        ack.leader_time_ms
                    );
                    self.mark_fair_slashed(expected_leader, slot, ack.order_start, now);
                    return false;
                };

                if order_start != ack.order_start {
                    self.mark_fair_slashed(expected_leader, slot, ack.order_start, now);
                    return false;
                }

                let Some(&chunk_total) = batch_chunk_total.get(batch_id) else {
                    self.mark_fair_slashed(expected_leader, slot, order_start, now);
                    return false;
                };

                let mut sigs: Vec<[u8; 64]> = Vec::new();
                for chunk_index in 0..chunk_total {
                    let key = (*batch_id, chunk_index);
                    let Some(chunk) = chunk_sigs.get(&key) else {
                        debug!(
                            "solanacdn: missing on-chain fair commit chunk for acked batch; leader={} slot={} batch_id={} order_start={} chunk_index={} chunk_total={}",
                            expected_leader.to_base58(),
                            slot,
                            batch_id,
                            order_start,
                            chunk_index,
                            chunk_total
                        );
                        self.mark_fair_slashed(expected_leader, slot, order_start, now);
                        return false;
                    };
                    sigs.extend_from_slice(chunk);
                }

                let tx_count: u32 = sigs.len().try_into().unwrap_or(0);
                let tx_merkle_root = fair_merkle_root(sigs.as_slice());
                if tx_count != ack.tx_count || tx_merkle_root != ack.tx_merkle_root {
                    debug!(
                        "solanacdn: on-chain fair commit mismatch for acked batch; leader={} slot={} batch_id={} order_start={} ack_tx_count={} ack_merkle_root={:02x?} commit_tx_count={} commit_merkle_root={:02x?}",
                        expected_leader.to_base58(),
                        slot,
                        batch_id,
                        order_start,
                        ack.tx_count,
                        ack.tx_merkle_root,
                        tx_count,
                        tx_merkle_root
                    );
                    self.mark_fair_slashed(expected_leader, slot, order_start, now);
                    return false;
                }
            }
        }

        if let Some(rejected) = reject_batches.as_ref().filter(|m| !m.is_empty()) {
            // If we observe a leader-signed reject for this leader+slot, the ledger must not
            // contain an on-chain fair commit for that rejected batch.
            for (batch_id, reject) in rejected.iter() {
                if batch_order_start.contains_key(batch_id) {
                    debug!(
                        "solanacdn: on-chain fair commit present for rejected batch; leader={} slot={} batch_id={} order_start={} reason={:?} leader_time_ms={}",
                        expected_leader.to_base58(),
                        slot,
                        batch_id,
                        reject.order_start,
                        reject.reason,
                        reject.leader_time_ms
                    );
                    self.mark_fair_slashed(expected_leader, slot, reject.order_start, now);
                    return false;
                }
            }
        }

        if self.cfg.tx_fair_slashing_nonresponse {
            if let Some(witnessed) = witness_batches.as_ref().filter(|m| !m.is_empty()) {
                // If we have POP witnesses for this leader+slot, each witnessed batch must either:
                // - appear as a matching on-chain fair commit for this slot, OR
                // - be explicitly rejected via a leader-signed reject message.
                for (batch_id, witness) in witnessed.iter() {
                    if let Some(rejected) = reject_batches.as_ref() {
                        if let Some(reject) = rejected.get(batch_id) {
                            if reject.origin_pop_id_hash == witness.origin_pop_id_hash
                                && reject.flow_id == witness.flow_id
                                && reject.order_start == witness.order_start
                            {
                                continue;
                            }
                            self.mark_fair_slashed(expected_leader, slot, witness.order_start, now);
                            return false;
                        }
                    }

                    let Some(&order_start) = batch_order_start.get(batch_id) else {
                        debug!(
                            "solanacdn: missing on-chain fair commit for witnessed batch; leader={} slot={} batch_id={} witness_order_start={} witness_pop_time_ms={}",
                            expected_leader.to_base58(),
                            slot,
                            batch_id,
                            witness.order_start,
                            witness.pop_time_ms
                        );
                        self.mark_fair_slashed(expected_leader, slot, witness.order_start, now);
                        return false;
                    };

                    let Some(&chunk_total) = batch_chunk_total.get(batch_id) else {
                        self.mark_fair_slashed(expected_leader, slot, order_start, now);
                        return false;
                    };

                    let mut sigs: Vec<[u8; 64]> = Vec::new();
                    for chunk_index in 0..chunk_total {
                        let key = (*batch_id, chunk_index);
                        let Some(chunk) = chunk_sigs.get(&key) else {
                            debug!(
                                "solanacdn: missing on-chain fair commit chunk for witnessed batch; leader={} slot={} batch_id={} order_start={} chunk_index={} chunk_total={}",
                                expected_leader.to_base58(),
                                slot,
                                batch_id,
                                order_start,
                                chunk_index,
                                chunk_total
                            );
                            self.mark_fair_slashed(expected_leader, slot, order_start, now);
                            return false;
                        };
                        sigs.extend_from_slice(chunk);
                    }

                    let tx_count: u32 = sigs.len().try_into().unwrap_or(0);
                    let tx_merkle_root = fair_merkle_root(sigs.as_slice());
                    if tx_count != witness.tx_count || tx_merkle_root != witness.tx_merkle_root {
                        debug!(
                            "solanacdn: on-chain fair commit mismatch for witnessed batch; leader={} slot={} batch_id={} order_start={} witness_tx_count={} witness_merkle_root={:02x?} commit_tx_count={} commit_merkle_root={:02x?}",
                            expected_leader.to_base58(),
                            slot,
                            batch_id,
                            order_start,
                            witness.tx_count,
                            witness.tx_merkle_root,
                            tx_count,
                            tx_merkle_root
                        );
                        self.mark_fair_slashed(expected_leader, slot, order_start, now);
                        return false;
                    }
                }
            }
        }

        // When we have external receipt evidence for this leader+slot (leader ACKs and/or POP
        // witnesses), treat the slot audit as strict to punish insertion ahead of the committed
        // fair prefix and committed drops.
        let strict = self.cfg.tx_fair_slashing_strict
            || ack_batches.is_some()
            || (self.cfg.tx_fair_slashing_nonresponse && witness_batches.is_some());

        if batch_order_start.is_empty() {
            return true;
        }

        let mut ordered_batches: Vec<(u64, u128)> = batch_order_start
            .into_iter()
            .map(|(batch_id, order_start)| (order_start, batch_id))
            .collect();
        ordered_batches.sort_by_key(|(order_start, batch_id)| (*order_start, *batch_id));

        let mut expected: Vec<[u8; 64]> = Vec::new();
        let mut incomplete = false;
        'batches: for (_order_start, batch_id) in ordered_batches {
            let Some(chunk_total) = batch_chunk_total.get(&batch_id).copied() else {
                continue;
            };
            for chunk_index in 0..chunk_total {
                let key = (batch_id, chunk_index);
                let Some(sigs) = chunk_sigs.get(&key) else {
                    // NOTE: Missing commit chunks are treated as inconclusive for now. This may
                    // change back to slashing once commit delivery is more reliable.
                    incomplete = true;
                    break 'batches;
                };
                expected.extend_from_slice(sigs);
            }
        }
        if incomplete {
            if strict {
                self.mark_fair_slashed(expected_leader, slot, 0, now_ms());
                return false;
            } else {
                self.fair_ledger_audit_inconclusive
                    .fetch_add(1, Ordering::Relaxed);
                return true;
            }
        }

        if expected.is_empty() {
            return true;
        }

        let mut expected_pos: HashMap<[u8; 64], usize> = HashMap::with_capacity(expected.len());
        for (idx, sig) in expected.iter().enumerate() {
            expected_pos.entry(*sig).or_insert(idx);
        }

        if strict {
            let mut cursor: usize = 0;
            for tx in slot_txs.iter() {
                if tx.is_exempt {
                    continue;
                }
                if cursor >= expected.len() {
                    break;
                }

                let sig = tx.sig0;
                if sig == expected[cursor] {
                    cursor = cursor.saturating_add(1);
                    continue;
                }

                if let Some(&pos) = expected_pos.get(&sig) {
                    if pos > cursor {
                        self.mark_fair_slashed(expected_leader, slot, cursor as u64, now_ms());
                        return false;
                    }
                    continue;
                }

                // Non-exempt transaction inserted ahead of the committed fair prefix.
                self.mark_fair_slashed(expected_leader, slot, cursor as u64, now_ms());
                return false;
            }

            // Strict mode requires that the entire committed fair list lands in the target slot.
            if cursor < expected.len() {
                self.mark_fair_slashed(expected_leader, slot, cursor as u64, now_ms());
                return false;
            }
        }

        let mut cursor: usize = 0;
        for tx in slot_txs.iter() {
            let sig = tx.sig0;
            let Some(&pos) = expected_pos.get(&sig) else {
                continue;
            };
            if pos == cursor {
                cursor = cursor.saturating_add(1);
            } else if pos > cursor {
                self.mark_fair_slashed(expected_leader, slot, cursor as u64, now_ms());
                return false;
            }
            if cursor >= expected.len() {
                break;
            }
        }

        if self.cfg.tx_fair_slashing_fence {
            let Some(bank) = bank else {
                debug!("solanacdn: skipping fair account-fence audit; no bank provided");
                return true;
            };
            let fence_reads = self.cfg.tx_fair_slashing_fence_reads;

            let exempt: HashSet<[u8; 64]> = slot_txs
                .iter()
                .filter(|tx| tx.is_exempt)
                .map(|tx| tx.sig0)
                .collect();

            let mut fenced_accounts: HashMap<Pubkey, usize> = HashMap::new();
            for entry in entries {
                for tx in entry.transactions.iter() {
                    let Some(sig0): Option<[u8; 64]> = tx
                        .signatures
                        .get(0)
                        .and_then(|s| s.as_ref().try_into().ok())
                    else {
                        continue;
                    };
                    let Some(&pos) = expected_pos.get(&sig0) else {
                        continue;
                    };
                    let Ok(sanitized) =
                        bank.verify_transaction(tx.clone(), TransactionVerificationMode::HashOnly)
                    else {
                        continue;
                    };
                    for (idx, key) in sanitized.account_keys().iter().enumerate() {
                        if sanitized.is_signer(idx) {
                            continue;
                        }
                        if !fence_reads && !sanitized.is_writable(idx) {
                            continue;
                        }
                        fenced_accounts
                            .entry(*key)
                            .and_modify(|v| *v = (*v).min(pos))
                            .or_insert(pos);
                    }
                }
            }

            if !fenced_accounts.is_empty() {
                for entry in entries {
                    for tx in entry.transactions.iter() {
                        let Some(sig0): Option<[u8; 64]> = tx
                            .signatures
                            .get(0)
                            .and_then(|s| s.as_ref().try_into().ok())
                        else {
                            continue;
                        };

                        if exempt.contains(&sig0) || expected_pos.contains_key(&sig0) {
                            continue;
                        }

                        let Ok(sanitized) = bank
                            .verify_transaction(tx.clone(), TransactionVerificationMode::HashOnly)
                        else {
                            continue;
                        };

                        for (idx, key) in sanitized.account_keys().iter().enumerate() {
                            if !sanitized.is_writable(idx) || sanitized.is_signer(idx) {
                                continue;
                            }
                            let Some(&pos) = fenced_accounts.get(key) else {
                                continue;
                            };
                            debug!(
                                "solanacdn: fair account-fence violation; leader={} slot={} offending_tx_sig={:02x?} conflicts_with_pos={}",
                                expected_leader.to_base58(),
                                slot,
                                sig0,
                                pos
                            );
                            self.mark_fair_slashed(expected_leader, slot, pos as u64, now_ms());
                            return false;
                        }
                    }
                }
            }
        }

        true
    }

    pub fn tvu_shred_ingest_mode(&self) -> TvuShredIngestMode {
        self.cfg.tvu_shred_ingest_mode
    }

    pub fn race_enabled(&self) -> bool {
        self.cfg.race_enabled && self.is_connected()
    }

    pub fn note_race_observation(&self, shred_id: LedgerShredId, src_ip: IpAddr) {
        if !self.cfg.race_enabled {
            return;
        }
        if !self.is_connected() {
            return;
        }
        let sample_bits = self.cfg.race_sample_bits.min(31);
        if sample_bits != 0 {
            let mut hash = FNV1A_128_OFFSET_BASIS;
            hash = fnv1a_128_update(hash, &shred_id.slot().to_le_bytes());
            hash = fnv1a_128_update(hash, &u8::from(shred_id.shred_type()).to_le_bytes());
            hash = fnv1a_128_update(hash, &shred_id.index().to_le_bytes());
            let h64 = (hash as u64) ^ ((hash >> 64) as u64);
            let mask = (1u64 << sample_bits).saturating_sub(1);
            if (h64 & mask) != 0 {
                return;
            }
        }

        let source = if self.should_ignore_src_ip(src_ip) {
            RaceSource::SolanaCdn
        } else {
            RaceSource::Gossip
        };

        let pop_endpoint = (source == RaceSource::SolanaCdn)
            .then(|| self.race_pop_endpoint_hint(src_ip))
            .flatten();
        let gossip_src_ip = (source == RaceSource::Gossip).then_some(src_ip);
        let mut tracker = match self.race_state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        tracker.observe(
            shred_id,
            source,
            now_ms(),
            self.cfg.race_window_ms,
            pop_endpoint,
            gossip_src_ip,
        );
    }

    pub fn note_race_observation_from_pop(
        &self,
        shred_id: LedgerShredId,
        pop_endpoint: SocketAddr,
    ) {
        if !self.cfg.race_enabled {
            return;
        }
        if !self.is_connected() {
            return;
        }
        let sample_bits = self.cfg.race_sample_bits.min(31);
        if sample_bits != 0 {
            let mut hash = FNV1A_128_OFFSET_BASIS;
            hash = fnv1a_128_update(hash, &shred_id.slot().to_le_bytes());
            hash = fnv1a_128_update(hash, &u8::from(shred_id.shred_type()).to_le_bytes());
            hash = fnv1a_128_update(hash, &shred_id.index().to_le_bytes());
            let h64 = (hash as u64) ^ ((hash >> 64) as u64);
            let mask = (1u64 << sample_bits).saturating_sub(1);
            if (h64 & mask) != 0 {
                return;
            }
        }

        let mut tracker = match self.race_state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        tracker.observe(
            shred_id,
            RaceSource::SolanaCdn,
            now_ms(),
            self.cfg.race_window_ms,
            Some(pop_endpoint),
            None,
        );
    }

    fn race_pop_endpoint_hint(&self, src_ip: IpAddr) -> Option<SocketAddr> {
        if src_ip.is_loopback() {
            if let Some(publisher) = self.publisher_endpoint.load_full() {
                if let Ok(ep) = publisher.parse::<SocketAddr>() {
                    return Some(ep);
                }
            }
            return None;
        }

        // Direct injection uses the POP IP as the packet source and may not include the QUIC port,
        // so best-effort map it back to a connected endpoint.
        for ep in self.connected_pops.iter() {
            if ep.ip() == src_ip {
                return Some(*ep);
            }
        }
        None
    }

    /// Returns true if this shred should be ingested by the validator pipeline.
    /// In `solanacdn-only` mode, this gates turbine shreds to SolanaCDN sources while connected,
    /// and falls back to the normal P2P path when disconnected. In `solanacdn-preferred` mode,
    /// it falls back to P2P when SolanaCDN looks stalled.
    pub fn should_ingest_tvu_shred(&self, src_ip: IpAddr) -> bool {
        match self.cfg.tvu_shred_ingest_mode {
            TvuShredIngestMode::All => return true,
            TvuShredIngestMode::SolanaCdnOnly => {}
            TvuShredIngestMode::SolanaCdnPreferred => {}
        }
        if !self.is_connected() {
            return true;
        }
        if self.cfg.tvu_shred_ingest_mode == TvuShredIngestMode::SolanaCdnPreferred
            && !self.is_solanacdn_shred_accepted_fresh_at(now_ms())
        {
            return true;
        }
        self.should_ignore_src_ip(src_ip)
    }

    #[allow(dead_code)]
    fn is_solanacdn_shred_rx_fresh_at(&self, now_ms: u64) -> bool {
        let last = self.last_solanacdn_shred_rx_ms.load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        now_ms.saturating_sub(last) <= self.cfg.tvu_shred_hybrid_stale_ms.max(250)
    }

    fn is_solanacdn_shred_accepted_fresh_at(&self, now_ms: u64) -> bool {
        let last = self
            .last_solanacdn_shred_accepted_ms
            .load(Ordering::Relaxed);
        if last == 0 {
            return false;
        }
        now_ms.saturating_sub(last) <= self.cfg.tvu_shred_hybrid_stale_ms.max(250)
    }

    pub fn udp_mode(&self) -> DataPlaneMode {
        self.cfg.udp_mode
    }

    fn is_pop_egress_ip_fresh(&self, ip: IpAddr, now: u64) -> bool {
        if let Some(entry) = self.pop_egress_ips.get(&ip) {
            let expires_at = *entry;
            drop(entry);
            if expires_at >= now {
                return true;
            }
            self.pop_egress_ips.remove(&ip);
        }
        false
    }

    pub fn should_ignore_src_ip(&self, ip: IpAddr) -> bool {
        if ip.is_loopback() {
            return true;
        }
        let now = now_ms();
        let mut egress_ok = false;
        if let Some(entry) = self.pop_egress_ips.get(&ip) {
            let expires_at = *entry;
            drop(entry);
            if expires_at >= now {
                egress_ok = true;
            } else {
                self.pop_egress_ips.remove(&ip);
            }
        }
        if self.is_connected() {
            let mut has_connected = false;
            for ep in self.connected_pops.iter() {
                has_connected = true;
                if ep.ip() == ip {
                    return true;
                }
            }
            if has_connected {
                return egress_ok;
            }
            // Fallback when connected_pops is not yet populated (startup/tests).
            if self.pop_endpoint_ips.contains(&ip) {
                return true;
            }
            return egress_ok;
        }
        if self.pop_endpoint_ips.contains(&ip) {
            return true;
        }
        egress_ok
    }

    /// Replace the POP endpoint allowlist with the provided set (used for discovery refreshes).
    pub fn note_pop_endpoints(&self, endpoints: &[SocketAddr]) {
        self.pop_endpoint_ips.clear();
        for ep in endpoints {
            self.pop_endpoint_ips.insert(ep.ip());
        }
    }

    pub fn note_pop_endpoint(&self, endpoint: SocketAddr) {
        self.pop_endpoint_ips.insert(endpoint.ip());
    }

    pub fn note_pop_egress_ip(&self, ip: IpAddr) {
        if ip.is_loopback() {
            return;
        }

        let now = now_ms();
        let expires_at = now.saturating_add(POP_EGRESS_IP_TTL_MS);

        if let Some(mut entry) = self.pop_egress_ips.get_mut(&ip) {
            *entry = expires_at;
            return;
        }

        // Bound memory: best-effort prune of expired entries when full. If still full, skip adding
        // new keys (but always allow TTL refreshes for existing keys above).
        if self.pop_egress_ips.len() >= POP_EGRESS_IP_MAX_ENTRIES {
            let mut expired: Vec<IpAddr> = Vec::new();
            for entry in self.pop_egress_ips.iter().take(1024) {
                if *entry.value() < now {
                    expired.push(*entry.key());
                }
            }
            for key in expired {
                self.pop_egress_ips.remove(&key);
            }
        }

        if self.pop_egress_ips.len() >= POP_EGRESS_IP_MAX_ENTRIES {
            return;
        }

        self.pop_egress_ips.insert(ip, expires_at);
    }

    fn heartbeat_stats(&self) -> HeartbeatStats {
        HeartbeatStats {
            published_shred_batches: self.published_shred_batches.load(Ordering::Relaxed),
            pushed_shred_batches: self.pushed_shred_batches.load(Ordering::Relaxed),
            tunneled_vote_packets: self.tunneled_vote_packets.load(Ordering::Relaxed),
            rx_vote_packets: self.rx_vote_packets.load(Ordering::Relaxed),
        }
    }

    fn set_publisher_uplink(
        &self,
        publisher: Option<SocketAddr>,
        uplink: Option<Arc<SessionUplink>>,
    ) {
        self.publisher_uplink.store(uplink);
        self.connected.store(
            self.publisher_uplink.load_full().is_some(),
            Ordering::Relaxed,
        );

        let publisher_str = publisher.map(|p| p.to_string());
        let current = self.publisher_endpoint.load_full().map(|p| (*p).clone());
        if current != publisher_str {
            self.publisher_switches_total
                .fetch_add(1, Ordering::Relaxed);
            self.publisher_endpoint.store(publisher_str.map(Arc::new));
        }
    }

    fn note_solanacdn_shreds_rx(&self, bytes: usize, shreds: u64, slot: Option<u64>) {
        if bytes == 0 || shreds == 0 {
            return;
        }
        self.rx_shred_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.rx_shred_payloads.fetch_add(shreds, Ordering::Relaxed);
        self.last_solanacdn_shred_rx_ms
            .store(now_ms(), Ordering::Relaxed);

        if let Some(slot) = slot {
            self.last_solanacdn_shred_slot_valid
                .store(true, Ordering::Relaxed);
            let mut current = self.last_solanacdn_shred_slot.load(Ordering::Relaxed);
            while slot > current {
                match self.last_solanacdn_shred_slot.compare_exchange(
                    current,
                    slot,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(next) => current = next,
                }
            }
        }
    }

    pub(crate) fn note_solanacdn_accepted_shred_with_slot(&self, slot: Option<u64>) {
        self.last_solanacdn_shred_accepted_ms
            .store(now_ms(), Ordering::Relaxed);

        if let Some(slot) = slot {
            self.last_solanacdn_shred_accepted_slot_valid
                .store(true, Ordering::Relaxed);
            let mut current = self
                .last_solanacdn_shred_accepted_slot
                .load(Ordering::Relaxed);
            while slot > current {
                match self.last_solanacdn_shred_accepted_slot.compare_exchange(
                    current,
                    slot,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(next) => current = next,
                }
            }
        }
    }

    pub fn status_snapshot(&self) -> SolanaCdnStatus {
        let now = now_ms();
        let publisher = self.publisher_endpoint.load_full().map(|p| (*p).clone());
        let publisher_switches_total = self.publisher_switches_total.load(Ordering::Relaxed);
        let tx_fair_ordering = self.cfg.tx_fair_ordering;
        let tx_fair_require_target_slot = self.cfg.tx_fair_require_target_slot;
        let tx_fair_slashing = self.cfg.tx_fair_slashing;
        let tx_fair_slashing_strict = self.cfg.tx_fair_slashing_strict;
        let tx_fair_slashing_witness = self.cfg.tx_fair_slashing_witness;
        let tx_fair_slashing_nonresponse = self.cfg.tx_fair_slashing_nonresponse;
        let tx_fair_slashing_publish_witness_memos =
            self.cfg.tx_fair_slashing_publish_witness_memos;
        let tx_fair_slashing_fence = self.cfg.tx_fair_slashing_fence;
        let tx_fair_slashing_enforce = self.tx_fair_slashing_enforce_enabled();
        let tx_fair_slashing_enforce_configured = self.cfg.tx_fair_slashing_enforce;
        let tx_fair_slashing_enforce_override = self.tx_fair_slashing_enforce_override();
        let tx_fair_batch_received_total = self.tx_fair_batch_received.load(Ordering::Relaxed);
        let tx_fair_batch_injected_total = self.tx_fair_batch_injected.load(Ordering::Relaxed);
        let tx_fair_batch_inject_failed_total =
            self.tx_fair_batch_inject_failed.load(Ordering::Relaxed);
        let tx_deduped_packets_total = self.tx_deduped_packets.load(Ordering::Relaxed);
        let tx_relay_dropped_fair_mode_total =
            self.tx_relay_dropped_fair_mode.load(Ordering::Relaxed);
        let fair_priority_lookups_total = FAIR_PRIORITY_LOOKUPS_TOTAL.load(Ordering::Relaxed);
        let fair_priority_hits_total = FAIR_PRIORITY_HITS_TOTAL.load(Ordering::Relaxed);
        let fair_commits_rx_total = self.fair_commits_rx.load(Ordering::Relaxed);
        let fair_commits_invalid_total = self.fair_commits_invalid.load(Ordering::Relaxed);
        let fair_equivocations_total = self.fair_equivocations.load(Ordering::Relaxed);
        let fair_votes_withheld_total = self.fair_votes_withheld.load(Ordering::Relaxed);
        let fair_ledger_audit_checked_total =
            self.fair_ledger_audit_checked.load(Ordering::Relaxed);
        let fair_ledger_audit_failed_total = self.fair_ledger_audit_failed.load(Ordering::Relaxed);
        let fair_ledger_audit_inconclusive_total =
            self.fair_ledger_audit_inconclusive.load(Ordering::Relaxed);
        let fair_ledger_audit_get_slot_entries_failed_total = self
            .fair_ledger_audit_get_slot_entries_failed
            .load(Ordering::Relaxed);
        let fair_ledger_commits_seen_total = self.fair_ledger_commits_seen.load(Ordering::Relaxed);
        let fair_ledger_commits_invalid_total =
            self.fair_ledger_commits_invalid.load(Ordering::Relaxed);
        let fair_order_witnesses_len = self.fair_order_witnesses.len() as u64;
        let fair_slashed_leaders_len = self.fair_slashed_leaders.len() as u64;
        let fair_ledger_audited_slots_len = self.fair_ledger_audited_slots.len() as u64;

        let mut pops: Vec<String> = self.connected_pops.iter().map(|e| e.to_string()).collect();
        pops.sort();

        let rx_shred_bytes_total = self.rx_shred_bytes.load(Ordering::Relaxed);
        let rx_shred_payloads_total = self.rx_shred_payloads.load(Ordering::Relaxed);
        let dropped_shred_batches_oversized_total =
            self.dropped_shred_batches_oversized.load(Ordering::Relaxed);
        let tunneled_vote_packets_total = self.tunneled_vote_packets.load(Ordering::Relaxed);
        let rx_vote_packets_total = self.rx_vote_packets.load(Ordering::Relaxed);
        let dropped_vote_datagrams_total = self.dropped_vote_datagrams.load(Ordering::Relaxed);
        let dropped_vote_datagrams_oversized_payload_total = self
            .dropped_vote_datagrams_oversized_payload
            .load(Ordering::Relaxed);
        let dropped_vote_datagrams_invalid_payload_total = self
            .dropped_vote_datagrams_invalid_payload
            .load(Ordering::Relaxed);
        let dropped_vote_datagrams_unexpected_dst_total = self
            .dropped_vote_datagrams_unexpected_dst
            .load(Ordering::Relaxed);
        let dropped_quic_shreds_unexpected_msg_total = self
            .dropped_quic_shreds_unexpected_msg
            .load(Ordering::Relaxed);
        let dropped_quic_votes_unexpected_msg_total = self
            .dropped_quic_votes_unexpected_msg
            .load(Ordering::Relaxed);
        let dropped_udp_shreds_unexpected_peer_total = self
            .dropped_udp_shreds_unexpected_peer
            .load(Ordering::Relaxed);
        let dropped_udp_shreds_unexpected_msg_total = self
            .dropped_udp_shreds_unexpected_msg
            .load(Ordering::Relaxed);
        let dropped_udp_votes_unexpected_peer_total = self
            .dropped_udp_votes_unexpected_peer
            .load(Ordering::Relaxed);
        let dropped_udp_votes_unexpected_msg_total = self
            .dropped_udp_votes_unexpected_msg
            .load(Ordering::Relaxed);
        let vote_tunnel_allowed_dsts_len = self.vote_tunnel_allowed_dsts.len() as u64;

        let (rx_shred_payloads_per_sec, tunneled_vote_packets_per_sec) = {
            let mut state = match self.rate_state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(last) = state.last {
                let dt_ms = now.saturating_sub(last.at_ms);
                if dt_ms >= 250 {
                    let dt = (dt_ms as f64) / 1000.0;
                    state.rx_shred_payloads_per_sec =
                        (rx_shred_payloads_total.saturating_sub(last.rx_shred_payloads) as f64)
                            / dt;
                    state.tunneled_vote_packets_per_sec = (tunneled_vote_packets_total
                        .saturating_sub(last.tunneled_vote_packets)
                        as f64)
                        / dt;
                    state.last = Some(RateSample {
                        at_ms: now,
                        rx_shred_payloads: rx_shred_payloads_total,
                        tunneled_vote_packets: tunneled_vote_packets_total,
                    });
                }
            } else {
                state.last = Some(RateSample {
                    at_ms: now,
                    rx_shred_payloads: rx_shred_payloads_total,
                    tunneled_vote_packets: tunneled_vote_packets_total,
                });
                state.rx_shred_payloads_per_sec = 0.0;
                state.tunneled_vote_packets_per_sec = 0.0;
            }
            (
                state.rx_shred_payloads_per_sec,
                state.tunneled_vote_packets_per_sec,
            )
        };

        let last_shred_timestamp_ms = {
            let v = self.last_solanacdn_shred_rx_ms.load(Ordering::Relaxed);
            (v != 0).then_some(v)
        };
        let last_shred_age_ms = last_shred_timestamp_ms.map(|v| now.saturating_sub(v));

        let last_shred_slot = self
            .last_solanacdn_shred_slot_valid
            .load(Ordering::Relaxed)
            .then_some(self.last_solanacdn_shred_slot.load(Ordering::Relaxed));

        let last_accepted_shred_timestamp_ms = {
            let v = self
                .last_solanacdn_shred_accepted_ms
                .load(Ordering::Relaxed);
            (v != 0).then_some(v)
        };
        let last_accepted_shred_age_ms =
            last_accepted_shred_timestamp_ms.map(|v| now.saturating_sub(v));

        let last_accepted_shred_slot = self
            .last_solanacdn_shred_accepted_slot_valid
            .load(Ordering::Relaxed)
            .then_some(
                self.last_solanacdn_shred_accepted_slot
                    .load(Ordering::Relaxed),
            );

        let tvu_shred_stale_for_ms = last_accepted_shred_age_ms;
        let tvu_shred_stale = match self.cfg.tvu_shred_ingest_mode {
            TvuShredIngestMode::SolanaCdnPreferred => {
                Some(!self.is_solanacdn_shred_accepted_fresh_at(now))
            }
            _ => None,
        };

        let race = {
            let tracker = match self.race_state.lock() {
                Ok(g) => g,
                Err(poisoned) => poisoned.into_inner(),
            };
            tracker.snapshot(&self.cfg)
        };

        SolanaCdnStatus {
            connected: self.is_connected(),
            publisher,
            connected_pops: pops,
            publisher_switches_total,
            tx_fair_ordering,
            tx_fair_require_target_slot,
            tx_fair_slashing,
            tx_fair_slashing_strict,
            tx_fair_slashing_witness,
            tx_fair_slashing_nonresponse,
            tx_fair_slashing_publish_witness_memos,
            tx_fair_slashing_fence,
            tx_fair_slashing_enforce,
            tx_fair_slashing_enforce_configured,
            tx_fair_slashing_enforce_override,
            tx_fair_batch_received_total,
            tx_fair_batch_injected_total,
            tx_fair_batch_inject_failed_total,
            tx_deduped_packets_total,
            tx_relay_dropped_fair_mode_total,
            fair_priority_lookups_total,
            fair_priority_hits_total,
            fair_commits_rx_total,
            fair_commits_invalid_total,
            fair_equivocations_total,
            fair_votes_withheld_total,
            fair_ledger_audit_checked_total,
            fair_ledger_audit_failed_total,
            fair_ledger_audit_inconclusive_total,
            fair_ledger_audit_get_slot_entries_failed_total,
            fair_ledger_commits_seen_total,
            fair_ledger_commits_invalid_total,
            fair_order_witnesses_len,
            fair_slashed_leaders_len,
            fair_ledger_audited_slots_len,
            rx_shred_bytes_total,
            rx_shred_payloads_total,
            dropped_shred_batches_oversized_total,
            rx_shred_payloads_per_sec,
            tunneled_vote_packets_total,
            tunneled_vote_packets_per_sec,
            rx_vote_packets_total,
            dropped_vote_datagrams_total,
            dropped_vote_datagrams_oversized_payload_total,
            dropped_vote_datagrams_invalid_payload_total,
            dropped_vote_datagrams_unexpected_dst_total,
            dropped_quic_shreds_unexpected_msg_total,
            dropped_quic_votes_unexpected_msg_total,
            dropped_udp_shreds_unexpected_peer_total,
            dropped_udp_shreds_unexpected_msg_total,
            dropped_udp_votes_unexpected_peer_total,
            dropped_udp_votes_unexpected_msg_total,
            vote_tunnel_allowed_dsts_len,
            last_shred_slot,
            last_shred_timestamp_ms,
            last_shred_age_ms,
            last_accepted_shred_slot,
            last_accepted_shred_timestamp_ms,
            last_accepted_shred_age_ms,
            tvu_shred_ingest_mode: self.cfg.tvu_shred_ingest_mode,
            tvu_shred_stale,
            tvu_shred_stale_for_ms,
            race_enabled: race.enabled,
            race_sample_bits: race.sample_bits,
            race_window_ms: race.window_ms,
            race_inflight: race.inflight as u64,
            race_pairs_total: race.pairs_total,
            race_wins_solanacdn_total: race.wins_solanacdn_total,
            race_wins_gossip_total: race.wins_gossip_total,
            race_ties_total: race.ties_total,
            race_last_winner: race.last_winner.map(|w| w.as_str().to_string()),
            race_last_lead_ms: race.last_lead_ms,
            race_last_shred_slot: race.last_shred_slot,
        }
    }

    pub fn try_publish_tvu_shred(&self, src_ip: IpAddr, payload: Bytes, discarded: bool) {
        if !self.cfg.publish_shreds {
            return;
        }
        if discarded && !self.cfg.publish_discarded_shreds {
            return;
        }
        if payload.is_empty() {
            return;
        }
        if self.should_ignore_src_ip(src_ip) {
            return;
        }
        self.try_publish_uplink_shred_payload(payload);
    }

    pub fn try_publish_local_tvu_shred(&self, payload: Bytes) {
        if !self.cfg.publish_shreds {
            return;
        }
        if payload.is_empty() {
            return;
        }
        self.try_publish_uplink_shred_payload(payload);
    }

    fn try_publish_uplink_shred_payload(&self, payload: Bytes) {
        let Some(uplink) = self.publisher_uplink.load_full() else {
            self.dropped_shred_payloads.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if uplink
            .tx
            .try_send(UplinkMsg::Shred(ShredPublish {
                kind: ShredKind::Tvu,
                payload,
            }))
            .is_err()
        {
            self.dropped_shred_payloads.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn try_publish_vote_datagram(&self, dst: SocketAddr, payload: Bytes) -> bool {
        if !self.cfg.vote_tunnel {
            return false;
        }
        if payload.is_empty() {
            return false;
        }
        self.note_vote_tunnel_allowed_dst(dst, now_ms());
        let Some(uplink) = self.publisher_uplink.load_full() else {
            return false;
        };
        match uplink
            .tx
            .try_send(UplinkMsg::Vote(VotePublish { dst, payload }))
        {
            Ok(()) => true,
            Err(_) => {
                self.dropped_vote_datagrams.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    fn note_connected_pop(&self, endpoint: SocketAddr) {
        self.connected_pops.insert(endpoint);
    }

    fn note_disconnected_pop(&self, endpoint: SocketAddr) {
        self.connected_pops.remove(&endpoint);
    }

    fn update_heartbeat_schema_version(&self, schema_version: u32) {
        self.heartbeat_schema_version
            .store(schema_version, Ordering::Relaxed);
    }

    fn ingest_runtime_snapshot(&self) -> serde_json::Value {
        let publisher = self.publisher_endpoint.load_full().map(|p| (*p).clone());
        let publisher_switches_total = self.publisher_switches_total.load(Ordering::Relaxed);

        let mut pops: Vec<String> = self.connected_pops.iter().map(|e| e.to_string()).collect();
        pops.sort();

        let tls_ca_cert_path = self
            .cfg
            .tls_ca_cert_path
            .as_ref()
            .map(|p| p.display().to_string());

        let last_shred_timestamp_ms = {
            let v = self.last_solanacdn_shred_rx_ms.load(Ordering::Relaxed);
            (v != 0).then_some(v)
        };
        let last_shred_slot = self
            .last_solanacdn_shred_slot_valid
            .load(Ordering::Relaxed)
            .then_some(self.last_solanacdn_shred_slot.load(Ordering::Relaxed));

        serde_json::json!({
            "publisher": publisher,
            "publisher_switches_total": publisher_switches_total,
            "pops": pops,
            "last_shred": {
                "slot": last_shred_slot,
                "received_at_ms": last_shred_timestamp_ms,
            },
            "tls": {
                "server_name": self.cfg.server_name.clone(),
                "ca_cert_path": tls_ca_cert_path,
                "insecure_skip_verify": self.cfg.tls_insecure_skip_verify,
            }
        })
    }

    fn ingest_counters_totals(&self) -> serde_json::Value {
        serde_json::json!({
            "published_shred_batches_total": self.published_shred_batches.load(Ordering::Relaxed) as i64,
            "pushed_shred_batches_total": self.pushed_shred_batches.load(Ordering::Relaxed) as i64,
            // Receiver-mode counters (Agave integrated): report shreds/bytes received from POPs.
            "rx_shred_batches_total": self.pushed_shred_batches.load(Ordering::Relaxed) as i64,
            "rx_shred_bytes_total": self.rx_shred_bytes.load(Ordering::Relaxed) as i64,
            "rx_shred_payloads_total": self.rx_shred_payloads.load(Ordering::Relaxed) as i64,
            "dropped_shred_batches_oversized_total": self.dropped_shred_batches_oversized.load(Ordering::Relaxed) as i64,
            "tunneled_vote_packets_total": self.tunneled_vote_packets.load(Ordering::Relaxed) as i64,
            "rx_vote_packets_total": self.rx_vote_packets.load(Ordering::Relaxed) as i64,
            "dropped_vote_datagrams_total": self.dropped_vote_datagrams.load(Ordering::Relaxed) as i64,
            "dropped_vote_datagrams_oversized_payload_total": self.dropped_vote_datagrams_oversized_payload.load(Ordering::Relaxed) as i64,
            "dropped_vote_datagrams_invalid_payload_total": self.dropped_vote_datagrams_invalid_payload.load(Ordering::Relaxed) as i64,
            "dropped_vote_datagrams_unexpected_dst_total": self.dropped_vote_datagrams_unexpected_dst.load(Ordering::Relaxed) as i64,
            "dropped_quic_shreds_unexpected_msg_total": self.dropped_quic_shreds_unexpected_msg.load(Ordering::Relaxed) as i64,
            "dropped_quic_votes_unexpected_msg_total": self.dropped_quic_votes_unexpected_msg.load(Ordering::Relaxed) as i64,
            "dropped_udp_shreds_unexpected_peer_total": self.dropped_udp_shreds_unexpected_peer.load(Ordering::Relaxed) as i64,
            "dropped_udp_shreds_unexpected_msg_total": self.dropped_udp_shreds_unexpected_msg.load(Ordering::Relaxed) as i64,
            "dropped_udp_votes_unexpected_peer_total": self.dropped_udp_votes_unexpected_peer.load(Ordering::Relaxed) as i64,
            "dropped_udp_votes_unexpected_msg_total": self.dropped_udp_votes_unexpected_msg.load(Ordering::Relaxed) as i64,
            "vote_tunnel_allowed_dsts_len": self.vote_tunnel_allowed_dsts.len() as i64,
            "rx_tx_packets_total": self.rx_tx_packets.load(Ordering::Relaxed) as i64,
            "tx_injected_packets_total": self.tx_injected_packets.load(Ordering::Relaxed) as i64,
            "tx_deduped_packets_total": self.tx_deduped_packets.load(Ordering::Relaxed) as i64,
            "tx_inject_failed_total": self.tx_inject_failed.load(Ordering::Relaxed) as i64,
            "tx_relay_dropped_fair_mode_total": self.tx_relay_dropped_fair_mode.load(Ordering::Relaxed) as i64,
            "fair_commits_rx_total": self.fair_commits_rx.load(Ordering::Relaxed) as i64,
            "fair_commits_invalid_total": self.fair_commits_invalid.load(Ordering::Relaxed) as i64,
            "fair_equivocations_total": self.fair_equivocations.load(Ordering::Relaxed) as i64,
            "fair_votes_withheld_total": self.fair_votes_withheld.load(Ordering::Relaxed) as i64,
            "fair_ledger_audit_checked_total": self.fair_ledger_audit_checked.load(Ordering::Relaxed) as i64,
            "fair_ledger_audit_failed_total": self.fair_ledger_audit_failed.load(Ordering::Relaxed) as i64,
            "fair_ledger_audit_inconclusive_total": self.fair_ledger_audit_inconclusive.load(Ordering::Relaxed) as i64,
            "fair_ledger_audit_get_slot_entries_failed_total": self.fair_ledger_audit_get_slot_entries_failed.load(Ordering::Relaxed) as i64,
            "fair_ledger_commits_seen_total": self.fair_ledger_commits_seen.load(Ordering::Relaxed) as i64,
            "fair_ledger_commits_invalid_total": self.fair_ledger_commits_invalid.load(Ordering::Relaxed) as i64,
            "uplink_dropped_shred_batches_total": self.dropped_shred_payloads.load(Ordering::Relaxed) as i64,
            "uplink_broadcast_lagged_total": self.uplink_broadcast_lagged.load(Ordering::Relaxed) as i64,
        })
    }

    fn pipe_ingest_race_snapshot_and_samples(
        &self,
        max_samples: usize,
    ) -> (RaceMetricsSnapshot, Vec<RaceSample>) {
        let tracker = match self.race_state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        (
            tracker.snapshot(&self.cfg),
            tracker.peek_samples(max_samples.min(2048)),
        )
    }

    fn pipe_ingest_consume_race_samples(&self, n: usize) {
        if n == 0 {
            return;
        }
        let mut tracker = match self.race_state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        tracker.consume_samples(n);
    }
}

pub fn global() -> Option<Arc<SolanaCdnHandle>> {
    GLOBAL.load_full()
}

#[cfg(test)]
pub(crate) fn set_global_for_tests(handle: Option<Arc<SolanaCdnHandle>>) {
    GLOBAL.store(handle);
}

#[cfg(test)]
pub(crate) fn new_handle_for_tests(cfg: SolanaCdnConfig) -> Arc<SolanaCdnHandle> {
    Arc::new(SolanaCdnHandle::new(cfg))
}

pub fn fair_slashing_is_slashed_leader(leader: &Pubkey, slot: u64) -> bool {
    let Some(handle) = global() else {
        return false;
    };
    handle.fair_slashing_is_slashed_leader(leader, slot)
}

pub fn fair_slashing_audit_slot(blockstore: &Blockstore, leader: &Pubkey, slot: u64) {
    let Some(handle) = global() else {
        return;
    };
    handle.audit_fair_ledger_commits_for_slot(blockstore, leader, slot);
}

pub fn fair_slashing_audit_slot_with_bank(
    blockstore: &Blockstore,
    bank: &Bank,
    leader: &Pubkey,
    slot: u64,
) {
    let Some(handle) = global() else {
        return;
    };
    handle.audit_fair_ledger_commits_for_slot_with_bank(blockstore, bank, leader, slot);
}

pub fn fair_slashing_note_vote_withheld(leader: &Pubkey, slot: u64) {
    let Some(handle) = global() else {
        return;
    };
    handle.note_fair_vote_withheld(leader, slot);
}

pub fn fair_note_recent_blockhash(blockhash: solana_hash::Hash) {
    let Some(handle) = global() else {
        return;
    };
    handle.note_fair_recent_blockhash(blockhash);
}

pub fn init(
    mut cfg: SolanaCdnConfig,
    identity_keypair: Arc<Keypair>,
    exit: Arc<AtomicBool>,
    vote_use_quic: bool,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
) {
    if vote_use_quic && cfg.vote_tunnel {
        warn!("solanacdn: vote tunneling requires UDP votes; disabling (vote_use_quic=true)");
        cfg.vote_tunnel = false;
    }

    if cfg.pop_endpoints.is_empty()
        && cfg.control_endpoint.is_none()
        && cfg
            .pipe_api_token
            .as_deref()
            .is_none_or(|s| s.trim().is_empty())
        && first_env(&["SOLANACDN_AGENT_API_TOKEN", "PIPE_API_KEY"]).is_none()
    {
        warn!(
            "solanacdn: no POP endpoints, control endpoint, or API token configured; skipping init"
        );
        return;
    }

    if !cfg.publish_shreds && !cfg.subscribe_shreds && !cfg.vote_tunnel {
        warn!("solanacdn: configured but all features are disabled; skipping init");
        return;
    }

    let handle = Arc::new(SolanaCdnHandle::new(cfg.clone()));
    GLOBAL.store(Some(handle.clone()));
    let _ = solana_turbine::solanacdn_hooks::set_leader_tvu_shred_publisher(Arc::new(
        AgaveLeaderTvuShredPublisher {
            handle: handle.clone(),
        },
    ));

    let thread_cfg = cfg.clone();
    std::thread::Builder::new()
        .name("solSolanaCdn".to_string())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .thread_name("solSolanaCdnRt")
                .build()
                .expect("solanacdn runtime");
            runtime.block_on(async move {
                if let Err(e) = run(
                    thread_cfg,
                    identity_keypair,
                    exit,
                    handle,
                    inject_tpu,
                    inject_tvu,
                    inject_gossip,
                    inject_vote,
                )
                .await
                {
                    warn!("solanacdn: client exited with error: {e}");
                }
            });
        })
        .expect("spawn solanacdn thread");
}

struct AgaveLeaderTvuShredPublisher {
    handle: Arc<SolanaCdnHandle>,
}

impl solana_turbine::solanacdn_hooks::LeaderTvuShredPublisher for AgaveLeaderTvuShredPublisher {
    fn publish_tvu_shred(&self, payload: Bytes) {
        self.handle.try_publish_local_tvu_shred(payload);
    }
}

#[derive(Debug, Error)]
pub enum SolanaCdnError {
    #[error("crypto error: {0}")]
    Crypto(#[from] solanacdn_protocol::crypto::CryptoError),
    #[error("frame error: {0}")]
    Frame(#[from] FrameError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("tls error: {0}")]
    Tls(String),
    #[error("tls server name invalid: {0}")]
    InvalidServerName(String),
    #[error("quic connect error: {0}")]
    QuicConnect(String),
    #[error("auth failed: {0}")]
    AuthFailed(String),
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(0)
}

fn now_ts() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .try_into()
        .unwrap_or(0)
}

fn env_trimmed(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn first_env(keys: &[&str]) -> Option<String> {
    for key in keys {
        if let Some(v) = env_trimmed(key) {
            return Some(v);
        }
    }
    None
}

fn normalize_base_url(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_string()
}

fn read_env_file_value(path: &str, key: &str) -> Option<String> {
    let contents = std::fs::read_to_string(path).ok()?;
    for raw_line in contents.lines() {
        let mut line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(rest) = line.strip_prefix("export ") {
            line = rest.trim();
        }
        let (k, v) = line.split_once('=')?;
        if k.trim() != key {
            continue;
        }
        let mut v = v.trim().to_string();
        if (v.starts_with('"') && v.ends_with('"')) || (v.starts_with('\'') && v.ends_with('\'')) {
            v = v[1..v.len().saturating_sub(1)].to_string();
        }
        v = v.trim().to_string();
        if v.is_empty() {
            continue;
        }
        return Some(v);
    }
    None
}

fn json_error_message(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return "empty response".to_string();
    }
    let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) else {
        return trimmed.to_string();
    };
    val.get("error")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| trimmed.to_string())
}

fn init_rustls() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        // rustls 0.23 requires selecting a process-wide CryptoProvider when multiple are enabled.
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn load_root_cert_store(path: &PathBuf) -> Result<RootCertStore, SolanaCdnError> {
    let mut certs = Vec::new();
    for item in CertificateDer::pem_file_iter(path).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("failed to read CA certs: {e}"),
        )
    })? {
        let cert = item.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("failed to parse PEM cert: {e}"),
            )
        })?;
        certs.push(cert);
    }

    let mut roots = RootCertStore::empty();
    let (_valid, invalid) = roots.add_parsable_certificates(certs);
    if invalid > 0 {
        warn!(
            "solanacdn: CA bundle {} contained {} invalid certs",
            path.display(),
            invalid
        );
    }
    if roots.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("no valid CA certs in {}", path.display()),
        )
        .into());
    }
    Ok(roots)
}

fn load_default_root_cert_store() -> Result<RootCertStore, SolanaCdnError> {
    let mut roots = RootCertStore::empty();

    let native = rustls_native_certs::load_native_certs();
    if !native.errors.is_empty() {
        warn!(
            "solanacdn: native cert store load had {} errors (showing first): {}",
            native.errors.len(),
            native
                .errors
                .first()
                .map(|e| e.to_string())
                .unwrap_or_else(|| "unknown".to_string())
        );
    }

    let (valid, invalid) = roots.add_parsable_certificates(native.certs);
    if invalid > 0 {
        warn!(
            "solanacdn: native cert store contained {} invalid certs",
            invalid
        );
    }

    if roots.roots.is_empty() {
        if valid == 0 {
            warn!("solanacdn: no native root certs loaded; falling back to webpki-roots");
        }
        roots
            .roots
            .extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    }

    if roots.roots.is_empty() {
        return Err(SolanaCdnError::Tls(
            "no trusted root certificates available for TLS verification".to_string(),
        ));
    }

    Ok(roots)
}

fn parse_hex_32(raw: &str) -> Option<[u8; 32]> {
    let raw = raw.trim();
    if raw.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    let bytes = raw.as_bytes();
    for i in 0..32 {
        let hi = bytes[2 * i];
        let lo = bytes[2 * i + 1];
        let hi = (hi as char).to_digit(16)? as u8;
        let lo = (lo as char).to_digit(16)? as u8;
        out[i] = (hi << 4) | lo;
    }
    Some(out)
}

fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    let digest = sha256_hasher::hash(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
}

#[derive(Debug, Deserialize)]
struct PipeApiSolanaCdnTlsResponse {
    ok: bool,
    pop_ca_cert_url: String,
    #[serde(default)]
    pop_ca_cert_sha256: Option<String>,
    tls_server_name: String,
}

async fn maybe_bootstrap_pop_tls_from_pipe_api(cfg: &mut SolanaCdnConfig) {
    if cfg.tls_insecure_skip_verify || cfg.tls_ca_cert_path.is_some() {
        return;
    }

    let default_paths = ["/etc/solanacdn/tls/ca.crt", "/opt/solanacdn/tls/ca.crt"];
    for p in default_paths {
        let path = PathBuf::from(p);
        if path.exists() {
            cfg.tls_ca_cert_path = Some(path);
            return;
        }
    }

    if !cfg.pipe_api_tls_bootstrap {
        return;
    }

    let base_url = normalize_base_url(&cfg.pipe_api_base_url);
    if base_url.trim().is_empty() || !base_url.starts_with("https://") {
        return;
    }

    let client = match build_pipe_api_http_client(&PipeApiClientConfig {
        base_url: base_url.clone(),
        api_key: String::new(),
        timeout: Duration::from_millis(cfg.pipe_api_timeout_ms),
        tls_insecure_skip_verify: cfg.pipe_api_tls_insecure_skip_verify,
        tls_ca_cert_path: cfg.pipe_api_tls_ca_cert_path.clone(),
    }) {
        Ok(c) => c,
        Err(e) => {
            warn!("solanacdn: failed to init HTTP client for POP TLS bootstrap: {e}");
            return;
        }
    };

    let tls_url = format!("{}/solanacdn/tls", base_url);
    let resp = match client.get(&tls_url).send().await {
        Ok(r) => r,
        Err(e) => {
            debug!("solanacdn: POP TLS bootstrap request failed: {e}");
            return;
        }
    };
    if !resp.status().is_success() {
        debug!(
            "solanacdn: POP TLS bootstrap returned non-success status: {}",
            resp.status()
        );
        return;
    }
    let body = match resp.text().await {
        Ok(b) => b,
        Err(e) => {
            debug!("solanacdn: POP TLS bootstrap failed to read body: {e}");
            return;
        }
    };
    let parsed: PipeApiSolanaCdnTlsResponse = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => {
            debug!("solanacdn: POP TLS bootstrap failed to parse JSON: {e}");
            return;
        }
    };
    if !parsed.ok {
        debug!("solanacdn: POP TLS bootstrap returned ok=false");
        return;
    }

    // If the operator didn't set an explicit SNI, prefer the control plane default.
    if cfg.server_name.trim().is_empty() {
        cfg.server_name = parsed.tls_server_name.clone();
    }

    let ca_url = parsed.pop_ca_cert_url.trim().to_string();
    if !ca_url.starts_with("https://") {
        debug!("solanacdn: POP TLS bootstrap CA URL is not https");
        return;
    }

    let ca_bytes = match client.get(&ca_url).send().await {
        Ok(r) => match r.bytes().await {
            Ok(b) => b.to_vec(),
            Err(e) => {
                debug!("solanacdn: failed to read POP CA cert bytes: {e}");
                return;
            }
        },
        Err(e) => {
            debug!("solanacdn: failed to download POP CA cert: {e}");
            return;
        }
    };

    if ca_bytes.is_empty() {
        debug!("solanacdn: downloaded POP CA cert is empty");
        return;
    }

    const MAX_POP_CA_CERT_BYTES: usize = 1024 * 1024;
    if ca_bytes.len() > MAX_POP_CA_CERT_BYTES {
        warn!(
            "solanacdn: downloaded POP CA cert too large ({} bytes)",
            ca_bytes.len()
        );
        return;
    }

    let expected_sha_hex = parsed
        .pop_ca_cert_sha256
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    if let Some(expected_sha_hex) = expected_sha_hex {
        if let Some(expected) = parse_hex_32(expected_sha_hex) {
            let got = sha256_bytes(&ca_bytes);
            if got != expected {
                warn!("solanacdn: downloaded POP CA cert sha256 mismatch; refusing to trust it");
                return;
            }
        } else {
            warn!("solanacdn: POP TLS bootstrap returned invalid pop_ca_cert_sha256; skipping sha verification");
        }
    }

    let prefix = expected_sha_hex
        .filter(|s| s.len() == 64 && s.chars().all(|c| c.is_ascii_hexdigit()))
        .map(|s| format!("solanacdn-pop-ca-{}-", &s[..16]))
        .unwrap_or_else(|| "solanacdn-pop-ca-".to_string());

    let mut file = match tempfile::Builder::new()
        .prefix(&prefix)
        .suffix(".crt")
        .tempfile_in(std::env::temp_dir())
    {
        Ok(f) => f,
        Err(e) => {
            warn!("solanacdn: failed to create temp file for POP CA cert: {e}");
            return;
        }
    };
    if let Err(e) = file.as_file_mut().write_all(&ca_bytes) {
        warn!("solanacdn: failed to write POP CA cert to disk: {e}");
        return;
    }
    let _ = file.as_file_mut().sync_all();
    let path = match file.into_temp_path().keep() {
        Ok(p) => p,
        Err(e) => {
            warn!("solanacdn: failed to persist POP CA cert to disk: {e}");
            return;
        }
    };

    cfg.tls_ca_cert_path = Some(path.clone());
    info!(
        "solanacdn: bootstrapped POP TLS CA cert from Pipe control plane ({})",
        path.display()
    );
}

#[derive(Clone, Debug)]
struct PipeApiClientConfig {
    base_url: String,
    api_key: String,
    timeout: Duration,
    tls_insecure_skip_verify: bool,
    tls_ca_cert_path: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct PipeApiVerifyRequest {
    schema_version: u32,
    agent_instance_id: String,
    validator_pubkey: String,
    version: String,
    capture_mode: String,
    iface: String,
    direct_shreds_from_pop: bool,
}

#[derive(Debug, Deserialize)]
struct PipeApiVerifyResponse {
    ok: bool,
    agent_id: String,
    run_id: String,
    run_token: String,
    #[serde(default)]
    heartbeat_schema_version: Option<u32>,
    ingest: PipeApiIngestConfig,
    /// POP endpoints for the agent to connect to (provided by control plane).
    #[serde(default)]
    pop_endpoints: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
struct PipeApiIngestConfig {
    url: String,
    interval_secs: u64,
    max_body_bytes: u64,
    max_events: u64,
}

#[derive(Clone, Debug)]
struct PipeApiVerifyResult {
    agent_id: String,
    run_id: String,
    run_token: String,
    heartbeat_schema_version: u32,
    ingest: PipeApiIngestConfig,
    pop_endpoints: Vec<SocketAddr>,
}

#[derive(Debug, Deserialize)]
struct PipeApiSessionTokenResponse {
    ok: bool,
    session_token: String,
    expires_in: i64,
}

fn build_pipe_api_http_client(
    cfg: &PipeApiClientConfig,
) -> Result<reqwest::Client, SolanaCdnError> {
    let mut builder = reqwest::Client::builder()
        .timeout(cfg.timeout.max(Duration::from_millis(250)))
        .user_agent(format!(
            "agave-validator-solanacdn/{}",
            env!("CARGO_PKG_VERSION")
        ));

    if cfg.tls_insecure_skip_verify {
        builder = builder
            .danger_accept_invalid_certs(true)
            .danger_accept_invalid_hostnames(true);
    } else if let Some(path) = cfg.tls_ca_cert_path.as_ref() {
        for item in CertificateDer::pem_file_iter(path).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("failed to read CA certs: {e}"),
            )
        })? {
            let cert = item.map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("failed to parse PEM cert: {e}"),
                )
            })?;
            let cert = reqwest::Certificate::from_der(cert.as_ref())
                .map_err(|e| SolanaCdnError::Tls(format!("invalid CA cert: {e}")))?;
            builder = builder.add_root_certificate(cert);
        }
    }

    builder
        .build()
        .map_err(|e| SolanaCdnError::Tls(e.to_string()))
}

async fn pipe_api_verify(
    client: &reqwest::Client,
    cfg: &PipeApiClientConfig,
    req: &PipeApiVerifyRequest,
) -> Result<PipeApiVerifyResult, SolanaCdnError> {
    let url = format!("{}/v1/solanacdn-agent/verify", cfg.base_url);
    let resp = client
        .post(&url)
        .header("x-api-key", &cfg.api_key)
        .json(req)
        .send()
        .await
        .map_err(|e| SolanaCdnError::Tls(e.to_string()))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if !status.is_success() {
        let msg = json_error_message(&body);
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API verify failed: {} ({})",
            status.as_u16(),
            msg
        )));
    }

    let parsed: PipeApiVerifyResponse =
        serde_json::from_str(&body).map_err(|e| SolanaCdnError::Tls(e.to_string()))?;
    if !parsed.ok {
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API verify returned unexpected response: {body}"
        )));
    }
    if parsed.run_token.trim().is_empty() {
        return Err(SolanaCdnError::AuthFailed(
            "Pipe API verify returned empty run_token".to_string(),
        ));
    }
    if parsed.agent_id.trim().is_empty() || parsed.run_id.trim().is_empty() {
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API verify returned invalid ids: agent_id='{}' run_id='{}'",
            parsed.agent_id, parsed.run_id
        )));
    }
    if parsed.ingest.url.trim().is_empty() || parsed.ingest.interval_secs == 0 {
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API verify returned invalid ingest config: url='{}' interval_secs={}",
            parsed.ingest.url, parsed.ingest.interval_secs
        )));
    }

    let mut pop_endpoints: Vec<SocketAddr> = parsed
        .pop_endpoints
        .iter()
        .filter_map(|s| match s.parse::<SocketAddr>() {
            Ok(addr) => Some(addr),
            Err(e) => {
                warn!("solanacdn: ignoring invalid pop_endpoint from Pipe API verify ({s}): {e}");
                None
            }
        })
        .collect();
    pop_endpoints.sort();
    pop_endpoints.dedup();

    Ok(PipeApiVerifyResult {
        agent_id: parsed.agent_id,
        run_id: parsed.run_id,
        run_token: parsed.run_token,
        heartbeat_schema_version: parsed.heartbeat_schema_version.unwrap_or(0),
        ingest: parsed.ingest,
        pop_endpoints,
    })
}

async fn pipe_api_pop_session_token(
    client: &reqwest::Client,
    base_url: &str,
    run_token: &str,
) -> Result<PipeApiSessionTokenResponse, SolanaCdnError> {
    let url = format!("{base_url}/v1/solanacdn-agent/session-token");
    let resp = client
        .post(&url)
        .bearer_auth(run_token)
        .send()
        .await
        .map_err(|e| SolanaCdnError::Tls(e.to_string()))?;
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();

    if !status.is_success() {
        let msg = json_error_message(&body);
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API pop session token failed: {} ({})",
            status.as_u16(),
            msg
        )));
    }

    let parsed: PipeApiSessionTokenResponse =
        serde_json::from_str(&body).map_err(|e| SolanaCdnError::Tls(e.to_string()))?;
    if !parsed.ok {
        return Err(SolanaCdnError::AuthFailed(format!(
            "Pipe API pop session token returned unexpected response: {body}"
        )));
    }
    if parsed.session_token.trim().is_empty() {
        return Err(SolanaCdnError::AuthFailed(
            "Pipe API pop session token returned empty session_token".to_string(),
        ));
    }

    Ok(parsed)
}

struct PipeApiRefresher {
    session_token_rx: watch::Receiver<Option<String>>,
    pop_endpoints_rx: watch::Receiver<Vec<SocketAddr>>,
    verify_rx: watch::Receiver<Option<PipeApiVerifyResult>>,
}

fn spawn_pipe_pop_session_token_refresher(
    cfg: PipeApiClientConfig,
    validator_pubkey_base58: String,
    direct_shreds_from_pop: bool,
) -> PipeApiRefresher {
    let (token_tx, token_rx) = watch::channel::<Option<String>>(None);
    let (pops_tx, pops_rx) = watch::channel::<Vec<SocketAddr>>(Vec::new());
    let (verify_tx, verify_rx) = watch::channel::<Option<PipeApiVerifyResult>>(None);
    tokio::spawn(async move {
        let client = match build_pipe_api_http_client(&cfg) {
            Ok(c) => c,
            Err(e) => {
                warn!("solanacdn: failed to init Pipe API client: {e}");
                return;
            }
        };

        let agent_instance_id = format!("agave-integrated-{}", validator_pubkey_base58);
        let verify_req = PipeApiVerifyRequest {
            schema_version: 1,
            agent_instance_id,
            validator_pubkey: validator_pubkey_base58.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            capture_mode: "agave".to_string(),
            iface: first_env(&["SOLANACDN_AGENT_IFACE", "SOLANACDN_IFACE"])
                .unwrap_or_else(|| "integrated".to_string()),
            direct_shreds_from_pop,
        };

        let mut current_expires_at: Option<std::time::Instant> = None;
        let mut backoff = Duration::from_secs(1);

        loop {
            let verify = match pipe_api_verify(&client, &cfg, &verify_req).await {
                Ok(v) => {
                    backoff = Duration::from_secs(1);
                    v
                }
                Err(e) => {
                    warn!("solanacdn: Pipe API verify failed: {e}");
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    continue;
                }
            };

            verify_tx.send_replace(Some(verify.clone()));

            if verify.pop_endpoints.is_empty() {
                debug!("solanacdn: Pipe API verify returned no pop_endpoints");
            } else if *pops_tx.borrow() != verify.pop_endpoints {
                info!(
                    "solanacdn: discovered POP endpoints via Pipe API: {:?}",
                    verify.pop_endpoints
                );
                pops_tx.send_replace(verify.pop_endpoints.clone());
            }
            let run_token = verify.run_token;

            loop {
                match pipe_api_pop_session_token(&client, &cfg.base_url, &run_token).await {
                    Ok(parsed) => {
                        let expires_in_secs = (parsed.expires_in.max(1) as u64).clamp(5, 3600);
                        let refresh_in_secs = (expires_in_secs / 2).clamp(5, expires_in_secs);
                        current_expires_at =
                            Some(std::time::Instant::now() + Duration::from_secs(expires_in_secs));

                        token_tx.send_replace(Some(parsed.session_token));
                        tokio::time::sleep(Duration::from_secs(refresh_in_secs)).await;
                    }
                    Err(e) => {
                        warn!("solanacdn: Pipe API pop session token refresh failed: {e}");

                        if current_expires_at.is_some_and(|t| t <= std::time::Instant::now()) {
                            token_tx.send_replace(None);
                        }

                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(Duration::from_secs(30));
                        break;
                    }
                }
            }
        }
    });
    PipeApiRefresher {
        session_token_rx: token_rx,
        pop_endpoints_rx: pops_rx,
        verify_rx,
    }
}

async fn run_pipe_ingest_reporter(
    client: reqwest::Client,
    mut verify_rx: watch::Receiver<Option<PipeApiVerifyResult>>,
    handle: Arc<SolanaCdnHandle>,
    cfg: Arc<SolanaCdnConfig>,
    validator_pubkey_base58: String,
) {
    let mut interval_secs = loop {
        if let Some(v) = verify_rx.borrow().clone() {
            if v.heartbeat_schema_version != 0 {
                handle.update_heartbeat_schema_version(v.heartbeat_schema_version);
            }
            break v.ingest.interval_secs.max(10).min(3600);
        }
        if verify_rx.changed().await.is_err() {
            return;
        }
    };

    let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = interval.tick() => {
                let Some(v) = verify_rx.borrow().clone() else {
                    continue;
                };
                if v.ingest.url.trim().is_empty() {
                    continue;
                }
                if v.ingest.max_events == 0 {
                    continue;
                }

                let sent_at = now_ts();
                let (body, consumed_race_samples) = build_pipe_ingest_body(
                    &v,
                    handle.as_ref(),
                    cfg.as_ref(),
                    &validator_pubkey_base58,
                    sent_at,
                );

                let resp = client
                    .post(&v.ingest.url)
                    .bearer_auth(v.run_token)
                    .json(&body)
                    .send()
                    .await;

                match resp {
                    Ok(r) if r.status().is_success() => {
                        handle.pipe_ingest_consume_race_samples(consumed_race_samples);
                    }
                    Ok(r) => {
                        let status = r.status();
                        let text = r.text().await.unwrap_or_default();
                        debug!("solanacdn: Pipe ingest failed: {} {}", status.as_u16(), text);
                    }
                    Err(e) => {
                        debug!("solanacdn: Pipe ingest failed: {e}");
                    }
                }
            }
            changed = verify_rx.changed() => {
                if changed.is_err() {
                    return;
                }
                let Some(v) = verify_rx.borrow().clone() else {
                    continue;
                };
                if v.heartbeat_schema_version != 0 {
                    handle.update_heartbeat_schema_version(v.heartbeat_schema_version);
                }
                let next = v.ingest.interval_secs.max(10).min(3600);
                if next != interval_secs {
                    interval_secs = next;
                    interval = tokio::time::interval(Duration::from_secs(interval_secs));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                }
            }
        }
    }
}

fn build_pipe_ingest_body(
    v: &PipeApiVerifyResult,
    handle: &SolanaCdnHandle,
    cfg: &SolanaCdnConfig,
    validator_pubkey_base58: &str,
    sent_at: i64,
) -> (serde_json::Value, usize) {
    let runtime = handle.ingest_runtime_snapshot();
    let counters = handle.ingest_counters_totals();
    let (race, race_samples) =
        handle.pipe_ingest_race_snapshot_and_samples(v.ingest.max_events as usize);

    let mut body = serde_json::json!({
        "sent_at": sent_at,
        "agent": {
            "agent_id": v.agent_id,
            "run_id": v.run_id,
            "validator_pubkey": validator_pubkey_base58,
            "version": env!("CARGO_PKG_VERSION"),
            "capture_mode": "agave",
            "iface": "",
            "direct_shreds_from_pop": cfg.direct_shreds_from_pop,
        },
        "counters": counters,
        "runtime": runtime,
    });

    if race.enabled || race.pairs_total > 0 {
        let mut race_json = serde_json::json!({
            "enabled": race.enabled,
            "sample_bits": race.sample_bits,
            "window_ms": race.window_ms,
            "pairs_total": race.pairs_total,
            "wins_solanacdn_total": race.wins_solanacdn_total,
            "wins_gossip_total": race.wins_gossip_total,
            "ties_total": race.ties_total,
            "inflight": race.inflight,
            "last_winner": race.last_winner.map(|w| w.as_str()),
            "last_lead_ms": race.last_lead_ms,
            "last_shred_slot": race.last_shred_slot,
        });
        if let Some(hist) = race.histogram {
            if let Some(obj) = race_json.as_object_mut() {
                obj.insert(
                    "histogram".to_string(),
                    serde_json::json!({
                        "delta_bucket_counts": hist.delta_bucket_counts,
                        "delta_sum_ms": hist.delta_sum_ms,
                        "delta_count": hist.delta_count,
                        "delta_by_pop_endpoint": hist.delta_by_pop_endpoint,
                        "delta_by_hour_utc": hist.delta_by_hour_utc,
                    }),
                );
            }
        }
        if let Some(obj) = body.as_object_mut() {
            obj.insert("race".to_string(), race_json);
        }
    }

    let max_body_bytes = v.ingest.max_body_bytes;
    if max_body_bytes > 0 && body_exceeds_max_bytes(&body, max_body_bytes) {
        // Drop optional fields rather than failing the ingest request.
        if let Some(obj) = body.as_object_mut() {
            obj.remove("runtime");
        }
    }
    if max_body_bytes > 0 && body_exceeds_max_bytes(&body, max_body_bytes) {
        // Keep totals, but drop histograms/segments first.
        if let Some(race_obj) = body.get_mut("race").and_then(|v| v.as_object_mut()) {
            race_obj.remove("histogram");
        }
    }
    if max_body_bytes > 0 && body_exceeds_max_bytes(&body, max_body_bytes) {
        if let Some(obj) = body.as_object_mut() {
            obj.remove("race");
        }
    }

    let mut consumed_race_samples: usize = 0;
    let max_samples = (v.ingest.max_events as usize).min(2048);
    if max_samples > 0 && !race_samples.is_empty() {
        let arr: Vec<serde_json::Value> = race_samples
            .into_iter()
            .take(max_samples)
            .map(|s| serde_json::json!({"delta_ms": s.delta_ms, "gossip_src_ip": s.gossip_src_ip.to_string()}))
            .collect();
        consumed_race_samples = arr.len();

        if let Some(obj) = body.as_object_mut() {
            obj.insert("race_samples".to_string(), serde_json::Value::Array(arr));
        }

        if max_body_bytes > 0 {
            // If the payload is too large, reduce race_samples aggressively to fit.
            let mut attempts: usize = 0;
            while body_exceeds_max_bytes(&body, max_body_bytes) {
                attempts = attempts.saturating_add(1);
                if attempts > 8 {
                    break;
                }
                if consumed_race_samples <= 1 {
                    consumed_race_samples = 0;
                    if let Some(obj) = body.as_object_mut() {
                        obj.remove("race_samples");
                    }
                    break;
                }
                consumed_race_samples = (consumed_race_samples / 2).max(1);
                if let Some(arr) = body.get_mut("race_samples").and_then(|v| v.as_array_mut()) {
                    arr.truncate(consumed_race_samples);
                }
            }

            if body_exceeds_max_bytes(&body, max_body_bytes) {
                consumed_race_samples = 0;
                if let Some(obj) = body.as_object_mut() {
                    obj.remove("race_samples");
                }
            }
        }
    }

    (body, consumed_race_samples)
}

fn body_exceeds_max_bytes(body: &serde_json::Value, max_body_bytes: u64) -> bool {
    let Ok(bytes) = serde_json::to_vec(body) else {
        return false;
    };
    (bytes.len() as u64) > max_body_bytes
}

#[derive(Debug)]
struct SkipServerVerification;

impl ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ED25519,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA256,
        ]
    }
}

fn make_quic_client_config(cfg: &SolanaCdnConfig) -> Result<quinn::ClientConfig, SolanaCdnError> {
    init_rustls();

    if cfg.tls_insecure_skip_verify {
        return Ok(make_quic_client_config_insecure());
    }

    let roots = match cfg.tls_ca_cert_path.as_ref() {
        Some(path) => load_root_cert_store(path)?,
        None => load_default_root_cert_store()?,
    };

    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.enable_early_data = false;
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("QUIC client crypto");
    Ok(quinn::ClientConfig::new(Arc::new(crypto)))
}

fn make_quic_client_config_insecure() -> quinn::ClientConfig {
    let mut tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
        .with_no_client_auth();
    tls.enable_early_data = true;
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("QUIC client crypto");
    quinn::ClientConfig::new(Arc::new(crypto))
}

fn fnv1a_128_update(mut hash: u128, bytes: &[u8]) -> u128 {
    for b in bytes {
        hash ^= *b as u128;
        hash = hash.wrapping_mul(FNV1A_128_PRIME);
    }
    hash
}

fn shred_kind_tag(kind: ShredKind) -> u8 {
    match kind {
        ShredKind::Tvu => 0,
        ShredKind::Gossip => 1,
    }
}

fn compute_shred_batch_id(items: &[(ShredKind, Bytes)]) -> u128 {
    let mut hash = FNV1A_128_OFFSET_BASIS;
    hash = fnv1a_128_update(
        hash,
        &u32::try_from(items.len()).unwrap_or(u32::MAX).to_le_bytes(),
    );
    for (kind, payload) in items {
        hash = fnv1a_128_update(hash, &[shred_kind_tag(*kind)]);
        hash = fnv1a_128_update(
            hash,
            &u32::try_from(payload.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        hash = fnv1a_128_update(hash, payload.as_ref());
    }
    hash
}

fn vote_flow_id(dst: &SocketAddr) -> u64 {
    let mut h = DefaultHasher::new();
    dst.hash(&mut h);
    h.finish()
}

fn vote_dedup_key(dst: &SocketAddr, payload: &[u8]) -> u128 {
    let mut hash = FNV1A_128_OFFSET_BASIS;
    match dst {
        SocketAddr::V4(v4) => {
            hash = fnv1a_128_update(hash, &v4.ip().octets());
            hash = fnv1a_128_update(hash, &v4.port().to_le_bytes());
        }
        SocketAddr::V6(v6) => {
            hash = fnv1a_128_update(hash, &v6.ip().octets());
            hash = fnv1a_128_update(hash, &v6.port().to_le_bytes());
            hash = fnv1a_128_update(hash, &v6.scope_id().to_le_bytes());
        }
    }
    hash = fnv1a_128_update(
        hash,
        &u32::try_from(payload.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    hash = fnv1a_128_update(hash, payload);
    hash
}

async fn write_len_prefixed<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
) -> Result<(), SolanaCdnError> {
    let len: u32 = payload
        .len()
        .try_into()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "frame too large"))?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_len_prefixed<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> Result<Vec<u8>, SolanaCdnError> {
    read_len_prefixed_with_limit(reader, DEFAULT_MAX_FRAME_BYTES).await
}

async fn read_len_prefixed_with_limit<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<Vec<u8>, SolanaCdnError> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > max_frame_bytes {
        return Err(SolanaCdnError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "frame too large",
        )));
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

async fn write_agent_msg<W: AsyncWrite + Unpin>(
    writer: &mut W,
    msg: &AgentToPop,
) -> Result<(), SolanaCdnError> {
    let payload = encode_envelope(msg)?;
    write_len_prefixed(writer, &payload).await
}

async fn read_pop_msg<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_frame_bytes: usize,
) -> Result<PopToAgent, SolanaCdnError> {
    let bytes = read_len_prefixed_with_limit(reader, max_frame_bytes).await?;
    Ok(decode_envelope(&bytes)?)
}

#[derive(Clone)]
struct QuicConnectConfig {
    client_config: quinn::ClientConfig,
    server_name: String,
}

#[derive(Clone)]
struct AuthContext {
    validator_pubkey: PubkeyBytes,
    signing_key: SigningKey,
    identity_keypair: Arc<Keypair>,
}

impl AuthContext {
    fn new(identity_keypair: Arc<Keypair>) -> Result<Self, SolanaCdnError> {
        let validator_pubkey = PubkeyBytes(identity_keypair.pubkey().to_bytes());
        let signing_key = signing_key_from_solana_keypair(identity_keypair.as_ref())?;
        Ok(Self {
            validator_pubkey,
            signing_key,
            identity_keypair,
        })
    }

    fn build_auth_request(&self) -> Result<AuthRequest, SolanaCdnError> {
        let payload = AuthRequestPayload {
            validator_pubkey: self.validator_pubkey,
            delegate_pubkey: None,
            delegation_cert: None,
            timestamp_ms: now_ms(),
            nonce: random_nonce_16(),
        };
        Ok(AuthRequest::sign(payload, &self.signing_key)?)
    }
}

fn signing_key_from_solana_keypair(identity: &Keypair) -> Result<SigningKey, SolanaCdnError> {
    let bytes = identity.secret_bytes();
    let sk = SigningKey::from_bytes(&bytes);
    let validator_pk = PubkeyBytes(identity.pubkey().to_bytes());
    let got_pk = PubkeyBytes(sk.verifying_key().to_bytes());
    if validator_pk != got_pk {
        return Err(SolanaCdnError::AuthFailed(
            "validator identity pubkey mismatch".to_string(),
        ));
    }
    Ok(sk)
}

#[derive(Clone)]
struct ShredBatchDeduper {
    inner: Arc<std::sync::Mutex<ShredBatchDeduperInner>>,
}

struct ShredBatchDeduperInner {
    max_entries: usize,
    order: VecDeque<u128>,
    seen: HashSet<u128>,
}

impl ShredBatchDeduper {
    fn new(max_entries: usize) -> Self {
        Self {
            inner: Arc::new(std::sync::Mutex::new(ShredBatchDeduperInner {
                max_entries,
                order: VecDeque::new(),
                seen: HashSet::new(),
            })),
        }
    }

    fn insert_if_new(&self, batch_id: u128) -> bool {
        let mut inner = match self.inner.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        if inner.seen.contains(&batch_id) {
            return false;
        }
        inner.seen.insert(batch_id);
        inner.order.push_back(batch_id);
        while inner.order.len() > inner.max_entries {
            if let Some(old) = inner.order.pop_front() {
                inner.seen.remove(&old);
            }
        }
        true
    }
}

#[derive(Debug)]
enum SessionEvent {
    Connected {
        endpoint: SocketAddr,
        udp_enabled: bool,
    },
    Disconnected {
        endpoint: SocketAddr,
    },
    RttSample {
        endpoint: SocketAddr,
        rtt_ms: u64,
    },
}

#[derive(Clone, Copy, Debug)]
struct ConnectedPop {
    udp_enabled: bool,
    rtt_ewma_ms: u64,
    rtt_valid: bool,
}

struct ManagedSession {
    uplink: mpsc::Sender<UplinkMsg>,
    stop_tx: watch::Sender<bool>,
}

#[derive(Clone)]
struct ControlTlsClient {
    connector: tokio_rustls::TlsConnector,
    server_name: ServerName<'static>,
}

fn make_control_tls_client(
    cfg: &SolanaCdnConfig,
) -> Result<Option<ControlTlsClient>, SolanaCdnError> {
    if cfg.control_endpoint.is_none() {
        return Ok(None);
    }

    init_rustls();
    let server_name = ServerName::try_from(cfg.control_server_name.clone())
        .map_err(|e| SolanaCdnError::InvalidServerName(e.to_string()))?;

    let tls_config = if cfg.control_tls_insecure_skip_verify {
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
            .with_no_client_auth()
    } else {
        let roots = match cfg.control_tls_ca_cert_path.as_ref() {
            Some(path) => load_root_cert_store(path)?,
            None => load_default_root_cert_store()?,
        };
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    };

    Ok(Some(ControlTlsClient {
        connector: tokio_rustls::TlsConnector::from(Arc::new(tls_config)),
        server_name,
    }))
}

async fn fetch_pops_from_control(
    control_endpoint: SocketAddr,
    tls: Option<&ControlTlsClient>,
) -> Result<Vec<SocketAddr>, SolanaCdnError> {
    let stream = TcpStream::connect(control_endpoint).await?;
    if let Err(e) = stream.set_nodelay(true) {
        debug!("solanacdn: failed to set TCP_NODELAY for control {control_endpoint}: {e}");
    }

    match tls {
        None => {
            let (reader, writer) = tokio::io::split(stream);
            fetch_pops_from_control_over_io(reader, writer).await
        }
        Some(tls) => {
            let tls_stream = tls
                .connector
                .connect(tls.server_name.clone(), stream)
                .await
                .map_err(|e| SolanaCdnError::Tls(e.to_string()))?;
            let (reader, writer) = tokio::io::split(tls_stream);
            fetch_pops_from_control_over_io(reader, writer).await
        }
    }
}

async fn fetch_pops_from_control_over_io<R, W>(
    mut reader: R,
    mut writer: W,
) -> Result<Vec<SocketAddr>, SolanaCdnError>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let req = ControlRequest::ListPops;
    let payload = encode_envelope(&req)?;
    write_len_prefixed(&mut writer, &payload).await?;

    let bytes = read_len_prefixed(&mut reader).await?;
    let resp: ControlResponse = decode_envelope(&bytes)?;
    match resp {
        ControlResponse::PopList(list) => {
            Ok(list.pops.into_iter().map(|p| p.public_addr).collect())
        }
        ControlResponse::Error(err) => Err(SolanaCdnError::AuthFailed(format!(
            "control error: {}: {}",
            err.code, err.message
        ))),
        other => Err(SolanaCdnError::AuthFailed(format!(
            "unexpected control response: {other:?}"
        ))),
    }
}

const METRICS_HTTP_MAX_REQUEST_BYTES: usize = 8 * 1024;

fn prometheus_escape_label_value(value: &str) -> String {
    value.replace('\\', r"\\").replace('"', r#"\""#)
}

fn histogram_quantile_seconds(
    q: f64,
    lower_bound_ms: i64,
    upper_bounds_ms: &[i64],
    cumulative_counts: &[u64],
    total_count: u64,
) -> Option<f64> {
    if !(0.0..=1.0).contains(&q) {
        return None;
    }
    if upper_bounds_ms.len() != cumulative_counts.len() {
        return None;
    }
    if total_count == 0 {
        return None;
    }

    let rank = (total_count as f64) * q;
    let mut prev_count: f64 = 0.0;
    let mut prev_bound_ms: f64 = lower_bound_ms as f64;
    for (i, bound_ms) in upper_bounds_ms.iter().enumerate() {
        let count = cumulative_counts[i] as f64;
        if count >= rank {
            let upper_bound_ms = *bound_ms as f64;
            let bucket_count = (count - prev_count).max(0.0);
            if bucket_count == 0.0 {
                return Some(upper_bound_ms / 1000.0);
            }
            let fraction = ((rank - prev_count) / bucket_count).clamp(0.0, 1.0);
            let estimate_ms = prev_bound_ms + (upper_bound_ms - prev_bound_ms) * fraction;
            return Some(estimate_ms / 1000.0);
        }
        prev_count = count;
        prev_bound_ms = *bound_ms as f64;
    }

    Some((*upper_bounds_ms.last()? as f64) / 1000.0)
}

fn format_prometheus_metrics(handle: &SolanaCdnHandle) -> String {
    let status = handle.status_snapshot();
    let race = {
        let tracker = match handle.race_state.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        tracker.snapshot(&handle.cfg)
    };
    let mut out = String::new();

    out.push_str(
        "# HELP solanacdn_connected Whether SolanaCDN has an active publisher session (0/1)\n",
    );
    out.push_str("# TYPE solanacdn_connected gauge\n");
    out.push_str(&format!(
        "solanacdn_connected {}\n",
        if status.connected { 1 } else { 0 }
    ));

    out.push_str(
        "# HELP solanacdn_publisher_switches_total Number of times the publisher POP changed\n",
    );
    out.push_str("# TYPE solanacdn_publisher_switches_total counter\n");
    out.push_str(&format!(
        "solanacdn_publisher_switches_total {}\n",
        status.publisher_switches_total
    ));

    out.push_str(
        "# HELP solanacdn_publisher_present Whether a publisher endpoint is selected (0/1)\n",
    );
    out.push_str("# TYPE solanacdn_publisher_present gauge\n");
    out.push_str(&format!(
        "solanacdn_publisher_present {}\n",
        if status.publisher.is_some() { 1 } else { 0 }
    ));
    if let Some(publisher) = status.publisher.as_deref() {
        let publisher = prometheus_escape_label_value(publisher);
        out.push_str("# HELP solanacdn_publisher_info Publisher POP identity as a label\n");
        out.push_str("# TYPE solanacdn_publisher_info gauge\n");
        out.push_str(&format!(
            "solanacdn_publisher_info{{publisher=\"{}\"}} 1\n",
            publisher
        ));
    }

    out.push_str("# HELP solanacdn_pop_connected POP connectivity by endpoint\n");
    out.push_str("# TYPE solanacdn_pop_connected gauge\n");
    for ep in &status.connected_pops {
        let ep = prometheus_escape_label_value(ep);
        out.push_str(&format!(
            "solanacdn_pop_connected{{endpoint=\"{}\"}} 1\n",
            ep
        ));
    }

    out.push_str("# HELP solanacdn_rx_shred_bytes_total Total shred bytes received from POPs\n");
    out.push_str("# TYPE solanacdn_rx_shred_bytes_total counter\n");
    out.push_str(&format!(
        "solanacdn_rx_shred_bytes_total {}\n",
        status.rx_shred_bytes_total
    ));

    out.push_str(
        "# HELP solanacdn_rx_shred_payloads_total Total shred payloads received from POPs\n",
    );
    out.push_str("# TYPE solanacdn_rx_shred_payloads_total counter\n");
    out.push_str(&format!(
        "solanacdn_rx_shred_payloads_total {}\n",
        status.rx_shred_payloads_total
    ));

    out.push_str("# HELP solanacdn_shred_batches_dropped_oversized_total PushShredBatch messages dropped due to oversized shred count\n");
    out.push_str("# TYPE solanacdn_shred_batches_dropped_oversized_total counter\n");
    out.push_str(&format!(
        "solanacdn_shred_batches_dropped_oversized_total {}\n",
        status.dropped_shred_batches_oversized_total
    ));

    out.push_str("# HELP solanacdn_rx_shred_payloads_per_sec Recent shred payload receive rate\n");
    out.push_str("# TYPE solanacdn_rx_shred_payloads_per_sec gauge\n");
    out.push_str(&format!(
        "solanacdn_rx_shred_payloads_per_sec {}\n",
        status.rx_shred_payloads_per_sec
    ));

    out.push_str(
        "# HELP solanacdn_tunneled_vote_packets_total Total vote packets tunneled to POPs\n",
    );
    out.push_str("# TYPE solanacdn_tunneled_vote_packets_total counter\n");
    out.push_str(&format!(
        "solanacdn_tunneled_vote_packets_total {}\n",
        status.tunneled_vote_packets_total
    ));

    out.push_str("# HELP solanacdn_tunneled_vote_packets_per_sec Recent vote tunnel rate\n");
    out.push_str("# TYPE solanacdn_tunneled_vote_packets_per_sec gauge\n");
    out.push_str(&format!(
        "solanacdn_tunneled_vote_packets_per_sec {}\n",
        status.tunneled_vote_packets_per_sec
    ));

    out.push_str("# HELP solanacdn_rx_vote_packets_total Total vote packets received from POPs\n");
    out.push_str("# TYPE solanacdn_rx_vote_packets_total counter\n");
    out.push_str(&format!(
        "solanacdn_rx_vote_packets_total {}\n",
        status.rx_vote_packets_total
    ));

    out.push_str("# HELP solanacdn_dropped_vote_datagrams_total Total vote datagrams dropped (deduped or failed injection)\n");
    out.push_str("# TYPE solanacdn_dropped_vote_datagrams_total counter\n");
    out.push_str(&format!(
        "solanacdn_dropped_vote_datagrams_total {}\n",
        status.dropped_vote_datagrams_total
    ));

    out.push_str("# HELP solanacdn_vote_tunnel_dropped_oversized_payload_total Vote datagrams dropped due to oversized payloads\n");
    out.push_str("# TYPE solanacdn_vote_tunnel_dropped_oversized_payload_total counter\n");
    out.push_str(&format!(
        "solanacdn_vote_tunnel_dropped_oversized_payload_total {}\n",
        status.dropped_vote_datagrams_oversized_payload_total
    ));

    out.push_str("# HELP solanacdn_vote_tunnel_dropped_invalid_payload_total Vote datagrams dropped due to invalid payloads (not a vote transaction)\n");
    out.push_str("# TYPE solanacdn_vote_tunnel_dropped_invalid_payload_total counter\n");
    out.push_str(&format!(
        "solanacdn_vote_tunnel_dropped_invalid_payload_total {}\n",
        status.dropped_vote_datagrams_invalid_payload_total
    ));

    out.push_str("# HELP solanacdn_vote_tunnel_dropped_unexpected_dst_total Vote datagrams dropped due to unexpected destinations\n");
    out.push_str("# TYPE solanacdn_vote_tunnel_dropped_unexpected_dst_total counter\n");
    out.push_str(&format!(
        "solanacdn_vote_tunnel_dropped_unexpected_dst_total {}\n",
        status.dropped_vote_datagrams_unexpected_dst_total
    ));

    out.push_str("# HELP solanacdn_vote_tunnel_allowed_dsts_len Number of currently allowed vote tunnel destinations\n");
    out.push_str("# TYPE solanacdn_vote_tunnel_allowed_dsts_len gauge\n");
    out.push_str(&format!(
        "solanacdn_vote_tunnel_allowed_dsts_len {}\n",
        status.vote_tunnel_allowed_dsts_len
    ));

    out.push_str("# HELP solanacdn_quic_shreds_dropped_unexpected_msg_total Shreds stream frames dropped due to unexpected message types\n");
    out.push_str("# TYPE solanacdn_quic_shreds_dropped_unexpected_msg_total counter\n");
    out.push_str(&format!(
        "solanacdn_quic_shreds_dropped_unexpected_msg_total {}\n",
        status.dropped_quic_shreds_unexpected_msg_total
    ));

    out.push_str("# HELP solanacdn_quic_votes_dropped_unexpected_msg_total Votes stream frames dropped due to unexpected message types\n");
    out.push_str("# TYPE solanacdn_quic_votes_dropped_unexpected_msg_total counter\n");
    out.push_str(&format!(
        "solanacdn_quic_votes_dropped_unexpected_msg_total {}\n",
        status.dropped_quic_votes_unexpected_msg_total
    ));

    out.push_str("# HELP solanacdn_udp_shreds_dropped_unexpected_peer_total UDP shred datagrams dropped due to unexpected peer IP\n");
    out.push_str("# TYPE solanacdn_udp_shreds_dropped_unexpected_peer_total counter\n");
    out.push_str(&format!(
        "solanacdn_udp_shreds_dropped_unexpected_peer_total {}\n",
        status.dropped_udp_shreds_unexpected_peer_total
    ));

    out.push_str("# HELP solanacdn_udp_shreds_dropped_unexpected_msg_total UDP shred datagrams dropped due to unexpected message types\n");
    out.push_str("# TYPE solanacdn_udp_shreds_dropped_unexpected_msg_total counter\n");
    out.push_str(&format!(
        "solanacdn_udp_shreds_dropped_unexpected_msg_total {}\n",
        status.dropped_udp_shreds_unexpected_msg_total
    ));

    out.push_str("# HELP solanacdn_udp_votes_dropped_unexpected_peer_total UDP vote datagrams dropped due to unexpected peer IP\n");
    out.push_str("# TYPE solanacdn_udp_votes_dropped_unexpected_peer_total counter\n");
    out.push_str(&format!(
        "solanacdn_udp_votes_dropped_unexpected_peer_total {}\n",
        status.dropped_udp_votes_unexpected_peer_total
    ));

    out.push_str("# HELP solanacdn_udp_votes_dropped_unexpected_msg_total UDP vote datagrams dropped due to unexpected message types\n");
    out.push_str("# TYPE solanacdn_udp_votes_dropped_unexpected_msg_total counter\n");
    out.push_str(&format!(
        "solanacdn_udp_votes_dropped_unexpected_msg_total {}\n",
        status.dropped_udp_votes_unexpected_msg_total
    ));

    out.push_str("# HELP solanacdn_tx_fair_ordering_enabled Whether fair transaction ordering is enabled (0/1)\n");
    out.push_str("# TYPE solanacdn_tx_fair_ordering_enabled gauge\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_ordering_enabled {}\n",
        if status.tx_fair_ordering { 1 } else { 0 }
    ));

    out.push_str("# HELP solanacdn_tx_fair_require_target_slot_enabled Whether fair batches are required to include a target_slot (0/1)\n");
    out.push_str("# TYPE solanacdn_tx_fair_require_target_slot_enabled gauge\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_require_target_slot_enabled {}\n",
        if status.tx_fair_require_target_slot {
            1
        } else {
            0
        }
    ));

    out.push_str("# HELP solanacdn_tx_fair_batch_received_total Total transactions received in fair batches\n");
    out.push_str("# TYPE solanacdn_tx_fair_batch_received_total counter\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_batch_received_total {}\n",
        status.tx_fair_batch_received_total
    ));

    out.push_str("# HELP solanacdn_tx_fair_batch_injected_total Total fair-batch transactions injected into the validator\n");
    out.push_str("# TYPE solanacdn_tx_fair_batch_injected_total counter\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_batch_injected_total {}\n",
        status.tx_fair_batch_injected_total
    ));

    out.push_str("# HELP solanacdn_tx_fair_batch_inject_failed_total Total fair-batch transactions that failed injection\n");
    out.push_str("# TYPE solanacdn_tx_fair_batch_inject_failed_total counter\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_batch_inject_failed_total {}\n",
        status.tx_fair_batch_inject_failed_total
    ));

    out.push_str("# HELP solanacdn_fair_batch_dropped_sig_mismatch_total Number of fair-batch transactions dropped because FairTx.sig did not match (or could not be parsed from) the wire transaction payload\n");
    out.push_str("# TYPE solanacdn_fair_batch_dropped_sig_mismatch_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_batch_dropped_sig_mismatch_total {}\n",
        FAIR_BATCH_DROPPED_SIG_MISMATCH_TOTAL.load(Ordering::Relaxed)
    ));

    out.push_str("# HELP solanacdn_fair_batch_dropped_duplicate_sig_total Number of fair-batch transactions dropped because they duplicated a signature already present in the same batch\n");
    out.push_str("# TYPE solanacdn_fair_batch_dropped_duplicate_sig_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_batch_dropped_duplicate_sig_total {}\n",
        FAIR_BATCH_DROPPED_DUP_SIG_TOTAL.load(Ordering::Relaxed)
    ));

    out.push_str("# HELP solanacdn_fair_batch_dropped_payload_too_large_total Number of fair-batch transactions dropped because their payload was larger than PACKET_DATA_SIZE\n");
    out.push_str("# TYPE solanacdn_fair_batch_dropped_payload_too_large_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_batch_dropped_payload_too_large_total {}\n",
        FAIR_BATCH_DROPPED_PAYLOAD_TOO_LARGE_TOTAL.load(Ordering::Relaxed)
    ));

    out.push_str("# HELP solanacdn_fair_batch_dropped_invalid_wire_tx_total Number of fair-batch transactions dropped because they failed bincode deserialization, sanitization, or signature verification\n");
    out.push_str("# TYPE solanacdn_fair_batch_dropped_invalid_wire_tx_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_batch_dropped_invalid_wire_tx_total {}\n",
        FAIR_BATCH_DROPPED_INVALID_WIRE_TX_TOTAL.load(Ordering::Relaxed)
    ));

    out.push_str("# HELP solanacdn_fair_batch_dropped_too_many_txs_total Number of fair-batch transactions dropped because the batch exceeded the transaction count cap\n");
    out.push_str("# TYPE solanacdn_fair_batch_dropped_too_many_txs_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_batch_dropped_too_many_txs_total {}\n",
        FAIR_BATCH_DROPPED_TOO_MANY_TXS_TOTAL.load(Ordering::Relaxed)
    ));

    out.push_str("# HELP solanacdn_fair_batch_dropped_total_bytes_exceeded_total Number of fair-batch transactions dropped because the batch exceeded the total payload bytes cap\n");
    out.push_str("# TYPE solanacdn_fair_batch_dropped_total_bytes_exceeded_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_batch_dropped_total_bytes_exceeded_total {}\n",
        FAIR_BATCH_DROPPED_TOTAL_BYTES_EXCEEDED_TOTAL.load(Ordering::Relaxed)
    ));

    out.push_str("# HELP solanacdn_tx_deduped_packets_total Total SolanaCDN transaction packets dropped due to deduplication\n");
    out.push_str("# TYPE solanacdn_tx_deduped_packets_total counter\n");
    out.push_str(&format!(
        "solanacdn_tx_deduped_packets_total {}\n",
        status.tx_deduped_packets_total
    ));

    out.push_str("# HELP solanacdn_tx_relay_dropped_fair_mode_total Total RelayTransaction packets dropped because fair ordering is enabled\n");
    out.push_str("# TYPE solanacdn_tx_relay_dropped_fair_mode_total counter\n");
    out.push_str(&format!(
        "solanacdn_tx_relay_dropped_fair_mode_total {}\n",
        status.tx_relay_dropped_fair_mode_total
    ));

    out.push_str(
        "# HELP solanacdn_fair_priority_lookups_total Total fair priority lookup attempts\n",
    );
    out.push_str("# TYPE solanacdn_fair_priority_lookups_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_priority_lookups_total {}\n",
        status.fair_priority_lookups_total
    ));

    out.push_str("# HELP solanacdn_fair_priority_hits_total Total fair priority lookups that returned a value\n");
    out.push_str("# TYPE solanacdn_fair_priority_hits_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_priority_hits_total {}\n",
        status.fair_priority_hits_total
    ));

    out.push_str("# HELP solanacdn_tx_fair_slashing_enabled Whether fair ordering slashing/auditing is enabled (0/1)\n");
    out.push_str("# TYPE solanacdn_tx_fair_slashing_enabled gauge\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_slashing_enabled {}\n",
        if status.tx_fair_slashing { 1 } else { 0 }
    ));

    out.push_str("# HELP solanacdn_tx_fair_slashing_strict_enabled Whether strict fair slashing rules are enabled (0/1)\n");
    out.push_str("# TYPE solanacdn_tx_fair_slashing_strict_enabled gauge\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_slashing_strict_enabled {}\n",
        if status.tx_fair_slashing_strict { 1 } else { 0 }
    ));

    out.push_str("# HELP solanacdn_tx_fair_slashing_witness_enabled Whether ACK-required fair slashing (missing on-chain commit for leader ACKs) is enabled (0/1)\n");
    out.push_str("# TYPE solanacdn_tx_fair_slashing_witness_enabled gauge\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_slashing_witness_enabled {}\n",
        if status.tx_fair_slashing_witness {
            1
        } else {
            0
        }
    ));

    out.push_str("# HELP solanacdn_tx_fair_slashing_nonresponse_enabled Whether POP-witness-based fair slashing (witnessed delivery but no commit/reject) is enabled (0/1)\n");
    out.push_str("# TYPE solanacdn_tx_fair_slashing_nonresponse_enabled gauge\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_slashing_nonresponse_enabled {}\n",
        if status.tx_fair_slashing_nonresponse {
            1
        } else {
            0
        }
    ));

    out.push_str("# HELP solanacdn_tx_fair_slashing_publish_witness_memos_enabled Whether POP witness receipts are published as on-chain memos for replayable audits (0/1)\n");
    out.push_str("# TYPE solanacdn_tx_fair_slashing_publish_witness_memos_enabled gauge\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_slashing_publish_witness_memos_enabled {}\n",
        if status.tx_fair_slashing_publish_witness_memos {
            1
        } else {
            0
        }
    ));

    out.push_str("# HELP solanacdn_tx_fair_slashing_fence_enabled Whether same-slot account-fence fair slashing rules are enabled (0/1)\n");
    out.push_str("# TYPE solanacdn_tx_fair_slashing_fence_enabled gauge\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_slashing_fence_enabled {}\n",
        if status.tx_fair_slashing_fence { 1 } else { 0 }
    ));

    out.push_str("# HELP solanacdn_tx_fair_slashing_enforce_enabled Whether fair slashing vote withholding is enabled (0/1)\n");
    out.push_str("# TYPE solanacdn_tx_fair_slashing_enforce_enabled gauge\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_slashing_enforce_enabled {}\n",
        if status.tx_fair_slashing_enforce {
            1
        } else {
            0
        }
    ));

    out.push_str("# HELP solanacdn_tx_fair_slashing_enforce_configured Whether fair slashing vote withholding is configured at startup (0/1)\n");
    out.push_str("# TYPE solanacdn_tx_fair_slashing_enforce_configured gauge\n");
    out.push_str(&format!(
        "solanacdn_tx_fair_slashing_enforce_configured {}\n",
        if status.tx_fair_slashing_enforce_configured {
            1
        } else {
            0
        }
    ));

    out.push_str("# HELP solanacdn_tx_fair_slashing_enforce_override Runtime override state for fair slashing enforcement (1 for the current state)\n");
    out.push_str("# TYPE solanacdn_tx_fair_slashing_enforce_override gauge\n");
    let enforce_override_state = match status.tx_fair_slashing_enforce_override {
        None => TxFairSlashingEnforceOverride::Inherit,
        Some(false) => TxFairSlashingEnforceOverride::ForceOff,
        Some(true) => TxFairSlashingEnforceOverride::ForceOn,
    };
    out.push_str(&format!(
        "solanacdn_tx_fair_slashing_enforce_override{{state=\"{}\"}} 1\n",
        enforce_override_state.label()
    ));

    out.push_str(
        "# HELP solanacdn_fair_commits_rx_total Number of fair ordering commit messages received\n",
    );
    out.push_str("# TYPE solanacdn_fair_commits_rx_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_commits_rx_total {}\n",
        status.fair_commits_rx_total
    ));

    out.push_str("# HELP solanacdn_fair_commits_invalid_total Number of invalid fair ordering commits received\n");
    out.push_str("# TYPE solanacdn_fair_commits_invalid_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_commits_invalid_total {}\n",
        status.fair_commits_invalid_total
    ));

    out.push_str("# HELP solanacdn_fair_equivocations_total Number of detected fair ordering equivocations or audit failures\n");
    out.push_str("# TYPE solanacdn_fair_equivocations_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_equivocations_total {}\n",
        status.fair_equivocations_total
    ));

    out.push_str("# HELP solanacdn_fair_votes_withheld_total Number of votes withheld due to fair ordering violations\n");
    out.push_str("# TYPE solanacdn_fair_votes_withheld_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_votes_withheld_total {}\n",
        status.fair_votes_withheld_total
    ));

    out.push_str("# HELP solanacdn_fair_ledger_audit_checked_total Number of slots audited for fair ordering\n");
    out.push_str("# TYPE solanacdn_fair_ledger_audit_checked_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_ledger_audit_checked_total {}\n",
        status.fair_ledger_audit_checked_total
    ));

    out.push_str("# HELP solanacdn_fair_ledger_audit_failed_total Number of slots that failed fair ordering ledger audit\n");
    out.push_str("# TYPE solanacdn_fair_ledger_audit_failed_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_ledger_audit_failed_total {}\n",
        status.fair_ledger_audit_failed_total
    ));

    out.push_str("# HELP solanacdn_fair_ledger_audit_inconclusive_total Number of slots where fair ordering audit was inconclusive (missing commit chunks)\n");
    out.push_str("# TYPE solanacdn_fair_ledger_audit_inconclusive_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_ledger_audit_inconclusive_total {}\n",
        status.fair_ledger_audit_inconclusive_total
    ));

    out.push_str("# HELP solanacdn_fair_ledger_audit_get_slot_entries_failed_total Number of slots where the fair ordering audit could not read entries from blockstore\n");
    out.push_str("# TYPE solanacdn_fair_ledger_audit_get_slot_entries_failed_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_ledger_audit_get_slot_entries_failed_total {}\n",
        status.fair_ledger_audit_get_slot_entries_failed_total
    ));

    out.push_str("# HELP solanacdn_fair_ledger_commits_seen_total Number of fair ordering ledger commit chunks observed\n");
    out.push_str("# TYPE solanacdn_fair_ledger_commits_seen_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_ledger_commits_seen_total {}\n",
        status.fair_ledger_commits_seen_total
    ));

    out.push_str("# HELP solanacdn_fair_ledger_commits_invalid_total Number of invalid fair ordering ledger commit chunks observed\n");
    out.push_str("# TYPE solanacdn_fair_ledger_commits_invalid_total counter\n");
    out.push_str(&format!(
        "solanacdn_fair_ledger_commits_invalid_total {}\n",
        status.fair_ledger_commits_invalid_total
    ));

    out.push_str("# HELP solanacdn_fair_order_witnesses_entries Number of in-memory fair ordering witnesses tracked\n");
    out.push_str("# TYPE solanacdn_fair_order_witnesses_entries gauge\n");
    out.push_str(&format!(
        "solanacdn_fair_order_witnesses_entries {}\n",
        status.fair_order_witnesses_len
    ));

    out.push_str("# HELP solanacdn_fair_slashed_leaders_entries Number of leader/slot slashing entries tracked\n");
    out.push_str("# TYPE solanacdn_fair_slashed_leaders_entries gauge\n");
    out.push_str(&format!(
        "solanacdn_fair_slashed_leaders_entries {}\n",
        status.fair_slashed_leaders_len
    ));

    out.push_str(
        "# HELP solanacdn_fair_ledger_audited_slots_entries Number of cached audited slots\n",
    );
    out.push_str("# TYPE solanacdn_fair_ledger_audited_slots_entries gauge\n");
    out.push_str(&format!(
        "solanacdn_fair_ledger_audited_slots_entries {}\n",
        status.fair_ledger_audited_slots_len
    ));

    if let Some(slot) = status.last_shred_slot {
        out.push_str("# HELP solanacdn_last_shred_slot Last Solana slot observed from POP-delivered shreds\n");
        out.push_str("# TYPE solanacdn_last_shred_slot gauge\n");
        out.push_str(&format!("solanacdn_last_shred_slot {}\n", slot));
    }
    if let Some(age_ms) = status.last_shred_age_ms {
        out.push_str("# HELP solanacdn_last_shred_age_seconds Age of last POP-delivered shred\n");
        out.push_str("# TYPE solanacdn_last_shred_age_seconds gauge\n");
        out.push_str(&format!(
            "solanacdn_last_shred_age_seconds {}\n",
            (age_ms as f64) / 1000.0
        ));
    }

    if let Some(slot) = status.last_accepted_shred_slot {
        out.push_str("# HELP solanacdn_last_accepted_shred_slot Last Solana slot observed from SolanaCDN shreds accepted into the validator pipeline\n");
        out.push_str("# TYPE solanacdn_last_accepted_shred_slot gauge\n");
        out.push_str(&format!("solanacdn_last_accepted_shred_slot {}\n", slot));
    }
    if let Some(age_ms) = status.last_accepted_shred_age_ms {
        out.push_str("# HELP solanacdn_last_accepted_shred_age_seconds Age of last SolanaCDN shred accepted into the validator pipeline\n");
        out.push_str("# TYPE solanacdn_last_accepted_shred_age_seconds gauge\n");
        out.push_str(&format!(
            "solanacdn_last_accepted_shred_age_seconds {}\n",
            (age_ms as f64) / 1000.0
        ));
    }

    out.push_str(
        "# HELP solanacdn_race_enabled Whether SolanaCDN race measurement is enabled (0/1)\n",
    );
    out.push_str("# TYPE solanacdn_race_enabled gauge\n");
    out.push_str(&format!(
        "solanacdn_race_enabled {}\n",
        if race.enabled { 1 } else { 0 }
    ));

    out.push_str(
        "# HELP solanacdn_race_sample_bits Deterministic sampling bits (1/(2^bits) shreds)\n",
    );
    out.push_str("# TYPE solanacdn_race_sample_bits gauge\n");
    out.push_str(&format!(
        "solanacdn_race_sample_bits {}\n",
        race.sample_bits
    ));

    out.push_str("# HELP solanacdn_race_window_seconds Race matching window seconds\n");
    out.push_str("# TYPE solanacdn_race_window_seconds gauge\n");
    out.push_str(&format!(
        "solanacdn_race_window_seconds {}\n",
        (race.window_ms as f64) / 1000.0
    ));

    out.push_str(
        "# HELP solanacdn_race_inflight Number of sampled shreds awaiting the other source\n",
    );
    out.push_str("# TYPE solanacdn_race_inflight gauge\n");
    out.push_str(&format!("solanacdn_race_inflight {}\n", race.inflight));

    out.push_str("# HELP solanacdn_race_pairs_total Number of shreds observed on both sources within the race window\n");
    out.push_str("# TYPE solanacdn_race_pairs_total counter\n");
    out.push_str(&format!(
        "solanacdn_race_pairs_total {}\n",
        race.pairs_total
    ));

    out.push_str(
        "# HELP solanacdn_race_wins_total Number of observed pairs where a source arrived first\n",
    );
    out.push_str("# TYPE solanacdn_race_wins_total counter\n");
    out.push_str(&format!(
        "solanacdn_race_wins_total{{winner=\"solanacdn\"}} {}\n",
        race.wins_solanacdn_total
    ));
    out.push_str(&format!(
        "solanacdn_race_wins_total{{winner=\"gossip\"}} {}\n",
        race.wins_gossip_total
    ));

    out.push_str("# HELP solanacdn_race_ties_total Number of observed pairs with identical first-seen timestamps\n");
    out.push_str("# TYPE solanacdn_race_ties_total counter\n");
    out.push_str(&format!("solanacdn_race_ties_total {}\n", race.ties_total));

    out.push_str(
        "# HELP solanacdn_race_lead_seconds Lead time where winner arrived before loser\n",
    );
    out.push_str("# TYPE solanacdn_race_lead_seconds histogram\n");
    if let Some(hist) = race.histogram {
        for (winner, buckets) in hist.lead_bucket_counts_by_winner {
            let winner_label = winner.as_str();
            for (i, bound_ms) in RACE_LEAD_BUCKETS_MS.iter().enumerate() {
                let le = (*bound_ms as f64) / 1000.0;
                out.push_str(&format!(
                    "solanacdn_race_lead_seconds_bucket{{winner=\"{}\",le=\"{:.3}\"}} {}\n",
                    winner_label, le, buckets[i]
                ));
            }

            let count = hist
                .lead_count_by_winner
                .iter()
                .find(|(w, _)| *w == winner)
                .map(|(_, v)| *v)
                .unwrap_or(0);
            let sum_ms = hist
                .lead_sum_ms_by_winner
                .iter()
                .find(|(w, _)| *w == winner)
                .map(|(_, v)| *v)
                .unwrap_or(0);

            out.push_str(&format!(
                "solanacdn_race_lead_seconds_bucket{{winner=\"{}\",le=\"+Inf\"}} {}\n",
                winner_label, count
            ));
            out.push_str(&format!(
                "solanacdn_race_lead_seconds_sum{{winner=\"{}\"}} {}\n",
                winner_label,
                (sum_ms as f64) / 1000.0
            ));
            out.push_str(&format!(
                "solanacdn_race_lead_seconds_count{{winner=\"{}\"}} {}\n",
                winner_label, count
            ));
        }

        out.push_str("# HELP solanacdn_race_lead_seconds_quantile Approximate lead-time quantiles (derived from histogram buckets)\n");
        out.push_str("# TYPE solanacdn_race_lead_seconds_quantile gauge\n");
        let lead_bounds_ms: [i64; RACE_LEAD_BUCKETS_MS.len()] =
            std::array::from_fn(|i| RACE_LEAD_BUCKETS_MS[i] as i64);
        for winner in [RaceSource::SolanaCdn, RaceSource::Gossip] {
            let winner_label = winner.as_str();
            let buckets = hist
                .lead_bucket_counts_by_winner
                .iter()
                .find(|(w, _)| *w == winner)
                .map(|(_, v)| v)
                .expect("winner buckets must exist");
            let count = hist
                .lead_count_by_winner
                .iter()
                .find(|(w, _)| *w == winner)
                .map(|(_, v)| *v)
                .unwrap_or(0);
            for (q_label, q) in [("0.50", 0.50), ("0.95", 0.95), ("0.99", 0.99)] {
                let value = histogram_quantile_seconds(q, 0, &lead_bounds_ms, &buckets[..], count)
                    .unwrap_or(0.0);
                out.push_str(&format!(
                    "solanacdn_race_lead_seconds_quantile{{winner=\"{}\",quantile=\"{}\"}} {:.6}\n",
                    winner_label, q_label, value
                ));
            }
        }

        out.push_str("# HELP solanacdn_race_delta_seconds Signed delta between first-seen times (solanacdn_first - gossip_first); negative means SolanaCDN arrived first\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds histogram\n");
        for (i, bound_ms) in RACE_DELTA_BUCKETS_MS.iter().enumerate() {
            let le = (*bound_ms as f64) / 1000.0;
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_bucket{{le=\"{:.3}\"}} {}\n",
                le, hist.delta_bucket_counts[i]
            ));
        }
        out.push_str(&format!(
            "solanacdn_race_delta_seconds_bucket{{le=\"+Inf\"}} {}\n",
            hist.delta_count
        ));
        out.push_str(&format!(
            "solanacdn_race_delta_seconds_sum {}\n",
            (hist.delta_sum_ms as f64) / 1000.0
        ));
        out.push_str(&format!(
            "solanacdn_race_delta_seconds_count {}\n",
            hist.delta_count
        ));

        out.push_str("# HELP solanacdn_race_delta_seconds_quantile Approximate signed-delta quantiles (derived from histogram buckets)\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_quantile gauge\n");
        for (q_label, q) in [("0.50", 0.50), ("0.95", 0.95), ("0.99", 0.99)] {
            let value = histogram_quantile_seconds(
                q,
                RACE_DELTA_BUCKETS_MS[0],
                &RACE_DELTA_BUCKETS_MS,
                &hist.delta_bucket_counts[..],
                hist.delta_count,
            )
            .unwrap_or(0.0);
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_quantile{{quantile=\"{}\"}} {:.6}\n",
                q_label, value
            ));
        }

        out.push_str("# HELP solanacdn_race_delta_seconds_by_pop_endpoint Signed delta segmented by SolanaCDN POP endpoint\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_by_pop_endpoint histogram\n");
        for (ep, seg) in &hist.delta_by_pop_endpoint {
            let ep = prometheus_escape_label_value(&ep.to_string());
            for (i, bound_ms) in RACE_DELTA_BUCKETS_MS.iter().enumerate() {
                let le = (*bound_ms as f64) / 1000.0;
                out.push_str(&format!(
                    "solanacdn_race_delta_seconds_by_pop_endpoint_bucket{{pop_endpoint=\"{}\",le=\"{:.3}\"}} {}\n",
                    ep, le, seg.bucket_counts[i]
                ));
            }
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_pop_endpoint_bucket{{pop_endpoint=\"{}\",le=\"+Inf\"}} {}\n",
                ep, seg.count
            ));
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_pop_endpoint_sum{{pop_endpoint=\"{}\"}} {}\n",
                ep,
                (seg.sum_ms as f64) / 1000.0
            ));
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_pop_endpoint_count{{pop_endpoint=\"{}\"}} {}\n",
                ep, seg.count
            ));
        }

        out.push_str("# HELP solanacdn_race_delta_seconds_by_hour_utc Signed delta segmented by UTC hour-of-day (00-23)\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_by_hour_utc histogram\n");
        for (hour, seg) in hist.delta_by_hour_utc.iter().enumerate() {
            let hour_label = format!("{hour:02}");
            for (i, bound_ms) in RACE_DELTA_BUCKETS_MS.iter().enumerate() {
                let le = (*bound_ms as f64) / 1000.0;
                out.push_str(&format!(
                    "solanacdn_race_delta_seconds_by_hour_utc_bucket{{hour_utc=\"{}\",le=\"{:.3}\"}} {}\n",
                    hour_label, le, seg.bucket_counts[i]
                ));
            }
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_hour_utc_bucket{{hour_utc=\"{}\",le=\"+Inf\"}} {}\n",
                hour_label, seg.count
            ));
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_hour_utc_sum{{hour_utc=\"{}\"}} {}\n",
                hour_label,
                (seg.sum_ms as f64) / 1000.0
            ));
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_by_hour_utc_count{{hour_utc=\"{}\"}} {}\n",
                hour_label, seg.count
            ));
        }
    } else {
        for winner_label in ["solanacdn", "gossip"] {
            for bound_ms in RACE_LEAD_BUCKETS_MS {
                let le = (bound_ms as f64) / 1000.0;
                out.push_str(&format!(
                    "solanacdn_race_lead_seconds_bucket{{winner=\"{}\",le=\"{:.3}\"}} 0\n",
                    winner_label, le
                ));
            }
            out.push_str(&format!(
                "solanacdn_race_lead_seconds_bucket{{winner=\"{}\",le=\"+Inf\"}} 0\n",
                winner_label
            ));
            out.push_str(&format!(
                "solanacdn_race_lead_seconds_sum{{winner=\"{}\"}} 0\n",
                winner_label
            ));
            out.push_str(&format!(
                "solanacdn_race_lead_seconds_count{{winner=\"{}\"}} 0\n",
                winner_label
            ));
        }

        out.push_str("# HELP solanacdn_race_lead_seconds_quantile Approximate lead-time quantiles (derived from histogram buckets)\n");
        out.push_str("# TYPE solanacdn_race_lead_seconds_quantile gauge\n");
        for winner_label in ["solanacdn", "gossip"] {
            for q_label in ["0.50", "0.95", "0.99"] {
                out.push_str(&format!(
                    "solanacdn_race_lead_seconds_quantile{{winner=\"{}\",quantile=\"{}\"}} 0\n",
                    winner_label, q_label
                ));
            }
        }

        out.push_str("# HELP solanacdn_race_delta_seconds Signed delta between first-seen times (solanacdn_first - gossip_first); negative means SolanaCDN arrived first\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds histogram\n");
        for bound_ms in RACE_DELTA_BUCKETS_MS {
            let le = (bound_ms as f64) / 1000.0;
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_bucket{{le=\"{:.3}\"}} 0\n",
                le
            ));
        }
        out.push_str("solanacdn_race_delta_seconds_bucket{le=\"+Inf\"} 0\n");
        out.push_str("solanacdn_race_delta_seconds_sum 0\n");
        out.push_str("solanacdn_race_delta_seconds_count 0\n");

        out.push_str("# HELP solanacdn_race_delta_seconds_quantile Approximate signed-delta quantiles (derived from histogram buckets)\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_quantile gauge\n");
        for q_label in ["0.50", "0.95", "0.99"] {
            out.push_str(&format!(
                "solanacdn_race_delta_seconds_quantile{{quantile=\"{}\"}} 0\n",
                q_label
            ));
        }

        out.push_str("# HELP solanacdn_race_delta_seconds_by_pop_endpoint Signed delta segmented by SolanaCDN POP endpoint\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_by_pop_endpoint histogram\n");

        out.push_str("# HELP solanacdn_race_delta_seconds_by_hour_utc Signed delta segmented by UTC hour-of-day (00-23)\n");
        out.push_str("# TYPE solanacdn_race_delta_seconds_by_hour_utc histogram\n");
    }

    out
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) {
    let headers = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(headers.as_bytes()).await;
    let _ = stream.write_all(body).await;
}

async fn handle_metrics_conn(mut stream: TcpStream, handle: Arc<SolanaCdnHandle>) {
    let mut buf = [0u8; 1024];
    let mut req = Vec::new();
    loop {
        match stream.read(&mut buf).await {
            Ok(0) => return,
            Ok(n) => {
                req.extend_from_slice(&buf[..n]);
                if req.len() > METRICS_HTTP_MAX_REQUEST_BYTES {
                    write_http_response(
                        &mut stream,
                        "413 Payload Too Large",
                        "text/plain; charset=utf-8",
                        b"request too large\n",
                    )
                    .await;
                    return;
                }
                if req.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            Err(_) => return,
        }
    }

    let req_str = String::from_utf8_lossy(&req);
    let first = req_str.lines().next().unwrap_or_default();
    let mut parts = first.split_whitespace();
    let method = parts.next().unwrap_or_default();
    let path = parts.next().unwrap_or_default();

    if method != "GET" {
        write_http_response(
            &mut stream,
            "405 Method Not Allowed",
            "text/plain; charset=utf-8",
            b"method not allowed\n",
        )
        .await;
        return;
    }

    match path {
        "/metrics" => {
            let body = format_prometheus_metrics(handle.as_ref());
            write_http_response(
                &mut stream,
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                body.as_bytes(),
            )
            .await;
        }
        "/solanacdn/status" | "/status" => {
            let status = handle.status_snapshot();
            let body = serde_json::to_vec(&status).unwrap_or_else(|_| b"{}".to_vec());
            write_http_response(
                &mut stream,
                "200 OK",
                "application/json; charset=utf-8",
                &body,
            )
            .await;
        }
        _ => {
            write_http_response(
                &mut stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"not found\n",
            )
            .await;
        }
    }
}

async fn run_metrics_server(
    listen_addr: SocketAddr,
    handle: Arc<SolanaCdnHandle>,
    exit: Arc<AtomicBool>,
) -> Result<(), SolanaCdnError> {
    let listener = TcpListener::bind(listen_addr).await?;
    let bound = listener.local_addr()?;
    info!("solanacdn: metrics listening on http://{bound}/metrics");

    loop {
        if exit.load(Ordering::Relaxed) {
            return Ok(());
        }
        let accept = tokio::select! {
            res = listener.accept() => res,
            _ = tokio::time::sleep(Duration::from_millis(200)) => continue,
        };
        let (stream, _) = match accept {
            Ok(v) => v,
            Err(_) => continue,
        };
        tokio::spawn(handle_metrics_conn(stream, handle.clone()));
    }
}

async fn run(
    cfg: SolanaCdnConfig,
    identity_keypair: Arc<Keypair>,
    exit: Arc<AtomicBool>,
    handle: Arc<SolanaCdnHandle>,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
) -> Result<(), SolanaCdnError> {
    let mut cfg = cfg;

    if cfg.pipe_api_base_url.trim().is_empty() {
        if let Some(v) = first_env(&["SOLANACDN_AGENT_API_BASE", "PIPE_API_BASE"]) {
            cfg.pipe_api_base_url = v;
        }
    }
    if cfg.pipe_api_base_url.trim().is_empty()
        || cfg.pipe_api_base_url.trim() == "https://api.pipedev.network"
    {
        if let Some(v) = read_env_file_value("/etc/solanacdn/agent.env", "SOLANACDN_AGENT_API_BASE")
        {
            cfg.pipe_api_base_url = v;
        }
    }
    cfg.pipe_api_base_url = normalize_base_url(&cfg.pipe_api_base_url);

    if cfg
        .pipe_api_token
        .as_deref()
        .is_none_or(|s| s.trim().is_empty())
    {
        cfg.pipe_api_token = first_env(&["SOLANACDN_AGENT_API_TOKEN", "PIPE_API_KEY"]);
    }
    if cfg
        .pipe_api_token
        .as_deref()
        .is_none_or(|s| s.trim().is_empty())
    {
        cfg.pipe_api_token =
            read_env_file_value("/etc/solanacdn/agent.env", "SOLANACDN_AGENT_API_TOKEN");
    }

    // If the operator hasn't configured a CA bundle for POP TLS verification, attempt to
    // bootstrap it from the Pipe control plane. This keeps "no flags" setups working when POPs
    // use a private CA.
    maybe_bootstrap_pop_tls_from_pipe_api(&mut cfg).await;

    if let Some(listen_addr) = cfg.metrics_listen_addr {
        let handle = handle.clone();
        let exit = exit.clone();
        tokio::spawn(async move {
            if let Err(e) = run_metrics_server(listen_addr, handle, exit).await {
                warn!("solanacdn: metrics server exited with error: {e}");
            }
        });
    }

    let validator_pubkey = PubkeyBytes(identity_keypair.pubkey().to_bytes());
    let validator_pubkey_base58 = validator_pubkey.to_base58();

    let pipe_api = cfg
        .pipe_api_token
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|token| {
            spawn_pipe_pop_session_token_refresher(
                PipeApiClientConfig {
                    base_url: cfg.pipe_api_base_url.clone(),
                    api_key: token.to_string(),
                    timeout: Duration::from_millis(cfg.pipe_api_timeout_ms),
                    tls_insecure_skip_verify: cfg.pipe_api_tls_insecure_skip_verify,
                    tls_ca_cert_path: cfg.pipe_api_tls_ca_cert_path.clone(),
                },
                validator_pubkey_base58.clone(),
                cfg.direct_shreds_from_pop,
            )
        });

    handle.note_pop_endpoints(cfg.pop_endpoints.as_slice());

    let cfg = Arc::new(cfg);
    let auth = Arc::new(AuthContext::new(identity_keypair.clone())?);
    let quic_connect = Arc::new(QuicConnectConfig {
        client_config: make_quic_client_config(&cfg)?,
        server_name: cfg.server_name.clone(),
    });
    let control_tls = make_control_tls_client(&cfg)?;

    let shred_deduper = ShredBatchDeduper::new(8192);

    let (publisher_tx, publisher_rx) = watch::channel::<Option<SocketAddr>>(None);
    let (events_tx, events_rx) = mpsc::unbounded_channel::<SessionEvent>();

    let pipe_session_token_rx = pipe_api.as_ref().map(|p| p.session_token_rx.clone());
    let pipe_pop_endpoints_rx = pipe_api.as_ref().map(|p| p.pop_endpoints_rx.clone());
    let pipe_verify_rx = pipe_api.as_ref().map(|p| p.verify_rx.clone());

    if let Some(verify_rx) = pipe_verify_rx {
        let cfg = cfg.clone();
        let handle = handle.clone();
        let validator_pubkey_base58 = validator_pubkey_base58.clone();
        tokio::spawn(async move {
            let client = match build_pipe_api_http_client(&PipeApiClientConfig {
                base_url: cfg.pipe_api_base_url.clone(),
                api_key: String::new(),
                timeout: Duration::from_millis(cfg.pipe_api_timeout_ms),
                tls_insecure_skip_verify: cfg.pipe_api_tls_insecure_skip_verify,
                tls_ca_cert_path: cfg.pipe_api_tls_ca_cert_path.clone(),
            }) {
                Ok(c) => c,
                Err(e) => {
                    warn!("solanacdn: failed to init Pipe API ingest client: {e}");
                    return;
                }
            };
            run_pipe_ingest_reporter(client, verify_rx, handle, cfg, validator_pubkey_base58).await;
        });
    }

    manage_pop_sessions(
        cfg,
        auth,
        quic_connect,
        control_tls,
        handle,
        inject_tpu,
        inject_tvu,
        inject_gossip,
        inject_vote,
        shred_deduper,
        pipe_session_token_rx,
        pipe_pop_endpoints_rx,
        publisher_tx,
        publisher_rx,
        events_tx,
        events_rx,
        exit,
    )
    .await;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn manage_pop_sessions(
    cfg: Arc<SolanaCdnConfig>,
    auth: Arc<AuthContext>,
    quic_connect: Arc<QuicConnectConfig>,
    control_tls: Option<ControlTlsClient>,
    handle: Arc<SolanaCdnHandle>,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
    shred_deduper: ShredBatchDeduper,
    pipe_session_token_rx: Option<watch::Receiver<Option<String>>>,
    mut pipe_pop_endpoints_rx: Option<watch::Receiver<Vec<SocketAddr>>>,
    publisher_tx: watch::Sender<Option<SocketAddr>>,
    publisher_rx: watch::Receiver<Option<SocketAddr>>,
    session_events_tx: mpsc::UnboundedSender<SessionEvent>,
    mut session_events_rx: mpsc::UnboundedReceiver<SessionEvent>,
    exit: Arc<AtomicBool>,
) {
    let static_endpoints: HashSet<SocketAddr> = cfg.pop_endpoints.iter().copied().collect();
    let preferred = cfg.pop_endpoints.first().copied();

    let mut control_discovered: HashSet<SocketAddr> = HashSet::new();
    let mut pipe_discovered: HashSet<SocketAddr> = pipe_pop_endpoints_rx
        .as_ref()
        .map(|rx| rx.borrow().iter().copied().collect())
        .unwrap_or_default();

    let mut desired: HashSet<SocketAddr> = static_endpoints
        .union(&control_discovered)
        .chain(pipe_discovered.iter())
        .copied()
        .collect();
    let mut sessions: HashMap<SocketAddr, ManagedSession> = HashMap::new();
    let mut connected: HashMap<SocketAddr, ConnectedPop> = HashMap::new();

    let mut exit_tick = tokio::time::interval(Duration::from_millis(200));
    exit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let control_refresh = Duration::from_millis(cfg.control_refresh_ms.max(250));
    let mut control_tick = tokio::time::interval(control_refresh);
    control_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut status_tick = tokio::time::interval(Duration::from_secs(30));
    status_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Spawn initial sessions.
    for endpoint in desired.iter().copied() {
        spawn_session(
            endpoint,
            cfg.clone(),
            auth.clone(),
            quic_connect.clone(),
            handle.clone(),
            inject_tpu,
            inject_tvu,
            inject_gossip,
            inject_vote,
            shred_deduper.clone(),
            pipe_session_token_rx.clone(),
            publisher_rx.clone(),
            session_events_tx.clone(),
            &mut sessions,
        );
    }

    loop {
        tokio::select! {
            _ = exit_tick.tick() => {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
            }
            _ = control_tick.tick(), if cfg.control_endpoint.is_some() => {
                if let Some(control_endpoint) = cfg.control_endpoint {
                    match fetch_pops_from_control(control_endpoint, control_tls.as_ref()).await {
                        Ok(list) => {
                            let discovered: HashSet<SocketAddr> = list.into_iter().collect();
                            if !discovered.is_empty() && discovered != control_discovered {
                                control_discovered = discovered;
                                desired = static_endpoints
                                    .union(&control_discovered)
                                    .chain(pipe_discovered.iter())
                                    .copied()
                                    .collect();
                                handle.note_pop_endpoints(desired.iter().copied().collect::<Vec<_>>().as_slice());
                            }
                        }
                        Err(e) => {
                            debug!("solanacdn: control discovery failed for {control_endpoint}: {e}");
                        }
                    }
                }
            }
            _ = status_tick.tick() => {
                let publisher = *publisher_tx.borrow();
                let mut pops: Vec<SocketAddr> = connected.keys().copied().collect();
                pops.sort();
                let status = handle.status_snapshot();
                info!(
                    "solanacdn: status publisher={:?} connected_pops={} pops={:?} tvu_shred_ingest_mode={:?} tvu_shred_stale={:?} tvu_shred_stale_for_ms={:?} last_shred_slot={:?} last_shred_age_ms={:?} last_accepted_shred_slot={:?} last_accepted_shred_age_ms={:?} rx_shred_payloads_total={} rx_shred_payloads_per_sec={:.1} published_shred_batches_total={} pushed_shred_batches_total={} tunneled_vote_packets_total={} rx_vote_packets_total={} rx_tx_packets_total={}",
                    publisher,
                    pops.len(),
                    pops,
                    status.tvu_shred_ingest_mode,
                    status.tvu_shred_stale,
                    status.tvu_shred_stale_for_ms,
                    status.last_shred_slot,
                    status.last_shred_age_ms,
                    status.last_accepted_shred_slot,
                    status.last_accepted_shred_age_ms,
                    status.rx_shred_payloads_total,
                    status.rx_shred_payloads_per_sec,
                    handle.published_shred_batches.load(Ordering::Relaxed),
                    handle.pushed_shred_batches.load(Ordering::Relaxed),
                    status.tunneled_vote_packets_total,
                    handle.rx_vote_packets.load(Ordering::Relaxed),
                    handle.rx_tx_packets.load(Ordering::Relaxed),
                );
            }
            _ = async {
                if let Some(rx) = pipe_pop_endpoints_rx.as_mut() {
                    let _ = rx.changed().await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                if let Some(rx) = pipe_pop_endpoints_rx.as_ref() {
                    let next: HashSet<SocketAddr> = rx.borrow().iter().copied().collect();
                    if !next.is_empty() && next != pipe_discovered {
                        pipe_discovered = next;
                        desired = static_endpoints
                            .union(&control_discovered)
                            .chain(pipe_discovered.iter())
                            .copied()
                            .collect();
                        handle.note_pop_endpoints(desired.iter().copied().collect::<Vec<_>>().as_slice());
                    }
                }
            }
            ev = session_events_rx.recv() => {
                let Some(ev) = ev else { break; };
                match ev {
                    SessionEvent::Connected{endpoint, udp_enabled} => {
                        connected.insert(endpoint, ConnectedPop { udp_enabled, rtt_ewma_ms: 0, rtt_valid: false });
                        handle.note_connected_pop(endpoint);
                        info!("solanacdn: connected to POP {endpoint} (udp_enabled={udp_enabled})");
                    }
                    SessionEvent::Disconnected{endpoint} => {
                        connected.remove(&endpoint);
                        handle.note_disconnected_pop(endpoint);
                        info!("solanacdn: disconnected from POP {endpoint}");
                    }
                    SessionEvent::RttSample{endpoint, rtt_ms} => {
                        if let Some(info) = connected.get_mut(&endpoint) {
                            let sample = rtt_ms.max(1);
                            if !info.rtt_valid {
                                info.rtt_valid = true;
                                info.rtt_ewma_ms = sample;
                            } else {
                                info.rtt_ewma_ms = (info.rtt_ewma_ms.saturating_mul(7).saturating_add(sample)) / 8;
                            }
                        }
                    }
                }
            }
        }

        // Reconcile sessions for desired endpoints.
        let to_add: Vec<SocketAddr> = desired
            .iter()
            .copied()
            .filter(|e| !sessions.contains_key(e))
            .collect();
        for endpoint in to_add {
            spawn_session(
                endpoint,
                cfg.clone(),
                auth.clone(),
                quic_connect.clone(),
                handle.clone(),
                inject_tpu,
                inject_tvu,
                inject_gossip,
                inject_vote,
                shred_deduper.clone(),
                pipe_session_token_rx.clone(),
                publisher_rx.clone(),
                session_events_tx.clone(),
                &mut sessions,
            );
        }

        let to_remove: Vec<SocketAddr> = sessions
            .keys()
            .copied()
            .filter(|e| !desired.contains(e))
            .collect();
        for endpoint in to_remove {
            if let Some(sess) = sessions.remove(&endpoint) {
                let _ = sess.stop_tx.send(true);
                connected.remove(&endpoint);
            }
        }

        // Select publisher.
        let prefer_udp = cfg.udp_mode != DataPlaneMode::Off;
        let new_publisher = select_publisher(preferred, &desired, &connected, prefer_udp);
        if *publisher_tx.borrow() != new_publisher {
            info!("solanacdn: selected publisher {:?}", new_publisher);
            let _ = publisher_tx.send(new_publisher);
        }

        // Update publisher uplink on the global handle.
        let uplink = new_publisher.and_then(|ep| {
            sessions.get(&ep).map(|s| {
                Arc::new(SessionUplink {
                    tx: s.uplink.clone(),
                })
            })
        });
        handle.set_publisher_uplink(new_publisher, uplink);
    }

    // Stop all sessions.
    for sess in sessions.into_values() {
        let _ = sess.stop_tx.send(true);
    }
    handle.set_publisher_uplink(None, None);
}

fn select_publisher(
    preferred: Option<SocketAddr>,
    desired: &HashSet<SocketAddr>,
    connected: &HashMap<SocketAddr, ConnectedPop>,
    prefer_udp: bool,
) -> Option<SocketAddr> {
    if connected.is_empty() {
        return None;
    }
    if let Some(pref) = preferred {
        if desired.contains(&pref) && connected.contains_key(&pref) {
            return Some(pref);
        }
    }

    let mut candidates: Vec<(SocketAddr, ConnectedPop)> = desired
        .iter()
        .copied()
        .filter_map(|e| connected.get(&e).copied().map(|info| (e, info)))
        .collect();
    if candidates.is_empty() {
        return None;
    }
    if prefer_udp {
        let has_udp = candidates.iter().any(|(_e, info)| info.udp_enabled);
        if has_udp {
            candidates.retain(|(_e, info)| info.udp_enabled);
        }
    }
    candidates.sort_by_key(|(e, info)| {
        if info.rtt_valid {
            (0u8, info.rtt_ewma_ms, *e)
        } else {
            (1u8, u64::MAX, *e)
        }
    });
    candidates.first().map(|(e, _)| *e)
}

#[allow(clippy::too_many_arguments)]
fn spawn_session(
    endpoint: SocketAddr,
    cfg: Arc<SolanaCdnConfig>,
    auth: Arc<AuthContext>,
    quic_connect: Arc<QuicConnectConfig>,
    handle: Arc<SolanaCdnHandle>,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
    shred_deduper: ShredBatchDeduper,
    pipe_session_token_rx: Option<watch::Receiver<Option<String>>>,
    publisher_rx: watch::Receiver<Option<SocketAddr>>,
    session_events_tx: mpsc::UnboundedSender<SessionEvent>,
    sessions: &mut HashMap<SocketAddr, ManagedSession>,
) {
    let capacity = cfg.shreds_queue_len.max(cfg.votes_queue_len).max(1024);
    let (uplink_tx, uplink_rx) = mpsc::channel::<UplinkMsg>(capacity);
    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(run_pop_session_forever(
        endpoint,
        cfg,
        auth,
        quic_connect,
        handle,
        uplink_rx,
        inject_tpu,
        inject_tvu,
        inject_gossip,
        inject_vote,
        shred_deduper,
        pipe_session_token_rx,
        publisher_rx,
        session_events_tx,
        stop_rx,
    ));
    sessions.insert(
        endpoint,
        ManagedSession {
            uplink: uplink_tx,
            stop_tx,
        },
    );
}

#[allow(clippy::too_many_arguments)]
async fn run_pop_session_forever(
    endpoint: SocketAddr,
    cfg: Arc<SolanaCdnConfig>,
    auth: Arc<AuthContext>,
    quic_connect: Arc<QuicConnectConfig>,
    handle: Arc<SolanaCdnHandle>,
    mut uplink_rx: mpsc::Receiver<UplinkMsg>,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
    shred_deduper: ShredBatchDeduper,
    pipe_session_token_rx: Option<watch::Receiver<Option<String>>>,
    publisher_rx: watch::Receiver<Option<SocketAddr>>,
    session_events_tx: mpsc::UnboundedSender<SessionEvent>,
    mut stop_rx: watch::Receiver<bool>,
) {
    let mut backoff = Duration::from_millis(200);
    loop {
        if *stop_rx.borrow() {
            return;
        }
        match run_pop_session(
            endpoint,
            cfg.clone(),
            auth.clone(),
            quic_connect.clone(),
            handle.clone(),
            &mut uplink_rx,
            inject_tpu,
            inject_tvu,
            inject_gossip,
            inject_vote,
            shred_deduper.clone(),
            pipe_session_token_rx.clone(),
            publisher_rx.clone(),
            session_events_tx.clone(),
            stop_rx.clone(),
        )
        .await
        {
            Ok(()) => {}
            Err(e) => debug!("solanacdn: session {endpoint} ended with error: {e}"),
        }

        tokio::select! {
            _ = stop_rx.changed() => return,
            _ = tokio::time::sleep(backoff) => {}
        }
        backoff = backoff.saturating_mul(2).min(Duration::from_secs(5));
    }
}

async fn wait_for_pipe_session_token(
    token_rx: &mut watch::Receiver<Option<String>>,
    mut stop_rx: watch::Receiver<bool>,
) -> Option<String> {
    loop {
        if *stop_rx.borrow() {
            return None;
        }
        if let Some(token) = token_rx.borrow().clone() {
            let trimmed = token.trim().to_string();
            if !trimmed.is_empty() {
                return Some(trimmed);
            }
        }
        tokio::select! {
            _ = stop_rx.changed() => {}
            res = token_rx.changed() => {
                if res.is_err() {
                    return None;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_pop_session(
    endpoint: SocketAddr,
    cfg: Arc<SolanaCdnConfig>,
    auth: Arc<AuthContext>,
    quic_connect: Arc<QuicConnectConfig>,
    handle: Arc<SolanaCdnHandle>,
    uplink_rx: &mut mpsc::Receiver<UplinkMsg>,
    inject_tpu: SocketAddr,
    inject_tvu: SocketAddr,
    inject_gossip: SocketAddr,
    inject_vote: SocketAddr,
    shred_deduper: ShredBatchDeduper,
    mut pipe_session_token_rx: Option<watch::Receiver<Option<String>>>,
    publisher_rx: watch::Receiver<Option<SocketAddr>>,
    session_events_tx: mpsc::UnboundedSender<SessionEvent>,
    mut stop_rx: watch::Receiver<bool>,
) -> Result<(), SolanaCdnError> {
    let pipe_session_token = if let Some(rx) = pipe_session_token_rx.as_mut() {
        wait_for_pipe_session_token(rx, stop_rx.clone()).await
    } else {
        None
    };
    if *stop_rx.borrow() {
        return Ok(());
    }

    let mut quic = Endpoint::client(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))?;
    quic.set_default_client_config(quic_connect.client_config.clone());
    let conn = quic
        .connect(endpoint, quic_connect.server_name.as_str())
        .map_err(|e| SolanaCdnError::QuicConnect(e.to_string()))?
        .await
        .map_err(|e| SolanaCdnError::QuicConnect(e.to_string()))?;

    let (mut ctrl_send, mut ctrl_recv) = conn
        .open_bi()
        .await
        .map_err(|e| SolanaCdnError::QuicConnect(format!("open_bi(control): {e}")))?;

    // Auth on control stream
    let auth_req = auth.build_auth_request()?;
    match pipe_session_token {
        Some(session_token) => {
            write_agent_msg(
                &mut ctrl_send,
                &AgentToPop::AuthWithSessionToken(AuthWithSessionToken {
                    auth: auth_req,
                    session_token,
                }),
            )
            .await?;
        }
        None => {
            write_agent_msg(&mut ctrl_send, &AgentToPop::Auth(auth_req)).await?;
        }
    }
    let auth_ok = match read_pop_msg(&mut ctrl_recv, CTRL_MAX_FRAME_BYTES).await? {
        PopToAgent::AuthOk(ok) => ok,
        PopToAgent::AuthError(err) => {
            if err.message.contains("missing pipe session token") {
                warn!(
                    "solanacdn: POP requires a Pipe session token. Set --solanacdn-api-token (or env SOLANACDN_AGENT_API_TOKEN/PIPE_API_KEY). If running with --solanacdn-only/--solanacdn-hybrid, the validator will not receive POP shreds until a token is configured."
                );
            }
            return Err(SolanaCdnError::AuthFailed(format!(
                "{}: {}",
                err.code, err.message
            )));
        }
        other => {
            return Err(SolanaCdnError::AuthFailed(format!(
                "unexpected auth response: {other:?}"
            )))
        }
    };
    let pop_pubkey = auth_ok.pop_pubkey;

    // Advertise per-session capabilities so POPs can decide whether to use fair TX ordering.
    write_agent_msg(
        &mut ctrl_send,
        &AgentToPop::Capabilities(AgentCapabilities {
            tx_fair_ordering: cfg.tx_fair_ordering,
            tx_fair_fifo_per_origin_flow: cfg.tx_fair_ordering,
            ..AgentCapabilities::default()
        }),
    )
    .await?;

    if cfg.tx_fair_slashing {
        write_agent_msg(&mut ctrl_send, &AgentToPop::SubscribeFairCommits).await?;
    }
    if cfg.tx_fair_slashing && cfg.tx_fair_slashing_witness {
        write_agent_msg(&mut ctrl_send, &AgentToPop::SubscribeFairAcks).await?;
    }
    if cfg.tx_fair_slashing
        && (cfg.tx_fair_slashing_witness
            || cfg.tx_fair_slashing_nonresponse
            || cfg.tx_fair_slashing_publish_witness_memos)
    {
        write_agent_msg(&mut ctrl_send, &AgentToPop::SubscribeFairWitnesses).await?;
    }
    if cfg.tx_fair_slashing && (cfg.tx_fair_slashing_nonresponse || cfg.tx_fair_slashing_witness) {
        write_agent_msg(&mut ctrl_send, &AgentToPop::SubscribeFairRejects).await?;
    }

    let udp_advertised = auth_ok.udp_shreds_port != 0 && auth_ok.udp_votes_port != 0;
    let udp_enabled = match cfg.udp_mode {
        DataPlaneMode::Off => false,
        DataPlaneMode::Auto => udp_advertised,
        DataPlaneMode::Always => {
            if !udp_advertised {
                return Err(SolanaCdnError::AuthFailed(
                    "udp_mode=always but POP did not advertise UDP ports".to_string(),
                ));
            }
            true
        }
    };

    handle.note_pop_endpoint(endpoint);

    let _ = session_events_tx.send(SessionEvent::Connected {
        endpoint,
        udp_enabled,
    });

    let udp_token = auth_ok.udp_token;
    let pop_shreds_addr = SocketAddr::new(endpoint.ip(), auth_ok.udp_shreds_port);
    let pop_votes_addr = SocketAddr::new(endpoint.ip(), auth_ok.udp_votes_port);

    let udp_shreds = if udp_enabled {
        Some(Arc::new(UdpSocket::bind("0.0.0.0:0").await?))
    } else {
        None
    };
    let udp_votes = if udp_enabled {
        Some(Arc::new(UdpSocket::bind("0.0.0.0:0").await?))
    } else {
        None
    };

    if let (Some(shreds), Some(votes)) = (udp_shreds.as_ref(), udp_votes.as_ref()) {
        write_agent_msg(
            &mut ctrl_send,
            &AgentToPop::RegisterUdpPorts {
                shreds_port: shreds.local_addr()?.port(),
                votes_port: votes.local_addr()?.port(),
            },
        )
        .await?;
    }

    let (mut shreds_send, mut shreds_recv) = conn
        .open_bi()
        .await
        .map_err(|e| SolanaCdnError::QuicConnect(format!("open_bi(shreds): {e}")))?;
    write_agent_msg(
        &mut shreds_send,
        &AgentToPop::StreamHello(StreamKind::Shreds),
    )
    .await?;
    let (mut votes_send, mut votes_recv) = conn
        .open_bi()
        .await
        .map_err(|e| SolanaCdnError::QuicConnect(format!("open_bi(votes): {e}")))?;
    write_agent_msg(&mut votes_send, &AgentToPop::StreamHello(StreamKind::Votes)).await?;

    let udp_inject_tpu = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    udp_inject_tpu.connect(inject_tpu).await?;
    let udp_inject_tvu = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    udp_inject_tvu.connect(inject_tvu).await?;
    let udp_inject_gossip = Arc::new(UdpSocket::bind("0.0.0.0:0").await?);
    udp_inject_gossip.connect(inject_gossip).await?;
    let udp_inject_votes = Arc::new(VoteInjectSockets::bind().await?);

    // Register validator ports + direct injection preference.
    let is_publisher = *publisher_rx.borrow() == Some(endpoint);
    let direct_shreds = udp_enabled && cfg.direct_shreds_from_pop && is_publisher;
    write_agent_msg(
        &mut ctrl_send,
        &AgentToPop::RegisterValidatorPorts {
            tvu_port: inject_tvu.port(),
            gossip_port: inject_gossip.port(),
            direct_shreds,
        },
    )
    .await?;

    if cfg.subscribe_shreds && is_publisher {
        write_agent_msg(&mut ctrl_send, &AgentToPop::SubscribeShreds).await?;
    }

    let (ctrl_out_tx, mut ctrl_out_rx) = mpsc::channel::<AgentToPop>(256);
    let ctrl_writer_task = tokio::spawn(async move {
        while let Some(msg) = ctrl_out_rx.recv().await {
            if write_agent_msg(&mut ctrl_send, &msg).await.is_err() {
                return;
            }
        }
    });

    // Pipe session token refresher: push AuthRefresh updates on the control stream.
    let auth_refresh_task = if let Some(mut token_rx) = pipe_session_token_rx.take() {
        let ctrl_out_tx = ctrl_out_tx.clone();
        Some(tokio::spawn(async move {
            let mut last_sent: Option<String> = token_rx.borrow().clone();
            loop {
                if token_rx.changed().await.is_err() {
                    return;
                }
                let Some(token) = token_rx.borrow().clone() else {
                    continue;
                };
                let token = token.trim().to_string();
                if token.is_empty() {
                    continue;
                }
                if Some(token.clone()) == last_sent {
                    continue;
                }
                last_sent = Some(token.clone());
                if ctrl_out_tx
                    .send(AgentToPop::AuthRefresh(AuthRefresh {
                        session_token: token,
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }))
    } else {
        None
    };

    let (shreds_out_tx, mut shreds_out_rx) = mpsc::channel::<AgentToPop>(4096);
    let shreds_writer_task = tokio::spawn(async move {
        while let Some(msg) = shreds_out_rx.recv().await {
            if write_agent_msg(&mut shreds_send, &msg).await.is_err() {
                return;
            }
        }
    });

    let (votes_out_tx, mut votes_out_rx) = mpsc::channel::<AgentToPop>(1024);
    let votes_writer_task = tokio::spawn(async move {
        while let Some(msg) = votes_out_rx.recv().await {
            if write_agent_msg(&mut votes_send, &msg).await.is_err() {
                return;
            }
        }
    });

    let last_hb_sent_ms = Arc::new(AtomicU64::new(0));

    // Heartbeat loop (control stream).
    let hb_task = {
        let ctrl_out_tx = ctrl_out_tx.clone();
        let hb_handle = handle.clone();
        let hb_last_sent_ms = last_hb_sent_ms.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let now = now_ms();
                hb_last_sent_ms.store(now, Ordering::Relaxed);
                let hb = Heartbeat {
                    now_ms: now,
                    stats: hb_handle.heartbeat_stats(),
                };
                if ctrl_out_tx.send(AgentToPop::Heartbeat(hb)).await.is_err() {
                    return;
                }
            }
        })
    };

    // Publisher watcher: subscribe/unsubscribe + direct_shreds toggle.
    let publisher_task = {
        let mut publisher_rx = publisher_rx.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        let cfg = cfg.clone();
        tokio::spawn(async move {
            let mut last_is_publisher = is_publisher;
            loop {
                let now_is_publisher = *publisher_rx.borrow() == Some(endpoint);
                if now_is_publisher != last_is_publisher {
                    last_is_publisher = now_is_publisher;
                    if cfg.subscribe_shreds {
                        let msg = if now_is_publisher {
                            AgentToPop::SubscribeShreds
                        } else {
                            AgentToPop::UnsubscribeShreds
                        };
                        let _ = ctrl_out_tx.send(msg).await;
                    }
                    if cfg.direct_shreds_from_pop && udp_enabled {
                        let _ = ctrl_out_tx
                            .send(AgentToPop::RegisterValidatorPorts {
                                tvu_port: inject_tvu.port(),
                                gossip_port: inject_gossip.port(),
                                direct_shreds: now_is_publisher,
                            })
                            .await;
                    }
                }
                if publisher_rx.changed().await.is_err() {
                    return;
                }
            }
        })
    };

    // Control stream reader.
    let ctrl_reader_task = {
        let mut publisher_rx = publisher_rx.clone();
        let shred_deduper = shred_deduper.clone();
        let udp_inject_tpu = udp_inject_tpu.clone();
        let udp_inject_tvu = udp_inject_tvu.clone();
        let udp_inject_gossip = udp_inject_gossip.clone();
        let udp_inject_votes = udp_inject_votes.clone();
        let last_hb_sent_ms = last_hb_sent_ms.clone();
        let session_events_tx = session_events_tx.clone();
        let cfg = cfg.clone();
        let auth = auth.clone();
        let handle = handle.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        tokio::spawn(async move {
            loop {
                let msg = match read_pop_msg(&mut ctrl_recv, CTRL_MAX_FRAME_BYTES).await {
                    Ok(v) => v,
                    Err(_) => return,
                };
                handle_pop_msg(
                    endpoint,
                    pop_pubkey,
                    &cfg,
                    auth.as_ref(),
                    &handle,
                    &ctrl_out_tx,
                    &mut publisher_rx,
                    &shred_deduper,
                    &udp_inject_tpu,
                    &udp_inject_tvu,
                    &udp_inject_gossip,
                    inject_vote,
                    &udp_inject_votes,
                    &session_events_tx,
                    &last_hb_sent_ms,
                    msg,
                )
                .await;
            }
        })
    };

    // Shreds stream reader.
    let shreds_reader_task = {
        let mut publisher_rx = publisher_rx.clone();
        let shred_deduper = shred_deduper.clone();
        let udp_inject_tpu = udp_inject_tpu.clone();
        let udp_inject_tvu = udp_inject_tvu.clone();
        let udp_inject_gossip = udp_inject_gossip.clone();
        let udp_inject_votes = udp_inject_votes.clone();
        let last_hb_sent_ms = last_hb_sent_ms.clone();
        let session_events_tx = session_events_tx.clone();
        let cfg = cfg.clone();
        let auth = auth.clone();
        let handle = handle.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        tokio::spawn(async move {
            loop {
                // Avoid processing shreds when not the selected publisher (reduces attack surface
                // and CPU for non-publisher sessions).
                while *publisher_rx.borrow() != Some(endpoint) {
                    if publisher_rx.changed().await.is_err() {
                        return;
                    }
                }

                tokio::select! {
                    res = read_pop_msg(&mut shreds_recv, SHREDS_MAX_FRAME_BYTES) => {
                        let msg = match res {
                            Ok(v) => v,
                            Err(_) => return,
                        };
                        if !matches!(msg, PopToAgent::PushShredBatch(_)) {
                            handle
                                .dropped_quic_shreds_unexpected_msg
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        handle_pop_msg(
                            endpoint,
                            pop_pubkey,
                            &cfg,
                            auth.as_ref(),
                            &handle,
                            &ctrl_out_tx,
                            &mut publisher_rx,
                            &shred_deduper,
                            &udp_inject_tpu,
                            &udp_inject_tvu,
                            &udp_inject_gossip,
                            inject_vote,
                            &udp_inject_votes,
                            &session_events_tx,
                            &last_hb_sent_ms,
                            msg,
                        )
                        .await;
                    }
                    changed = publisher_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
        })
    };

    // Votes stream reader.
    let votes_reader_task = {
        let mut publisher_rx = publisher_rx.clone();
        let shred_deduper = shred_deduper.clone();
        let udp_inject_tpu = udp_inject_tpu.clone();
        let udp_inject_tvu = udp_inject_tvu.clone();
        let udp_inject_gossip = udp_inject_gossip.clone();
        let udp_inject_votes = udp_inject_votes.clone();
        let last_hb_sent_ms = last_hb_sent_ms.clone();
        let session_events_tx = session_events_tx.clone();
        let cfg = cfg.clone();
        let auth = auth.clone();
        let handle = handle.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        tokio::spawn(async move {
            loop {
                // Avoid processing vote downlink when not the selected publisher.
                while *publisher_rx.borrow() != Some(endpoint) {
                    if publisher_rx.changed().await.is_err() {
                        return;
                    }
                }

                tokio::select! {
                    res = read_pop_msg(&mut votes_recv, VOTES_MAX_FRAME_BYTES) => {
                        let msg = match res {
                            Ok(v) => v,
                            Err(_) => return,
                        };
                        if !matches!(msg, PopToAgent::PushVoteDatagram(_)) {
                            handle
                                .dropped_quic_votes_unexpected_msg
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        handle_pop_msg(
                            endpoint,
                            pop_pubkey,
                            &cfg,
                            auth.as_ref(),
                            &handle,
                            &ctrl_out_tx,
                            &mut publisher_rx,
                            &shred_deduper,
                            &udp_inject_tpu,
                            &udp_inject_tvu,
                            &udp_inject_gossip,
                            inject_vote,
                            &udp_inject_votes,
                            &session_events_tx,
                            &last_hb_sent_ms,
                            msg,
                        )
                        .await;
                    }
                    changed = publisher_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
        })
    };

    // UDP shreds downlink (PushShredBatch + PushShredFecChunk + DirectShredsProbe).
    let udp_shreds_task = if let Some(sock) = udp_shreds.clone() {
        let mut publisher_rx = publisher_rx.clone();
        let shred_deduper = shred_deduper.clone();
        let udp_inject_tpu = udp_inject_tpu.clone();
        let udp_inject_tvu = udp_inject_tvu.clone();
        let udp_inject_gossip = udp_inject_gossip.clone();
        let udp_inject_votes = udp_inject_votes.clone();
        let last_hb_sent_ms = last_hb_sent_ms.clone();
        let session_events_tx = session_events_tx.clone();
        let cfg = cfg.clone();
        let auth = auth.clone();
        let handle = handle.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        Some(tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            let mut fec: HashMap<u64, (solanacdn_protocol::fec::RaptorqDecoder, u64)> =
                HashMap::new();
            let mut last_cleanup_ms = now_ms();
            const FEC_MAX_OBJECTS: usize = 1024;
            const FEC_EXPIRE_MS: u64 = 5_000;
            loop {
                // Avoid spending CPU on shreds UDP downlink when not the selected publisher.
                while *publisher_rx.borrow() != Some(endpoint) {
                    fec.clear();
                    if publisher_rx.changed().await.is_err() {
                        return;
                    }
                }

                tokio::select! {
                    res = sock.recv_from(&mut buf) => {
                        let (len, peer) = match res {
                            Ok(v) => v,
                            Err(_) => return,
                        };
                        let bytes = &buf[..len];
                        if !bytes.starts_with(&udp_token) {
                            continue;
                        }
                        let msg_bytes = match bytes.get(solanacdn_protocol::udp::UDP_TOKEN_LEN..) {
                            Some(v) => v,
                            None => continue,
                        };
                        let msg: PopToAgent = match solanacdn_protocol::frame::decode_envelope(msg_bytes) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        if let PopToAgent::DirectShredsProbe { .. } = msg {
                            // Only learn/update egress IPs when direct POP→validator injection is enabled
                            // for the current publisher session. This avoids letting non-publisher sessions
                            // expand the allowlist.
                            if cfg.direct_shreds_from_pop && *publisher_rx.borrow() == Some(endpoint) {
                                handle.note_pop_egress_ip(peer.ip());
                            }
                            continue;
                        }

                        let now = now_ms();
                        let peer_ip = peer.ip();
                        if peer_ip != endpoint.ip() && !handle.is_pop_egress_ip_fresh(peer_ip, now) {
                            handle
                                .dropped_udp_shreds_unexpected_peer
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }

                        match msg {
                            PopToAgent::PushShredFecChunk(chunk) => {
                                if !cfg.inject_shreds {
                                    continue;
                                }
                                if chunk.packet.len() > 2048 {
                                    continue;
                                }

                                if !fec.contains_key(&chunk.object_id) && fec.len() >= FEC_MAX_OBJECTS {
                                    continue;
                                }
                                let entry = match fec.entry(chunk.object_id) {
                                    std::collections::hash_map::Entry::Occupied(mut entry) => {
                                        entry.get_mut().1 = now;
                                        entry.into_mut()
                                    }
                                    std::collections::hash_map::Entry::Vacant(entry) => {
                                        let dec = match solanacdn_protocol::fec::RaptorqDecoder::new(chunk.oti) {
                                            Ok(v) => v,
                                            Err(_) => continue,
                                        };
                                        entry.insert((dec, now))
                                    }
                                };
                                if let Some(bytes) = entry.0.push_packet(&chunk.packet) {
                                    fec.remove(&chunk.object_id);
                                    if bytes.len() > SHREDS_MAX_FRAME_BYTES {
                                        continue;
                                    }
                                    let decoded: PopToAgent = match solanacdn_protocol::frame::decode_envelope(&bytes) {
                                        Ok(v) => v,
                                        Err(_) => continue,
                                    };
                                    if !matches!(decoded, PopToAgent::PushShredBatch(_)) {
                                        handle
                                            .dropped_udp_shreds_unexpected_msg
                                            .fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                    handle_pop_msg(
                                        endpoint,
                                        pop_pubkey,
                                        &cfg,
                                        auth.as_ref(),
                                        &handle,
                                        &ctrl_out_tx,
                                        &mut publisher_rx,
                                        &shred_deduper,
                                        &udp_inject_tpu,
                                        &udp_inject_tvu,
                                        &udp_inject_gossip,
                                        inject_vote,
                                        &udp_inject_votes,
                                        &session_events_tx,
                                        &last_hb_sent_ms,
                                        decoded,
                                    )
                                    .await;
                                }
                            }
                            other @ PopToAgent::PushShredBatch(_) => {
                                handle_pop_msg(
                                    endpoint,
                                    pop_pubkey,
                                    &cfg,
                                    auth.as_ref(),
                                    &handle,
                                    &ctrl_out_tx,
                                    &mut publisher_rx,
                                    &shred_deduper,
                                    &udp_inject_tpu,
                                    &udp_inject_tvu,
                                    &udp_inject_gossip,
                                    inject_vote,
                                    &udp_inject_votes,
                                    &session_events_tx,
                                    &last_hb_sent_ms,
                                    other,
                                )
                                .await;
                            }
                            _ => {
                                handle
                                    .dropped_udp_shreds_unexpected_msg
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }

                        if now.saturating_sub(last_cleanup_ms) > 1_000 {
                            last_cleanup_ms = now;
                            let expire_before = now.saturating_sub(FEC_EXPIRE_MS);
                            fec.retain(|_, (_, last)| *last >= expire_before);
                        }
                    }
                    changed = publisher_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                        fec.clear();
                    }
                }
            }
        }))
    } else {
        None
    };

    // UDP votes downlink.
    let udp_votes_task = if let Some(sock) = udp_votes.clone() {
        let mut publisher_rx = publisher_rx.clone();
        let shred_deduper = shred_deduper.clone();
        let udp_inject_tpu = udp_inject_tpu.clone();
        let udp_inject_tvu = udp_inject_tvu.clone();
        let udp_inject_gossip = udp_inject_gossip.clone();
        let udp_inject_votes = udp_inject_votes.clone();
        let last_hb_sent_ms = last_hb_sent_ms.clone();
        let session_events_tx = session_events_tx.clone();
        let cfg = cfg.clone();
        let auth = auth.clone();
        let handle = handle.clone();
        let ctrl_out_tx = ctrl_out_tx.clone();
        Some(tokio::spawn(async move {
            let mut buf = vec![0u8; 2048];
            loop {
                // Avoid spending CPU on votes UDP downlink when not the selected publisher.
                while *publisher_rx.borrow() != Some(endpoint) {
                    if publisher_rx.changed().await.is_err() {
                        return;
                    }
                }

                tokio::select! {
                    res = sock.recv_from(&mut buf) => {
                        let (len, peer) = match res {
                            Ok(v) => v,
                            Err(_) => return,
                        };
                        let bytes = &buf[..len];
                        if !bytes.starts_with(&udp_token) {
                            continue;
                        }
                        let msg_bytes = match bytes.get(solanacdn_protocol::udp::UDP_TOKEN_LEN..) {
                            Some(v) => v,
                            None => continue,
                        };
                        let msg: PopToAgent = match solanacdn_protocol::frame::decode_envelope(msg_bytes) {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        let now = now_ms();
                        let peer_ip = peer.ip();
                        if peer_ip != endpoint.ip() && !handle.is_pop_egress_ip_fresh(peer_ip, now) {
                            handle
                                .dropped_udp_votes_unexpected_peer
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        if !matches!(msg, PopToAgent::PushVoteDatagram(_)) {
                            handle
                                .dropped_udp_votes_unexpected_msg
                                .fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        handle_pop_msg(
                            endpoint,
                            pop_pubkey,
                            &cfg,
                            auth.as_ref(),
                            &handle,
                            &ctrl_out_tx,
                            &mut publisher_rx,
                            &shred_deduper,
                            &udp_inject_tpu,
                            &udp_inject_tvu,
                            &udp_inject_gossip,
                            inject_vote,
                            &udp_inject_votes,
                            &session_events_tx,
                            &last_hb_sent_ms,
                            msg,
                        )
                        .await;
                    }
                    changed = publisher_rx.changed() => {
                        if changed.is_err() {
                            return;
                        }
                    }
                }
            }
        }))
    } else {
        None
    };

    // Main loop: exit when connection closes or stop requested.
    let conn_closed = conn.closed();
    tokio::pin!(conn_closed);
    loop {
        tokio::select! {
            _ = &mut conn_closed => break,
            _ = stop_rx.changed() => break,
            msg = uplink_rx.recv() => {
                let Some(msg) = msg else { break; };
                if *publisher_rx.borrow() != Some(endpoint) {
                    continue;
                }
                match msg {
                    UplinkMsg::Shred(shred) => {
                        if !cfg.publish_shreds || shred.payload.is_empty() {
                            continue;
                        }
                        let batch = make_single_shred_batch(shred.kind, shred.payload);
                        let msg = AgentToPop::PublishShredBatch(batch);

                        if cfg.udp_mode != DataPlaneMode::Off {
                            if let Some(sock) = udp_shreds.as_ref() {
                                if let Ok(bytes) =
                                    solanacdn_protocol::udp::encode_udp_datagram(udp_token, &msg)
                                {
                                    if sock.send_to(&bytes, pop_shreds_addr).await.is_ok() {
                                        handle
                                            .published_shred_batches
                                            .fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                }
                            }
                        }

                        let _ = shreds_out_tx.send(msg).await;
                        handle.published_shred_batches.fetch_add(1, Ordering::Relaxed);
                    }
                    UplinkMsg::Vote(vote) => {
                        if !cfg.vote_tunnel || vote.payload.is_empty() {
                            continue;
                        }
                        handle.note_vote_tunnel_allowed_dst(vote.dst, now_ms());
                        let dg = VoteDatagram {
                            flow_id: vote_flow_id(&vote.dst),
                            src: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
                            dst: vote.dst,
                            payload: vote.payload.to_vec(),
                        };
                        let msg = AgentToPop::PublishVoteDatagram(dg);

                        if cfg.udp_mode != DataPlaneMode::Off {
                            if let Some(sock) = udp_votes.as_ref() {
                                if let Ok(bytes) =
                                    solanacdn_protocol::udp::encode_udp_datagram(udp_token, &msg)
                                {
                                    if sock.send_to(&bytes, pop_votes_addr).await.is_ok() {
                                        handle
                                            .tunneled_vote_packets
                                            .fetch_add(1, Ordering::Relaxed);
                                        continue;
                                    }
                                }
                            }
                        }

                        let _ = votes_out_tx.send(msg).await;
                        handle.tunneled_vote_packets.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    let _ = session_events_tx.send(SessionEvent::Disconnected { endpoint });

    ctrl_writer_task.abort();
    shreds_writer_task.abort();
    votes_writer_task.abort();
    hb_task.abort();
    if let Some(t) = auth_refresh_task {
        t.abort();
    }
    publisher_task.abort();
    ctrl_reader_task.abort();
    shreds_reader_task.abort();
    votes_reader_task.abort();
    if let Some(t) = udp_shreds_task {
        t.abort();
    }
    if let Some(t) = udp_votes_task {
        t.abort();
    }

    Ok(())
}

async fn handle_pop_msg(
    endpoint: SocketAddr,
    pop_pubkey: PubkeyBytes,
    cfg: &SolanaCdnConfig,
    auth: &AuthContext,
    handle: &SolanaCdnHandle,
    ctrl_out_tx: &mpsc::Sender<AgentToPop>,
    publisher_rx: &mut watch::Receiver<Option<SocketAddr>>,
    shred_deduper: &ShredBatchDeduper,
    udp_inject_tpu: &UdpSocket,
    udp_inject_tvu: &UdpSocket,
    udp_inject_gossip: &UdpSocket,
    inject_vote: SocketAddr,
    udp_inject_votes: &VoteInjectSockets,
    session_events_tx: &mpsc::UnboundedSender<SessionEvent>,
    last_hb_sent_ms: &AtomicU64,
    msg: PopToAgent,
) {
    match msg {
        PopToAgent::HeartbeatAck(_ack) => {
            let sent_at = last_hb_sent_ms.load(Ordering::Relaxed);
            if sent_at != 0 {
                let rtt_ms = now_ms().saturating_sub(sent_at);
                let _ = session_events_tx.send(SessionEvent::RttSample { endpoint, rtt_ms });
            }
        }
        PopToAgent::PushShredBatch(batch) => {
            if !cfg.inject_shreds {
                return;
            }
            if *publisher_rx.borrow() != Some(endpoint) {
                return;
            }
            if batch.shreds.len() > PUSH_SHRED_BATCH_MAX_SHREDS {
                handle
                    .dropped_shred_batches_oversized
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            if !shred_deduper.insert_if_new(batch.batch_id) {
                return;
            }
            handle.pushed_shred_batches.fetch_add(1, Ordering::Relaxed);
            let mut rx_bytes: usize = 0;
            let mut rx_shreds: u64 = 0;
            let mut max_slot: Option<u64> = None;
            for shred in &batch.shreds {
                if shred.payload.is_empty() {
                    continue;
                }
                if shred.payload.len() > PACKET_DATA_SIZE {
                    continue;
                }
                if handle.race_enabled() {
                    if let Some(shred_id) =
                        solana_ledger::shred::layout::get_shred_id(shred.payload.as_slice())
                    {
                        handle.note_race_observation_from_pop(shred_id, endpoint);
                    }
                }
                rx_shreds = rx_shreds.saturating_add(1);
                rx_bytes = rx_bytes.saturating_add(shred.payload.len());
                max_slot = Some(max_slot.unwrap_or(0).max(shred.id.slot));
                let dst = match shred.kind {
                    ShredKind::Tvu => udp_inject_tvu,
                    ShredKind::Gossip => udp_inject_gossip,
                };
                let _ = dst.send(&shred.payload).await;
            }
            handle.note_solanacdn_shreds_rx(rx_bytes, rx_shreds, max_slot);
        }
        PopToAgent::PushVoteDatagram(dg) => {
            if !cfg.vote_tunnel {
                return;
            }
            if *publisher_rx.borrow() != Some(endpoint) {
                return;
            }
            handle.rx_vote_packets.fetch_add(1, Ordering::Relaxed);
            if dg.payload.is_empty() {
                return;
            }
            let now = now_ms();
            if dg.payload.len() > PACKET_DATA_SIZE {
                handle
                    .dropped_vote_datagrams
                    .fetch_add(1, Ordering::Relaxed);
                handle
                    .dropped_vote_datagrams_oversized_payload
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            if dg.dst.port() != inject_vote.port() {
                handle
                    .dropped_vote_datagrams
                    .fetch_add(1, Ordering::Relaxed);
                handle
                    .dropped_vote_datagrams_unexpected_dst
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            let vote_program_id = solana_vote_program::id().to_bytes();
            if !wire_tx_has_program_id(dg.payload.as_slice(), &vote_program_id) {
                handle
                    .dropped_vote_datagrams
                    .fetch_add(1, Ordering::Relaxed);
                handle
                    .dropped_vote_datagrams_invalid_payload
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            if handle.should_dedup_vote_payload(inject_vote, &dg.payload, now) {
                handle
                    .dropped_vote_datagrams
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            if udp_inject_votes
                .send_to(&dg.payload, inject_vote)
                .await
                .is_err()
            {
                handle
                    .dropped_vote_datagrams
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        PopToAgent::RelayTransaction(tx) => {
            handle.rx_tx_packets.fetch_add(1, Ordering::Relaxed);
            if cfg.tx_fair_ordering {
                handle
                    .tx_relay_dropped_fair_mode
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            if tx.payload.is_empty() || tx.payload.len() > PACKET_DATA_SIZE {
                handle.tx_inject_failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
            // Transactions may be routed via multiple POPs (home-POP forwarding, multi-POP, etc),
            // so do not gate this on the current shred/vote publisher selection.
            let now = now_ms();
            let Some(sig) = try_first_signature_bytes_from_wire_tx(tx.payload.as_slice()) else {
                handle.tx_inject_failed.fetch_add(1, Ordering::Relaxed);
                return;
            };
            if handle.should_dedup_tx_sig(sig, now) {
                handle.tx_deduped_packets.fetch_add(1, Ordering::Relaxed);
                return;
            }
            handle.note_dedup_tx_sig(sig, now);
            if udp_inject_tpu.send(&tx.payload).await.is_ok() {
                handle.tx_injected_packets.fetch_add(1, Ordering::Relaxed);
            } else {
                handle.tx_inject_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
        PopToAgent::FairBatch(batch) => {
            let solanacdn_protocol::messages::FairBatch {
                origin_pop_id,
                flow_id,
                batch_id,
                tx_seq_start,
                created_at_ms,
                batch_ms,
                target_slot,
                attestation,
                txs: incoming_txs,
            } = batch;

            if incoming_txs.is_empty() {
                return;
            }

            handle
                .rx_tx_packets
                .fetch_add(incoming_txs.len() as u64, Ordering::Relaxed);
            handle
                .tx_fair_batch_received
                .fetch_add(incoming_txs.len() as u64, Ordering::Relaxed);

            if incoming_txs.len() > FAIR_BATCH_MAX_TXS {
                FAIR_BATCH_DROPPED_TOO_MANY_TXS_TOTAL
                    .fetch_add(incoming_txs.len() as u64, Ordering::Relaxed);
                debug!(
                    "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; tx_count={} over cap={FAIR_BATCH_MAX_TXS}",
                    incoming_txs.len()
                );
                if cfg.tx_fair_ordering {
                    try_send_fair_batch_reject(
                        &ctrl_out_tx,
                        auth,
                        &origin_pop_id,
                        flow_id,
                        batch_id,
                        tx_seq_start,
                        target_slot,
                        FairBatchRejectReason::TooManyTxs,
                    )
                    .await;
                }
                return;
            }

            let now = now_ms();

            if cfg.tx_fair_ordering {
                // Best-effort recent blockhash for injecting on-chain ack/reject memos. Seed from
                // a bank-provided cache so early rejects (before validating any wire tx) can still
                // land on-chain.
                let mut recent_blockhash: Option<solana_hash::Hash> =
                    handle.fair_recent_blockhash();

                if cfg.tx_fair_require_target_slot && target_slot.is_none() {
                    debug!(
                        "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; missing target_slot (required)"
                    );
                    try_send_fair_batch_reject(
                        &ctrl_out_tx,
                        auth,
                        &origin_pop_id,
                        flow_id,
                        batch_id,
                        tx_seq_start,
                        target_slot,
                        FairBatchRejectReason::Unknown,
                    )
                    .await;
                    try_inject_fair_batch_reject_memo(
                        udp_inject_tpu,
                        auth,
                        recent_blockhash,
                        &origin_pop_id,
                        flow_id,
                        batch_id,
                        tx_seq_start,
                        target_slot,
                        FairBatchRejectReason::Unknown,
                    )
                    .await;
                    return;
                }

                if let Err(e) = attestation.verify(pop_pubkey) {
                    debug!(
                        "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; invalid POP attestation: {e}"
                    );
                    try_send_fair_batch_reject(
                        &ctrl_out_tx,
                        auth,
                        &origin_pop_id,
                        flow_id,
                        batch_id,
                        tx_seq_start,
                        target_slot,
                        FairBatchRejectReason::InvalidAttestation,
                    )
                    .await;
                    try_inject_fair_batch_reject_memo(
                        udp_inject_tpu,
                        auth,
                        recent_blockhash,
                        &origin_pop_id,
                        flow_id,
                        batch_id,
                        tx_seq_start,
                        target_slot,
                        FairBatchRejectReason::InvalidAttestation,
                    )
                    .await;
                    return;
                }

                let expected_tx_count: u32 = match incoming_txs.len().try_into() {
                    Ok(v) => v,
                    Err(_) => 0,
                };
                if expected_tx_count == 0 {
                    return;
                }

                let att = &attestation.payload;
                if att.origin_pop_id != origin_pop_id
                    || att.flow_id != flow_id
                    || att.batch_id != batch_id
                    || att.tx_seq_start != tx_seq_start
                    || att.tx_count != expected_tx_count
                    || att.created_at_ms != created_at_ms
                    || att.batch_ms != batch_ms
                    || att.target_slot != target_slot
                {
                    debug!(
                        "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; attestation payload mismatch"
                    );
                    try_send_fair_batch_reject(
                        &ctrl_out_tx,
                        auth,
                        &origin_pop_id,
                        flow_id,
                        batch_id,
                        tx_seq_start,
                        target_slot,
                        FairBatchRejectReason::AttestationMismatch,
                    )
                    .await;
                    try_inject_fair_batch_reject_memo(
                        udp_inject_tpu,
                        auth,
                        recent_blockhash,
                        &origin_pop_id,
                        flow_id,
                        batch_id,
                        tx_seq_start,
                        target_slot,
                        FairBatchRejectReason::AttestationMismatch,
                    )
                    .await;
                    return;
                }

                // In fair mode, the leader must accept/reject the entire POP-attested batch.
                let mut sigs: Vec<solanacdn_protocol::crypto::SignatureBytes> =
                    Vec::with_capacity(incoming_txs.len());
                let mut sig_bytes: Vec<[u8; 64]> = Vec::with_capacity(incoming_txs.len());
                let mut total_bytes: usize = 0;
                let mut seen: HashSet<[u8; 64]> = HashSet::with_capacity(incoming_txs.len());

                for tx in incoming_txs.iter() {
                    if tx.payload.len() > PACKET_DATA_SIZE {
                        FAIR_BATCH_DROPPED_PAYLOAD_TOO_LARGE_TOTAL.fetch_add(1, Ordering::Relaxed);
                        debug!(
                            "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; payload too large (len={}, max={PACKET_DATA_SIZE})",
                            tx.payload.len()
                        );
                        try_send_fair_batch_reject(
                            &ctrl_out_tx,
                            auth,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::InvalidWireTx,
                        )
                        .await;
                        try_inject_fair_batch_reject_memo(
                            udp_inject_tpu,
                            auth,
                            recent_blockhash,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::InvalidWireTx,
                        )
                        .await;
                        return;
                    }

                    total_bytes = total_bytes.saturating_add(tx.payload.len());
                    if total_bytes > FAIR_BATCH_MAX_TOTAL_BYTES {
                        FAIR_BATCH_DROPPED_TOTAL_BYTES_EXCEEDED_TOTAL
                            .fetch_add(incoming_txs.len() as u64, Ordering::Relaxed);
                        debug!(
                            "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; total_bytes exceeded cap={FAIR_BATCH_MAX_TOTAL_BYTES}"
                        );
                        try_send_fair_batch_reject(
                            &ctrl_out_tx,
                            auth,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::TotalBytesExceeded,
                        )
                        .await;
                        try_inject_fair_batch_reject_memo(
                            udp_inject_tpu,
                            auth,
                            recent_blockhash,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::TotalBytesExceeded,
                        )
                        .await;
                        return;
                    }

                    let Some(sig_from_payload) =
                        try_first_signature_bytes_from_wire_tx(tx.payload.as_slice())
                    else {
                        FAIR_BATCH_DROPPED_SIG_MISMATCH_TOTAL.fetch_add(1, Ordering::Relaxed);
                        debug!(
                            "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; unable to parse tx signature from payload"
                        );
                        try_send_fair_batch_reject(
                            &ctrl_out_tx,
                            auth,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::InvalidWireTx,
                        )
                        .await;
                        try_inject_fair_batch_reject_memo(
                            udp_inject_tpu,
                            auth,
                            recent_blockhash,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::InvalidWireTx,
                        )
                        .await;
                        return;
                    };
                    if sig_from_payload != tx.sig.0 {
                        FAIR_BATCH_DROPPED_SIG_MISMATCH_TOTAL.fetch_add(1, Ordering::Relaxed);
                        debug!(
                            "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; FairTx.sig does not match tx payload"
                        );
                        try_send_fair_batch_reject(
                            &ctrl_out_tx,
                            auth,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::TxSigMismatch,
                        )
                        .await;
                        try_inject_fair_batch_reject_memo(
                            udp_inject_tpu,
                            auth,
                            recent_blockhash,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::TxSigMismatch,
                        )
                        .await;
                        return;
                    }
                    if !seen.insert(tx.sig.0) {
                        FAIR_BATCH_DROPPED_DUP_SIG_TOTAL.fetch_add(1, Ordering::Relaxed);
                        debug!(
                            "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; duplicate tx signature in batch"
                        );
                        try_send_fair_batch_reject(
                            &ctrl_out_tx,
                            auth,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::DuplicateSig,
                        )
                        .await;
                        try_inject_fair_batch_reject_memo(
                            udp_inject_tpu,
                            auth,
                            recent_blockhash,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::DuplicateSig,
                        )
                        .await;
                        return;
                    }
                    if handle.should_dedup_tx_sig(tx.sig.0, now) {
                        handle.tx_deduped_packets.fetch_add(1, Ordering::Relaxed);
                        debug!(
                            "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; tx signature already seen"
                        );
                        try_send_fair_batch_reject(
                            &ctrl_out_tx,
                            auth,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::AlreadySeen,
                        )
                        .await;
                        try_inject_fair_batch_reject_memo(
                            udp_inject_tpu,
                            auth,
                            recent_blockhash,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::AlreadySeen,
                        )
                        .await;
                        return;
                    }
                    let Some(blockhash) =
                        verified_recent_blockhash_from_wire_tx(tx.payload.as_slice())
                    else {
                        FAIR_BATCH_DROPPED_INVALID_WIRE_TX_TOTAL.fetch_add(1, Ordering::Relaxed);
                        debug!(
                            "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; failed deserialization/sanitization/sigverify"
                        );
                        try_send_fair_batch_reject(
                            &ctrl_out_tx,
                            auth,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::InvalidWireTx,
                        )
                        .await;
                        try_inject_fair_batch_reject_memo(
                            udp_inject_tpu,
                            auth,
                            recent_blockhash,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            tx_seq_start,
                            target_slot,
                            FairBatchRejectReason::InvalidWireTx,
                        )
                        .await;
                        return;
                    };
                    recent_blockhash.get_or_insert(blockhash);
                    sig_bytes.push(tx.sig.0);
                    sigs.push(tx.sig);
                }

                let tx_merkle_root = fair_merkle_root(sig_bytes.as_slice());
                if tx_merkle_root != att.tx_merkle_root {
                    debug!(
                        "solanacdn: rejecting FairBatch batch_id={batch_id} origin_pop_id={origin_pop_id}; attestation merkle root mismatch"
                    );
                    try_send_fair_batch_reject(
                        &ctrl_out_tx,
                        auth,
                        &origin_pop_id,
                        flow_id,
                        batch_id,
                        tx_seq_start,
                        target_slot,
                        FairBatchRejectReason::MerkleRootMismatch,
                    )
                    .await;
                    try_inject_fair_batch_reject_memo(
                        udp_inject_tpu,
                        auth,
                        recent_blockhash,
                        &origin_pop_id,
                        flow_id,
                        batch_id,
                        tx_seq_start,
                        target_slot,
                        FairBatchRejectReason::MerkleRootMismatch,
                    )
                    .await;
                    return;
                }

                let order_start = tx_seq_start;
                for (idx, tx) in incoming_txs.iter().enumerate() {
                    // `should_dedup_tx_sig()` is authoritative for the fair ordering contract, so
                    // only mark as seen after the full batch is accepted.
                    handle.note_dedup_tx_sig(tx.sig.0, now);
                    let order_ix = order_start.wrapping_add(idx as u64);
                    let priority = u64::MAX.wrapping_sub(order_ix).saturating_sub(1);
                    insert_fair_priority(tx.sig.0, priority);
                }

                if let Some(slot) = target_slot {
                    if let Some(recent_blockhash) = recent_blockhash {
                        if let Some(ack_tx_bytes) = build_fair_ledger_ack_memo_tx(
                            auth,
                            recent_blockhash,
                            slot,
                            &origin_pop_id,
                            flow_id,
                            batch_id,
                            order_start,
                            expected_tx_count,
                            tx_merkle_root,
                        ) {
                            let _ = udp_inject_tpu.send(&ack_tx_bytes).await;
                        }

                        let commit_txs = build_fair_ledger_commit_memo_txs(
                            auth,
                            recent_blockhash,
                            slot,
                            batch_id,
                            order_start,
                            sig_bytes.as_slice(),
                        );
                        for tx_bytes in commit_txs {
                            let _ = udp_inject_tpu.send(&tx_bytes).await;
                        }
                    }
                }

                for tx in incoming_txs.iter() {
                    if udp_inject_tpu.send(&tx.payload).await.is_ok() {
                        handle.tx_injected_packets.fetch_add(1, Ordering::Relaxed);
                        handle
                            .tx_fair_batch_injected
                            .fetch_add(1, Ordering::Relaxed);
                    } else {
                        handle.tx_inject_failed.fetch_add(1, Ordering::Relaxed);
                        handle
                            .tx_fair_batch_inject_failed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }

                let leader_time_ms = now_ms();
                let receipt_payload = FairBatchReceiptCommitPayload {
                    origin_pop_id: origin_pop_id.clone(),
                    flow_id,
                    batch_id,
                    order_start,
                    target_slot,
                    tx_count: expected_tx_count,
                    tx_merkle_root,
                    leader_pubkey: auth.validator_pubkey,
                    leader_time_ms,
                };
                let receipt_commit =
                    match FairBatchReceiptCommit::sign(receipt_payload, &auth.signing_key) {
                        Ok(v) => v,
                        Err(e) => {
                            debug!("solanacdn: failed to sign fair batch ack: {e}");
                            return;
                        }
                    };

                // Explicit ACK stream for slashing/auditing (protocol v7+).
                if target_slot.is_some() {
                    let _ = ctrl_out_tx
                        .send(AgentToPop::FairBatchAck(receipt_commit.clone()))
                        .await;
                }

                let payload = FairBatchCommitPayload {
                    origin_pop_id,
                    flow_id,
                    batch_id,
                    order_start,
                    target_slot,
                    tx_sigs: sigs,
                    leader_pubkey: auth.validator_pubkey,
                    leader_time_ms,
                };
                match FairBatchCommit::sign(payload, &auth.signing_key) {
                    Ok(mut commit) => {
                        commit.receipt_commit = Some(receipt_commit);
                        let _ = ctrl_out_tx.send(AgentToPop::FairBatchCommit(commit)).await;
                    }
                    Err(e) => {
                        debug!("solanacdn: failed to sign fair batch commit: {e}");
                    }
                }
                return;
            }

            // Not in fair mode: best-effort inject without ordering commit.
            let mut deduped: u64 = 0;
            for tx in incoming_txs.iter() {
                if handle.should_dedup_tx_sig(tx.sig.0, now) {
                    deduped = deduped.saturating_add(1);
                    continue;
                }
                handle.note_dedup_tx_sig(tx.sig.0, now);
                if udp_inject_tpu.send(&tx.payload).await.is_ok() {
                    handle.tx_injected_packets.fetch_add(1, Ordering::Relaxed);
                    handle
                        .tx_fair_batch_injected
                        .fetch_add(1, Ordering::Relaxed);
                } else {
                    handle.tx_inject_failed.fetch_add(1, Ordering::Relaxed);
                    handle
                        .tx_fair_batch_inject_failed
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            if deduped > 0 {
                handle
                    .tx_deduped_packets
                    .fetch_add(deduped, Ordering::Relaxed);
            }
        }
        PopToAgent::FairBatchWitness(witness) => {
            handle.fair_batch_witness_rx.fetch_add(1, Ordering::Relaxed);
            if !cfg.tx_fair_slashing
                || (!cfg.tx_fair_slashing_witness
                    && !cfg.tx_fair_slashing_nonresponse
                    && !cfg.tx_fair_slashing_publish_witness_memos)
            {
                return;
            }
            if let Err(e) = witness.verify(pop_pubkey) {
                handle
                    .fair_batch_witness_invalid
                    .fetch_add(1, Ordering::Relaxed);
                debug!("solanacdn: invalid fair witness from {endpoint}: {e}");
                return;
            }
            let witnesser_added = handle.note_fair_batch_witness_for_slashing(pop_pubkey, &witness);
            if cfg.tx_fair_slashing_publish_witness_memos && witnesser_added {
                let recent_blockhash = handle.fair_recent_blockhash();
                try_inject_fair_ledger_witness_memo(
                    udp_inject_tpu,
                    auth,
                    recent_blockhash,
                    pop_pubkey,
                    &witness,
                )
                .await;
            }
        }
        PopToAgent::FairBatchAck(ack) => {
            if !cfg.tx_fair_slashing || !cfg.tx_fair_slashing_witness {
                return;
            }
            if let Err(e) = ack.verify() {
                debug!("solanacdn: invalid fair ack from {endpoint}: {e}");
                return;
            }
            handle.note_fair_batch_ack_for_slashing(&ack);
        }
        PopToAgent::FairBatchReject(reject) => {
            if !cfg.tx_fair_slashing
                || (!cfg.tx_fair_slashing_nonresponse && !cfg.tx_fair_slashing_witness)
            {
                return;
            }
            if let Err(e) = reject.verify() {
                debug!("solanacdn: invalid fair reject from {endpoint}: {e}");
                return;
            }
            handle.note_fair_batch_reject_for_slashing(&reject);
        }
        PopToAgent::FairBatchCommit(commit) => {
            handle.fair_commits_rx.fetch_add(1, Ordering::Relaxed);
            if !cfg.tx_fair_slashing {
                return;
            }
            if let Err(e) = commit.verify() {
                handle.fair_commits_invalid.fetch_add(1, Ordering::Relaxed);
                debug!("solanacdn: invalid fair commit from {endpoint}: {e}");
                return;
            }
            handle.note_fair_commit_for_slashing(&commit);
        }
        PopToAgent::AuthError(err) => {
            debug!(
                "solanacdn: server auth error from {endpoint}: {}: {}",
                err.code, err.message
            );
        }
        _ => {}
    }
}

fn make_single_shred_batch(kind: ShredKind, payload: Bytes) -> ShredBatch {
    let created_at_ms = now_ms();
    let items = &[(kind, payload.clone())];
    let batch_id = compute_shred_batch_id(items);
    let shreds = vec![Shred {
        id: ShredId { slot: 0, index: 0 },
        kind,
        payload: payload.to_vec(),
    }];
    ShredBatch {
        batch_id,
        created_at_ms,
        shreds,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solanacdn_protocol::messages::{
        AuthOk, FairBatchAttestation, FairBatchAttestationPayload, FairBatchWitness,
        FairBatchWitnessPayload,
    };

    fn test_pop_signing_key() -> SigningKey {
        SigningKey::from_bytes(&[7u8; 32])
    }

    fn test_pop_pubkey() -> PubkeyBytes {
        PubkeyBytes::from(test_pop_signing_key().verifying_key())
    }

    #[test]
    fn parse_hex_32_parses_sha256_hex() {
        let hex = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let parsed = parse_hex_32(hex).expect("valid hex");
        assert_eq!(parsed.len(), 32);
        assert!(parse_hex_32("not-hex").is_none());
        assert!(parse_hex_32("ba78").is_none());
    }

    #[test]
    fn sha256_bytes_matches_known_vector() {
        let expected_hex = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";
        let expected = parse_hex_32(expected_hex).expect("expected hex");
        let got = sha256_bytes(b"abc");
        assert_eq!(got, expected);
    }

    #[test]
    fn shred_batch_id_is_stable_and_ordered() {
        let a = (ShredKind::Tvu, Bytes::from_static(b"a"));
        let b = (ShredKind::Tvu, Bytes::from_static(b"b"));

        let id1 = compute_shred_batch_id(&[a.clone(), b.clone()]);
        let id2 = compute_shred_batch_id(&[a.clone(), b.clone()]);
        assert_eq!(id1, id2);

        let id_swapped = compute_shred_batch_id(&[b, a]);
        assert_ne!(id1, id_swapped);
    }

    #[test]
    fn shred_batch_deduper_eviction_allows_reinsert() {
        let deduper = ShredBatchDeduper::new(2);
        assert!(deduper.insert_if_new(1));
        assert!(deduper.insert_if_new(2));
        assert!(!deduper.insert_if_new(1));

        assert!(deduper.insert_if_new(3)); // evicts 1
        assert!(deduper.insert_if_new(1));
    }

    #[test]
    fn publish_discarded_shreds_toggle_is_respected() {
        let src_ip = IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4));

        let mut cfg = SolanaCdnConfig::default();
        cfg.publish_shreds = true;
        cfg.publish_discarded_shreds = false;
        let handle = SolanaCdnHandle::new(cfg);
        let (tx, mut rx) = mpsc::channel::<UplinkMsg>(1);
        handle.set_publisher_uplink(None, Some(Arc::new(SessionUplink { tx })));

        handle.try_publish_tvu_shred(src_ip, Bytes::from_static(b"discarded"), true);
        assert!(rx.try_recv().is_err());

        handle.try_publish_tvu_shred(src_ip, Bytes::from_static(b"kept"), false);
        match rx.try_recv().unwrap() {
            UplinkMsg::Shred(shred) => {
                assert!(matches!(shred.kind, ShredKind::Tvu));
                assert_eq!(shred.payload.as_ref(), b"kept");
            }
            other => panic!("expected shred uplink msg, got {other:?}"),
        }

        let mut cfg = SolanaCdnConfig::default();
        cfg.publish_shreds = true;
        cfg.publish_discarded_shreds = true;
        let handle = SolanaCdnHandle::new(cfg);
        let (tx, mut rx) = mpsc::channel::<UplinkMsg>(1);
        handle.set_publisher_uplink(None, Some(Arc::new(SessionUplink { tx })));

        handle.try_publish_tvu_shred(src_ip, Bytes::from_static(b"discarded_ok"), true);
        match rx.try_recv().unwrap() {
            UplinkMsg::Shred(shred) => {
                assert!(matches!(shred.kind, ShredKind::Tvu));
                assert_eq!(shred.payload.as_ref(), b"discarded_ok");
            }
            other => panic!("expected shred uplink msg, got {other:?}"),
        }
    }

    #[test]
    fn solanacdn_only_shreds_gates_ingest_when_connected() {
        let pop: SocketAddr = "198.51.100.1:4444".parse().unwrap();
        let pop_ip = pop.ip();
        let p2p_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 10));

        let mut cfg = SolanaCdnConfig::default();
        cfg.tvu_shred_ingest_mode = TvuShredIngestMode::SolanaCdnOnly;
        let handle = SolanaCdnHandle::new(cfg);

        // Not connected yet: fallback to normal P2P ingest.
        assert!(handle.should_ingest_tvu_shred(pop_ip));
        assert!(handle.should_ingest_tvu_shred(p2p_ip));

        // Connected: ingest only loopback or known POP sources.
        let (tx, _rx) = mpsc::channel::<UplinkMsg>(1);
        handle.set_publisher_uplink(Some(pop), Some(Arc::new(SessionUplink { tx })));
        handle.note_pop_endpoints(&[pop]);

        assert!(handle.should_ingest_tvu_shred(IpAddr::V4(Ipv4Addr::LOCALHOST)));
        assert!(handle.should_ingest_tvu_shred(pop_ip));
        assert!(!handle.should_ingest_tvu_shred(p2p_ip));
    }

    #[test]
    fn solanacdn_preferred_shreds_falls_back_when_stalled() {
        let pop: SocketAddr = "198.51.100.2:4444".parse().unwrap();
        let pop_ip = pop.ip();
        let p2p_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 11));

        let mut cfg = SolanaCdnConfig::default();
        cfg.tvu_shred_ingest_mode = TvuShredIngestMode::SolanaCdnPreferred;
        cfg.tvu_shred_hybrid_stale_ms = 1_000;
        let handle = SolanaCdnHandle::new(cfg);

        let (tx, _rx) = mpsc::channel::<UplinkMsg>(1);
        handle.set_publisher_uplink(Some(pop), Some(Arc::new(SessionUplink { tx })));
        handle.note_pop_endpoints(&[pop]);

        // Never saw POP shreds => stale => allow P2P.
        assert!(handle.should_ingest_tvu_shred(pop_ip));
        assert!(handle.should_ingest_tvu_shred(p2p_ip));

        // Observing POP delivery is not enough. Shreds must be accepted into the validator
        // pipeline (pass discard checks) for SolanaCDN to be considered healthy.
        handle.note_pop_delivered_shred_with_slot(123, Some(10));
        assert!(handle.should_ingest_tvu_shred(pop_ip));
        assert!(handle.should_ingest_tvu_shred(p2p_ip));

        // Once we accept SolanaCDN shreds recently, gate P2P again.
        handle.note_solanacdn_accepted_shred_with_slot(Some(10));
        assert!(handle.should_ingest_tvu_shred(pop_ip));
        assert!(!handle.should_ingest_tvu_shred(p2p_ip));
    }

    #[test]
    fn fair_slashing_enforcement_gates_vote_withholding() {
        let leader = Pubkey::new_unique();
        let leader_bytes = PubkeyBytes(leader.to_bytes());
        let slot = 42;
        let now = now_ms();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);
        handle.mark_fair_slashed(leader_bytes, slot, 0, now);
        assert!(!handle.fair_slashing_is_slashed_leader(&leader, slot));
        handle.set_tx_fair_slashing_enforce_override(Some(true));
        assert!(handle.fair_slashing_is_slashed_leader(&leader, slot));
        handle.set_tx_fair_slashing_enforce_override(Some(false));
        assert!(!handle.fair_slashing_is_slashed_leader(&leader, slot));
        handle.set_tx_fair_slashing_enforce_override(None);
        assert!(!handle.fair_slashing_is_slashed_leader(&leader, slot));

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_enforce = true;
        let handle = SolanaCdnHandle::new(cfg);
        handle.mark_fair_slashed(leader_bytes, slot, 0, now);
        assert!(handle.fair_slashing_is_slashed_leader(&leader, slot));
        handle.set_tx_fair_slashing_enforce_override(Some(false));
        assert!(!handle.fair_slashing_is_slashed_leader(&leader, slot));
        handle.set_tx_fair_slashing_enforce_override(None);
        assert!(handle.fair_slashing_is_slashed_leader(&leader, slot));
    }

    #[test]
    fn fair_ledger_audit_violation_marks_slashed() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let recent_blockhash = solana_hash::Hash::default();
        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            &[sig_a, sig_b],
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into(), tx_b.into(), tx_a.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);
        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
        assert!(!handle.fair_slashing_is_slashed_leader(&leader, slot));

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_enforce = true;
        let handle = SolanaCdnHandle::new(cfg);
        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert!(handle.fair_slashing_is_slashed_leader(&leader, slot));
    }

    #[test]
    fn fair_ledger_audit_orders_batches_by_order_start_not_memo_position() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let recent_blockhash = solana_hash::Hash::default();
        let slot = 42;

        let batch_a_id = 7u128;
        let batch_b_id = 8u128;
        let order_start_a = 0u64;
        let order_start_b = 2u64;

        let tx_a_signer_1 = Keypair::new();
        let tx_a_1 = Transaction::new(
            &[&tx_a_signer_1],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer_1.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a_1: [u8; 64] = tx_a_1.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_a_signer_2 = Keypair::new();
        let tx_a_2 = Transaction::new(
            &[&tx_a_signer_2],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_a_signer_2.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a_2: [u8; 64] = tx_a_2.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b_1 = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(3)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b_1: [u8; 64] = tx_b_1.signatures[0].as_ref().try_into().expect("sig bytes");

        let commit_a_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_a_id,
            order_start_a,
            &[sig_a_1, sig_a_2],
        );
        assert_eq!(commit_a_txs.len(), 1);
        let commit_a: Transaction = bincode::deserialize(&commit_a_txs[0]).expect("commit tx");

        let commit_b_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_b_id,
            order_start_b,
            &[sig_b_1],
        );
        assert_eq!(commit_b_txs.len(), 1);
        let commit_b: Transaction = bincode::deserialize(&commit_b_txs[0]).expect("commit tx");

        // Commit-memo tx ordering in the ledger is not guaranteed. The audit should order batches
        // by the signed `order_start`, not by the memo-tx position within the slot.
        let entry = solana_entry::entry::Entry {
            transactions: vec![
                commit_b.into(),
                commit_a.into(),
                tx_a_1.into(),
                tx_a_2.into(),
                tx_b_1.into(),
            ],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);
        assert!(handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 0);
    }

    #[test]
    fn fair_ledger_audit_allows_missing_tail_without_overtake() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let recent_blockhash = solana_hash::Hash::default();
        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            &[sig_a, sig_b],
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into(), tx_a.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);
        assert!(handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 0);
    }

    #[test]
    fn fair_ledger_audit_strict_missing_tail_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let recent_blockhash = solana_hash::Hash::default();
        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            &[sig_a, sig_b],
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into(), tx_a.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_strict = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);
        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_strict_rejects_insertion_ahead_of_fair_prefix() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let recent_blockhash = solana_hash::Hash::default();
        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let inserted_signer = Keypair::new();
        let inserted = Transaction::new(
            &[&inserted_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(99)],
                Some(&inserted_signer.pubkey()),
            ),
            recent_blockhash,
        );

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            &[sig_a, sig_b],
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into(), tx_a.into(), inserted.into(), tx_b.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_strict = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);
        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_strict_vote_tx_with_extra_instruction_is_not_exempt() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let recent_blockhash = solana_hash::Hash::default();
        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let inserted_signer = Keypair::new();
        let vote_ix = Instruction {
            program_id: solana_vote_program::id(),
            accounts: Vec::new(),
            data: Vec::new(),
        };
        let non_vote_ix = Instruction {
            program_id: solana_system_program::id(),
            accounts: Vec::new(),
            data: Vec::new(),
        };
        let inserted = Transaction::new(
            &[&inserted_signer],
            Message::new(&[vote_ix, non_vote_ix], Some(&inserted_signer.pubkey())),
            recent_blockhash,
        );

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            &[sig_a, sig_b],
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into(), tx_a.into(), inserted.into(), tx_b.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_strict = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);
        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_missing_chunks_are_inconclusive() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let recent_blockhash = solana_hash::Hash::default();
        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;

        let sigs: Vec<[u8; 64]> = (0..(FAIR_LEDGER_COMMIT_MAX_SIGS_PER_CHUNK + 1))
            .map(|i| [i as u8; 64])
            .collect();

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs.as_slice(),
        );
        assert!(commit_txs.len() >= 2);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_enforce = true;
        let handle = SolanaCdnHandle::new(cfg);

        assert!(handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        let status = handle.status_snapshot();
        assert_eq!(status.fair_slashed_leaders_len, 0);
        assert_eq!(status.fair_ledger_audit_inconclusive_total, 1);
    }

    #[test]
    fn fair_ledger_audit_strict_missing_chunks_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let recent_blockhash = solana_hash::Hash::default();
        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;

        let sigs: Vec<[u8; 64]> = (0..(FAIR_LEDGER_COMMIT_MAX_SIGS_PER_CHUNK + 1))
            .map(|i| [i as u8; 64])
            .collect();

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs.as_slice(),
        );
        assert!(commit_txs.len() >= 2);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_strict = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        let status = handle.status_snapshot();
        assert_eq!(status.fair_slashed_leaders_len, 1);
        assert_eq!(status.fair_ledger_audit_inconclusive_total, 0);
    }

    #[test]
    fn fair_ledger_audit_witness_does_not_slash_without_ack() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_witness = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let slot = 42;
        let batch_id = 7u128;
        let sigs = vec![[1u8; 64], [2u8; 64]];

        let witness_payload = FairBatchWitnessPayload {
            attestation: FairBatchAttestationPayload {
                origin_pop_id: "pop-test-1".to_string(),
                flow_id: 0,
                batch_id,
                tx_seq_start: 0,
                tx_count: sigs.len() as u32,
                tx_merkle_root: fair_merkle_root(sigs.as_slice()),
                created_at_ms: now_ms(),
                batch_ms: 0,
                target_slot: Some(slot),
            },
            leader_pubkey: PubkeyBytes(leader.to_bytes()),
            pop_time_ms: now_ms(),
        };
        let mut rng = rand::rngs::OsRng;
        let pop_signing_key = SigningKey::generate(&mut rng);
        let witness =
            FairBatchWitness::sign(witness_payload, &pop_signing_key).expect("sign witness");
        let pop_pubkey = PubkeyBytes::from(pop_signing_key.verifying_key());
        handle.note_fair_batch_witness_for_slashing(pop_pubkey, &witness);

        // No on-chain commit memos in this slot => should NOT be a slashing violation without a
        // leader ACK.
        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::default();
        let tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let entry = solana_entry::entry::Entry {
            transactions: vec![VersionedTransaction::from(tx).into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 0);
    }

    #[test]
    fn fair_ledger_audit_witness_slashes_without_ack_when_nonresponse_enabled() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_nonresponse = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let slot = 42;
        let batch_id = 7u128;
        let sigs = vec![[1u8; 64], [2u8; 64]];

        let witness_payload = FairBatchWitnessPayload {
            attestation: FairBatchAttestationPayload {
                origin_pop_id: "pop-test-1".to_string(),
                flow_id: 0,
                batch_id,
                tx_seq_start: 0,
                tx_count: sigs.len() as u32,
                tx_merkle_root: fair_merkle_root(sigs.as_slice()),
                created_at_ms: now_ms(),
                batch_ms: 0,
                target_slot: Some(slot),
            },
            leader_pubkey: PubkeyBytes(leader.to_bytes()),
            pop_time_ms: now_ms(),
        };
        let mut rng = rand::rngs::OsRng;
        let pop_signing_key = SigningKey::generate(&mut rng);
        let witness =
            FairBatchWitness::sign(witness_payload, &pop_signing_key).expect("sign witness");
        let pop_pubkey = PubkeyBytes::from(pop_signing_key.verifying_key());
        handle.note_fair_batch_witness_for_slashing(pop_pubkey, &witness);

        // No on-chain commit memos and no leader reject => violation when POP witness is present.
        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::default();
        let tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let entry = solana_entry::entry::Entry {
            transactions: vec![VersionedTransaction::from(tx).into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_witness_quorum_blocks_nonresponse_slash_until_met() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_nonresponse = true;
        cfg.tx_fair_slashing_witness_quorum = 2;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let slot = 42;
        let batch_id = 7u128;
        let sigs = vec![[1u8; 64], [2u8; 64]];

        let witness_payload = FairBatchWitnessPayload {
            attestation: FairBatchAttestationPayload {
                origin_pop_id: "pop-test-1".to_string(),
                flow_id: 0,
                batch_id,
                tx_seq_start: 0,
                tx_count: sigs.len() as u32,
                tx_merkle_root: fair_merkle_root(sigs.as_slice()),
                created_at_ms: now_ms(),
                batch_ms: 0,
                target_slot: Some(slot),
            },
            leader_pubkey: PubkeyBytes(leader.to_bytes()),
            pop_time_ms: now_ms(),
        };

        let mut rng = rand::rngs::OsRng;
        let pop_signing_key_a = SigningKey::generate(&mut rng);
        let witness_a = FairBatchWitness::sign(witness_payload.clone(), &pop_signing_key_a)
            .expect("sign witness");
        let pop_pubkey_a = PubkeyBytes::from(pop_signing_key_a.verifying_key());
        handle.note_fair_batch_witness_for_slashing(pop_pubkey_a, &witness_a);

        // No on-chain commit memos and no leader reject: would normally be a violation, but quorum
        // is not met yet.
        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::default();
        let tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let entry = solana_entry::entry::Entry {
            transactions: vec![VersionedTransaction::from(tx).into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 0);

        let pop_signing_key_b = SigningKey::generate(&mut rng);
        let witness_b =
            FairBatchWitness::sign(witness_payload, &pop_signing_key_b).expect("sign witness");
        let pop_pubkey_b = PubkeyBytes::from(pop_signing_key_b.verifying_key());
        handle.note_fair_batch_witness_for_slashing(pop_pubkey_b, &witness_b);

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_onchain_witness_memo_slashes_without_ack_when_nonresponse_enabled() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_nonresponse = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let slot = 42;
        let batch_id = 7u128;
        let sigs = vec![[1u8; 64], [2u8; 64]];

        let witness_payload = FairBatchWitnessPayload {
            attestation: FairBatchAttestationPayload {
                origin_pop_id: "pop-test-1".to_string(),
                flow_id: 0,
                batch_id,
                tx_seq_start: 0,
                tx_count: sigs.len() as u32,
                tx_merkle_root: fair_merkle_root(sigs.as_slice()),
                created_at_ms: now_ms(),
                batch_ms: 0,
                target_slot: Some(slot),
            },
            leader_pubkey: PubkeyBytes(leader.to_bytes()),
            pop_time_ms: now_ms(),
        };
        let mut rng = rand::rngs::OsRng;
        let pop_signing_key = SigningKey::generate(&mut rng);
        let witness =
            FairBatchWitness::sign(witness_payload, &pop_signing_key).expect("sign witness");
        let pop_pubkey = PubkeyBytes::from(pop_signing_key.verifying_key());

        let payer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::default();
        let witness_tx_bytes =
            build_fair_ledger_witness_memo_tx(&payer, recent_blockhash, pop_pubkey, &witness)
                .expect("witness memo tx");
        let witness_tx: Transaction = bincode::deserialize(&witness_tx_bytes).expect("witness tx");

        let entries = vec![solana_entry::entry::Entry {
            transactions: vec![witness_tx.into()],
            ..solana_entry::entry::Entry::default()
        }];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_witness_quorum_combines_offchain_and_onchain() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_nonresponse = true;
        cfg.tx_fair_slashing_witness_quorum = 2;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let slot = 42;
        let batch_id = 7u128;
        let sigs = vec![[1u8; 64], [2u8; 64]];

        let witness_payload = FairBatchWitnessPayload {
            attestation: FairBatchAttestationPayload {
                origin_pop_id: "pop-test-1".to_string(),
                flow_id: 0,
                batch_id,
                tx_seq_start: 0,
                tx_count: sigs.len() as u32,
                tx_merkle_root: fair_merkle_root(sigs.as_slice()),
                created_at_ms: now_ms(),
                batch_ms: 0,
                target_slot: Some(slot),
            },
            leader_pubkey: PubkeyBytes(leader.to_bytes()),
            pop_time_ms: now_ms(),
        };

        let mut rng = rand::rngs::OsRng;
        let pop_signing_key_a = SigningKey::generate(&mut rng);
        let witness_a = FairBatchWitness::sign(witness_payload.clone(), &pop_signing_key_a)
            .expect("sign witness");
        let pop_pubkey_a = PubkeyBytes::from(pop_signing_key_a.verifying_key());
        handle.note_fair_batch_witness_for_slashing(pop_pubkey_a, &witness_a);

        let pop_signing_key_b = SigningKey::generate(&mut rng);
        let witness_b =
            FairBatchWitness::sign(witness_payload, &pop_signing_key_b).expect("sign witness");
        let pop_pubkey_b = PubkeyBytes::from(pop_signing_key_b.verifying_key());

        let payer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::default();
        let witness_tx_bytes =
            build_fair_ledger_witness_memo_tx(&payer, recent_blockhash, pop_pubkey_b, &witness_b)
                .expect("witness memo tx");
        let witness_tx: Transaction = bincode::deserialize(&witness_tx_bytes).expect("witness tx");

        let entries = vec![solana_entry::entry::Entry {
            transactions: vec![witness_tx.into()],
            ..solana_entry::entry::Entry::default()
        }];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_witness_does_not_slash_if_rejected_in_nonresponse_mode() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_nonresponse = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let slot = 42;
        let origin_pop_id = "pop-test-1".to_string();
        let batch_id = 7u128;
        let order_start = 0u64;
        let sigs = vec![[1u8; 64], [2u8; 64]];

        let witness_payload = FairBatchWitnessPayload {
            attestation: FairBatchAttestationPayload {
                origin_pop_id: origin_pop_id.clone(),
                flow_id: 0,
                batch_id,
                tx_seq_start: order_start,
                tx_count: sigs.len() as u32,
                tx_merkle_root: fair_merkle_root(sigs.as_slice()),
                created_at_ms: now_ms(),
                batch_ms: 0,
                target_slot: Some(slot),
            },
            leader_pubkey: PubkeyBytes(leader.to_bytes()),
            pop_time_ms: now_ms(),
        };
        let mut rng = rand::rngs::OsRng;
        let pop_signing_key = SigningKey::generate(&mut rng);
        let witness =
            FairBatchWitness::sign(witness_payload, &pop_signing_key).expect("sign witness");
        let pop_pubkey = PubkeyBytes::from(pop_signing_key.verifying_key());
        handle.note_fair_batch_witness_for_slashing(pop_pubkey, &witness);

        let reject_payload = FairBatchRejectPayload {
            origin_pop_id,
            flow_id: 0,
            batch_id,
            order_start,
            target_slot: Some(slot),
            reason: FairBatchRejectReason::InternalError,
            leader_pubkey: auth.validator_pubkey,
            leader_time_ms: now_ms(),
        };
        let reject = FairBatchReject::sign(reject_payload, &auth.signing_key).expect("sign reject");
        reject.verify().expect("verify reject");
        handle.note_fair_batch_reject_for_slashing(&reject);

        // No on-chain commit memos => should NOT slash when a leader reject is present.
        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::default();
        let tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let entry = solana_entry::entry::Entry {
            transactions: vec![VersionedTransaction::from(tx).into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 0);
    }

    #[test]
    fn fair_ledger_audit_witness_does_not_slash_if_rejected_onchain_in_nonresponse_mode() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_nonresponse = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let slot = 42;
        let origin_pop_id = "pop-test-1".to_string();
        let flow_id = 0u128;
        let batch_id = 7u128;
        let order_start = 0u64;
        let sigs = vec![[1u8; 64], [2u8; 64]];

        let witness_payload = FairBatchWitnessPayload {
            attestation: FairBatchAttestationPayload {
                origin_pop_id: origin_pop_id.clone(),
                flow_id,
                batch_id,
                tx_seq_start: order_start,
                tx_count: sigs.len() as u32,
                tx_merkle_root: fair_merkle_root(sigs.as_slice()),
                created_at_ms: now_ms(),
                batch_ms: 0,
                target_slot: Some(slot),
            },
            leader_pubkey: PubkeyBytes(leader.to_bytes()),
            pop_time_ms: now_ms(),
        };
        let mut rng = rand::rngs::OsRng;
        let pop_signing_key = SigningKey::generate(&mut rng);
        let witness =
            FairBatchWitness::sign(witness_payload, &pop_signing_key).expect("sign witness");
        let pop_pubkey = PubkeyBytes::from(pop_signing_key.verifying_key());
        handle.note_fair_batch_witness_for_slashing(pop_pubkey, &witness);

        let recent_blockhash = solana_hash::Hash::default();
        let reject_tx_bytes = build_fair_ledger_reject_memo_tx(
            &auth,
            recent_blockhash,
            slot,
            origin_pop_id.as_str(),
            flow_id,
            batch_id,
            order_start,
            FairBatchRejectReason::InternalError,
        )
        .expect("reject memo tx");
        let reject_tx: Transaction = bincode::deserialize(&reject_tx_bytes).expect("reject tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![reject_tx.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 0);
    }

    #[test]
    fn fair_ledger_audit_onchain_reject_with_onchain_commit_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let slot = 42;
        let origin_pop_id = "pop-test-1".to_string();
        let flow_id = 0u128;
        let batch_id = 7u128;
        let order_start = 0u64;
        let sigs = vec![[1u8; 64], [2u8; 64]];

        let recent_blockhash = solana_hash::Hash::default();
        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs.as_slice(),
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let reject_tx_bytes = build_fair_ledger_reject_memo_tx(
            &auth,
            recent_blockhash,
            slot,
            origin_pop_id.as_str(),
            flow_id,
            batch_id,
            order_start,
            FairBatchRejectReason::InternalError,
        )
        .expect("reject memo tx");
        let reject_tx: Transaction = bincode::deserialize(&reject_tx_bytes).expect("reject tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into(), reject_tx.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_witness_enables_strict_audit_insertion_ahead_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;
        let recent_blockhash = solana_hash::Hash::default();

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let sigs = vec![sig_a, sig_b];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_nonresponse = true;
        cfg.tx_fair_slashing_strict = false;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let witness_payload = FairBatchWitnessPayload {
            attestation: FairBatchAttestationPayload {
                origin_pop_id: "pop-test-1".to_string(),
                flow_id: 0,
                batch_id,
                tx_seq_start: order_start,
                tx_count: sigs.len() as u32,
                tx_merkle_root: fair_merkle_root(sigs.as_slice()),
                created_at_ms: now_ms(),
                batch_ms: 0,
                target_slot: Some(slot),
            },
            leader_pubkey: PubkeyBytes(leader.to_bytes()),
            pop_time_ms: now_ms(),
        };
        let mut rng = rand::rngs::OsRng;
        let pop_signing_key = SigningKey::generate(&mut rng);
        let witness =
            FairBatchWitness::sign(witness_payload, &pop_signing_key).expect("sign witness");
        let pop_pubkey = PubkeyBytes::from(pop_signing_key.verifying_key());
        handle.note_fair_batch_witness_for_slashing(pop_pubkey, &witness);

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs.as_slice(),
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let inserted_signer = Keypair::new();
        let inserted_tx = Transaction::new(
            &[&inserted_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(42)],
                Some(&inserted_signer.pubkey()),
            ),
            recent_blockhash,
        );

        let entry = solana_entry::entry::Entry {
            transactions: vec![
                commit_tx.into(),
                inserted_tx.into(),
                tx_a.into(),
                tx_b.into(),
            ],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_ack_missing_onchain_commit_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let origin_pop_id = "pop-test-1".to_string();
        let batch_id = 7u128;
        let order_start = 0u64;

        let sigs: Vec<[u8; 64]> = vec![[1u8; 64], [2u8; 64]];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_witness = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let leader_time_ms = now_ms();
        let payload = FairBatchReceiptCommitPayload {
            origin_pop_id,
            flow_id: 0,
            batch_id,
            order_start,
            target_slot: Some(slot),
            leader_pubkey: auth.validator_pubkey,
            leader_time_ms,
            tx_count: sigs.len() as u32,
            tx_merkle_root: fair_merkle_root(sigs.as_slice()),
        };
        let ack = FairBatchReceiptCommit::sign(payload, &auth.signing_key).expect("sign ack");
        ack.verify().expect("verify ack");
        handle.note_fair_batch_ack_for_slashing(&ack);

        // No on-chain commit memos in this slot => violation when leader ACK is present.
        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::default();
        let tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let entry = solana_entry::entry::Entry {
            transactions: vec![VersionedTransaction::from(tx).into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_onchain_ack_missing_onchain_commit_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let origin_pop_id = "pop-test-1";
        let flow_id = 0u128;
        let batch_id = 7u128;
        let order_start = 0u64;
        let sigs: Vec<[u8; 64]> = vec![[1u8; 64], [2u8; 64]];
        let tx_count = sigs.len() as u32;
        let tx_merkle_root = fair_merkle_root(sigs.as_slice());

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_witness = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let recent_blockhash = solana_hash::Hash::default();
        let ack_tx_bytes = build_fair_ledger_ack_memo_tx(
            &auth,
            recent_blockhash,
            slot,
            origin_pop_id,
            flow_id,
            batch_id,
            order_start,
            tx_count,
            tx_merkle_root,
        )
        .expect("ack memo tx bytes");
        let ack_tx: Transaction = bincode::deserialize(&ack_tx_bytes).expect("ack memo tx");

        // No on-chain commit memos in this slot => violation when on-chain leader ACK is present.
        let signer = Keypair::new();
        let tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let entry = solana_entry::entry::Entry {
            transactions: vec![ack_tx.into(), VersionedTransaction::from(tx).into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_slashing_ack_witness_mismatch_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;
        let origin_pop_id = "pop-test-1".to_string();

        let sigs_witness = vec![[1u8; 64], [2u8; 64]];
        let sigs_ack = vec![[1u8; 64], [3u8; 64]];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_witness = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let witness_payload = FairBatchWitnessPayload {
            attestation: FairBatchAttestationPayload {
                origin_pop_id: origin_pop_id.clone(),
                flow_id: 0,
                batch_id,
                tx_seq_start: order_start,
                tx_count: sigs_witness.len() as u32,
                tx_merkle_root: fair_merkle_root(sigs_witness.as_slice()),
                created_at_ms: now_ms(),
                batch_ms: 0,
                target_slot: Some(slot),
            },
            leader_pubkey: PubkeyBytes(leader.to_bytes()),
            pop_time_ms: now_ms(),
        };
        let mut rng = rand::rngs::OsRng;
        let pop_signing_key = SigningKey::generate(&mut rng);
        let witness =
            FairBatchWitness::sign(witness_payload, &pop_signing_key).expect("sign witness");
        let pop_pubkey = PubkeyBytes::from(pop_signing_key.verifying_key());
        handle.note_fair_batch_witness_for_slashing(pop_pubkey, &witness);

        let leader_time_ms = now_ms();
        let ack_payload = FairBatchReceiptCommitPayload {
            origin_pop_id,
            flow_id: 0,
            batch_id,
            order_start,
            target_slot: Some(slot),
            tx_count: sigs_ack.len() as u32,
            tx_merkle_root: fair_merkle_root(sigs_ack.as_slice()),
            leader_pubkey: auth.validator_pubkey,
            leader_time_ms,
        };
        let ack = FairBatchReceiptCommit::sign(ack_payload, &auth.signing_key).expect("sign ack");
        ack.verify().expect("verify ack");
        handle.note_fair_batch_ack_for_slashing(&ack);

        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_onchain_ack_witness_mismatch_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;
        let origin_pop_id = "pop-test-1".to_string();
        let recent_blockhash = solana_hash::Hash::default();

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let sigs_ack = vec![sig_a, sig_b];
        let tx_count = sigs_ack.len() as u32;
        let tx_merkle_root_ack = fair_merkle_root(sigs_ack.as_slice());

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_witness = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs_ack.as_slice(),
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let ack_tx_bytes = build_fair_ledger_ack_memo_tx(
            &auth,
            recent_blockhash,
            slot,
            &origin_pop_id,
            0,
            batch_id,
            order_start,
            tx_count,
            tx_merkle_root_ack,
        )
        .expect("ack memo tx");
        let ack_tx: Transaction = bincode::deserialize(&ack_tx_bytes).expect("ack tx");

        let sigs_witness = vec![sig_a, [9u8; 64]];
        let witness_payload = FairBatchWitnessPayload {
            attestation: FairBatchAttestationPayload {
                origin_pop_id,
                flow_id: 0,
                batch_id,
                tx_seq_start: order_start,
                tx_count: sigs_witness.len() as u32,
                tx_merkle_root: fair_merkle_root(sigs_witness.as_slice()),
                created_at_ms: now_ms(),
                batch_ms: 0,
                target_slot: Some(slot),
            },
            leader_pubkey: PubkeyBytes(leader.to_bytes()),
            pop_time_ms: now_ms(),
        };
        let mut rng = rand::rngs::OsRng;
        let pop_signing_key = SigningKey::generate(&mut rng);
        let witness =
            FairBatchWitness::sign(witness_payload, &pop_signing_key).expect("sign witness");
        let pop_pubkey = PubkeyBytes::from(pop_signing_key.verifying_key());

        let payer = Keypair::new();
        let witness_tx_bytes =
            build_fair_ledger_witness_memo_tx(&payer, recent_blockhash, pop_pubkey, &witness)
                .expect("witness memo tx");
        let witness_tx: Transaction = bincode::deserialize(&witness_tx_bytes).expect("witness tx");

        let entries = vec![solana_entry::entry::Entry {
            transactions: vec![
                commit_tx.into(),
                ack_tx.into(),
                witness_tx.into(),
                tx_a.into(),
                tx_b.into(),
            ],
            ..solana_entry::entry::Entry::default()
        }];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_ack_commit_mismatch_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let origin_pop_id = "pop-test-1".to_string();
        let batch_id = 7u128;
        let order_start = 0u64;

        let sigs_ack = vec![[1u8; 64], [2u8; 64]];
        let mut sigs_commit = sigs_ack.clone();
        sigs_commit.reverse();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_witness = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let leader_time_ms = now_ms();
        let payload = FairBatchReceiptCommitPayload {
            origin_pop_id,
            flow_id: 0,
            batch_id,
            order_start,
            target_slot: Some(slot),
            leader_pubkey: auth.validator_pubkey,
            leader_time_ms,
            tx_count: sigs_ack.len() as u32,
            tx_merkle_root: fair_merkle_root(sigs_ack.as_slice()),
        };
        let ack = FairBatchReceiptCommit::sign(payload, &auth.signing_key).expect("sign ack");
        ack.verify().expect("verify ack");
        handle.note_fair_batch_ack_for_slashing(&ack);

        let recent_blockhash = solana_hash::Hash::default();
        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs_commit.as_slice(),
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_ack_commit_match_is_ok() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let origin_pop_id = "pop-test-1".to_string();
        let batch_id = 7u128;
        let order_start = 0u64;

        let recent_blockhash = solana_hash::Hash::default();

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let sigs = vec![sig_a, sig_b];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_witness = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let leader_time_ms = now_ms();
        let payload = FairBatchReceiptCommitPayload {
            origin_pop_id,
            flow_id: 0,
            batch_id,
            order_start,
            target_slot: Some(slot),
            leader_pubkey: auth.validator_pubkey,
            leader_time_ms,
            tx_count: sigs.len() as u32,
            tx_merkle_root: fair_merkle_root(sigs.as_slice()),
        };
        let ack = FairBatchReceiptCommit::sign(payload, &auth.signing_key).expect("sign ack");
        ack.verify().expect("verify ack");
        handle.note_fair_batch_ack_for_slashing(&ack);

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs.as_slice(),
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into(), tx_a.into(), tx_b.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 0);
    }

    #[test]
    fn fair_ledger_audit_ack_enables_strict_audit_committed_drop_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let origin_pop_id = "pop-test-1".to_string();
        let batch_id = 7u128;
        let order_start = 0u64;
        let recent_blockhash = solana_hash::Hash::default();

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let sigs = vec![sig_a, sig_b];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_witness = true;
        cfg.tx_fair_slashing_strict = false;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let leader_time_ms = now_ms();
        let payload = FairBatchReceiptCommitPayload {
            origin_pop_id,
            flow_id: 0,
            batch_id,
            order_start,
            target_slot: Some(slot),
            leader_pubkey: auth.validator_pubkey,
            leader_time_ms,
            tx_count: sigs.len() as u32,
            tx_merkle_root: fair_merkle_root(sigs.as_slice()),
        };
        let ack = FairBatchReceiptCommit::sign(payload, &auth.signing_key).expect("sign ack");
        ack.verify().expect("verify ack");
        handle.note_fair_batch_ack_for_slashing(&ack);

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs.as_slice(),
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into(), tx_a.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_ack_enables_strict_audit_insertion_ahead_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let origin_pop_id = "pop-test-1".to_string();
        let batch_id = 7u128;
        let order_start = 0u64;
        let recent_blockhash = solana_hash::Hash::default();

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let sigs = vec![sig_a, sig_b];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_witness = true;
        cfg.tx_fair_slashing_strict = false;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let leader_time_ms = now_ms();
        let payload = FairBatchReceiptCommitPayload {
            origin_pop_id,
            flow_id: 0,
            batch_id,
            order_start,
            target_slot: Some(slot),
            leader_pubkey: auth.validator_pubkey,
            leader_time_ms,
            tx_count: sigs.len() as u32,
            tx_merkle_root: fair_merkle_root(sigs.as_slice()),
        };
        let ack = FairBatchReceiptCommit::sign(payload, &auth.signing_key).expect("sign ack");
        ack.verify().expect("verify ack");
        handle.note_fair_batch_ack_for_slashing(&ack);

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs.as_slice(),
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let inserted_signer = Keypair::new();
        let inserted_tx = Transaction::new(
            &[&inserted_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(42)],
                Some(&inserted_signer.pubkey()),
            ),
            recent_blockhash,
        );

        let entry = solana_entry::entry::Entry {
            transactions: vec![
                commit_tx.into(),
                inserted_tx.into(),
                tx_a.into(),
                tx_b.into(),
            ],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_onchain_ack_enables_strict_audit_insertion_ahead_is_violation() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let origin_pop_id = "pop-test-1";
        let flow_id = 0u128;
        let batch_id = 7u128;
        let order_start = 0u64;
        let recent_blockhash = solana_hash::Hash::default();

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let sigs = vec![sig_a, sig_b];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_witness = true;
        cfg.tx_fair_slashing_strict = false;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let ack_tx_bytes = build_fair_ledger_ack_memo_tx(
            &auth,
            recent_blockhash,
            slot,
            origin_pop_id,
            flow_id,
            batch_id,
            order_start,
            sigs.len() as u32,
            fair_merkle_root(sigs.as_slice()),
        )
        .expect("ack memo tx bytes");
        let ack_tx: Transaction = bincode::deserialize(&ack_tx_bytes).expect("ack memo tx");

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs.as_slice(),
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let inserted_signer = Keypair::new();
        let inserted_tx = Transaction::new(
            &[&inserted_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(42)],
                Some(&inserted_signer.pubkey()),
            ),
            recent_blockhash,
        );

        let entry = solana_entry::entry::Entry {
            transactions: vec![
                commit_tx.into(),
                ack_tx.into(),
                inserted_tx.into(),
                tx_a.into(),
                tx_b.into(),
            ],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(!handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_strict_ignores_ack_memo_for_other_slot() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 43;
        let other_slot = 42;
        let origin_pop_id = "pop-test-1".to_string();
        let batch_id = 7u128;
        let order_start = 0u64;
        let recent_blockhash = solana_hash::Hash::default();

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let sigs = vec![sig_a, sig_b];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_witness = true;
        cfg.tx_fair_slashing_strict = false;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        // Enable strict audit for this slot via an off-chain leader ACK.
        let leader_time_ms = now_ms();
        let payload = FairBatchReceiptCommitPayload {
            origin_pop_id: origin_pop_id.clone(),
            flow_id: 0,
            batch_id,
            order_start,
            target_slot: Some(slot),
            leader_pubkey: auth.validator_pubkey,
            leader_time_ms,
            tx_count: sigs.len() as u32,
            tx_merkle_root: fair_merkle_root(sigs.as_slice()),
        };
        let ack = FairBatchReceiptCommit::sign(payload, &auth.signing_key).expect("sign ack");
        ack.verify().expect("verify ack");
        handle.note_fair_batch_ack_for_slashing(&ack);

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs.as_slice(),
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        // A valid on-chain ACK memo for a different slot should be treated as exempt metadata and
        // must not trip strict insertion-ahead rules for this slot.
        let ack_tx_bytes = build_fair_ledger_ack_memo_tx(
            &auth,
            recent_blockhash,
            other_slot,
            &origin_pop_id,
            0,
            batch_id,
            order_start,
            sigs.len() as u32,
            fair_merkle_root(sigs.as_slice()),
        )
        .expect("ack memo tx bytes");
        let ack_tx: Transaction = bincode::deserialize(&ack_tx_bytes).expect("ack memo tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into(), ack_tx.into(), tx_a.into(), tx_b.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 0);
    }

    #[test]
    fn fair_ledger_audit_strict_ignores_witness_memo_for_other_slot() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 43;
        let other_slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;
        let recent_blockhash = solana_hash::Hash::default();

        let tx_a_signer = Keypair::new();
        let tx_a = Transaction::new(
            &[&tx_a_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&tx_a_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_a: [u8; 64] = tx_a.signatures[0].as_ref().try_into().expect("sig bytes");

        let tx_b_signer = Keypair::new();
        let tx_b = Transaction::new(
            &[&tx_b_signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&tx_b_signer.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_b: [u8; 64] = tx_b.signatures[0].as_ref().try_into().expect("sig bytes");

        let sigs = vec![sig_a, sig_b];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_strict = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            sigs.as_slice(),
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let witness_payload = FairBatchWitnessPayload {
            attestation: FairBatchAttestationPayload {
                origin_pop_id: "pop-test-1".to_string(),
                flow_id: 0,
                batch_id,
                tx_seq_start: order_start,
                tx_count: sigs.len() as u32,
                tx_merkle_root: fair_merkle_root(sigs.as_slice()),
                created_at_ms: now_ms(),
                batch_ms: 0,
                target_slot: Some(other_slot),
            },
            leader_pubkey: PubkeyBytes(leader.to_bytes()),
            pop_time_ms: now_ms(),
        };
        let mut rng = rand::rngs::OsRng;
        let pop_signing_key = SigningKey::generate(&mut rng);
        let witness =
            FairBatchWitness::sign(witness_payload, &pop_signing_key).expect("sign witness");
        let pop_pubkey = PubkeyBytes::from(pop_signing_key.verifying_key());

        let payer = Keypair::new();
        let witness_tx_bytes =
            build_fair_ledger_witness_memo_tx(&payer, recent_blockhash, pop_pubkey, &witness)
                .expect("witness memo tx");
        let witness_tx: Transaction = bincode::deserialize(&witness_tx_bytes).expect("witness tx");

        let entries = vec![solana_entry::entry::Entry {
            transactions: vec![
                commit_tx.into(),
                witness_tx.into(),
                tx_a.into(),
                tx_b.into(),
            ],
            ..solana_entry::entry::Entry::default()
        }];

        assert!(handle.audit_fair_ledger_commits_in_entries(entries.as_slice(), &leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 0);
    }

    #[test]
    fn fair_ledger_audit_account_fence_violation_is_slashed() {
        use solana_genesis_config::GenesisConfig;
        use solana_system_interface::instruction as system_instruction;

        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;
        let recent_blockhash = solana_hash::Hash::new_unique();

        let bank = Bank::new_for_tests(&GenesisConfig::default());

        let payer_a = Keypair::new();
        let dst = Pubkey::new_unique();
        let fair_tx = Transaction::new(
            &[&payer_a],
            Message::new(
                &[system_instruction::transfer(&payer_a.pubkey(), &dst, 1)],
                Some(&payer_a.pubkey()),
            ),
            recent_blockhash,
        );
        let sig_fair: [u8; 64] = fair_tx.signatures[0]
            .as_ref()
            .try_into()
            .expect("sig bytes");

        let payer_b = Keypair::new();
        let inserted_tx = Transaction::new(
            &[&payer_b],
            Message::new(
                &[system_instruction::transfer(&payer_b.pubkey(), &dst, 1)],
                Some(&payer_b.pubkey()),
            ),
            recent_blockhash,
        );

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_fence = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            &[sig_fair],
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entry = solana_entry::entry::Entry {
            transactions: vec![commit_tx.into(), fair_tx.into(), inserted_tx.into()],
            ..solana_entry::entry::Entry::default()
        };
        let entries = vec![entry];

        assert!(!handle.audit_fair_ledger_commits_in_entries_impl(
            entries.as_slice(),
            Some(&bank),
            &leader,
            slot
        ));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_ledger_audit_account_read_fence_only_when_enabled() {
        use solana_genesis_config::GenesisConfig;
        use solana_instruction::AccountMeta;
        use solana_system_interface::instruction as system_instruction;

        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let batch_id = 7u128;
        let order_start = 0u64;
        let recent_blockhash = solana_hash::Hash::new_unique();

        let bank = Bank::new_for_tests(&GenesisConfig::default());

        let read_account = Pubkey::new_unique();
        let payer_a = Keypair::new();
        let fair_ix = Instruction {
            program_id: solana_system_program::id(),
            accounts: vec![AccountMeta::new_readonly(read_account, false)],
            data: vec![0u8],
        };
        let fair_tx = Transaction::new(
            &[&payer_a],
            Message::new(&[fair_ix], Some(&payer_a.pubkey())),
            recent_blockhash,
        );
        let sig_fair: [u8; 64] = fair_tx.signatures[0]
            .as_ref()
            .try_into()
            .expect("sig bytes");

        let payer_b = Keypair::new();
        let inserted_tx = Transaction::new(
            &[&payer_b],
            Message::new(
                &[system_instruction::transfer(
                    &payer_b.pubkey(),
                    &read_account,
                    1,
                )],
                Some(&payer_b.pubkey()),
            ),
            recent_blockhash,
        );

        let commit_txs = build_fair_ledger_commit_memo_txs(
            &auth,
            recent_blockhash,
            slot,
            batch_id,
            order_start,
            &[sig_fair],
        );
        assert_eq!(commit_txs.len(), 1);
        let commit_tx: Transaction = bincode::deserialize(&commit_txs[0]).expect("commit tx");

        let entries = vec![solana_entry::entry::Entry {
            transactions: vec![
                commit_tx.into(),
                fair_tx.clone().into(),
                inserted_tx.clone().into(),
            ],
            ..solana_entry::entry::Entry::default()
        }];

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_fence = true;
        cfg.tx_fair_slashing_fence_reads = false;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);
        assert!(handle.audit_fair_ledger_commits_in_entries_impl(
            entries.as_slice(),
            Some(&bank),
            &leader,
            slot
        ));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 0);

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_fence = true;
        cfg.tx_fair_slashing_fence_reads = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);
        assert!(!handle.audit_fair_ledger_commits_in_entries_impl(
            entries.as_slice(),
            Some(&bank),
            &leader,
            slot
        ));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
    }

    #[test]
    fn fair_commit_equivocation_marks_slashed() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();
        let auth = AuthContext::new(leader_identity).expect("auth context");

        let slot = 42;
        let origin_pop_id = "pop-test-1".to_string();
        let batch_id = 7u128;
        let order_start = 0u64;
        let leader_time_ms = now_ms();

        let payload_1 = FairBatchCommitPayload {
            origin_pop_id: origin_pop_id.clone(),
            flow_id: 0,
            batch_id,
            order_start,
            target_slot: Some(slot),
            tx_sigs: vec![SignatureBytes([1u8; 64])],
            leader_pubkey: auth.validator_pubkey,
            leader_time_ms,
        };
        let commit_1 = FairBatchCommit::sign(payload_1, &auth.signing_key).expect("sign commit");
        commit_1.verify().expect("verify commit");

        let payload_2 = FairBatchCommitPayload {
            origin_pop_id,
            flow_id: 0,
            batch_id,
            order_start,
            target_slot: Some(slot),
            tx_sigs: vec![SignatureBytes([2u8; 64])],
            leader_pubkey: auth.validator_pubkey,
            leader_time_ms,
        };
        let commit_2 = FairBatchCommit::sign(payload_2, &auth.signing_key).expect("sign commit");
        commit_2.verify().expect("verify commit");

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_enforce = true;
        let handle = SolanaCdnHandle::new(cfg);

        handle.note_fair_commit_for_slashing(&commit_1);
        assert!(!handle.fair_slashing_is_slashed_leader(&leader, slot));

        handle.note_fair_commit_for_slashing(&commit_2);
        assert!(handle.fair_slashing_is_slashed_leader(&leader, slot));
        assert_eq!(handle.status_snapshot().fair_slashed_leaders_len, 1);
        assert_eq!(handle.status_snapshot().fair_equivocations_total, 1);
    }

    #[test]
    fn try_first_signature_bytes_from_wire_tx_parses_legacy_tx() {
        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::new_unique();
        let tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let vtx = VersionedTransaction::from(tx);
        let expected: [u8; 64] = vtx.signatures[0].as_ref().try_into().expect("sig bytes");
        let bytes = bincode::serialize(&vtx).expect("serialize");
        let sig = try_first_signature_bytes_from_wire_tx(bytes.as_slice()).expect("sig");
        assert_eq!(sig, expected);
    }

    #[test]
    fn verified_recent_blockhash_from_wire_tx_rejects_bad_signature() {
        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::new_unique();
        let tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let vtx = VersionedTransaction::from(tx);
        let mut bytes = bincode::serialize(&vtx).expect("serialize");

        assert_eq!(
            verified_recent_blockhash_from_wire_tx(bytes.as_slice()).as_ref(),
            Some(&recent_blockhash)
        );

        let (_sig_count, consumed) = parse_shortvec_len(bytes.as_slice()).expect("sig vec len");
        bytes[consumed] ^= 0x01;
        assert!(verified_recent_blockhash_from_wire_tx(bytes.as_slice()).is_none());
    }

    fn fair_tx_from_wire_bytes(bytes: Vec<u8>) -> solanacdn_protocol::messages::FairTx {
        let sig = try_first_signature_bytes_from_wire_tx(bytes.as_slice()).expect("sig bytes");
        solanacdn_protocol::messages::FairTx {
            sig: SignatureBytes(sig),
            payload: bytes,
        }
    }

    fn make_test_fair_batch(
        origin_pop_id: &str,
        batch_id: u128,
        tx_seq_start: u64,
        target_slot: Option<u64>,
        txs: Vec<solanacdn_protocol::messages::FairTx>,
    ) -> solanacdn_protocol::messages::FairBatch {
        let created_at_ms = now_ms();
        let batch_ms: u16 = 0;
        let flow_id: u128 = 0;

        let sigs: Vec<[u8; 64]> = txs.iter().map(|tx| tx.sig.0).collect();
        let tx_merkle_root = fair_merkle_root(sigs.as_slice());
        let tx_count: u32 = txs.len().try_into().unwrap_or(0);

        let attestation_payload = FairBatchAttestationPayload {
            origin_pop_id: origin_pop_id.to_string(),
            flow_id,
            batch_id,
            tx_seq_start,
            tx_count,
            tx_merkle_root,
            created_at_ms,
            batch_ms,
            target_slot,
        };
        let pop_signing_key = test_pop_signing_key();
        let attestation = FairBatchAttestation::sign(attestation_payload, &pop_signing_key)
            .expect("sign attestation");

        solanacdn_protocol::messages::FairBatch {
            origin_pop_id: origin_pop_id.to_string(),
            flow_id,
            batch_id,
            tx_seq_start,
            created_at_ms,
            batch_ms,
            target_slot,
            attestation,
            txs,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fair_batch_commit_order_start_matches_tx_seq_start() {
        let endpoint: SocketAddr = "198.51.100.1:4444".parse().unwrap();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_ordering = true;
        cfg.tx_fair_slashing = false;
        let handle = SolanaCdnHandle::new(cfg.clone());

        let auth = AuthContext::new(Arc::new(Keypair::new())).unwrap();

        let (ctrl_out_tx, mut ctrl_out_rx) = mpsc::channel::<AgentToPop>(8);
        let (_publisher_tx, mut publisher_rx) = watch::channel::<Option<SocketAddr>>(None);
        let shred_deduper = ShredBatchDeduper::new(64);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let last_hb_sent_ms = AtomicU64::new(0);

        let sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sink_addr = sink.local_addr().unwrap();

        let udp_inject_tpu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tpu.connect(sink_addr).await.unwrap();
        let udp_inject_tvu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tvu.connect(sink_addr).await.unwrap();
        let udp_inject_gossip = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_gossip.connect(sink_addr).await.unwrap();
        let udp_inject_votes = VoteInjectSockets::bind().await.unwrap();

        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::new_unique();

        let tx_1 = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let bytes_1 = bincode::serialize(&VersionedTransaction::from(tx_1)).unwrap();

        let tx_2 = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let bytes_2 = bincode::serialize(&VersionedTransaction::from(tx_2)).unwrap();

        let batch_1 = make_test_fair_batch(
            "pop-test-1",
            1,
            0,
            None,
            vec![
                fair_tx_from_wire_bytes(bytes_1),
                fair_tx_from_wire_bytes(bytes_2),
            ],
        );

        handle_pop_msg(
            endpoint,
            test_pop_pubkey(),
            &cfg,
            &auth,
            &handle,
            &ctrl_out_tx,
            &mut publisher_rx,
            &shred_deduper,
            &udp_inject_tpu,
            &udp_inject_tvu,
            &udp_inject_gossip,
            sink_addr,
            &udp_inject_votes,
            &events_tx,
            &last_hb_sent_ms,
            PopToAgent::FairBatch(batch_1),
        )
        .await;

        let commit_1 = match tokio::time::timeout(Duration::from_secs(2), ctrl_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            AgentToPop::FairBatchCommit(commit) => commit,
            other => panic!("expected FairBatchCommit, got {other:?}"),
        };
        assert_eq!(commit_1.payload.order_start, 0);
        assert_eq!(commit_1.payload.tx_sigs.len(), 2);

        let tx_3 = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(3)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let bytes_3 = bincode::serialize(&VersionedTransaction::from(tx_3)).unwrap();

        let batch_2 = make_test_fair_batch(
            "pop-test-1",
            2,
            2,
            None,
            vec![fair_tx_from_wire_bytes(bytes_3)],
        );

        handle_pop_msg(
            endpoint,
            test_pop_pubkey(),
            &cfg,
            &auth,
            &handle,
            &ctrl_out_tx,
            &mut publisher_rx,
            &shred_deduper,
            &udp_inject_tpu,
            &udp_inject_tvu,
            &udp_inject_gossip,
            sink_addr,
            &udp_inject_votes,
            &events_tx,
            &last_hb_sent_ms,
            PopToAgent::FairBatch(batch_2),
        )
        .await;

        let commit_2 = match tokio::time::timeout(Duration::from_secs(2), ctrl_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            AgentToPop::FairBatchCommit(commit) => commit,
            other => panic!("expected FairBatchCommit, got {other:?}"),
        };
        assert_eq!(commit_2.payload.order_start, 2);
        assert_eq!(commit_2.payload.tx_sigs.len(), 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fair_batch_enforces_tx_count_cap() {
        let endpoint: SocketAddr = "198.51.100.1:4444".parse().unwrap();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_ordering = true;
        cfg.tx_fair_slashing = false;
        let handle = SolanaCdnHandle::new(cfg.clone());

        let auth = AuthContext::new(Arc::new(Keypair::new())).unwrap();

        let (ctrl_out_tx, mut ctrl_out_rx) = mpsc::channel::<AgentToPop>(8);
        let (_publisher_tx, mut publisher_rx) = watch::channel::<Option<SocketAddr>>(None);
        let shred_deduper = ShredBatchDeduper::new(64);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let last_hb_sent_ms = AtomicU64::new(0);

        let sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sink_addr = sink.local_addr().unwrap();

        let udp_inject_tpu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tpu.connect(sink_addr).await.unwrap();
        let udp_inject_tvu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tvu.connect(sink_addr).await.unwrap();
        let udp_inject_gossip = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_gossip.connect(sink_addr).await.unwrap();
        let udp_inject_votes = VoteInjectSockets::bind().await.unwrap();

        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::new_unique();

        let txs: Vec<solanacdn_protocol::messages::FairTx> = (0..(FAIR_BATCH_MAX_TXS + 1))
            .map(|i| {
                let tx = Transaction::new(
                    &[&signer],
                    Message::new(
                        &[ComputeBudgetInstruction::set_compute_unit_limit(
                            (i + 1) as u32,
                        )],
                        Some(&signer.pubkey()),
                    ),
                    recent_blockhash,
                );
                let bytes = bincode::serialize(&VersionedTransaction::from(tx)).unwrap();
                fair_tx_from_wire_bytes(bytes)
            })
            .collect();

        let target_slot = Some(999);
        let batch = make_test_fair_batch("pop-test-1", 1, 0, target_slot, txs);

        handle_pop_msg(
            endpoint,
            test_pop_pubkey(),
            &cfg,
            &auth,
            &handle,
            &ctrl_out_tx,
            &mut publisher_rx,
            &shred_deduper,
            &udp_inject_tpu,
            &udp_inject_tvu,
            &udp_inject_gossip,
            sink_addr,
            &udp_inject_votes,
            &events_tx,
            &last_hb_sent_ms,
            PopToAgent::FairBatch(batch),
        )
        .await;

        let reject = match tokio::time::timeout(Duration::from_secs(10), ctrl_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            AgentToPop::FairBatchReject(reject) => reject,
            other => panic!("expected FairBatchReject, got {other:?}"),
        };
        reject.verify().expect("verify reject");
        assert_eq!(reject.payload.order_start, 0);
        assert_eq!(reject.payload.target_slot, target_slot);
        assert!(matches!(
            reject.payload.reason,
            FairBatchRejectReason::TooManyTxs
        ));
    }

    #[test]
    fn fair_ledger_audit_does_not_cache_on_blockstore_read_errors() {
        let leader_identity = Arc::new(Keypair::new());
        let leader = leader_identity.pubkey();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_slashing = true;
        cfg.tx_fair_slashing_enforce = false;
        let handle = SolanaCdnHandle::new(cfg);

        let ledger_path = tempfile::TempDir::new().unwrap();
        let blockstore = Blockstore::open(ledger_path.path()).unwrap();

        let slot = 123;
        blockstore.set_dead_slot(slot).unwrap();

        handle.audit_fair_ledger_commits_for_slot(&blockstore, &leader, slot);
        assert_eq!(
            handle
                .status_snapshot()
                .fair_ledger_audit_get_slot_entries_failed_total,
            1
        );
        assert_eq!(handle.status_snapshot().fair_ledger_audited_slots_len, 0);

        // Not cached => can retry.
        handle.audit_fair_ledger_commits_for_slot(&blockstore, &leader, slot);
        assert_eq!(
            handle
                .status_snapshot()
                .fair_ledger_audit_get_slot_entries_failed_total,
            2
        );
        assert_eq!(handle.status_snapshot().fair_ledger_audited_slots_len, 0);
    }

    #[test]
    fn prometheus_metrics_includes_core_status_fields() {
        let cfg = SolanaCdnConfig::default();
        let handle = SolanaCdnHandle::new(cfg);

        handle.note_pop_delivered_shred_with_slot(1234, Some(42));
        handle.note_solanacdn_accepted_shred_with_slot(Some(42));
        let text = format_prometheus_metrics(&handle);

        assert!(text.contains("solanacdn_connected "));
        assert!(text.contains("solanacdn_rx_shred_bytes_total "));
        assert!(text.contains("solanacdn_rx_shred_payloads_total "));
        assert!(text.contains("solanacdn_tunneled_vote_packets_total "));
        assert!(text.contains("solanacdn_rx_vote_packets_total "));
        assert!(text.contains("solanacdn_dropped_vote_datagrams_total "));
        assert!(text.contains("solanacdn_tx_fair_ordering_enabled "));
        assert!(text.contains("solanacdn_tx_fair_require_target_slot_enabled "));
        assert!(text.contains("solanacdn_tx_fair_batch_received_total "));
        assert!(text.contains("solanacdn_tx_fair_batch_injected_total "));
        assert!(text.contains("solanacdn_tx_fair_batch_inject_failed_total "));
        assert!(text.contains("solanacdn_fair_batch_dropped_sig_mismatch_total "));
        assert!(text.contains("solanacdn_fair_batch_dropped_duplicate_sig_total "));
        assert!(text.contains("solanacdn_fair_batch_dropped_payload_too_large_total "));
        assert!(text.contains("solanacdn_fair_batch_dropped_invalid_wire_tx_total "));
        assert!(text.contains("solanacdn_fair_batch_dropped_too_many_txs_total "));
        assert!(text.contains("solanacdn_fair_batch_dropped_total_bytes_exceeded_total "));
        assert!(text.contains("solanacdn_tx_deduped_packets_total "));
        assert!(text.contains("solanacdn_tx_relay_dropped_fair_mode_total "));
        assert!(text.contains("solanacdn_fair_priority_lookups_total "));
        assert!(text.contains("solanacdn_fair_priority_hits_total "));
        assert!(text.contains("solanacdn_tx_fair_slashing_enabled "));
        assert!(text.contains("solanacdn_tx_fair_slashing_strict_enabled "));
        assert!(text.contains("solanacdn_tx_fair_slashing_witness_enabled "));
        assert!(text.contains("solanacdn_tx_fair_slashing_nonresponse_enabled "));
        assert!(text.contains("solanacdn_tx_fair_slashing_fence_enabled "));
        assert!(text.contains("solanacdn_tx_fair_slashing_enforce_enabled "));
        assert!(text.contains("solanacdn_tx_fair_slashing_enforce_configured "));
        assert!(text.contains("solanacdn_tx_fair_slashing_enforce_override{state=\"inherit\"} 1"));
        assert!(text.contains("solanacdn_fair_commits_rx_total "));
        assert!(text.contains("solanacdn_fair_commits_invalid_total "));
        assert!(text.contains("solanacdn_fair_equivocations_total "));
        assert!(text.contains("solanacdn_fair_votes_withheld_total "));
        assert!(text.contains("solanacdn_fair_ledger_audit_checked_total "));
        assert!(text.contains("solanacdn_fair_ledger_audit_failed_total "));
        assert!(text.contains("solanacdn_fair_ledger_audit_inconclusive_total "));
        assert!(text.contains("solanacdn_fair_ledger_audit_get_slot_entries_failed_total "));
        assert!(text.contains("solanacdn_fair_ledger_commits_seen_total "));
        assert!(text.contains("solanacdn_fair_ledger_commits_invalid_total "));
        assert!(text.contains("solanacdn_fair_order_witnesses_entries "));
        assert!(text.contains("solanacdn_fair_slashed_leaders_entries "));
        assert!(text.contains("solanacdn_fair_ledger_audited_slots_entries "));
        assert!(text.contains("solanacdn_last_shred_slot 42"));
        assert!(text.contains("solanacdn_last_shred_age_seconds "));
        assert!(text.contains("solanacdn_last_accepted_shred_slot 42"));
        assert!(text.contains("solanacdn_last_accepted_shred_age_seconds "));
        assert!(text.contains("solanacdn_race_enabled "));
        assert!(text.contains("solanacdn_race_pairs_total "));
        assert!(text.contains("solanacdn_race_wins_total{winner=\"solanacdn\"}"));
        assert!(text.contains("solanacdn_race_lead_seconds_bucket{winner=\"solanacdn\""));
        assert!(text.contains(
            "solanacdn_race_lead_seconds_quantile{winner=\"solanacdn\",quantile=\"0.50\"}"
        ));
        assert!(text.contains("solanacdn_race_delta_seconds_bucket{le=\"0.000\"}"));
        assert!(text.contains("solanacdn_race_delta_seconds_quantile{quantile=\"0.50\"}"));
    }

    #[test]
    fn race_tracker_records_winner_and_lead() {
        let mut cfg = SolanaCdnConfig::default();
        cfg.race_enabled = true;
        cfg.race_sample_bits = 0;
        cfg.race_window_ms = 5_000;
        cfg.tvu_shred_ingest_mode = TvuShredIngestMode::All;
        let handle = SolanaCdnHandle::new(cfg);

        let pop: SocketAddr = "198.51.100.9:4444".parse().unwrap();
        let (tx, _rx) = mpsc::channel::<UplinkMsg>(1);
        handle.set_publisher_uplink(Some(pop), Some(Arc::new(SessionUplink { tx })));

        let shred_id = LedgerShredId::new(100, 7, solana_ledger::shred::ShredType::Data);
        {
            let mut tracker = handle.race_state.lock().unwrap();
            tracker.observe(
                shred_id,
                RaceSource::Gossip,
                1_000,
                handle.cfg.race_window_ms,
                None,
                Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 99))),
            );
            tracker.observe(
                shred_id,
                RaceSource::SolanaCdn,
                900,
                handle.cfg.race_window_ms,
                Some(pop),
                None,
            );
        }

        let status = handle.status_snapshot();
        assert_eq!(status.race_enabled, true);
        assert_eq!(status.race_pairs_total, 1);
        assert_eq!(status.race_wins_solanacdn_total, 1);
        assert_eq!(status.race_wins_gossip_total, 0);
        assert_eq!(status.race_last_winner.as_deref(), Some("solanacdn"));
        assert_eq!(status.race_last_lead_ms, Some(100));
        assert_eq!(status.race_last_shred_slot, Some(100));
    }

    #[test]
    fn fair_priority_insert_and_lookup() {
        let sig: [u8; 64] = {
            let mut s = [0u8; 64];
            s[0] = 0xF1;
            s[1] = 0x01;
            s
        };
        insert_fair_priority(sig, 42);
        assert_eq!(fair_priority_for_tx_signature(&sig), Some(42));

        let unknown: [u8; 64] = {
            let mut s = [0u8; 64];
            s[0] = 0xF1;
            s[1] = 0x02;
            s
        };
        assert_eq!(fair_priority_for_tx_signature(&unknown), None);

        // cleanup
        fair_priorities().remove(&sig);
    }

    #[test]
    fn fair_priority_expires_after_ttl() {
        let sig: [u8; 64] = {
            let mut s = [0u8; 64];
            s[0] = 0xF2;
            s[1] = 0x01;
            s
        };
        // Insert with an already-expired timestamp directly into the map.
        fair_priorities().insert(
            sig,
            FairPriorityEntry {
                priority: 99,
                expires_at_ms: 1, // expired long ago
            },
        );

        // Lookup should detect the expiry and return None.
        assert_eq!(fair_priority_for_tx_signature(&sig), None);

        // The expired entry should have been removed from the map.
        assert!(!fair_priorities().contains_key(&sig));
    }

    #[test]
    fn fair_priority_prune_removes_expired() {
        let map = fair_priorities();

        let mut live_sigs: Vec<[u8; 64]> = Vec::new();
        let mut expired_sigs: Vec<[u8; 64]> = Vec::new();

        for i in 0u8..5 {
            let mut sig = [0u8; 64];
            sig[0] = 0xF3;
            sig[1] = i;
            map.insert(
                sig,
                FairPriorityEntry {
                    priority: i as u64,
                    expires_at_ms: 1000, // will be expired at now=2000
                },
            );
            expired_sigs.push(sig);
        }
        for i in 5u8..10 {
            let mut sig = [0u8; 64];
            sig[0] = 0xF3;
            sig[1] = i;
            map.insert(
                sig,
                FairPriorityEntry {
                    priority: i as u64,
                    expires_at_ms: 9999, // still live at now=2000
                },
            );
            live_sigs.push(sig);
        }

        prune_expired_fair_priorities(map, 2000);

        for sig in &expired_sigs {
            assert!(!map.contains_key(sig), "expired entry should be pruned");
        }
        for sig in &live_sigs {
            assert!(map.contains_key(sig), "live entry should remain");
        }

        // cleanup
        for sig in &live_sigs {
            map.remove(sig);
        }
    }

    #[test]
    fn fair_priority_overflow_clears_map() {
        let map = fair_priorities();

        // Insert FAIR_PRIORITY_MAX_ENTRIES + 1 entries so the map is over capacity.
        let mut overflow_sigs: Vec<[u8; 64]> = Vec::new();
        for i in 0..=(FAIR_PRIORITY_MAX_ENTRIES as u64) {
            let mut sig = [0u8; 64];
            sig[0] = 0xF4;
            // spread the index across bytes to avoid collisions
            sig[1..9].copy_from_slice(&i.to_le_bytes());
            map.insert(
                sig,
                FairPriorityEntry {
                    priority: i,
                    expires_at_ms: now_ms().saturating_add(60_000),
                },
            );
            overflow_sigs.push(sig);
        }
        assert!(map.len() > FAIR_PRIORITY_MAX_ENTRIES);

        // Inserting via insert_fair_priority should trigger overflow → clear.
        let trigger_sig: [u8; 64] = {
            let mut s = [0u8; 64];
            s[0] = 0xF4;
            s[63] = 0xFF;
            s
        };
        insert_fair_priority(trigger_sig, 777);

        // The map was cleared and only the new entry exists.
        assert!(
            map.len() <= 1,
            "map should be cleared on overflow, len={}",
            map.len()
        );
        assert_eq!(
            map.get(&trigger_sig).map(|e| e.priority),
            Some(777),
            "newly inserted entry must exist"
        );

        // cleanup
        map.remove(&trigger_sig);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fair_batch_rejects_duplicate_signatures() {
        let endpoint: SocketAddr = "198.51.100.1:4444".parse().unwrap();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_ordering = true;
        cfg.tx_fair_slashing = false;
        let handle = SolanaCdnHandle::new(cfg.clone());

        let auth = AuthContext::new(Arc::new(Keypair::new())).unwrap();

        let (ctrl_out_tx, mut ctrl_out_rx) = mpsc::channel::<AgentToPop>(8);
        let (_publisher_tx, mut publisher_rx) = watch::channel::<Option<SocketAddr>>(None);
        let shred_deduper = ShredBatchDeduper::new(64);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let last_hb_sent_ms = AtomicU64::new(0);

        let sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sink_addr = sink.local_addr().unwrap();

        let udp_inject_tpu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tpu.connect(sink_addr).await.unwrap();
        let udp_inject_tvu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tvu.connect(sink_addr).await.unwrap();
        let udp_inject_gossip = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_gossip.connect(sink_addr).await.unwrap();
        let udp_inject_votes = VoteInjectSockets::bind().await.unwrap();

        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::new_unique();

        let tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let bytes = bincode::serialize(&VersionedTransaction::from(tx)).unwrap();
        let fair_tx = fair_tx_from_wire_bytes(bytes);

        let before = FAIR_BATCH_DROPPED_DUP_SIG_TOTAL.load(Ordering::Relaxed);

        let target_slot = Some(999);
        let batch = make_test_fair_batch(
            "pop-dedup-test",
            99,
            0,
            target_slot,
            vec![fair_tx.clone(), fair_tx.clone(), fair_tx],
        );

        handle_pop_msg(
            endpoint,
            test_pop_pubkey(),
            &cfg,
            &auth,
            &handle,
            &ctrl_out_tx,
            &mut publisher_rx,
            &shred_deduper,
            &udp_inject_tpu,
            &udp_inject_tvu,
            &udp_inject_gossip,
            sink_addr,
            &udp_inject_votes,
            &events_tx,
            &last_hb_sent_ms,
            PopToAgent::FairBatch(batch),
        )
        .await;

        let reject = match tokio::time::timeout(Duration::from_secs(10), ctrl_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            AgentToPop::FairBatchReject(reject) => reject,
            other => panic!("expected FairBatchReject, got {other:?}"),
        };
        reject.verify().expect("verify reject");
        assert_eq!(reject.payload.target_slot, target_slot);
        assert!(matches!(
            reject.payload.reason,
            FairBatchRejectReason::DuplicateSig
        ));

        let after = FAIR_BATCH_DROPPED_DUP_SIG_TOTAL.load(Ordering::Relaxed);
        assert!(
            after > before,
            "FAIR_BATCH_DROPPED_DUP_SIG_TOTAL should have incremented"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fair_batch_rejects_oversized_payload() {
        let endpoint: SocketAddr = "198.51.100.1:4444".parse().unwrap();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_ordering = true;
        cfg.tx_fair_slashing = false;
        let handle = SolanaCdnHandle::new(cfg.clone());

        let auth = AuthContext::new(Arc::new(Keypair::new())).unwrap();

        let (ctrl_out_tx, mut ctrl_out_rx) = mpsc::channel::<AgentToPop>(8);
        let (_publisher_tx, mut publisher_rx) = watch::channel::<Option<SocketAddr>>(None);
        let shred_deduper = ShredBatchDeduper::new(64);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let last_hb_sent_ms = AtomicU64::new(0);

        let sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sink_addr = sink.local_addr().unwrap();

        let udp_inject_tpu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tpu.connect(sink_addr).await.unwrap();
        let udp_inject_tvu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tvu.connect(sink_addr).await.unwrap();
        let udp_inject_gossip = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_gossip.connect(sink_addr).await.unwrap();
        let udp_inject_votes = VoteInjectSockets::bind().await.unwrap();

        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::new_unique();

        // Build a valid tx to include alongside the oversized one.
        let good_tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let good_bytes = bincode::serialize(&VersionedTransaction::from(good_tx)).unwrap();
        let good_fair_tx = fair_tx_from_wire_bytes(good_bytes);

        // Build an oversized payload (> PACKET_DATA_SIZE).
        let oversized_payload = vec![0u8; PACKET_DATA_SIZE + 100];
        let oversized_sig = [0xFFu8; 64];
        let oversized_fair_tx = solanacdn_protocol::messages::FairTx {
            sig: SignatureBytes(oversized_sig),
            payload: oversized_payload,
        };

        let before = FAIR_BATCH_DROPPED_PAYLOAD_TOO_LARGE_TOTAL.load(Ordering::Relaxed);

        let target_slot = Some(999);
        let batch = make_test_fair_batch(
            "pop-oversize-test",
            200,
            0,
            target_slot,
            vec![oversized_fair_tx, good_fair_tx],
        );

        handle_pop_msg(
            endpoint,
            test_pop_pubkey(),
            &cfg,
            &auth,
            &handle,
            &ctrl_out_tx,
            &mut publisher_rx,
            &shred_deduper,
            &udp_inject_tpu,
            &udp_inject_tvu,
            &udp_inject_gossip,
            sink_addr,
            &udp_inject_votes,
            &events_tx,
            &last_hb_sent_ms,
            PopToAgent::FairBatch(batch),
        )
        .await;

        let reject = match tokio::time::timeout(Duration::from_secs(10), ctrl_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            AgentToPop::FairBatchReject(reject) => reject,
            other => panic!("expected FairBatchReject, got {other:?}"),
        };
        reject.verify().expect("verify reject");
        assert_eq!(reject.payload.target_slot, target_slot);
        assert!(matches!(
            reject.payload.reason,
            FairBatchRejectReason::InvalidWireTx
        ));
        let after = FAIR_BATCH_DROPPED_PAYLOAD_TOO_LARGE_TOTAL.load(Ordering::Relaxed);
        assert!(
            after > before,
            "FAIR_BATCH_DROPPED_PAYLOAD_TOO_LARGE_TOTAL should have incremented"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fair_batch_rejects_total_bytes_exceeded() {
        let endpoint: SocketAddr = "198.51.100.1:4444".parse().unwrap();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_ordering = true;
        cfg.tx_fair_slashing = false;
        let handle = SolanaCdnHandle::new(cfg.clone());

        let auth = AuthContext::new(Arc::new(Keypair::new())).unwrap();

        let (ctrl_out_tx, mut ctrl_out_rx) = mpsc::channel::<AgentToPop>(8);
        let (_publisher_tx, mut publisher_rx) = watch::channel::<Option<SocketAddr>>(None);
        let shred_deduper = ShredBatchDeduper::new(64);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let last_hb_sent_ms = AtomicU64::new(0);

        let sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sink_addr = sink.local_addr().unwrap();

        let udp_inject_tpu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tpu.connect(sink_addr).await.unwrap();
        let udp_inject_tvu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tvu.connect(sink_addr).await.unwrap();
        let udp_inject_gossip = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_gossip.connect(sink_addr).await.unwrap();
        let udp_inject_votes = VoteInjectSockets::bind().await.unwrap();

        let recent_blockhash = solana_hash::Hash::new_unique();

        // Each padded tx is ~1100 bytes. FAIR_BATCH_MAX_TOTAL_BYTES = 256 * 1232 ≈ 315K.
        // So ~300 txs at ~1100 bytes each (~330K) should exceed the aggregate cap.
        let num_txs = 300;
        let txs: Vec<solanacdn_protocol::messages::FairTx> = (0..num_txs)
            .map(|i| {
                // Each tx gets a unique signer to avoid sig collisions.
                let tx_signer = Keypair::new();
                // Use set_compute_unit_limit with unique values to make unique txs.
                // Pad the tx with a memo to approach PACKET_DATA_SIZE.
                let memo_data = vec![0x41u8; 900]; // 'A' repeated — makes tx ~1100 bytes
                let memo_ix = solana_instruction::Instruction::new_with_bytes(
                    Pubkey::new_unique(), // arbitrary program id
                    &memo_data,
                    vec![],
                );
                let tx = Transaction::new(
                    &[&tx_signer],
                    Message::new(
                        &[
                            ComputeBudgetInstruction::set_compute_unit_limit((i + 1) as u32),
                            memo_ix,
                        ],
                        Some(&tx_signer.pubkey()),
                    ),
                    recent_blockhash,
                );
                let bytes = bincode::serialize(&VersionedTransaction::from(tx)).unwrap();
                assert!(
                    bytes.len() <= PACKET_DATA_SIZE,
                    "each individual tx must fit in PACKET_DATA_SIZE, got {}",
                    bytes.len()
                );
                fair_tx_from_wire_bytes(bytes)
            })
            .collect();

        let before = FAIR_BATCH_DROPPED_TOTAL_BYTES_EXCEEDED_TOTAL.load(Ordering::Relaxed);

        let target_slot = Some(999);
        let batch = make_test_fair_batch("pop-bytes-cap-test", 201, 0, target_slot, txs);

        handle_pop_msg(
            endpoint,
            test_pop_pubkey(),
            &cfg,
            &auth,
            &handle,
            &ctrl_out_tx,
            &mut publisher_rx,
            &shred_deduper,
            &udp_inject_tpu,
            &udp_inject_tvu,
            &udp_inject_gossip,
            sink_addr,
            &udp_inject_votes,
            &events_tx,
            &last_hb_sent_ms,
            PopToAgent::FairBatch(batch),
        )
        .await;

        let reject = match tokio::time::timeout(Duration::from_secs(10), ctrl_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            AgentToPop::FairBatchReject(reject) => reject,
            other => panic!("expected FairBatchReject, got {other:?}"),
        };
        reject.verify().expect("verify reject");
        assert_eq!(reject.payload.target_slot, target_slot);
        assert!(matches!(
            reject.payload.reason,
            FairBatchRejectReason::TotalBytesExceeded
        ));
        let after = FAIR_BATCH_DROPPED_TOTAL_BYTES_EXCEEDED_TOTAL.load(Ordering::Relaxed);
        assert!(
            after > before,
            "FAIR_BATCH_DROPPED_TOTAL_BYTES_EXCEEDED_TOTAL should have incremented"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fair_batch_rejects_sig_mismatch() {
        let endpoint: SocketAddr = "198.51.100.1:4444".parse().unwrap();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_ordering = true;
        cfg.tx_fair_slashing = false;
        let handle = SolanaCdnHandle::new(cfg.clone());

        let auth = AuthContext::new(Arc::new(Keypair::new())).unwrap();

        let (ctrl_out_tx, mut ctrl_out_rx) = mpsc::channel::<AgentToPop>(8);
        let (_publisher_tx, mut publisher_rx) = watch::channel::<Option<SocketAddr>>(None);
        let shred_deduper = ShredBatchDeduper::new(64);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let last_hb_sent_ms = AtomicU64::new(0);

        let sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sink_addr = sink.local_addr().unwrap();

        let udp_inject_tpu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tpu.connect(sink_addr).await.unwrap();
        let udp_inject_tvu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tvu.connect(sink_addr).await.unwrap();
        let udp_inject_gossip = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_gossip.connect(sink_addr).await.unwrap();
        let udp_inject_votes = VoteInjectSockets::bind().await.unwrap();

        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::new_unique();

        // Build a valid tx.
        let tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let bytes = bincode::serialize(&VersionedTransaction::from(tx)).unwrap();

        // Construct a FairTx with a WRONG sig field (doesn't match payload).
        let wrong_sig = [0xDDu8; 64];
        let mismatched_fair_tx = solanacdn_protocol::messages::FairTx {
            sig: SignatureBytes(wrong_sig),
            payload: bytes.clone(),
        };

        // Also include a valid tx so the batch isn't empty and we get a commit.
        let tx2 = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(2)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let bytes2 = bincode::serialize(&VersionedTransaction::from(tx2)).unwrap();
        let good_fair_tx = fair_tx_from_wire_bytes(bytes2);

        let before = FAIR_BATCH_DROPPED_SIG_MISMATCH_TOTAL.load(Ordering::Relaxed);

        let target_slot = Some(999);
        let batch = make_test_fair_batch(
            "pop-sig-mismatch-test",
            202,
            0,
            target_slot,
            vec![mismatched_fair_tx, good_fair_tx],
        );

        handle_pop_msg(
            endpoint,
            test_pop_pubkey(),
            &cfg,
            &auth,
            &handle,
            &ctrl_out_tx,
            &mut publisher_rx,
            &shred_deduper,
            &udp_inject_tpu,
            &udp_inject_tvu,
            &udp_inject_gossip,
            sink_addr,
            &udp_inject_votes,
            &events_tx,
            &last_hb_sent_ms,
            PopToAgent::FairBatch(batch),
        )
        .await;

        let reject = match tokio::time::timeout(Duration::from_secs(10), ctrl_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            AgentToPop::FairBatchReject(reject) => reject,
            other => panic!("expected FairBatchReject, got {other:?}"),
        };
        reject.verify().expect("verify reject");
        assert_eq!(reject.payload.target_slot, target_slot);
        assert!(matches!(
            reject.payload.reason,
            FairBatchRejectReason::TxSigMismatch
        ));
        let after = FAIR_BATCH_DROPPED_SIG_MISMATCH_TOTAL.load(Ordering::Relaxed);
        assert!(
            after > before,
            "FAIR_BATCH_DROPPED_SIG_MISMATCH_TOTAL should have incremented"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fair_batch_rejects_unparseable_payload() {
        let endpoint: SocketAddr = "198.51.100.1:4444".parse().unwrap();

        let mut cfg = SolanaCdnConfig::default();
        cfg.tx_fair_ordering = true;
        cfg.tx_fair_slashing = false;
        let handle = SolanaCdnHandle::new(cfg.clone());

        let auth = AuthContext::new(Arc::new(Keypair::new())).unwrap();

        let (ctrl_out_tx, mut ctrl_out_rx) = mpsc::channel::<AgentToPop>(8);
        let (_publisher_tx, mut publisher_rx) = watch::channel::<Option<SocketAddr>>(None);
        let shred_deduper = ShredBatchDeduper::new(64);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let last_hb_sent_ms = AtomicU64::new(0);

        let sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sink_addr = sink.local_addr().unwrap();

        let udp_inject_tpu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tpu.connect(sink_addr).await.unwrap();
        let udp_inject_tvu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_tvu.connect(sink_addr).await.unwrap();
        let udp_inject_gossip = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp_inject_gossip.connect(sink_addr).await.unwrap();
        let udp_inject_votes = VoteInjectSockets::bind().await.unwrap();

        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::new_unique();

        // Garbage payload that can't be parsed as a wire tx.
        // Use 3 bytes that encode shortvec len=0 so try_first_signature_bytes returns None.
        let garbage_payload = vec![0x00u8; 10]; // shortvec len 0 → sig_count=0 → returns None
        let garbage_sig = [0xEEu8; 64];
        let garbage_fair_tx = solanacdn_protocol::messages::FairTx {
            sig: SignatureBytes(garbage_sig),
            payload: garbage_payload,
        };

        // Include a valid tx so we get a commit.
        let good_tx = Transaction::new(
            &[&signer],
            Message::new(
                &[ComputeBudgetInstruction::set_compute_unit_limit(1)],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        );
        let good_bytes = bincode::serialize(&VersionedTransaction::from(good_tx)).unwrap();
        let good_fair_tx = fair_tx_from_wire_bytes(good_bytes);

        let before = FAIR_BATCH_DROPPED_SIG_MISMATCH_TOTAL.load(Ordering::Relaxed);

        let target_slot = Some(999);
        let batch = make_test_fair_batch(
            "pop-garbage-test",
            203,
            0,
            target_slot,
            vec![garbage_fair_tx, good_fair_tx],
        );

        handle_pop_msg(
            endpoint,
            test_pop_pubkey(),
            &cfg,
            &auth,
            &handle,
            &ctrl_out_tx,
            &mut publisher_rx,
            &shred_deduper,
            &udp_inject_tpu,
            &udp_inject_tvu,
            &udp_inject_gossip,
            sink_addr,
            &udp_inject_votes,
            &events_tx,
            &last_hb_sent_ms,
            PopToAgent::FairBatch(batch),
        )
        .await;

        let reject = match tokio::time::timeout(Duration::from_secs(10), ctrl_out_rx.recv())
            .await
            .unwrap()
            .unwrap()
        {
            AgentToPop::FairBatchReject(reject) => reject,
            other => panic!("expected FairBatchReject, got {other:?}"),
        };
        reject.verify().expect("verify reject");
        assert_eq!(reject.payload.target_slot, target_slot);
        assert!(matches!(
            reject.payload.reason,
            FairBatchRejectReason::InvalidWireTx
        ));
        let after = FAIR_BATCH_DROPPED_SIG_MISMATCH_TOTAL.load(Ordering::Relaxed);
        assert!(
            after > before,
            "FAIR_BATCH_DROPPED_SIG_MISMATCH_TOTAL should have incremented for unparseable payload"
        );
    }

    #[test]
    fn fair_merkle_root_deterministic() {
        let sig_a: [u8; 64] = {
            let mut s = [0u8; 64];
            s[0] = 0xAA;
            s
        };
        let sig_b: [u8; 64] = {
            let mut s = [0u8; 64];
            s[0] = 0xBB;
            s
        };

        let root1 = fair_merkle_root(&[sig_a, sig_b]);
        let root2 = fair_merkle_root(&[sig_a, sig_b]);
        assert_eq!(root1, root2, "same inputs must produce same root");

        let root3 = fair_merkle_root(&[sig_b, sig_a]);
        assert_ne!(root1, root3, "different order must produce different root");

        let root4 = fair_merkle_root(&[sig_a]);
        assert_ne!(root1, root4, "different inputs must produce different root");
    }

    #[test]
    fn fair_merkle_root_single_and_empty() {
        // Empty input returns zero hash.
        let empty = fair_merkle_root(&[]);
        assert_eq!(empty, [0u8; 32]);

        // Single sig returns a deterministic non-zero hash.
        let sig: [u8; 64] = {
            let mut s = [0u8; 64];
            s[0] = 0xCC;
            s
        };
        let single = fair_merkle_root(&[sig]);
        assert_ne!(single, [0u8; 32], "single-element root should not be zero");

        // Repeatable.
        let single2 = fair_merkle_root(&[sig]);
        assert_eq!(single, single2);
    }

    #[test]
    fn race_tracker_exports_delta_histogram_segments() {
        let mut cfg = SolanaCdnConfig::default();
        cfg.race_enabled = true;
        cfg.race_sample_bits = 0;
        cfg.race_window_ms = 5_000;
        cfg.tvu_shred_ingest_mode = TvuShredIngestMode::All;
        let handle = SolanaCdnHandle::new(cfg);

        let pop: SocketAddr = "198.51.100.9:4444".parse().unwrap();
        let (tx, _rx) = mpsc::channel::<UplinkMsg>(1);
        handle.set_publisher_uplink(Some(pop), Some(Arc::new(SessionUplink { tx })));

        let shred_id = LedgerShredId::new(100, 7, solana_ledger::shred::ShredType::Data);
        {
            let mut tracker = handle.race_state.lock().unwrap();
            tracker.observe(
                shred_id,
                RaceSource::SolanaCdn,
                900,
                handle.cfg.race_window_ms,
                Some(pop),
                None,
            );
            tracker.observe(
                shred_id,
                RaceSource::Gossip,
                1_000,
                handle.cfg.race_window_ms,
                None,
                Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 99))),
            );
        }

        let text = format_prometheus_metrics(&handle);

        assert!(text.contains("solanacdn_race_delta_seconds_count 1"));
        assert!(text.contains("solanacdn_race_delta_seconds_bucket{le=\"-0.100\"} 1"));
        assert!(text.contains("solanacdn_race_delta_seconds_by_pop_endpoint_count{pop_endpoint=\"198.51.100.9:4444\"} 1"));
        assert!(text.contains("solanacdn_race_delta_seconds_by_hour_utc_count{hour_utc=\"00\"} 1"));
    }

    #[test]
    fn race_tracker_buffers_race_samples_for_pipe_ingest() {
        let mut cfg = SolanaCdnConfig::default();
        cfg.race_enabled = true;
        cfg.race_sample_bits = 0;
        cfg.tvu_shred_ingest_mode = TvuShredIngestMode::All;
        let handle = SolanaCdnHandle::new(cfg);

        let pop: SocketAddr = "198.51.100.9:4444".parse().unwrap();
        let (tx, _rx) = mpsc::channel::<UplinkMsg>(1);
        handle.set_publisher_uplink(Some(pop), Some(Arc::new(SessionUplink { tx })));

        let gossip_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 99));
        let shred_id = LedgerShredId::new(100, 7, solana_ledger::shred::ShredType::Data);
        {
            let mut tracker = handle.race_state.lock().unwrap();
            tracker.observe(
                shred_id,
                RaceSource::SolanaCdn,
                900,
                handle.cfg.race_window_ms,
                Some(pop),
                None,
            );
            tracker.observe(
                shred_id,
                RaceSource::Gossip,
                1_000,
                handle.cfg.race_window_ms,
                None,
                Some(gossip_ip),
            );
        }

        let (_snapshot, samples) = handle.pipe_ingest_race_snapshot_and_samples(10);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].gossip_src_ip, gossip_ip);
        assert_eq!(samples[0].delta_ms, -100);

        handle.pipe_ingest_consume_race_samples(1);
        let (_snapshot, samples) = handle.pipe_ingest_race_snapshot_and_samples(10);
        assert!(samples.is_empty());
    }

    #[test]
    fn pipe_ingest_body_includes_race_and_samples_when_available() {
        let mut cfg = SolanaCdnConfig::default();
        cfg.race_enabled = true;
        cfg.race_sample_bits = 0;
        cfg.tvu_shred_ingest_mode = TvuShredIngestMode::All;
        let handle = SolanaCdnHandle::new(cfg.clone());

        let pop: SocketAddr = "198.51.100.9:4444".parse().unwrap();
        let (tx, _rx) = mpsc::channel::<UplinkMsg>(1);
        handle.set_publisher_uplink(Some(pop), Some(Arc::new(SessionUplink { tx })));

        let gossip_ip = IpAddr::V4(Ipv4Addr::new(203, 0, 113, 99));
        let shred_id = LedgerShredId::new(100, 7, solana_ledger::shred::ShredType::Data);
        {
            let mut tracker = handle.race_state.lock().unwrap();
            tracker.observe(
                shred_id,
                RaceSource::SolanaCdn,
                900,
                handle.cfg.race_window_ms,
                Some(pop),
                None,
            );
            tracker.observe(
                shred_id,
                RaceSource::Gossip,
                1_000,
                handle.cfg.race_window_ms,
                None,
                Some(gossip_ip),
            );
        }

        let v = PipeApiVerifyResult {
            agent_id: "agent".to_string(),
            run_id: "run".to_string(),
            run_token: "token".to_string(),
            heartbeat_schema_version: 0,
            ingest: PipeApiIngestConfig {
                url: "https://example.invalid/ingest".to_string(),
                interval_secs: 10,
                max_body_bytes: 0,
                max_events: 100,
            },
            pop_endpoints: Vec::new(),
        };

        let (body, consumed) = build_pipe_ingest_body(&v, &handle, &cfg, "validator", 123);
        let race = body.get("race").and_then(|v| v.as_object()).expect("race");
        assert_eq!(race.get("pairs_total").and_then(|v| v.as_u64()), Some(1));

        let hist = race
            .get("histogram")
            .and_then(|v| v.as_object())
            .expect("histogram");
        assert_eq!(
            hist.get("delta_bucket_counts")
                .and_then(|v| v.as_array())
                .map(|a| a.len()),
            Some(RACE_DELTA_BUCKETS_MS.len())
        );

        let samples = body
            .get("race_samples")
            .and_then(|v| v.as_array())
            .expect("race_samples");
        assert_eq!(samples.len(), 1);
        assert_eq!(consumed, 1);
    }

    #[test]
    fn select_publisher_prefers_preferred_endpoint() {
        let preferred: SocketAddr = "127.0.0.1:10000".parse().unwrap();
        let other: SocketAddr = "127.0.0.1:10001".parse().unwrap();

        let desired: HashSet<SocketAddr> = [preferred, other].into_iter().collect();
        let connected: HashMap<SocketAddr, ConnectedPop> = [
            (
                preferred,
                ConnectedPop {
                    udp_enabled: false,
                    rtt_ewma_ms: 1_000,
                    rtt_valid: false,
                },
            ),
            (
                other,
                ConnectedPop {
                    udp_enabled: true,
                    rtt_ewma_ms: 1,
                    rtt_valid: true,
                },
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(
            select_publisher(Some(preferred), &desired, &connected, true),
            Some(preferred)
        );
    }

    #[test]
    fn select_publisher_prefers_udp_when_requested() {
        let a: SocketAddr = "127.0.0.1:10010".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:10011".parse().unwrap();

        let desired: HashSet<SocketAddr> = [a, b].into_iter().collect();
        let connected: HashMap<SocketAddr, ConnectedPop> = [
            (
                a,
                ConnectedPop {
                    udp_enabled: false,
                    rtt_ewma_ms: 1,
                    rtt_valid: true,
                },
            ),
            (
                b,
                ConnectedPop {
                    udp_enabled: true,
                    rtt_ewma_ms: 500,
                    rtt_valid: true,
                },
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(select_publisher(None, &desired, &connected, true), Some(b));
    }

    #[test]
    fn select_publisher_prefers_valid_rtt_then_lowest_rtt() {
        let a: SocketAddr = "127.0.0.1:10020".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:10021".parse().unwrap();
        let c: SocketAddr = "127.0.0.1:10022".parse().unwrap();

        let desired: HashSet<SocketAddr> = [a, b, c].into_iter().collect();
        let connected: HashMap<SocketAddr, ConnectedPop> = [
            (
                a,
                ConnectedPop {
                    udp_enabled: true,
                    rtt_ewma_ms: 9,
                    rtt_valid: true,
                },
            ),
            (
                b,
                ConnectedPop {
                    udp_enabled: true,
                    rtt_ewma_ms: 1,
                    rtt_valid: true,
                },
            ),
            (
                c,
                ConnectedPop {
                    udp_enabled: true,
                    rtt_ewma_ms: 0,
                    rtt_valid: false,
                },
            ),
        ]
        .into_iter()
        .collect();

        assert_eq!(select_publisher(None, &desired, &connected, false), Some(b));
    }

    async fn read_agent_msg<R: AsyncRead + Unpin>(reader: &mut R) -> AgentToPop {
        let bytes = read_len_prefixed(reader).await.unwrap();
        decode_envelope(&bytes).unwrap()
    }

    async fn write_pop_msg<W: AsyncWrite + Unpin>(writer: &mut W, msg: &PopToAgent) {
        let bytes = encode_envelope(msg).unwrap();
        write_len_prefixed(writer, &bytes).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn len_prefixed_rejects_oversized_frame() {
        let (mut tx, mut rx) = tokio::io::duplex(64);
        let too_large = (DEFAULT_MAX_FRAME_BYTES as u32) + 1;
        tx.write_all(&too_large.to_be_bytes()).await.unwrap();

        let err = read_len_prefixed(&mut rx).await.unwrap_err();
        match err {
            SolanaCdnError::Io(e) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
            other => panic!("expected io invalid input, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn udp_push_shred_batch_is_injected_to_tvu() {
        init_rustls();

        let pop_udp_shreds = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pop_udp_votes = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pop_udp_shreds_port = pop_udp_shreds.local_addr().unwrap().port();
        let pop_udp_votes_port = pop_udp_votes.local_addr().unwrap().port();

        let pop_keypair = Keypair::new();
        let (cert, key) = solana_tls_utils::new_dummy_x509_certificate(&pop_keypair);
        let server_cfg = quinn::ServerConfig::with_single_cert(vec![cert], key).unwrap();
        let endpoint = Endpoint::server(server_cfg, "127.0.0.1:0".parse().unwrap()).unwrap();
        let pop_addr = endpoint.local_addr().unwrap();

        let udp_token = random_nonce_16();
        let payload = Bytes::from_static(b"test_shred_payload");

        let pop_task = tokio::spawn(async move {
            let Some(connecting) = endpoint.accept().await else {
                panic!("expected incoming QUIC connection");
            };
            let conn = connecting.await.unwrap();

            let (mut ctrl_send, mut ctrl_recv) = conn.accept_bi().await.unwrap();
            match read_agent_msg(&mut ctrl_recv).await {
                AgentToPop::Auth(_) => {}
                other => panic!("expected Auth, got {other:?}"),
            }

            write_pop_msg(
                &mut ctrl_send,
                &PopToAgent::AuthOk(AuthOk {
                    pop_id: "test-pop".to_string(),
                    pop_pubkey: test_pop_pubkey(),
                    server_time_ms: now_ms(),
                    udp_token,
                    udp_shreds_port: pop_udp_shreds_port,
                    udp_votes_port: pop_udp_votes_port,
                }),
            )
            .await;

            let mut agent_shreds_port: Option<u16> = None;
            let mut got_register_validator_ports = false;
            let mut got_subscribe = false;

            while agent_shreds_port.is_none() || !got_register_validator_ports || !got_subscribe {
                match read_agent_msg(&mut ctrl_recv).await {
                    AgentToPop::RegisterUdpPorts { shreds_port, .. } => {
                        agent_shreds_port = Some(shreds_port);
                    }
                    AgentToPop::RegisterValidatorPorts { direct_shreds, .. } => {
                        assert!(
                            direct_shreds,
                            "expected direct_shreds=true when publisher and UDP enabled"
                        );
                        got_register_validator_ports = true;
                    }
                    AgentToPop::SubscribeShreds => {
                        got_subscribe = true;
                    }
                    AgentToPop::Heartbeat(_) => {}
                    other => {
                        debug!("unexpected ctrl msg from client: {other:?}");
                    }
                }
            }

            // Best-effort drain of the shreds/votes StreamHello messages.
            for _ in 0..2 {
                if let Ok(Ok((mut _send, mut recv))) =
                    tokio::time::timeout(Duration::from_secs(1), conn.accept_bi()).await
                {
                    let _ = read_agent_msg(&mut recv).await;
                }
            }

            let dst = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), agent_shreds_port.unwrap());
            let batch = make_single_shred_batch(ShredKind::Tvu, payload);
            let msg = PopToAgent::PushShredBatch(batch);
            let bytes = solanacdn_protocol::udp::encode_udp_datagram(udp_token, &msg).unwrap();
            pop_udp_shreds.send_to(&bytes, dst).await.unwrap();

            // Keep the QUIC connection alive briefly while the client processes the UDP packet.
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let inject_tpu_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_tvu_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_gossip_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_tpu = inject_tpu_socket.local_addr().unwrap();
        let inject_tvu = inject_tvu_socket.local_addr().unwrap();
        let inject_gossip = inject_gossip_socket.local_addr().unwrap();

        let mut cfg = SolanaCdnConfig::new(pop_addr);
        cfg.pop_endpoints = vec![pop_addr];
        cfg.tls_insecure_skip_verify = true;
        cfg.udp_mode = DataPlaneMode::Auto;

        let cfg = Arc::new(cfg);
        let handle = Arc::new(SolanaCdnHandle::new((*cfg).clone()));
        let handle_for_client = Arc::clone(&handle);

        let identity_keypair = Arc::new(Keypair::new());
        let auth = Arc::new(AuthContext::new(identity_keypair.clone()).unwrap());
        let quic_connect = Arc::new(QuicConnectConfig {
            client_config: make_quic_client_config(&cfg).unwrap(),
            server_name: cfg.server_name.clone(),
        });

        let (_uplink_tx, mut uplink_rx) = mpsc::channel::<UplinkMsg>(16);
        let (_events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let (_publisher_tx, publisher_rx) = watch::channel::<Option<SocketAddr>>(Some(pop_addr));
        let (stop_tx, stop_rx) = watch::channel(false);

        let client_task = tokio::spawn(async move {
            run_pop_session(
                pop_addr,
                cfg,
                auth,
                quic_connect,
                handle_for_client,
                &mut uplink_rx,
                inject_tpu,
                inject_tvu,
                inject_gossip,
                inject_tpu,
                ShredBatchDeduper::new(64),
                None,
                publisher_rx,
                _events_tx,
                stop_rx,
            )
            .await
            .unwrap();
        });

        let mut buf = [0u8; 2048];
        let (len, _peer) = tokio::time::timeout(
            Duration::from_secs(5),
            inject_tvu_socket.recv_from(&mut buf),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(&buf[..len], b"test_shred_payload");

        let _ = stop_tx.send(true);
        tokio::time::timeout(Duration::from_secs(5), client_task)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), pop_task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn udp_push_vote_datagram_is_injected_and_deduped() {
        init_rustls();

        let pop_udp_shreds = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pop_udp_votes = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pop_udp_shreds_port = pop_udp_shreds.local_addr().unwrap().port();
        let pop_udp_votes_port = pop_udp_votes.local_addr().unwrap().port();

        let pop_keypair = Keypair::new();
        let (cert, key) = solana_tls_utils::new_dummy_x509_certificate(&pop_keypair);
        let server_cfg = quinn::ServerConfig::with_single_cert(vec![cert], key).unwrap();
        let endpoint = Endpoint::server(server_cfg, "127.0.0.1:0".parse().unwrap()).unwrap();
        let pop_addr = endpoint.local_addr().unwrap();

        let udp_token = random_nonce_16();
        let vote_sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let vote_sink_addr = vote_sink.local_addr().unwrap();

        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::new_unique();
        let vote_ix = Instruction {
            program_id: solana_vote_program::id(),
            accounts: Vec::new(),
            data: vec![0],
        };
        let vote_payload = bincode::serialize(&VersionedTransaction::from(Transaction::new(
            &[&signer],
            Message::new(
                &[
                    ComputeBudgetInstruction::set_compute_unit_limit(1),
                    vote_ix,
                ],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        )))
        .unwrap();
        let vote_payload_for_pop = vote_payload.clone();

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        let pop_task = tokio::spawn(async move {
            let Some(connecting) = endpoint.accept().await else {
                panic!("expected incoming QUIC connection");
            };
            let conn = connecting.await.unwrap();

            let (mut ctrl_send, mut ctrl_recv) = conn.accept_bi().await.unwrap();
            match read_agent_msg(&mut ctrl_recv).await {
                AgentToPop::Auth(_) => {}
                other => panic!("expected Auth, got {other:?}"),
            }

            write_pop_msg(
                &mut ctrl_send,
                &PopToAgent::AuthOk(AuthOk {
                    pop_id: "test-pop".to_string(),
                    pop_pubkey: test_pop_pubkey(),
                    server_time_ms: now_ms(),
                    udp_token,
                    udp_shreds_port: pop_udp_shreds_port,
                    udp_votes_port: pop_udp_votes_port,
                }),
            )
            .await;

            let mut agent_votes_port: Option<u16> = None;
            let mut got_register_validator_ports = false;
            let mut got_subscribe = false;

            while agent_votes_port.is_none() || !got_register_validator_ports || !got_subscribe {
                match read_agent_msg(&mut ctrl_recv).await {
                    AgentToPop::RegisterUdpPorts { votes_port, .. } => {
                        agent_votes_port = Some(votes_port);
                    }
                    AgentToPop::RegisterValidatorPorts { direct_shreds, .. } => {
                        assert!(
                            direct_shreds,
                            "expected direct_shreds=true when publisher and UDP enabled"
                        );
                        got_register_validator_ports = true;
                    }
                    AgentToPop::SubscribeShreds => {
                        got_subscribe = true;
                    }
                    AgentToPop::Heartbeat(_) => {}
                    other => {
                        debug!("unexpected ctrl msg from client: {other:?}");
                    }
                }
            }

            // Best-effort drain of the shreds/votes StreamHello messages.
            for _ in 0..2 {
                if let Ok(Ok((mut _send, mut recv))) =
                    tokio::time::timeout(Duration::from_secs(1), conn.accept_bi()).await
                {
                    let _ = read_agent_msg(&mut recv).await;
                }
            }

            let dst = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), agent_votes_port.unwrap());
            let dg = VoteDatagram {
                flow_id: vote_flow_id(&vote_sink_addr),
                src: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
                dst: vote_sink_addr,
                payload: vote_payload_for_pop,
            };
            let msg = PopToAgent::PushVoteDatagram(dg);
            let bytes = solanacdn_protocol::udp::encode_udp_datagram(udp_token, &msg).unwrap();
            pop_udp_votes.send_to(&bytes, dst).await.unwrap();
            pop_udp_votes.send_to(&bytes, dst).await.unwrap();

            let _ = ready_tx.send(());

            // Keep the QUIC connection alive briefly while the client processes the UDP packet.
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let inject_tpu_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_tvu_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_gossip_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_tpu = inject_tpu_socket.local_addr().unwrap();
        let inject_tvu = inject_tvu_socket.local_addr().unwrap();
        let inject_gossip = inject_gossip_socket.local_addr().unwrap();

        let mut cfg = SolanaCdnConfig::new(pop_addr);
        cfg.pop_endpoints = vec![pop_addr];
        cfg.tls_insecure_skip_verify = true;
        cfg.udp_mode = DataPlaneMode::Auto;

        let cfg = Arc::new(cfg);
        let handle = Arc::new(SolanaCdnHandle::new((*cfg).clone()));
        let handle_for_client = Arc::clone(&handle);

        let identity_keypair = Arc::new(Keypair::new());
        let auth = Arc::new(AuthContext::new(identity_keypair.clone()).unwrap());
        let quic_connect = Arc::new(QuicConnectConfig {
            client_config: make_quic_client_config(&cfg).unwrap(),
            server_name: cfg.server_name.clone(),
        });

        let (_uplink_tx, mut uplink_rx) = mpsc::channel::<UplinkMsg>(16);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let (_publisher_tx, publisher_rx) = watch::channel::<Option<SocketAddr>>(Some(pop_addr));
        let (stop_tx, stop_rx) = watch::channel(false);

        let client_task = tokio::spawn(async move {
            run_pop_session(
                pop_addr,
                cfg,
                auth,
                quic_connect,
                handle_for_client,
                &mut uplink_rx,
                inject_tpu,
                inject_tvu,
                inject_gossip,
                vote_sink_addr,
                ShredBatchDeduper::new(64),
                None,
                publisher_rx,
                events_tx,
                stop_rx,
            )
            .await
            .unwrap();
        });

        ready_rx.await.unwrap();

        let mut buf = [0u8; 2048];
        let (len, _peer) =
            tokio::time::timeout(Duration::from_secs(5), vote_sink.recv_from(&mut buf))
                .await
                .unwrap()
                .unwrap();
        assert_eq!(&buf[..len], vote_payload.as_slice());

        let second =
            tokio::time::timeout(Duration::from_millis(200), vote_sink.recv_from(&mut buf)).await;
        assert!(
            second.is_err(),
            "expected deduped vote datagram to be dropped"
        );

        let _ = stop_tx.send(true);
        tokio::time::timeout(Duration::from_secs(5), client_task)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), pop_task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn udp_push_vote_datagram_ipv6_is_injected() {
        init_rustls();

        let vote_sink = match UdpSocket::bind("[::1]:0").await {
            Ok(sock) => sock,
            Err(_) => return,
        };
        let vote_sink_addr = match vote_sink.local_addr() {
            Ok(addr) => addr,
            Err(_) => return,
        };
        if !vote_sink_addr.is_ipv6() {
            return;
        }

        let signer = Keypair::new();
        let recent_blockhash = solana_hash::Hash::new_unique();
        let vote_ix = Instruction {
            program_id: solana_vote_program::id(),
            accounts: Vec::new(),
            data: vec![0],
        };
        let vote_payload = bincode::serialize(&VersionedTransaction::from(Transaction::new(
            &[&signer],
            Message::new(
                &[
                    ComputeBudgetInstruction::set_compute_unit_limit(1),
                    vote_ix,
                ],
                Some(&signer.pubkey()),
            ),
            recent_blockhash,
        )))
        .unwrap();
        let vote_payload_for_pop = vote_payload.clone();

        let pop_udp_shreds = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pop_udp_votes = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pop_udp_shreds_port = pop_udp_shreds.local_addr().unwrap().port();
        let pop_udp_votes_port = pop_udp_votes.local_addr().unwrap().port();

        let pop_keypair = Keypair::new();
        let (cert, key) = solana_tls_utils::new_dummy_x509_certificate(&pop_keypair);
        let server_cfg = quinn::ServerConfig::with_single_cert(vec![cert], key).unwrap();
        let endpoint = Endpoint::server(server_cfg, "127.0.0.1:0".parse().unwrap()).unwrap();
        let pop_addr = endpoint.local_addr().unwrap();

        let udp_token = random_nonce_16();

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        let pop_task = tokio::spawn(async move {
            let Some(connecting) = endpoint.accept().await else {
                panic!("expected incoming QUIC connection");
            };
            let conn = connecting.await.unwrap();

            let (mut ctrl_send, mut ctrl_recv) = conn.accept_bi().await.unwrap();
            match read_agent_msg(&mut ctrl_recv).await {
                AgentToPop::Auth(_) => {}
                other => panic!("expected Auth, got {other:?}"),
            }

            write_pop_msg(
                &mut ctrl_send,
                &PopToAgent::AuthOk(AuthOk {
                    pop_id: "test-pop".to_string(),
                    pop_pubkey: test_pop_pubkey(),
                    server_time_ms: now_ms(),
                    udp_token,
                    udp_shreds_port: pop_udp_shreds_port,
                    udp_votes_port: pop_udp_votes_port,
                }),
            )
            .await;

            let mut agent_votes_port: Option<u16> = None;
            let mut got_register_validator_ports = false;
            let mut got_subscribe = false;

            while agent_votes_port.is_none() || !got_register_validator_ports || !got_subscribe {
                match read_agent_msg(&mut ctrl_recv).await {
                    AgentToPop::RegisterUdpPorts { votes_port, .. } => {
                        agent_votes_port = Some(votes_port);
                    }
                    AgentToPop::RegisterValidatorPorts { direct_shreds, .. } => {
                        assert!(
                            direct_shreds,
                            "expected direct_shreds=true when publisher and UDP enabled"
                        );
                        got_register_validator_ports = true;
                    }
                    AgentToPop::SubscribeShreds => {
                        got_subscribe = true;
                    }
                    AgentToPop::Heartbeat(_) => {}
                    other => {
                        debug!("unexpected ctrl msg from client: {other:?}");
                    }
                }
            }

            // Best-effort drain of the shreds/votes StreamHello messages.
            for _ in 0..2 {
                if let Ok(Ok((mut _send, mut recv))) =
                    tokio::time::timeout(Duration::from_secs(1), conn.accept_bi()).await
                {
                    let _ = read_agent_msg(&mut recv).await;
                }
            }

            let dst = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), agent_votes_port.unwrap());
            let dg = VoteDatagram {
                flow_id: vote_flow_id(&vote_sink_addr),
                src: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
                dst: vote_sink_addr,
                payload: vote_payload_for_pop,
            };
            let msg = PopToAgent::PushVoteDatagram(dg);
            let bytes = solanacdn_protocol::udp::encode_udp_datagram(udp_token, &msg).unwrap();
            pop_udp_votes.send_to(&bytes, dst).await.unwrap();

            let _ = ready_tx.send(());

            // Keep the QUIC connection alive briefly while the client processes the UDP packet.
            tokio::time::sleep(Duration::from_millis(200)).await;
        });

        let inject_tpu_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_tvu_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_gossip_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_tpu = inject_tpu_socket.local_addr().unwrap();
        let inject_tvu = inject_tvu_socket.local_addr().unwrap();
        let inject_gossip = inject_gossip_socket.local_addr().unwrap();

        let mut cfg = SolanaCdnConfig::new(pop_addr);
        cfg.pop_endpoints = vec![pop_addr];
        cfg.tls_insecure_skip_verify = true;
        cfg.udp_mode = DataPlaneMode::Auto;

        let cfg = Arc::new(cfg);
        let handle = Arc::new(SolanaCdnHandle::new((*cfg).clone()));
        let handle_for_client = Arc::clone(&handle);

        let identity_keypair = Arc::new(Keypair::new());
        let auth = Arc::new(AuthContext::new(identity_keypair.clone()).unwrap());
        let quic_connect = Arc::new(QuicConnectConfig {
            client_config: make_quic_client_config(&cfg).unwrap(),
            server_name: cfg.server_name.clone(),
        });

        let (_uplink_tx, mut uplink_rx) = mpsc::channel::<UplinkMsg>(16);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let (_publisher_tx, publisher_rx) = watch::channel::<Option<SocketAddr>>(Some(pop_addr));
        let (stop_tx, stop_rx) = watch::channel(false);

        let client_task = tokio::spawn(async move {
            run_pop_session(
                pop_addr,
                cfg,
                auth,
                quic_connect,
                handle_for_client,
                &mut uplink_rx,
                inject_tpu,
                inject_tvu,
                inject_gossip,
                vote_sink_addr,
                ShredBatchDeduper::new(64),
                None,
                publisher_rx,
                events_tx,
                stop_rx,
            )
            .await
            .unwrap();
        });

        ready_rx.await.unwrap();

        let mut buf = [0u8; 2048];
        let (len, _peer) = match tokio::time::timeout(
            Duration::from_secs(5),
            vote_sink.recv_from(&mut buf),
        )
        .await
        {
            Ok(Ok(v)) => v,
            other => panic!(
                "timed out waiting for IPv6 vote injection: {other:?}; status={:?}",
                handle.status_snapshot()
            ),
        };
        assert_eq!(&buf[..len], vote_payload.as_slice());

        let _ = stop_tx.send(true);
        tokio::time::timeout(Duration::from_secs(5), client_task)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), pop_task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn push_vote_datagram_rejects_unexpected_dst() {
        let endpoint: SocketAddr = "198.51.100.1:4444".parse().unwrap();

        let cfg = SolanaCdnConfig::default();
        let handle = SolanaCdnHandle::new(cfg.clone());

        let auth = AuthContext::new(Arc::new(Keypair::new())).unwrap();

        let (ctrl_out_tx, _ctrl_out_rx) = mpsc::channel::<AgentToPop>(1);
        let (_publisher_tx, mut publisher_rx) = watch::channel::<Option<SocketAddr>>(Some(endpoint));
        let shred_deduper = ShredBatchDeduper::new(64);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let last_hb_sent_ms = AtomicU64::new(0);

        let vote_sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let vote_sink_addr = vote_sink.local_addr().unwrap();

        let udp_inject_tpu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let udp_inject_tvu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let udp_inject_gossip = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let udp_inject_votes = VoteInjectSockets::bind().await.unwrap();

        let wrong_dst = SocketAddr::new(vote_sink_addr.ip(), vote_sink_addr.port() ^ 1);
        let dg = VoteDatagram {
            flow_id: vote_flow_id(&wrong_dst),
            src: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            dst: wrong_dst,
            payload: b"vote_payload".to_vec(),
        };

        handle_pop_msg(
            endpoint,
            test_pop_pubkey(),
            &cfg,
            &auth,
            &handle,
            &ctrl_out_tx,
            &mut publisher_rx,
            &shred_deduper,
            &udp_inject_tpu,
            &udp_inject_tvu,
            &udp_inject_gossip,
            vote_sink_addr,
            &udp_inject_votes,
            &events_tx,
            &last_hb_sent_ms,
            PopToAgent::PushVoteDatagram(dg),
        )
        .await;

        let mut buf = [0u8; 2048];
        let recv =
            tokio::time::timeout(Duration::from_millis(200), vote_sink.recv_from(&mut buf)).await;
        assert!(
            recv.is_err(),
            "expected vote datagram with unexpected dst to be dropped"
        );
        assert_eq!(
            handle
                .dropped_vote_datagrams_unexpected_dst
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn push_vote_datagram_rejects_oversized_payload() {
        let endpoint: SocketAddr = "198.51.100.1:4444".parse().unwrap();

        let cfg = SolanaCdnConfig::default();
        let handle = SolanaCdnHandle::new(cfg.clone());

        let auth = AuthContext::new(Arc::new(Keypair::new())).unwrap();

        let (ctrl_out_tx, _ctrl_out_rx) = mpsc::channel::<AgentToPop>(1);
        let (_publisher_tx, mut publisher_rx) = watch::channel::<Option<SocketAddr>>(Some(endpoint));
        let shred_deduper = ShredBatchDeduper::new(64);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let last_hb_sent_ms = AtomicU64::new(0);

        let vote_sink = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let vote_sink_addr = vote_sink.local_addr().unwrap();

        let udp_inject_tpu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let udp_inject_tvu = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let udp_inject_gossip = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let udp_inject_votes = VoteInjectSockets::bind().await.unwrap();

        let dg = VoteDatagram {
            flow_id: vote_flow_id(&vote_sink_addr),
            src: SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
            dst: vote_sink_addr,
            payload: vec![0u8; PACKET_DATA_SIZE + 1],
        };

        handle_pop_msg(
            endpoint,
            test_pop_pubkey(),
            &cfg,
            &auth,
            &handle,
            &ctrl_out_tx,
            &mut publisher_rx,
            &shred_deduper,
            &udp_inject_tpu,
            &udp_inject_tvu,
            &udp_inject_gossip,
            vote_sink_addr,
            &udp_inject_votes,
            &events_tx,
            &last_hb_sent_ms,
            PopToAgent::PushVoteDatagram(dg),
        )
        .await;

        let mut buf = [0u8; 2048];
        let recv =
            tokio::time::timeout(Duration::from_millis(200), vote_sink.recv_from(&mut buf)).await;
        assert!(
            recv.is_err(),
            "expected vote datagram with oversized payload to be dropped"
        );
        assert_eq!(
            handle
                .dropped_vote_datagrams_oversized_payload
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn udp_publish_vote_datagram_is_sent_to_pop() {
        init_rustls();

        let pop_udp_shreds = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pop_udp_votes = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let pop_udp_shreds_port = pop_udp_shreds.local_addr().unwrap().port();
        let pop_udp_votes_port = pop_udp_votes.local_addr().unwrap().port();

        let pop_keypair = Keypair::new();
        let (cert, key) = solana_tls_utils::new_dummy_x509_certificate(&pop_keypair);
        let server_cfg = quinn::ServerConfig::with_single_cert(vec![cert], key).unwrap();
        let endpoint = Endpoint::server(server_cfg, "127.0.0.1:0".parse().unwrap()).unwrap();
        let pop_addr = endpoint.local_addr().unwrap();

        let udp_token = random_nonce_16();

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<()>();
        let pop_task = tokio::spawn(async move {
            let Some(connecting) = endpoint.accept().await else {
                panic!("expected incoming QUIC connection");
            };
            let conn = connecting.await.unwrap();

            let (mut ctrl_send, mut ctrl_recv) = conn.accept_bi().await.unwrap();
            match read_agent_msg(&mut ctrl_recv).await {
                AgentToPop::Auth(_) => {}
                other => panic!("expected Auth, got {other:?}"),
            }

            write_pop_msg(
                &mut ctrl_send,
                &PopToAgent::AuthOk(AuthOk {
                    pop_id: "test-pop".to_string(),
                    pop_pubkey: test_pop_pubkey(),
                    server_time_ms: now_ms(),
                    udp_token,
                    udp_shreds_port: pop_udp_shreds_port,
                    udp_votes_port: pop_udp_votes_port,
                }),
            )
            .await;

            let mut got_register_udp_ports = false;
            let mut got_register_validator_ports = false;
            let mut got_subscribe = false;

            while !got_register_udp_ports || !got_register_validator_ports || !got_subscribe {
                match read_agent_msg(&mut ctrl_recv).await {
                    AgentToPop::RegisterUdpPorts {
                        shreds_port,
                        votes_port,
                    } => {
                        assert_ne!(shreds_port, 0);
                        assert_ne!(votes_port, 0);
                        got_register_udp_ports = true;
                    }
                    AgentToPop::RegisterValidatorPorts { direct_shreds, .. } => {
                        assert!(
                            direct_shreds,
                            "expected direct_shreds=true when publisher and UDP enabled"
                        );
                        got_register_validator_ports = true;
                    }
                    AgentToPop::SubscribeShreds => {
                        got_subscribe = true;
                    }
                    AgentToPop::Heartbeat(_) => {}
                    other => {
                        debug!("unexpected ctrl msg from client: {other:?}");
                    }
                }
            }

            // Best-effort drain of the shreds/votes StreamHello messages.
            for _ in 0..2 {
                if let Ok(Ok((mut _send, mut recv))) =
                    tokio::time::timeout(Duration::from_secs(1), conn.accept_bi()).await
                {
                    let _ = read_agent_msg(&mut recv).await;
                }
            }

            let _ = ready_tx.send(());

            let mut buf = [0u8; 2048];
            let (len, _peer) =
                tokio::time::timeout(Duration::from_secs(5), pop_udp_votes.recv_from(&mut buf))
                    .await
                    .unwrap()
                    .unwrap();
            let bytes = &buf[..len];
            let (token, msg): ([u8; solanacdn_protocol::udp::UDP_TOKEN_LEN], AgentToPop) =
                solanacdn_protocol::udp::decode_udp_datagram(bytes).unwrap();
            assert_eq!(token, udp_token);
            match msg {
                AgentToPop::PublishVoteDatagram(dg) => {
                    assert_eq!(dg.dst, "127.0.0.1:4242".parse::<SocketAddr>().unwrap());
                    assert_eq!(dg.flow_id, vote_flow_id(&dg.dst));
                    assert_eq!(dg.payload.as_slice(), b"vote_payload");
                }
                other => panic!("expected PublishVoteDatagram, got {other:?}"),
            }
        });

        let inject_tpu_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_tvu_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_gossip_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_tpu = inject_tpu_socket.local_addr().unwrap();
        let inject_tvu = inject_tvu_socket.local_addr().unwrap();
        let inject_gossip = inject_gossip_socket.local_addr().unwrap();

        let mut cfg = SolanaCdnConfig::new(pop_addr);
        cfg.pop_endpoints = vec![pop_addr];
        cfg.tls_insecure_skip_verify = true;
        cfg.udp_mode = DataPlaneMode::Auto;

        let cfg = Arc::new(cfg);
        let handle = Arc::new(SolanaCdnHandle::new((*cfg).clone()));

        let identity_keypair = Arc::new(Keypair::new());
        let auth = Arc::new(AuthContext::new(identity_keypair.clone()).unwrap());
        let quic_connect = Arc::new(QuicConnectConfig {
            client_config: make_quic_client_config(&cfg).unwrap(),
            server_name: cfg.server_name.clone(),
        });

        let (uplink_tx, mut uplink_rx) = mpsc::channel::<UplinkMsg>(16);
        let (events_tx, _events_rx) = mpsc::unbounded_channel::<SessionEvent>();
        let (_publisher_tx, publisher_rx) = watch::channel::<Option<SocketAddr>>(Some(pop_addr));
        let (stop_tx, stop_rx) = watch::channel(false);

        let client_task = tokio::spawn(async move {
            run_pop_session(
                pop_addr,
                cfg,
                auth,
                quic_connect,
                handle,
                &mut uplink_rx,
                inject_tpu,
                inject_tvu,
                inject_gossip,
                inject_tpu,
                ShredBatchDeduper::new(64),
                None,
                publisher_rx,
                events_tx,
                stop_rx,
            )
            .await
            .unwrap();
        });

        ready_rx.await.unwrap();

        uplink_tx
            .send(UplinkMsg::Vote(VotePublish {
                dst: "127.0.0.1:4242".parse().unwrap(),
                payload: Bytes::from_static(b"vote_payload"),
            }))
            .await
            .unwrap();

        tokio::time::timeout(Duration::from_secs(5), pop_task)
            .await
            .unwrap()
            .unwrap();

        let _ = stop_tx.send(true);
        tokio::time::timeout(Duration::from_secs(5), client_task)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pipe_api_verify_pop_endpoints_enable_pop_connections() {
        init_rustls();

        let pop_keypair = Keypair::new();
        let (cert, key) = solana_tls_utils::new_dummy_x509_certificate(&pop_keypair);
        let server_cfg = quinn::ServerConfig::with_single_cert(vec![cert], key).unwrap();
        let pop_endpoint = Endpoint::server(server_cfg, "127.0.0.1:0".parse().unwrap()).unwrap();
        let pop_addr = pop_endpoint.local_addr().unwrap();

        let pop_done = Arc::new(AtomicBool::new(false));
        let pop_done_server = Arc::clone(&pop_done);
        let pop_task = tokio::spawn(async move {
            let Some(connecting) = pop_endpoint.accept().await else {
                return;
            };
            let conn = connecting.await.unwrap();

            let (mut ctrl_send, mut ctrl_recv) = conn.accept_bi().await.unwrap();
            match read_agent_msg(&mut ctrl_recv).await {
                AgentToPop::Auth(_) | AgentToPop::AuthWithSessionToken(_) => {}
                other => panic!("expected Auth, got {other:?}"),
            }

            write_pop_msg(
                &mut ctrl_send,
                &PopToAgent::AuthOk(AuthOk {
                    pop_id: "test-pop".to_string(),
                    pop_pubkey: test_pop_pubkey(),
                    server_time_ms: now_ms(),
                    udp_token: random_nonce_16(),
                    udp_shreds_port: 0,
                    udp_votes_port: 0,
                }),
            )
            .await;

            for _ in 0..2 {
                if let Ok(Ok((mut _send, mut recv))) =
                    tokio::time::timeout(Duration::from_secs(2), conn.accept_bi()).await
                {
                    let _ = tokio::time::timeout(Duration::from_secs(2), read_agent_msg(&mut recv))
                        .await;
                }
            }

            while !pop_done_server.load(Ordering::Relaxed) {
                match tokio::time::timeout(
                    Duration::from_millis(50),
                    read_len_prefixed(&mut ctrl_recv),
                )
                .await
                {
                    Ok(Ok(bytes)) => {
                        let _ = decode_envelope::<AgentToPop>(&bytes);
                    }
                    Ok(Err(_)) => return,
                    Err(_) => {}
                }
            }
        });

        let api_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        api_listener.set_nonblocking(true).unwrap();
        let api_addr = api_listener.local_addr().unwrap();

        let api_done = Arc::new(AtomicBool::new(false));
        let api_done_server = Arc::clone(&api_done);
        let pop_addr_str = pop_addr.to_string();
        let api_task = std::thread::spawn(move || {
            use std::io::{Read, Write};
            while !api_done_server.load(Ordering::Relaxed) {
                let (mut stream, _peer) = match api_listener.accept() {
                    Ok(v) => v,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(e) => panic!("pipe api accept failed: {e}"),
                };
                stream.set_nonblocking(false).unwrap();

                let mut req = Vec::new();
                let mut buf = [0u8; 1024];
                loop {
                    let n = stream.read(&mut buf).unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    req.extend_from_slice(&buf[..n]);
                    if req.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                    if req.len() > 64 * 1024 {
                        break;
                    }
                }
                let req_str = String::from_utf8_lossy(&req);
                let path = req_str
                    .lines()
                    .next()
                    .and_then(|l| l.split_whitespace().nth(1))
                    .unwrap_or("");

                let (status, body) = match path {
                    "/v1/solanacdn-agent/verify" => (
                        "200 OK",
                        format!(
                            "{{\"ok\":true,\"agent_id\":\"test-agent\",\"run_id\":\"test-run\",\"run_token\":\"rt\",\"ingest\":{{\"url\":\"http://127.0.0.1:1\",\"interval_secs\":10,\"max_body_bytes\":1048576,\"max_events\":100}},\"pop_endpoints\":[\"{pop_addr_str}\"]}}"
                        ),
                    ),
                    "/v1/solanacdn-agent/session-token" => (
                        "200 OK",
                        "{\"ok\":true,\"session_token\":\"st\",\"expires_in\":10}".to_string(),
                    ),
                    _ => ("404 Not Found", "{}".to_string()),
                };

                let header = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                stream.write_all(header.as_bytes()).unwrap();
                stream.write_all(body.as_bytes()).unwrap();
            }
        });

        let inject_tpu_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_tvu_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_gossip_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inject_tpu = inject_tpu_socket.local_addr().unwrap();
        let inject_tvu = inject_tvu_socket.local_addr().unwrap();
        let inject_gossip = inject_gossip_socket.local_addr().unwrap();

        let mut cfg = SolanaCdnConfig::default();
        cfg.pop_endpoints.clear();
        cfg.control_endpoint = None;
        cfg.tls_insecure_skip_verify = true;
        cfg.udp_mode = DataPlaneMode::Off;
        cfg.pipe_api_base_url = format!("http://{api_addr}");
        cfg.pipe_api_token = Some("test-api-key".to_string());

        let handle = Arc::new(SolanaCdnHandle::new(cfg.clone()));
        let handle_wait = Arc::clone(&handle);
        let exit = Arc::new(AtomicBool::new(false));
        let identity_keypair = Arc::new(Keypair::new());

        let run_exit = Arc::clone(&exit);
        let run_task = tokio::spawn(async move {
            run(
                cfg,
                identity_keypair,
                run_exit,
                handle,
                inject_tpu,
                inject_tvu,
                inject_gossip,
                inject_tpu,
            )
            .await
            .unwrap();
        });

        let start = tokio::time::Instant::now();
        loop {
            if handle_wait.is_connected() {
                break;
            }
            if start.elapsed() > Duration::from_secs(5) {
                panic!("timed out waiting for SolanaCDN to connect via Pipe API pop_endpoints");
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        exit.store(true, Ordering::Relaxed);
        tokio::time::timeout(Duration::from_secs(5), run_task)
            .await
            .unwrap()
            .unwrap();

        api_done.store(true, Ordering::Relaxed);
        let _ = api_task.join();

        pop_done.store(true, Ordering::Relaxed);
        tokio::time::timeout(Duration::from_secs(5), pop_task)
            .await
            .unwrap()
            .unwrap();
    }
}
