//! `ForwardingStage` is a stage parallel to `BankingStage` that forwards
//! packets to a node that is or will be leader soon.

use {
    crate::next_leader::next_leaders,
    agave_banking_stage_ingress_types::BankingPacketBatch,
    agave_transaction_view::transaction_view::SanitizedTransactionView,
    async_trait::async_trait,
    crossbeam_channel::{Receiver, RecvTimeoutError},
    ed25519_dalek_v2::SigningKey,
    packet_container::PacketContainer,
    solana_clock::FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET,
    solana_client::connection_cache::ConnectionCache,
    solana_connection_cache::client_connection::ClientConnection,
    solana_cost_model::cost_model::CostModel,
    solana_fee_structure::{FeeBudgetLimits, FeeDetails},
    solana_gossip::{cluster_info::ClusterInfo, contact_info::Protocol, node::NodeMultihoming},
    solana_keypair::Keypair,
    solana_net_utils::multihomed_sockets::BindIpAddrs,
    solana_packet as packet,
    solana_perf::data_budget::DataBudget,
    solana_poh::poh_recorder::PohRecorder,
    solana_quic_definitions::NotifyKeyUpdate,
    solana_runtime::{
        bank::{Bank, CollectorFeeDetails},
        bank_forks::SharableBanks,
    },
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
    },
    solana_signer::Signer,
    solana_streamer::sendmmsg::{batch_send, SendPktsError},
    solana_tpu_client_next::{
        connection_workers_scheduler::{
            BindTarget, ConnectionWorkersSchedulerConfig, Fanout, StakeIdentity,
        },
        leader_updater::LeaderUpdater,
        transaction_batch::TransactionBatch,
        ConnectionWorkersScheduler,
    },
    solana_transaction::sanitized::MessageHash,
    solana_transaction::Transaction,
    solana_transaction_error::TransportError,
    std::{
        collections::{HashMap, HashSet, VecDeque},
        net::{SocketAddr, UdpSocket},
        sync::{Arc, RwLock},
        thread::{Builder, JoinHandle},
        time::{Duration, Instant},
    },
    tokio::{
        runtime::Handle as RuntimeHandle,
        sync::{mpsc, watch},
    },
    tokio_util::sync::CancellationToken,
};

mod packet_container;

fn try_first_signature_bytes(payload: &[u8]) -> Option<[u8; 64]> {
    let (sig_count, consumed) = parse_shortvec_len(payload)?;
    if sig_count == 0 {
        return None;
    }
    let start = consumed;
    let end = start.saturating_add(64);
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

fn bid_hint_for_forwarded_tx(tx: &[u8]) -> crate::mcp::BidHintV1 {
    let mut bid_hint = crate::mcp::bid_hint_from_wire_tx_v1(tx).unwrap_or(crate::mcp::BidHintV1 {
        cu_price: 0,
        cu_limit: 0,
        sig_count: 0,
    });
    if let Some(sig) = try_first_signature_bytes(tx) {
        if let Some(priority) = crate::solanacdn::fair_priority_for_tx_signature(&sig) {
            bid_hint.cu_price = priority;
        }
    }
    bid_hint
}

fn mcp_memo_kind_from_wire_tx(tx_bytes: &[u8]) -> Option<crate::mcp::McpLedgerMemoKindV1> {
    let tx: Transaction = bincode::deserialize(tx_bytes).ok()?;
    for ix in tx.message.instructions.iter() {
        let program_id = tx
            .message
            .account_keys
            .get(ix.program_id_index as usize)?;
        let parse = crate::mcp::parse_mcp_memo_chunk_payload_v1(program_id, ix.data.as_slice())?;
        if let crate::mcp::McpMemoChunkParseV1::Valid(p) = parse {
            return Some(p.kind);
        }
    }
    None
}

/// [`ForwardingClientOption`] enum represents the available client types for
/// TPU communication:
/// * [`ConnectionCacheClient`]: Uses a shared [`ConnectionCache`] to manage
///   connections.
/// * [`TpuClientNextClient`]: Relies on the `tpu-client-next` crate.
pub enum ForwardingClientOption<'a> {
    ConnectionCache(Arc<ConnectionCache>),
    TpuClientNext(
        (
            &'a Keypair,
            Box<[UdpSocket]>,
            RuntimeHandle,
            CancellationToken,
            Arc<NodeMultihoming>,
        ),
    ),
}

/// Value chosen because it was used historically, at some point
/// was found to be optimal. If we need to improve performance
/// this should be evaluated with new stage.
const FORWARD_BATCH_SIZE: usize = 128;

/// How far ahead to look in the leader schedule when determining forwarding
/// addresses. The unit is `NUM_CONSECUTIVE_LEADER_SLOTS`.
///
/// This lookahead is needed because the immediate next leader might not have
/// shared their forwarding ports. In such cases, we skip them and attempt to
/// forward to the next available leader (up to this limit).
///
/// The value is chosen to ensure that the likelihood of the same leader occupying
/// all lookahead slots is negligible.
const NUM_LOOKAHEAD_LEADERS: u64 = 3;

fn mcp_forwarding_fanout_v1() -> usize {
    const MAX_FANOUT: usize = 8;
    crate::mcp::status_snapshot()
        .map(|s| usize::from(s.cfg.lanes_per_slot).max(1).min(MAX_FANOUT))
        .unwrap_or(1)
}

fn mcp_signing_key_from_solana_keypair_v1(identity: &Keypair) -> Option<SigningKey> {
    let bytes = identity.secret_bytes();
    let sk = SigningKey::from_bytes(&bytes);
    if sk.verifying_key().to_bytes() != identity.pubkey().to_bytes() {
        return None;
    }
    Some(sk)
}

struct McpForwardingProposerV1 {
    identity_keypair: Arc<Keypair>,
    signing_key: SigningKey,
    mcp: Arc<crate::mcp::McpHandle>,
    cfg: crate::mcp::McpConfig,
    pending_slot: Option<u64>,
    pending_lane_id: Option<crate::mcp::LaneId>,
    pending_refs: Vec<crate::mcp::McpTxRefV1>,
    seen_blob_ids: HashSet<crate::mcp::TxBlobId>,
    last_emit: Instant,
    pending_da: VecDeque<McpPendingDaCertV1>,
}

struct McpPendingDaCertV1 {
    epoch: u64,
    checkpoint: crate::mcp::McpCheckpointV1,
    sigs: HashMap<[u8; 32], [u8; 64]>,
    requested_at: Instant,
}

impl McpForwardingProposerV1 {
    fn maybe_new(identity_keypair: &Keypair) -> Option<Self> {
        let mcp = crate::mcp::global()?;
        let cfg = mcp.status_snapshot().cfg;
        if cfg.leader_mode != crate::mcp::McpLeaderMode::ScheduledLeaderSchedule {
            return None;
        }
        let signing_key = mcp_signing_key_from_solana_keypair_v1(identity_keypair)?;
        Some(Self {
            identity_keypair: Arc::new(identity_keypair.insecure_clone()),
            signing_key,
            mcp,
            cfg,
            pending_slot: None,
            pending_lane_id: None,
            pending_refs: Vec::new(),
            seen_blob_ids: HashSet::new(),
            last_emit: Instant::now(),
            pending_da: VecDeque::new(),
        })
    }

    fn reset_for_slot_lane(&mut self, slot: u64, lane_id: crate::mcp::LaneId) {
        self.pending_slot = Some(slot);
        self.pending_lane_id = Some(lane_id);
        self.pending_refs.clear();
        self.seen_blob_ids.clear();
        self.last_emit = Instant::now();
    }

    fn maybe_build_da_cert_memos(&mut self, bank: &Bank) -> Vec<Vec<u8>> {
        let recent_blockhash = bank.last_blockhash();
        let mut out: Vec<Vec<u8>> = Vec::new();

        // Drain any newly-arrived DA attests from SolanaCDN and emit DA cert memos when the
        // configured stake threshold is met.
        const MCP_DA_MAX_PENDING_CHECKPOINTS: usize = 64;
        let pending_len = self.pending_da.len();
        for _ in 0..pending_len {
            let mut pending = self.pending_da.pop_front().expect("len checked");

            let ckpt = &pending.checkpoint;
            let ckpt_id = ckpt.header.checkpoint_id_v1();
            for (pk, sig) in crate::solanacdn::mcp_da_take_attests_for_checkpoint(
                pending.epoch,
                ckpt.header.slot,
                ckpt.header.lane_id,
                ckpt.header.checkpoint_ix,
                ckpt_id,
            ) {
                pending.sigs.insert(pk, sig);
            }

            let mut sigs: Vec<([u8; 32], [u8; 64])> =
                pending.sigs.iter().map(|(k, v)| (*k, *v)).collect();
            sigs.sort_by(|a, b| a.0.cmp(&b.0));

            let cert = crate::mcp::DaCertV1 {
                epoch: pending.epoch,
                slot: ckpt.header.slot,
                lane_id: ckpt.header.lane_id,
                checkpoint_ix: ckpt.header.checkpoint_ix,
                checkpoint_id: ckpt_id,
                stake_threshold_bps: self.cfg.da_threshold_bps,
                sigs,
            };

            let stakes = bank
                .epoch_staked_nodes(pending.epoch)
                .unwrap_or_else(|| bank.current_epoch_staked_nodes());
            if cert.verify_with_stakes_v1(stakes.as_ref()).is_ok() {
                out.extend(crate::mcp::build_mcp_da_cert_memo_txs_v1(
                    self.identity_keypair.as_ref(),
                    &self.signing_key,
                    recent_blockhash,
                    &cert,
                ));
            } else if pending.requested_at.elapsed() <= Duration::from_secs(2) {
                self.pending_da.push_back(pending);
            }
        }
        while self.pending_da.len() > MCP_DA_MAX_PENDING_CHECKPOINTS {
            self.pending_da.pop_front();
        }

        out
    }

    fn ingest_wire_tx_for_slot_lane(&mut self, slot: u64, lane_id: crate::mcp::LaneId, tx: &[u8]) {
        if self.pending_slot != Some(slot) || self.pending_lane_id != Some(lane_id) {
            self.reset_for_slot_lane(slot, lane_id);
        }

        // Cap buffered refs to avoid unbounded memory growth.
        let max_pending = usize::from(self.cfg.microblock_max_refs)
            .max(1)
            .saturating_mul(8)
            .min(2048);
        if self.pending_refs.len() >= max_pending {
            return;
        }

        // Avoid self-referential proposals by skipping MCP memo transactions.
        const MCP_LEDGER_MEMO_MAGIC_V1: &[u8; 8] = b"SCDNMCP\0";
        if tx
            .windows(MCP_LEDGER_MEMO_MAGIC_V1.len())
            .any(|w| w == MCP_LEDGER_MEMO_MAGIC_V1)
        {
            return;
        }

        let lanes_per_slot = self.cfg.lanes_per_slot.max(1);
        let blob_id = crate::mcp::tx_blob_id_v1(tx);
        if crate::mcp::select_lane_for_blob_id_v1(slot, blob_id, lanes_per_slot) != lane_id {
            return;
        }

        if !self.seen_blob_ids.insert(blob_id) {
            return;
        }

        let bid_hint = bid_hint_for_forwarded_tx(tx);

        self.pending_refs.push(crate::mcp::McpTxRefV1 { blob_id, bid_hint });
    }

    fn maybe_build_memos_for_slot_lane(
        &mut self,
        slot: u64,
        lane_id: crate::mcp::LaneId,
        bank: &Bank,
    ) -> Vec<Vec<u8>> {
        if self.pending_slot != Some(slot) || self.pending_lane_id != Some(lane_id) {
            self.reset_for_slot_lane(slot, lane_id);
        }

        let mut out = self.maybe_build_da_cert_memos(bank);

        if self.pending_refs.is_empty() {
            return out;
        }

        // Emit once the lane has enough refs for a full microblock, or after a short delay
        // to keep latency bounded under low traffic.
        let max_refs = usize::from(self.cfg.microblock_max_refs).max(1);
        let min_emit_ms: u64 = 25;
        if self.pending_refs.len() < max_refs && self.last_emit.elapsed() < Duration::from_millis(min_emit_ms) {
            return out;
        }

        // Propose at most one microblock worth of refs per emission to bound memo overhead.
        self.pending_refs.sort_by(|a, b| {
            crate::mcp::bid_key_from_hint_v1(a.blob_id, a.bid_hint)
                .cmp(&crate::mcp::bid_key_from_hint_v1(b.blob_id, b.bid_hint))
        });
        let take = self.pending_refs.len().min(max_refs);
        let refs: Vec<crate::mcp::McpTxRefV1> = self.pending_refs.drain(..take).collect();

        self.last_emit = Instant::now();

        let recent_blockhash = bank.last_blockhash();
        let (mut memos, checkpoint) = self
            .mcp
            .build_lane_microblocks_and_checkpoint_memos_with_checkpoint_v1(
                self.identity_keypair.as_ref(),
                &self.signing_key,
                recent_blockhash,
                slot,
                lane_id,
                refs,
            );
        out.append(&mut memos);

        if let Some(checkpoint) = checkpoint {
            let epoch: u64 = bank.epoch_schedule().get_epoch(slot);
            let _sent = crate::solanacdn::mcp_da_try_send_request_for_checkpoint(epoch, &checkpoint);

            // Seed with our own attestation so low-stake/dev clusters can reach threshold without
            // waiting for POP delivery.
            let mut attest = crate::mcp::DaAttestV1 {
                header: crate::mcp::DaAttestHeaderV1 {
                    epoch,
                    slot,
                    lane_id,
                    checkpoint_ix: checkpoint.header.checkpoint_ix,
                    checkpoint_id: checkpoint.header.checkpoint_id_v1(),
                    validator_pubkey: self.identity_keypair.pubkey(),
                },
                sig: [0u8; 64],
            };
            attest.sign_v1(&self.signing_key);

            let mut sigs = HashMap::new();
            sigs.insert(self.identity_keypair.pubkey().to_bytes(), attest.sig);
            self.pending_da.push_back(McpPendingDaCertV1 {
                epoch,
                checkpoint,
                sigs,
                requested_at: Instant::now(),
            });
        }

        out
    }
}

/// [`ForwardAddressGetter`] provides helper methods for retrieving forwarding
/// addresses for both vote and non-vote transactions.
#[derive(Clone)]
pub(crate) struct ForwardAddressGetter {
    cluster_info: Arc<ClusterInfo>,
    poh_recorder: Arc<RwLock<PohRecorder>>,
}

impl ForwardAddressGetter {
    pub fn new(cluster_info: Arc<ClusterInfo>, poh_recorder: Arc<RwLock<PohRecorder>>) -> Self {
        Self {
            cluster_info,
            poh_recorder,
        }
    }

    /// Returns a list of forwarding addresses for non-vote transactions.
    fn get_non_vote_forwarding_addresses(
        &self,
        max_count: u64,
        protocol: Protocol,
    ) -> Vec<SocketAddr> {
        if let Some(mcp) = crate::mcp::status_snapshot() {
            if mcp.cfg.leader_mode == crate::mcp::McpLeaderMode::ScheduledLeaderSchedule {
                return self.get_mcp_scheduled_forwarding_addresses_v1(max_count, protocol);
            }
        }
        next_leaders(&self.cluster_info, &self.poh_recorder, max_count, |node| {
            node.tpu_forwards(protocol)
        })
    }

    fn get_mcp_scheduled_forwarding_addresses_v1(
        &self,
        max_count: u64,
        protocol: Protocol,
    ) -> Vec<SocketAddr> {
        const SCAN_SLOTS: u64 = 256;

        let Some(mcp) = crate::mcp::global() else {
            return Vec::new();
        };
        let lanes_per_slot: u8 = mcp.status_snapshot().cfg.lanes_per_slot.max(1);

        let (lane_leaders, protocol) = {
            let recorder = self.poh_recorder.read().unwrap();
            let Some((_slot_leader, target_slot)) =
                recorder.leader_and_slot_after_n_slots(FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET)
            else {
                return Vec::new();
            };

            let want = usize::from(lanes_per_slot).min(max_count as usize).max(1);
            let mut by_lane: HashMap<u8, solana_pubkey::Pubkey> = HashMap::with_capacity(want);
            for offset in 0..SCAN_SLOTS {
                if by_lane.len() >= want {
                    break;
                }
                let slots_ahead = FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET.saturating_add(offset);
                let Some(leader) = recorder.leader_after_n_slots(slots_ahead) else {
                    continue;
                };
                let lane_id = mcp.select_lane_for_slot_v1(target_slot, leader);
                by_lane.entry(lane_id).or_insert(leader);
            }

            let mut lane_ids: Vec<u8> = by_lane.keys().copied().collect();
            lane_ids.sort_unstable();
            let leaders: Vec<solana_pubkey::Pubkey> = lane_ids
                .into_iter()
                .filter_map(|lane_id| by_lane.get(&lane_id).copied())
                .collect();
            (leaders, protocol)
        };

        let mut out: Vec<SocketAddr> = Vec::new();
        let mut seen: HashSet<SocketAddr> = HashSet::new();

        for leader in lane_leaders {
            if let Some(addr) = self
                .cluster_info
                .lookup_contact_info(&leader, |node| node.tpu_forwards(protocol))
                .flatten()
            {
                if seen.insert(addr) {
                    out.push(addr);
                }
            }
        }

        // Fallback to legacy next-leaders forwarding (fill up to max_count).
        if out.len() < max_count as usize {
            let fallback = next_leaders(&self.cluster_info, &self.poh_recorder, max_count, |node| {
                node.tpu_forwards(protocol)
            });
            for addr in fallback {
                if out.len() >= max_count as usize {
                    break;
                }
                if seen.insert(addr) {
                    out.push(addr);
                }
            }
        }

        out
    }

    /// Returns the TPU vote forwarding address of the next leader, if
    /// available.
    fn get_vote_forwarding_addresses(&self, max_count: u64) -> Vec<SocketAddr> {
        next_leaders(&self.cluster_info, &self.poh_recorder, max_count, |node| {
            node.tpu_vote(Protocol::UDP)
        })
    }
}

/// [`SpawnForwardingStageResult`] contains the result of spawning the
/// [`ForwardingStage`], including the background task handle and a shared
/// notifier for client address updates.
pub(crate) struct SpawnForwardingStageResult {
    pub join_handle: JoinHandle<()>,
    pub client_updater: Arc<dyn NotifyKeyUpdate + Send + Sync>,
}

pub(crate) fn spawn_forwarding_stage(
    receiver: Receiver<(BankingPacketBatch, bool)>,
    client: ForwardingClientOption<'_>,
    vote_client_udp_socket: UdpSocket,
    sharable_banks: SharableBanks,
    forward_address_getter: ForwardAddressGetter,
    data_budget: DataBudget,
    identity_keypair: &Keypair,
) -> SpawnForwardingStageResult {
    let vote_client = VoteClient::new(vote_client_udp_socket, forward_address_getter.clone());
    let mcp_proposer = McpForwardingProposerV1::maybe_new(identity_keypair);
    match client {
        ForwardingClientOption::ConnectionCache(connection_cache) => {
            let non_vote_client = ConnectionCacheClient::new(
                connection_cache.clone(),
                forward_address_getter.clone(),
            );
            let forwarding_stage = ForwardingStage::new(
                receiver,
                vote_client,
                Box::new([non_vote_client]),
                sharable_banks,
                data_budget,
                None,
                Some(forward_address_getter),
                mcp_proposer,
            );
            SpawnForwardingStageResult {
                join_handle: Builder::new()
                    .name("solFwdStage".to_string())
                    .spawn(move || forwarding_stage.run())
                    .unwrap(),
                client_updater: connection_cache as Arc<dyn NotifyKeyUpdate + Send + Sync>,
            }
        }
        ForwardingClientOption::TpuClientNext((
            stake_identity,
            tpu_client_sockets,
            runtime_handle,
            cancel,
            node_multihoming,
        )) => {
            // Create TPU clients for each socket provided.
            // Number of clients is same as number of bind IP addresses.
            let non_vote_clients: Box<[TpuClientNextClient]> = tpu_client_sockets
                .into_vec()
                .into_iter()
                .map(|socket| {
                    TpuClientNextClient::new(
                        runtime_handle.clone(),
                        forward_address_getter.clone(),
                        Some(stake_identity),
                        socket,
                        cancel.clone(),
                    )
                })
                .collect();
            let forwarding_stage = ForwardingStage::new(
                receiver,
                vote_client,
                non_vote_clients.clone(),
                sharable_banks,
                data_budget,
                Some(node_multihoming.bind_ip_addrs.clone()),
                Some(forward_address_getter),
                mcp_proposer,
            );
            SpawnForwardingStageResult {
                join_handle: Builder::new()
                    .name("solFwdStage".to_string())
                    .spawn(move || forwarding_stage.run())
                    .unwrap(),
                client_updater: Arc::new(UpdateHandles(non_vote_clients))
                    as Arc<dyn NotifyKeyUpdate + Send + Sync>,
            }
        }
    }
}

/// Local struct to be able to update keys on all clients at once
struct UpdateHandles(Box<[TpuClientNextClient]>);
impl NotifyKeyUpdate for UpdateHandles {
    fn update_key(&self, key: &Keypair) -> Result<(), Box<dyn std::error::Error>> {
        self.0.iter().try_for_each(|client| client.update_key(key))
    }
}

struct ForwardingStage<VoteClient: ForwardingClient, NonVoteClient: ForwardingClient> {
    receiver: Receiver<(BankingPacketBatch, bool)>,
    packet_container: PacketContainer,
    sharable_banks: SharableBanks,
    vote_client: VoteClient,
    non_vote_clients: Box<[NonVoteClient]>,
    data_budget: DataBudget,
    metrics: ForwardingStageMetrics,
    bind_ip_addrs: Option<Arc<BindIpAddrs>>,
    forward_address_getter: Option<ForwardAddressGetter>,
    mcp_proposer: Option<McpForwardingProposerV1>,
}

impl<VoteClient: ForwardingClient, NonVoteClient: ForwardingClient>
    ForwardingStage<VoteClient, NonVoteClient>
{
    fn new(
        receiver: Receiver<(BankingPacketBatch, bool)>,
        vote_client: VoteClient,
        non_vote_clients: Box<[NonVoteClient]>,
        sharable_banks: SharableBanks,
        data_budget: DataBudget,
        bind_ip_addrs: Option<Arc<BindIpAddrs>>,
        forward_address_getter: Option<ForwardAddressGetter>,
        mcp_proposer: Option<McpForwardingProposerV1>,
    ) -> Self {
        Self {
            receiver,
            packet_container: PacketContainer::with_capacity(4 * 4096),
            sharable_banks,
            non_vote_clients,
            vote_client,
            data_budget,
            metrics: ForwardingStageMetrics::default(),
            bind_ip_addrs,
            forward_address_getter,
            mcp_proposer,
        }
    }

    /// Runs `ForwardingStage`'s main loop, to receive, order, and forward packets.
    fn run(mut self) {
        loop {
            let root_bank = self.sharable_banks.root();
            if !self.receive_and_buffer(&root_bank) {
                break;
            }
            self.forward_buffered_packets();
            self.metrics.maybe_report();
        }
    }

    /// Receive packets from previous stage and insert them into the buffer.
    fn receive_and_buffer(&mut self, bank: &Bank) -> bool {
        // Timeout is long enough to receive packets but not too long that we
        // forward infrequently.
        const TIMEOUT: Duration = Duration::from_millis(10);

        let now = Instant::now();
        match self.receiver.recv_timeout(TIMEOUT) {
            Ok((packet_batches, tpu_vote_batch)) => {
                self.metrics.did_something = true;
                self.buffer_packet_batches(packet_batches, tpu_vote_batch, bank);

                // Drain the channel up to timeout
                while now.elapsed() < TIMEOUT {
                    match self.receiver.try_recv() {
                        Ok((packet_batches, tpu_vote_batch)) => {
                            self.buffer_packet_batches(packet_batches, tpu_vote_batch, bank)
                        }
                        Err(_) => break,
                    }
                }

                true
            }
            Err(RecvTimeoutError::Timeout) => true,
            Err(RecvTimeoutError::Disconnected) => false,
        }
    }

    /// Insert received packets into the packet container.
    fn buffer_packet_batches(
        &mut self,
        packet_batches: BankingPacketBatch,
        is_tpu_vote_batch: bool,
        bank: &Bank,
    ) {
        let enable_static_instruction_limit = bank
            .feature_set
            .is_active(&agave_feature_set::static_instruction_limit::id());
        for batch in packet_batches.iter() {
            for packet in batch
                .iter()
                .filter(|p| initial_packet_meta_filter(p.meta()))
            {
                let Some(packet_data) = packet.data(..) else {
                    unreachable!(
                        "packet.meta().discard() was already checked. If not discarded, packet \
                         MUST have data"
                    );
                };

                let vote_count = usize::from(is_tpu_vote_batch);
                let non_vote_count = usize::from(!is_tpu_vote_batch);

                self.metrics.votes_received += vote_count;
                self.metrics.non_votes_received += non_vote_count;

                // Perform basic sanitization checks and calculate priority.
                // If any steps fail, drop the packet.
                let Some(priority) = SanitizedTransactionView::try_new_sanitized(
                    packet_data,
                    enable_static_instruction_limit,
                )
                .map_err(|_| ())
                .and_then(|transaction| {
                    RuntimeTransaction::<SanitizedTransactionView<_>>::try_from(
                        transaction,
                        MessageHash::Compute,
                        Some(packet.meta().is_simple_vote_tx()),
                    )
                    .map_err(|_| ())
                })
                .ok()
                .and_then(|transaction| calculate_priority(&transaction, bank)) else {
                    self.metrics.votes_dropped_on_receive += vote_count;
                    self.metrics.non_votes_dropped_on_receive += non_vote_count;
                    continue;
                };

                // If at capacity, check lowest priority item.
                if self.packet_container.is_full() {
                    let min_priority = self.packet_container.min_priority().expect("not empty");
                    // If priority of current packet is not higher than the min
                    // drop the current packet.
                    if min_priority >= priority {
                        self.metrics.votes_dropped_on_capacity += vote_count;
                        self.metrics.non_votes_dropped_on_capacity += non_vote_count;
                        continue;
                    }

                    let dropped_packet = self.packet_container.pop_min().expect("not empty");
                    self.metrics.votes_dropped_on_capacity +=
                        usize::from(dropped_packet.meta().is_simple_vote_tx());
                    self.metrics.non_votes_dropped_on_capacity +=
                        usize::from(!dropped_packet.meta().is_simple_vote_tx());
                }

                self.packet_container
                    .insert(packet.to_bytes_packet(), priority);
            }
        }
    }

    /// Forwards packets that have been buffered. This will loop through all
    /// packets. If the data budget is exceeded then remaining packets are
    /// dropped.
    fn forward_buffered_packets(&mut self) {
        self.metrics.did_something |= !self.packet_container.is_empty();
        self.refresh_data_budget();

        let mcp_bank = self.sharable_banks.working();

        let mcp_slot_lane: Option<(u64, crate::mcp::LaneId)> = (|| {
            let proposer = self.mcp_proposer.as_ref()?;
            let forward_address_getter = self.forward_address_getter.as_ref()?;

            let (target_slot, leader_schedule_cache) = {
                let recorder = forward_address_getter.poh_recorder.read().unwrap();
                let (_slot_leader, target_slot) = recorder
                    .leader_and_slot_after_n_slots(FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET)?;
                (target_slot, recorder.leader_schedule_cache())
            };

            let my_pubkey = proposer.identity_keypair.pubkey();
            let lane_id = proposer.mcp.select_lane_for_slot_v1(target_slot, my_pubkey);
            let scheduled_lane_leaders = proposer.mcp.scheduled_lane_leaders_from_leader_schedule_v1(
                target_slot,
                &mcp_bank,
                leader_schedule_cache.as_ref(),
            );
            let my_pk_bytes = my_pubkey.to_bytes();

            (scheduled_lane_leaders.get(&lane_id) == Some(&my_pk_bytes))
                .then_some((target_slot, lane_id))
        })();

        let maybe_send_mcp_memos = |me: &mut Self| {
            let Some(proposer) = me.mcp_proposer.as_mut() else {
                return;
            };

            let memos = if let Some((target_slot, lane_id)) = mcp_slot_lane {
                proposer.maybe_build_memos_for_slot_lane(target_slot, lane_id, &mcp_bank)
            } else {
                proposer.maybe_build_da_cert_memos(&mcp_bank)
            };
            if memos.is_empty() {
                return;
            }

            me.metrics.did_something = true;
            me.metrics.mcp_memos_built += memos.len();
            for tx_bytes in memos.iter() {
                match mcp_memo_kind_from_wire_tx(tx_bytes.as_slice()) {
                    Some(crate::mcp::McpLedgerMemoKindV1::Microblock) => {
                        me.metrics.mcp_microblock_memo_txs_built += 1;
                    }
                    Some(crate::mcp::McpLedgerMemoKindV1::Checkpoint) => {
                        me.metrics.mcp_checkpoint_memo_txs_built += 1;
                    }
                    Some(crate::mcp::McpLedgerMemoKindV1::DaCert) => {
                        me.metrics.mcp_da_cert_memo_txs_built += 1;
                    }
                    None => {}
                }
            }

            let active_non_vote_client_index = {
                let active_index = me
                    .bind_ip_addrs
                    .as_ref()
                    .map(|binds| binds.active_index())
                    .unwrap_or(0);
                active_index
            };
            let active_non_vote_client = &me.non_vote_clients[active_non_vote_client_index];

            let mut to_send: Vec<Vec<u8>> = Vec::with_capacity(memos.len());
            for bytes in memos {
                if me.data_budget.take(bytes.len()) {
                    to_send.push(bytes);
                } else {
                    me.metrics.mcp_memos_dropped_on_data_budget += 1;
                }
            }

            if to_send.is_empty() {
                return;
            }

            let num = to_send.len();
            me.metrics.mcp_memos_forwarded += num;
            if active_non_vote_client
                .send_transactions_in_batch(to_send)
                .is_err()
            {
                me.metrics.mcp_memos_dropped_on_send += num;
            }
        };

        let mut non_vote_batch = Vec::with_capacity(FORWARD_BATCH_SIZE);
        let mut vote_batch = Vec::with_capacity(FORWARD_BATCH_SIZE);

        // determine the client to use for next batch based on current active interface
        // use primary interface bind (index 0) if not in multihoming context.
        let active_non_vote_client_index = {
            let active_index = self
                .bind_ip_addrs
                .as_ref()
                .map(|binds| binds.active_index())
                .unwrap_or(0);
            active_index
        };
        // Loop through packets creating batches of packets to forward.
        while let Some(packet) = self.packet_container.pop_max() {
            // If it exceeds our data-budget, drop.
            if !self.data_budget.take(packet.meta().size) {
                self.metrics.votes_dropped_on_data_budget +=
                    usize::from(packet.meta().is_simple_vote_tx());
                self.metrics.non_votes_dropped_on_data_budget +=
                    usize::from(!packet.meta().is_simple_vote_tx());
                continue;
            }

            let packet_data_vec = packet.data(..).expect("packet has data").to_vec();

            if packet.meta().is_simple_vote_tx() {
                vote_batch.push(packet_data_vec);
                send_batch_if_full(
                    &mut vote_batch,
                    &self.vote_client,
                    &mut self.metrics.votes_forwarded,
                    &mut self.metrics.votes_dropped_on_send,
                );
            } else {
                if let Some((target_slot, lane_id)) = mcp_slot_lane {
                    if let Some(proposer) = self.mcp_proposer.as_mut() {
                        proposer.ingest_wire_tx_for_slot_lane(
                            target_slot,
                            lane_id,
                            packet_data_vec.as_slice(),
                        );
                    }
                }
                non_vote_batch.push(packet_data_vec);
                if non_vote_batch.len() == FORWARD_BATCH_SIZE {
                    // Prefer to send lane microblock memos before sending the referenced payloads.
                    maybe_send_mcp_memos(self);

                    self.metrics.non_votes_forwarded += non_vote_batch.len();
                    let mut swap_batch = Vec::with_capacity(FORWARD_BATCH_SIZE);
                    std::mem::swap(&mut non_vote_batch, &mut swap_batch);
                    let active_non_vote_client = &self.non_vote_clients[active_non_vote_client_index];
                    if active_non_vote_client
                        .send_transactions_in_batch(swap_batch)
                        .is_err()
                    {
                        self.metrics.non_votes_dropped_on_send += FORWARD_BATCH_SIZE;
                    }
                }
            }
        }

        // Send out remaining packets
        if !vote_batch.is_empty() {
            let num_votes = vote_batch.len();
            self.metrics.votes_forwarded += num_votes;
            if self
                .vote_client
                .send_transactions_in_batch(vote_batch)
                .is_err()
            {
                self.metrics.votes_dropped_on_send += num_votes;
            }
        }
        if !non_vote_batch.is_empty() {
            // Prefer to send lane microblock memos before sending the referenced payloads.
            maybe_send_mcp_memos(self);

            let num_non_votes = non_vote_batch.len();
            self.metrics.non_votes_forwarded += num_non_votes;
            let active_non_vote_client = &self.non_vote_clients[active_non_vote_client_index];
            if active_non_vote_client
                .send_transactions_in_batch(non_vote_batch)
                .is_err()
            {
                self.metrics.non_votes_dropped_on_send += num_non_votes;
            }
        }

        // Flush any remaining MCP memos after forwarding payloads.
        maybe_send_mcp_memos(self);
    }

    /// Re-fill the data budget if enough time has passed
    fn refresh_data_budget(&self) {
        const INTERVAL_MS: u64 = 100;
        // 12 MB outbound limit per second
        const MAX_BYTES_PER_SECOND: usize = 12_000_000;
        const MAX_BYTES_PER_INTERVAL: usize = MAX_BYTES_PER_SECOND * INTERVAL_MS as usize / 1000;
        const MAX_BYTES_BUDGET: usize = MAX_BYTES_PER_INTERVAL * 5;
        self.data_budget.update(INTERVAL_MS, |bytes| {
            std::cmp::min(
                bytes.saturating_add(MAX_BYTES_PER_INTERVAL),
                MAX_BYTES_BUDGET,
            )
        });
    }
}

/// [`ForwardingClientError`] enum represents failure when sending transactions
/// over the network.
#[derive(Debug)]
enum ForwardingClientError {
    /// Failed to send transaction to the provided host.
    Failed,
    /// Failed to send the transaction because no contact information was found
    /// for any of the next `NUM_LOOKAHEAD_LEADERS` scheduled leaders.
    LeaderContactMissing,
}

impl From<SendPktsError> for ForwardingClientError {
    fn from(_err: SendPktsError) -> Self {
        ForwardingClientError::Failed
    }
}

impl From<TransportError> for ForwardingClientError {
    fn from(_err: TransportError) -> Self {
        ForwardingClientError::Failed
    }
}

/// [`ForwardingClient`] trait defines a generic interface for clients that can
/// forward transactions to other validators.
trait ForwardingClient: Send + Sync + 'static {
    /// Sends a batch of serialized transactions to the currently configured
    /// address.
    fn send_transactions_in_batch(
        &self,
        wire_transactions: Vec<Vec<u8>>,
    ) -> Result<(), ForwardingClientError>;
}

struct VoteClient {
    bind_socket: UdpSocket,
    forward_address_getter: ForwardAddressGetter,
}

impl VoteClient {
    fn new(bind_socket: UdpSocket, forward_address_getter: ForwardAddressGetter) -> Self {
        Self {
            bind_socket,
            forward_address_getter,
        }
    }

    fn get_next_valid_leader(&self) -> Option<SocketAddr> {
        let node_addresses = self
            .forward_address_getter
            .get_vote_forwarding_addresses(NUM_LOOKAHEAD_LEADERS);
        node_addresses.first().copied()
    }
}

impl ForwardingClient for VoteClient {
    fn send_transactions_in_batch(
        &self,
        wire_transactions: Vec<Vec<u8>>,
    ) -> Result<(), ForwardingClientError> {
        let Some(current_address) = self.get_next_valid_leader() else {
            return Err(ForwardingClientError::LeaderContactMissing);
        };
        let batch_with_addresses = wire_transactions
            .iter()
            .map(|bytes| (bytes, current_address));
        batch_send(&self.bind_socket, batch_with_addresses)?;
        Ok(())
    }
}

#[derive(Clone)]
struct ConnectionCacheClient {
    connection_cache: Arc<ConnectionCache>,
    forward_address_getter: ForwardAddressGetter,
}

impl ConnectionCacheClient {
    fn new(
        connection_cache: Arc<ConnectionCache>,
        forward_address_getter: ForwardAddressGetter,
    ) -> Self {
        Self {
            connection_cache,
            forward_address_getter,
        }
    }
    fn get_next_valid_leaders_v1(&self, fanout: usize) -> Vec<SocketAddr> {
        let max_count = NUM_LOOKAHEAD_LEADERS.max(fanout as u64);
        let node_addresses = self
            .forward_address_getter
            .get_non_vote_forwarding_addresses(
                max_count,
                self.connection_cache.protocol(),
            );
        node_addresses.into_iter().take(fanout).collect()
    }
}

impl ForwardingClient for ConnectionCacheClient {
    fn send_transactions_in_batch(
        &self,
        wire_transactions: Vec<Vec<u8>>,
    ) -> Result<(), ForwardingClientError> {
        let fanout = mcp_forwarding_fanout_v1();
        let node_addresses = self.get_next_valid_leaders_v1(fanout);
        if node_addresses.is_empty() {
            return Err(ForwardingClientError::LeaderContactMissing);
        }
        let node_count = node_addresses.len();

        // ConnectionCache's send API consumes the batch, so clone for fanout>1.
        let mut iter = node_addresses.into_iter();
        let Some(first) = iter.next() else {
            return Err(ForwardingClientError::LeaderContactMissing);
        };
        if node_count <= 1 {
            let conn = self.connection_cache.get_connection(&first);
            conn.send_data_batch_async(wire_transactions)?;
            return Ok(());
        }

        // Fanout>1: send original batch to the first leader, clones to the rest.
        let rest_template = wire_transactions.clone();
        {
            let conn = self.connection_cache.get_connection(&first);
            conn.send_data_batch_async(wire_transactions)?;
        }
        for addr in iter {
            let conn = self.connection_cache.get_connection(&addr);
            conn.send_data_batch_async(rest_template.clone())?;
        }
        Ok(())
    }
}

#[async_trait]
impl LeaderUpdater for ForwardAddressGetter {
    fn next_leaders(&mut self, lookahead_slots: usize) -> Vec<SocketAddr> {
        self.get_non_vote_forwarding_addresses(lookahead_slots as u64, Protocol::QUIC)
    }

    async fn stop(&mut self) {}
}

#[derive(Clone)]
struct TpuClientNextClient {
    sender: mpsc::Sender<TransactionBatch>,
    update_certificate_sender: watch::Sender<Option<StakeIdentity>>,
}

const METRICS_REPORTING_INTERVAL: Duration = Duration::from_secs(3);

impl TpuClientNextClient {
    fn new(
        runtime_handle: tokio::runtime::Handle,
        forward_address_getter: ForwardAddressGetter,
        stake_identity: Option<&Keypair>,
        bind_socket: UdpSocket,
        cancel: CancellationToken,
    ) -> Self {
        // For now use large channel, the more suitable size to be found later.
        let (sender, receiver) = mpsc::channel(128);
        let leader_updater = forward_address_getter.clone();

        let config = Self::create_config(bind_socket, stake_identity);
        let (update_certificate_sender, update_certificate_receiver) = watch::channel(None);
        let scheduler: ConnectionWorkersScheduler = ConnectionWorkersScheduler::new(
            Box::new(leader_updater),
            receiver,
            update_certificate_receiver,
            cancel.clone(),
        );
        // leaking handle to this task, as it will run until the cancel signal is received
        runtime_handle.spawn(scheduler.get_stats().report_to_influxdb(
            "forwarding-stage-tpu-client",
            METRICS_REPORTING_INTERVAL,
            cancel.clone(),
        ));
        let _handle = runtime_handle.spawn(scheduler.run(config));
        Self {
            sender,
            update_certificate_sender,
        }
    }

    fn create_config(
        bind_socket: UdpSocket,
        stake_identity: Option<&Keypair>,
    ) -> ConnectionWorkersSchedulerConfig {
        let fanout_send = mcp_forwarding_fanout_v1();
        ConnectionWorkersSchedulerConfig {
            bind: BindTarget::Socket(bind_socket),
            stake_identity: stake_identity.map(StakeIdentity::new),
            // Cache size of 128 covers all nodes above the P90 slot count threshold,
            // which together account for ~75% of total slots in the epoch.
            num_connections: 128,
            skip_check_transaction_age: true,
            worker_channel_size: 2,
            max_reconnect_attempts: 4,
            // Send to the next leader only, but verify that connections exist
            // for the leaders of the next `4 * NUM_CONSECUTIVE_SLOTS`.
            leaders_fanout: Fanout {
                send: fanout_send,
                connect: 4usize.max(fanout_send.saturating_mul(2)),
            },
        }
    }
}

impl ForwardingClient for TpuClientNextClient {
    fn send_transactions_in_batch(
        &self,
        wire_transactions: Vec<Vec<u8>>,
    ) -> Result<(), ForwardingClientError> {
        self.sender
            .try_send(TransactionBatch::new(wire_transactions))
            .map_err(|_e| ForwardingClientError::Failed)
    }
}

impl NotifyKeyUpdate for TpuClientNextClient {
    fn update_key(&self, identity: &Keypair) -> Result<(), Box<dyn std::error::Error>> {
        let stake_identity = StakeIdentity::new(identity);
        self.update_certificate_sender
            .send(Some(stake_identity))
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
    }
}

/// Calculate priority for a transaction:
///
/// The priority is calculated as:
/// P = R / (1 + C)
/// where P is the priority, R is the reward,
/// and C is the cost towards block-limits.
///
/// Current minimum costs are on the order of several hundred,
/// so the denominator is effectively C, and the +1 is simply
/// to avoid any division by zero due to a bug - these costs
/// are estimate by the cost-model and are not direct
/// from user input. They should never be zero.
/// Any difference in the prioritization is negligible for
/// the current transaction costs.
fn calculate_priority(
    transaction: &RuntimeTransaction<SanitizedTransactionView<&[u8]>>,
    bank: &Bank,
) -> Option<u64> {
    let compute_budget_limits = transaction
        .compute_budget_instruction_details()
        .sanitize_and_convert_to_compute_budget_limits(&bank.feature_set)
        .ok()?;
    let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);

    // Manually estimate fee here since currently interface doesn't allow a on SVM type.
    // Doesn't need to be 100% accurate so long as close and consistent.
    let prioritization_fee = fee_budget_limits.prioritization_fee;
    let signature_details = transaction.signature_details();
    let signature_fee = signature_details
        .total_signatures()
        .saturating_mul(bank.fee_structure().lamports_per_signature);
    let fee_details = FeeDetails::new(signature_fee, prioritization_fee);

    let reward = bank
        .calculate_reward_and_burn_fee_details(&CollectorFeeDetails::from(fee_details))
        .get_deposit();

    let cost = CostModel::estimate_cost(
        transaction,
        transaction.program_instructions_iter(),
        transaction.num_requested_write_locks(),
        &bank.feature_set,
    );

    // We need a multiplier here to avoid rounding down too aggressively.
    // For many transactions, the cost will be greater than the fees in terms of raw lamports.
    // For the purposes of calculating prioritization, we multiply the fees by a large number so that
    // the cost is a small fraction.
    // An offset of 1 is used in the denominator to explicitly avoid division by zero.
    const MULTIPLIER: u64 = 1_000_000;
    Some(
        MULTIPLIER
            .saturating_mul(reward)
            .wrapping_div(cost.sum().saturating_add(1)),
    )
}

fn send_batch_if_full(
    batch: &mut Vec<Vec<u8>>,
    client: &impl ForwardingClient,
    forwarded_counter: &mut usize,
    dropped_counter: &mut usize,
) {
    if batch.len() == FORWARD_BATCH_SIZE {
        *forwarded_counter += batch.len();

        let mut swap_batch = Vec::with_capacity(FORWARD_BATCH_SIZE);
        std::mem::swap(batch, &mut swap_batch);

        if client.send_transactions_in_batch(swap_batch).is_err() {
            *dropped_counter += FORWARD_BATCH_SIZE;
        }
    }
}

struct ForwardingStageMetrics {
    last_reported: Instant,
    did_something: bool,

    /// Number of votes received for forwarding.
    votes_received: usize,
    /// Number of votes that failed basic sanitization or priority calculation.
    votes_dropped_on_receive: usize,
    /// Number of votes dropped because forwarding container is full and the
    /// priority of transaction is lower than the priority of other transaction
    /// in the container.
    votes_dropped_on_capacity: usize,
    /// Number of votes dropped due to exceeding outbound data traffic limit.
    votes_dropped_on_data_budget: usize,
    /// Number of votes we tried to forward.
    votes_forwarded: usize,
    /// Number of votes dropped due to send failure.
    votes_dropped_on_send: usize,

    non_votes_received: usize,
    non_votes_dropped_on_receive: usize,
    non_votes_dropped_on_capacity: usize,
    non_votes_dropped_on_data_budget: usize,
    non_votes_forwarded: usize,
    non_votes_dropped_on_send: usize,

    mcp_memos_built: usize,
    mcp_microblock_memo_txs_built: usize,
    mcp_checkpoint_memo_txs_built: usize,
    mcp_da_cert_memo_txs_built: usize,
    mcp_memos_forwarded: usize,
    mcp_memos_dropped_on_data_budget: usize,
    mcp_memos_dropped_on_send: usize,
}

impl ForwardingStageMetrics {
    fn maybe_report(&mut self) {
        const REPORTING_INTERVAL: Duration = Duration::from_secs(1);

        if self.last_reported.elapsed() > REPORTING_INTERVAL {
            // Reset time and all counts.
            let metrics = core::mem::take(self);

            // Only report if something happened.
            if !metrics.did_something {
                return;
            }

            datapoint_info!(
                "forwarding_stage",
                ("votes_received", metrics.votes_received, i64),
                (
                    "votes_dropped_on_receive",
                    metrics.votes_dropped_on_receive,
                    i64
                ),
                (
                    "votes_dropped_on_capacity",
                    metrics.votes_dropped_on_capacity,
                    i64
                ),
                (
                    "votes_dropped_on_data_budget",
                    metrics.votes_dropped_on_data_budget,
                    i64
                ),
                ("votes_forwarded", metrics.votes_forwarded, i64),
                ("votes_dropped_on_send", metrics.votes_dropped_on_send, i64),
                ("non_votes_received", metrics.non_votes_received, i64),
                (
                    "non_votes_dropped_on_receive",
                    metrics.non_votes_dropped_on_receive,
                    i64
                ),
                (
                    "non_votes_dropped_on_capacity",
                    metrics.non_votes_dropped_on_capacity,
                    i64
                ),
                (
                    "non_votes_dropped_on_data_budget",
                    metrics.non_votes_dropped_on_data_budget,
                    i64
                ),
                ("non_votes_forwarded", metrics.non_votes_forwarded, i64),
                (
                    "non_votes_dropped_on_send",
                    metrics.non_votes_dropped_on_send,
                    i64
                ),
                ("mcp_memos_built", metrics.mcp_memos_built, i64),
                ("mcp_memos_forwarded", metrics.mcp_memos_forwarded, i64),
                (
                    "mcp_memos_dropped_on_data_budget",
                    metrics.mcp_memos_dropped_on_data_budget,
                    i64
                ),
                (
                    "mcp_memos_dropped_on_send",
                    metrics.mcp_memos_dropped_on_send,
                    i64
                ),
                (
                    "mcp_microblock_memo_txs_built",
                    metrics.mcp_microblock_memo_txs_built,
                    i64
                ),
                (
                    "mcp_checkpoint_memo_txs_built",
                    metrics.mcp_checkpoint_memo_txs_built,
                    i64
                ),
                (
                    "mcp_da_cert_memo_txs_built",
                    metrics.mcp_da_cert_memo_txs_built,
                    i64
                ),
            );
        }
    }
}

impl Default for ForwardingStageMetrics {
    fn default() -> Self {
        Self {
            last_reported: Instant::now(),
            did_something: false,
            votes_received: 0,
            votes_dropped_on_receive: 0,
            votes_dropped_on_capacity: 0,
            votes_dropped_on_data_budget: 0,
            votes_forwarded: 0,
            votes_dropped_on_send: 0,
            non_votes_received: 0,
            non_votes_dropped_on_receive: 0,
            non_votes_dropped_on_capacity: 0,
            non_votes_dropped_on_data_budget: 0,
            non_votes_forwarded: 0,
            non_votes_dropped_on_send: 0,
            mcp_memos_built: 0,
            mcp_microblock_memo_txs_built: 0,
            mcp_checkpoint_memo_txs_built: 0,
            mcp_da_cert_memo_txs_built: 0,
            mcp_memos_forwarded: 0,
            mcp_memos_dropped_on_data_budget: 0,
            mcp_memos_dropped_on_send: 0,
        }
    }
}

fn initial_packet_meta_filter(meta: &packet::Meta) -> bool {
    !meta.discard() && !meta.forwarded() && meta.is_from_staked_node()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::unbounded,
        packet::PacketFlags,
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::Message,
        solana_perf::packet::{Packet, PacketBatch, PinnedPacketBatch},
        solana_pubkey::Pubkey,
        solana_runtime::genesis_utils::create_genesis_config,
        solana_signer::Signer,
        solana_system_transaction as system_transaction,
        solana_transaction::Transaction,
        solana_transaction::versioned::VersionedTransaction,
        std::sync::{Arc, Mutex},
    };

    #[derive(Clone)]
    pub struct MockClient {
        packets: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl MockClient {
        pub fn new() -> Self {
            Self {
                packets: Arc::new(Mutex::new(Vec::new())),
            }
        }

        pub fn get_packets(&self) -> Vec<Vec<u8>> {
            self.packets.lock().unwrap().clone()
        }
    }

    impl ForwardingClient for MockClient {
        fn send_transactions_in_batch(
            &self,
            wire_transactions: Vec<Vec<u8>>,
        ) -> Result<(), ForwardingClientError> {
            self.packets.lock().unwrap().extend(wire_transactions);
            Ok(())
        }
    }

    fn meta_with_flags(packet_flags: PacketFlags) -> packet::Meta {
        packet::Meta {
            flags: packet_flags,
            ..packet::Meta::default()
        }
    }

    fn simple_transfer_with_flags(packet_flags: PacketFlags) -> Packet {
        let transaction = system_transaction::transfer(
            &Keypair::new(),
            &Pubkey::new_unique(),
            1,
            Hash::default(),
        );
        let mut packet = Packet::from_data(None, &transaction).unwrap();
        packet.meta_mut().flags = packet_flags;
        packet
    }

    #[test]
    fn test_initial_packet_meta_filter() {
        assert!(!initial_packet_meta_filter(&meta_with_flags(
            PacketFlags::empty()
        )));
        assert!(initial_packet_meta_filter(&meta_with_flags(
            PacketFlags::FROM_STAKED_NODE
        )));
        assert!(!initial_packet_meta_filter(&meta_with_flags(
            PacketFlags::DISCARD
        )));
        assert!(!initial_packet_meta_filter(&meta_with_flags(
            PacketFlags::FORWARDED
        )));
        assert!(!initial_packet_meta_filter(&meta_with_flags(
            PacketFlags::FROM_STAKED_NODE | PacketFlags::DISCARD
        )));
    }

    #[test]
    fn test_forwarding() {
        let (packet_batch_sender, packet_batch_receiver) = unbounded();

        let (_bank, bank_forks) =
            Bank::new_with_bank_forks_for_tests(&create_genesis_config(1).genesis_config);
        let sharable_banks = bank_forks.read().unwrap().sharable_banks();
        let vote_mock_client = MockClient::new();
        let non_vote_mock_client = MockClient::new();
        let mut forwarding_stage = ForwardingStage::new(
            packet_batch_receiver,
            vote_mock_client.clone(),
            Box::new([non_vote_mock_client.clone()]),
            sharable_banks,
            DataBudget::default(),
            None,
            None,
            None,
        );

        // Send packet batches.
        let non_vote_packets =
            BankingPacketBatch::new(vec![PacketBatch::from(PinnedPacketBatch::new(vec![
                simple_transfer_with_flags(PacketFlags::FROM_STAKED_NODE),
                simple_transfer_with_flags(PacketFlags::FROM_STAKED_NODE | PacketFlags::DISCARD),
                simple_transfer_with_flags(PacketFlags::FROM_STAKED_NODE | PacketFlags::FORWARDED),
            ]))]);
        let vote_packets =
            BankingPacketBatch::new(vec![PacketBatch::from(PinnedPacketBatch::new(vec![
                simple_transfer_with_flags(
                    PacketFlags::SIMPLE_VOTE_TX | PacketFlags::FROM_STAKED_NODE,
                ),
                simple_transfer_with_flags(
                    PacketFlags::SIMPLE_VOTE_TX
                        | PacketFlags::FROM_STAKED_NODE
                        | PacketFlags::DISCARD,
                ),
                simple_transfer_with_flags(
                    PacketFlags::SIMPLE_VOTE_TX
                        | PacketFlags::FROM_STAKED_NODE
                        | PacketFlags::FORWARDED,
                ),
            ]))]);

        packet_batch_sender
            .send((non_vote_packets.clone(), false))
            .unwrap();
        packet_batch_sender
            .send((vote_packets.clone(), true))
            .unwrap();

        let bank = forwarding_stage.sharable_banks.root();
        forwarding_stage.receive_and_buffer(&bank);
        if !packet_batch_sender.is_empty() {
            forwarding_stage.receive_and_buffer(&bank);
        }
        forwarding_stage.forward_buffered_packets();

        assert_eq!(forwarding_stage.metrics.non_votes_forwarded, 1);
        assert_eq!(forwarding_stage.metrics.votes_forwarded, 1);

        let vote_wired_txs = vote_mock_client.get_packets();
        assert_eq!(vote_wired_txs.len(), 1);
        assert_eq!(
            vote_wired_txs[0],
            vote_packets[0].first().unwrap().data(..).unwrap()
        );

        let non_vote_wired_txs = non_vote_mock_client.get_packets();
        assert_eq!(non_vote_wired_txs.len(), 1);
        assert_eq!(
            non_vote_wired_txs[0],
            non_vote_packets[0].first().unwrap().data(..).unwrap()
        );
    }

    #[test]
    fn test_mcp_forwarding_proposer_overrides_bid_with_fair_priority() {
        // Build a versioned transaction with a non-zero CU price so we can see it overridden.
        let recent_blockhash = Hash::new_unique();
        let ix_price = ComputeBudgetInstruction::set_compute_unit_price(123);
        let payer = Keypair::new();
        let message = Message::new(&[ix_price], Some(&payer.pubkey()));
        let tx = Transaction::new(&[&payer], message, recent_blockhash);
        let vtx: VersionedTransaction = tx.into();
        let tx_bytes = bincode::serialize(&vtx).unwrap();

        // Insert a fair priority for this tx signature and ensure MCP uses it as the bid hint.
        let sig = try_first_signature_bytes(&tx_bytes).unwrap();
        crate::solanacdn::insert_fair_priority(sig, 999);

        let bid_hint = bid_hint_for_forwarded_tx(&tx_bytes);
        assert_eq!(bid_hint.cu_price, 999);
    }
}
