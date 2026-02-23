use {
    anyhow::{anyhow, bail, Context, Result},
    quinn::{crypto::rustls::QuicServerConfig, Endpoint, ServerConfig},
    rand::random,
    reqwest::Client,
    serde_json::json,
    solana_compute_budget_interface::ComputeBudgetInstruction,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_message::Message,
    solana_pubkey::Pubkey,
    solana_sha256_hasher as sha256_hasher,
    solana_signer::Signer,
    solana_tls_utils::{crypto_provider, new_dummy_x509_certificate},
    solana_transaction::{versioned::VersionedTransaction, Transaction},
    solanacdn_protocol::{
        crypto::{random_nonce_16, PubkeyBytes, SignatureBytes},
        frame::{decode_envelope, encode_envelope},
        messages::{
            AgentToPop, AuthOk, AuthRequest, FairBatch, FairBatchAttestation,
            FairBatchAttestationPayload, FairBatchCommit, FairBatchReceiptCommit, FairBatchReject,
            FairBatchWitness, FairBatchWitnessPayload, FairTx, PopToAgent,
        },
    },
    std::{
        collections::HashMap,
        net::SocketAddr,
        str::FromStr,
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
        sync::{mpsc, RwLock},
    },
};

#[derive(Clone, Debug)]
struct Config {
    listens: Vec<SocketAddr>,
    batches: u64,
    txs_per_batch: usize,
    interval_ms: u64,
    exit_after_ms: u64,
    origin_pop_id: String,
    flow_id: u128,
    target_slot: Option<u64>,
    target_slot_offset: u64,
    rpc_url: Option<String>,
    broadcast_evidence: bool,
    echo_commits: bool,
    emit_witnesses: bool,
    airdrop_lamports: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let cfg = parse_args()?;
    if cfg.listens.is_empty() {
        bail!("no POP listen addresses configured");
    }
    eprintln!(
        "solanacdn-pop-stub: listens={:?}, batches={}, txs_per_batch={}, interval_ms={}, exit_after_ms={}",
        cfg.listens, cfg.batches, cfg.txs_per_batch, cfg.interval_ms, cfg.exit_after_ms
    );
    eprintln!(
        "solanacdn-pop-stub: start validator with --solanacdn-pop <POP_ADDR> --solanacdn-tls-insecure-skip-verify --fair"
    );

    let pops = make_pops(&cfg)?;
    let shared = Arc::new(RwLock::new(SharedState::new(pops.len())));

    let mut endpoints: Vec<Endpoint> = Vec::with_capacity(pops.len());
    let mut server_tasks = Vec::with_capacity(pops.len());
    for pop in pops.iter() {
        let server_config = make_server_config()?;
        let endpoint = Endpoint::server(server_config, pop.listen)?;
        endpoints.push(endpoint.clone());
        let cfg = cfg.clone();
        let shared = shared.clone();
        let pop = pop.clone();
        server_tasks.push(tokio::spawn(async move {
            accept_loop(endpoint, pop, cfg, shared).await;
        }));
    }

    if cfg.exit_after_ms > 0 {
        let endpoints = endpoints.clone();
        let exit_after_ms = cfg.exit_after_ms;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(exit_after_ms)).await;
            for ep in endpoints {
                ep.close(0u32.into(), b"done");
            }
        });
    }

    let batch_task = tokio::spawn(batch_loop(cfg.clone(), pops.clone(), shared.clone()));

    for task in server_tasks {
        let _ = task.await;
    }
    let _ = batch_task.await;

    Ok(())
}

fn usage() -> &'static str {
    "solanacdn-pop-stub [--listen HOST:PORT]... [--batches N] [--txs-per-batch N] [--interval-ms MS] [--exit-after-ms MS] [--origin-pop-id ID] [--target-slot SLOT] [--target-slot-offset N] [--rpc-url URL] [--broadcast-evidence] [--emit-witnesses] [--echo-commits] [--airdrop-lamports N]"
}

fn parse_args() -> Result<Config> {
    let mut cfg = Config {
        listens: Vec::new(),
        batches: 1,
        txs_per_batch: 1,
        interval_ms: 500,
        exit_after_ms: 1_000,
        origin_pop_id: "local-pop".to_string(),
        flow_id: 0,
        target_slot: None,
        // When --target-slot is not explicitly set, default to a small positive offset so the
        // batch lands in a future slot (required under `--fair` in this fork).
        target_slot_offset: 2,
        rpc_url: None,
        broadcast_evidence: false,
        echo_commits: false,
        emit_witnesses: false,
        airdrop_lamports: 2_000_000_000,
    };

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                let value = next_arg(&mut args, "--listen")?;
                let addr = value
                    .parse()
                    .with_context(|| format!("invalid --listen: {value}"))?;
                cfg.listens.push(addr);
            }
            "--batches" => {
                let value = next_arg(&mut args, "--batches")?;
                cfg.batches = value
                    .parse()
                    .with_context(|| format!("invalid --batches: {value}"))?;
            }
            "--txs-per-batch" => {
                let value = next_arg(&mut args, "--txs-per-batch")?;
                cfg.txs_per_batch = value
                    .parse()
                    .with_context(|| format!("invalid --txs-per-batch: {value}"))?;
            }
            "--interval-ms" => {
                let value = next_arg(&mut args, "--interval-ms")?;
                cfg.interval_ms = value
                    .parse()
                    .with_context(|| format!("invalid --interval-ms: {value}"))?;
            }
            "--exit-after-ms" => {
                let value = next_arg(&mut args, "--exit-after-ms")?;
                cfg.exit_after_ms = value
                    .parse()
                    .with_context(|| format!("invalid --exit-after-ms: {value}"))?;
            }
            "--origin-pop-id" => {
                cfg.origin_pop_id = next_arg(&mut args, "--origin-pop-id")?;
            }
            "--flow-id" => {
                let value = next_arg(&mut args, "--flow-id")?;
                cfg.flow_id = value
                    .parse()
                    .with_context(|| format!("invalid --flow-id: {value}"))?;
            }
            "--target-slot" => {
                let value = next_arg(&mut args, "--target-slot")?;
                cfg.target_slot = Some(
                    value
                        .parse()
                        .with_context(|| format!("invalid --target-slot: {value}"))?,
                );
            }
            "--target-slot-offset" => {
                let value = next_arg(&mut args, "--target-slot-offset")?;
                cfg.target_slot_offset = value
                    .parse()
                    .with_context(|| format!("invalid --target-slot-offset: {value}"))?;
            }
            "--rpc-url" => {
                cfg.rpc_url = Some(next_arg(&mut args, "--rpc-url")?);
            }
            "--broadcast-evidence" => {
                cfg.broadcast_evidence = true;
            }
            "--echo-commits" => {
                cfg.echo_commits = true;
            }
            "--emit-witnesses" => {
                cfg.emit_witnesses = true;
            }
            "--airdrop-lamports" => {
                let value = next_arg(&mut args, "--airdrop-lamports")?;
                cfg.airdrop_lamports = value
                    .parse()
                    .with_context(|| format!("invalid --airdrop-lamports: {value}"))?;
            }
            "-h" | "--help" => {
                eprintln!("Usage: {}", usage());
                std::process::exit(0);
            }
            other => bail!("unknown arg: {other}\nUsage: {}", usage()),
        }
    }

    if cfg.txs_per_batch == 0 {
        bail!("--txs-per-batch must be >= 1");
    }

    if cfg.listens.is_empty() {
        cfg.listens
            .push("127.0.0.1:9002".parse().expect("default listen"));
    }

    Ok(cfg)
}

fn next_arg<I: Iterator<Item = String>>(args: &mut I, name: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| anyhow::anyhow!("missing value for {name}"))
}

#[derive(Clone, Debug)]
struct PopNode {
    index: usize,
    listen: SocketAddr,
    pop_id: String,
    signing_key: Arc<ed25519_dalek_v2::SigningKey>,
    pop_pubkey: PubkeyBytes,
}

impl PopNode {
    fn label(&self) -> String {
        format!("[pop{} {}]", self.index, self.listen)
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct Subscriptions {
    fair_commits: bool,
    fair_acks: bool,
    fair_rejects: bool,
    fair_witnesses: bool,
}

#[derive(Clone, Debug)]
struct Session {
    out_tx: mpsc::Sender<PopToAgent>,
    subs: Subscriptions,
}

#[derive(Debug)]
struct SharedState {
    sessions_by_pop: Vec<HashMap<PubkeyBytes, Session>>,
}

impl SharedState {
    fn new(pop_count: usize) -> Self {
        let mut sessions_by_pop = Vec::with_capacity(pop_count);
        for _ in 0..pop_count {
            sessions_by_pop.push(HashMap::new());
        }
        Self { sessions_by_pop }
    }
}

fn make_pops(cfg: &Config) -> Result<Arc<Vec<PopNode>>> {
    let mut pops = Vec::with_capacity(cfg.listens.len());
    for (idx, listen) in cfg.listens.iter().copied().enumerate() {
        let mut rng = rand::rngs::OsRng;
        let signing_key = Arc::new(ed25519_dalek_v2::SigningKey::generate(&mut rng));
        let pop_pubkey = PubkeyBytes(signing_key.verifying_key().to_bytes());
        let pop_id = if idx == 0 {
            cfg.origin_pop_id.clone()
        } else {
            format!("{}-w{idx}", cfg.origin_pop_id)
        };
        pops.push(PopNode {
            index: idx,
            listen,
            pop_id,
            signing_key,
            pop_pubkey,
        });
    }
    Ok(Arc::new(pops))
}

async fn accept_loop(
    endpoint: Endpoint,
    pop: PopNode,
    cfg: Config,
    shared: Arc<RwLock<SharedState>>,
) {
    loop {
        let incoming = endpoint.accept().await;
        let Some(connecting) = incoming else {
            break;
        };
        let cfg = cfg.clone();
        let shared = shared.clone();
        let pop_label = pop.label();
        let pop = pop.clone();
        tokio::spawn(async move {
            match connecting.await {
                Ok(conn) => {
                    if let Err(err) = handle_connection(conn, pop, cfg, shared).await {
                        eprintln!("solanacdn-pop-stub: {pop_label} connection error: {err:#}");
                    }
                }
                Err(err) => {
                    eprintln!("solanacdn-pop-stub: {pop_label} failed to accept connection: {err}")
                }
            }
        });
    }
}

async fn handle_connection(
    conn: quinn::Connection,
    pop: PopNode,
    cfg: Config,
    shared: Arc<RwLock<SharedState>>,
) -> Result<()> {
    loop {
        let (send, mut recv) = match conn.accept_bi().await {
            Ok(stream) => stream,
            Err(err) => {
                eprintln!("solanacdn-pop-stub: {} accept_bi ended: {err}", pop.label());
                return Ok(());
            }
        };

        let msg = match read_agent_msg(&mut recv).await {
            Ok(msg) => msg,
            Err(err) => {
                eprintln!(
                    "solanacdn-pop-stub: {} failed to read first msg: {err:#}",
                    pop.label()
                );
                continue;
            }
        };

        let validator_pubkey = match msg {
            AgentToPop::Auth(AuthRequest { payload, .. }) => payload.validator_pubkey,
            AgentToPop::AuthWithSessionToken(v) => v.auth.payload.validator_pubkey,
            other => {
                eprintln!(
                    "solanacdn-pop-stub: {} ignoring stream message: {other:?}",
                    pop.label()
                );
                tokio::spawn(async move {
                    drain_stream(recv).await;
                });
                continue;
            }
        };

        eprintln!(
            "solanacdn-pop-stub: {} received auth validator_pubkey={}",
            pop.label(),
            validator_pubkey.to_base58()
        );

        let drain_conn = conn.clone();
        let drain_task = tokio::spawn(async move {
            loop {
                let stream = drain_conn.accept_bi().await;
                let Ok((send, recv)) = stream else {
                    break;
                };
                tokio::spawn(async move {
                    // Keep the send-side of non-control streams open so the validator does not
                    // treat stream EOF as a protocol violation (it expects POP→agent traffic on
                    // shreds/votes streams, even if we never send anything in this stub).
                    let _keep_send_open = send;
                    drain_stream(recv).await;
                });
            }
        });

        let res = handle_control_stream(
            send,
            recv,
            pop.clone(),
            validator_pubkey,
            cfg.clone(),
            shared.clone(),
        )
        .await;
        let _ = drain_task.await;
        return res;
    }
}

async fn handle_control_stream(
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    pop: PopNode,
    validator_pubkey: PubkeyBytes,
    cfg: Config,
    shared: Arc<RwLock<SharedState>>,
) -> Result<()> {
    let (out_tx, mut out_rx) = mpsc::channel::<PopToAgent>(256);
    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if write_pop_msg(&mut send, &msg).await.is_err() {
                return;
            }
        }
    });

    let auth_ok = AuthOk {
        pop_id: pop.pop_id.clone(),
        pop_pubkey: pop.pop_pubkey,
        server_time_ms: now_ms(),
        udp_token: random_nonce_16(),
        udp_shreds_port: 0,
        udp_votes_port: 0,
    };
    if out_tx.send(PopToAgent::AuthOk(auth_ok)).await.is_err() {
        return Ok(());
    }

    {
        let mut s = shared.write().await;
        if pop.index < s.sessions_by_pop.len() {
            s.sessions_by_pop[pop.index].insert(
                validator_pubkey,
                Session {
                    out_tx: out_tx.clone(),
                    subs: Subscriptions::default(),
                },
            );
        }
    }

    loop {
        let msg = match read_agent_msg(&mut recv).await {
            Ok(msg) => msg,
            Err(_) => break,
        };

        match msg {
            AgentToPop::FairBatchCommit(commit) => {
                eprintln!(
                    "solanacdn-pop-stub: {} received FairBatchCommit batch_id={} order_start={} txs={}",
                    pop.label(),
                    commit.payload.batch_id,
                    commit.payload.order_start,
                    commit.payload.tx_sigs.len()
                );

                if cfg.echo_commits {
                    let _ = out_tx
                        .send(PopToAgent::FairBatchCommit(commit.clone()))
                        .await;
                }
                if cfg.broadcast_evidence {
                    broadcast_commit(shared.clone(), commit, Some((pop.index, validator_pubkey)))
                        .await;
                }
            }
            AgentToPop::FairBatchAck(ack) => {
                eprintln!(
                    "solanacdn-pop-stub: {} received FairBatchAck batch_id={} order_start={} tx_count={}",
                    pop.label(),
                    ack.payload.batch_id,
                    ack.payload.order_start,
                    ack.payload.tx_count
                );
                if cfg.broadcast_evidence {
                    broadcast_ack(shared.clone(), ack, Some((pop.index, validator_pubkey))).await;
                }
            }
            AgentToPop::FairBatchReject(reject) => {
                eprintln!(
                    "solanacdn-pop-stub: {} received FairBatchReject batch_id={} order_start={} reason={:?}",
                    pop.label(),
                    reject.payload.batch_id,
                    reject.payload.order_start,
                    reject.payload.reason
                );
                if cfg.broadcast_evidence {
                    broadcast_reject(shared.clone(), reject, Some((pop.index, validator_pubkey)))
                        .await;
                }
            }
            AgentToPop::Heartbeat(_) => {}
            AgentToPop::Capabilities(cap) => {
                eprintln!(
                    "solanacdn-pop-stub: {} capabilities tx_fair_ordering={}",
                    pop.label(),
                    cap.tx_fair_ordering
                );
            }
            AgentToPop::SubscribeFairCommits => {
                update_sub(shared.clone(), pop.index, validator_pubkey, |s| {
                    s.fair_commits = true
                })
                .await
            }
            AgentToPop::UnsubscribeFairCommits => {
                update_sub(shared.clone(), pop.index, validator_pubkey, |s| {
                    s.fair_commits = false
                })
                .await
            }
            AgentToPop::SubscribeFairAcks => {
                update_sub(shared.clone(), pop.index, validator_pubkey, |s| {
                    s.fair_acks = true
                })
                .await
            }
            AgentToPop::UnsubscribeFairAcks => {
                update_sub(shared.clone(), pop.index, validator_pubkey, |s| {
                    s.fair_acks = false
                })
                .await
            }
            AgentToPop::SubscribeFairRejects => {
                update_sub(shared.clone(), pop.index, validator_pubkey, |s| {
                    s.fair_rejects = true
                })
                .await
            }
            AgentToPop::UnsubscribeFairRejects => {
                update_sub(shared.clone(), pop.index, validator_pubkey, |s| {
                    s.fair_rejects = false
                })
                .await
            }
            AgentToPop::SubscribeFairWitnesses => {
                update_sub(shared.clone(), pop.index, validator_pubkey, |s| {
                    s.fair_witnesses = true
                })
                .await
            }
            AgentToPop::UnsubscribeFairWitnesses => {
                update_sub(shared.clone(), pop.index, validator_pubkey, |s| {
                    s.fair_witnesses = false
                })
                .await
            }
            other => {
                eprintln!("solanacdn-pop-stub: {} ctrl msg {other:?}", pop.label());
            }
        }
    }

    {
        let mut s = shared.write().await;
        if pop.index < s.sessions_by_pop.len() {
            s.sessions_by_pop[pop.index].remove(&validator_pubkey);
        }
    }
    drop(out_tx);

    let _ = writer.await;
    Ok(())
}

async fn update_sub<F>(
    shared: Arc<RwLock<SharedState>>,
    pop_index: usize,
    validator_pubkey: PubkeyBytes,
    f: F,
) where
    F: FnOnce(&mut Subscriptions),
{
    let mut s = shared.write().await;
    if let Some(sess) = s
        .sessions_by_pop
        .get_mut(pop_index)
        .and_then(|m| m.get_mut(&validator_pubkey))
    {
        f(&mut sess.subs);
    }
}

async fn broadcast_commit(
    shared: Arc<RwLock<SharedState>>,
    commit: FairBatchCommit,
    exclude: Option<(usize, PubkeyBytes)>,
) {
    broadcast_evidence(shared, Evidence::Commit(commit), exclude).await;
}

async fn broadcast_ack(
    shared: Arc<RwLock<SharedState>>,
    ack: FairBatchReceiptCommit,
    exclude: Option<(usize, PubkeyBytes)>,
) {
    broadcast_evidence(shared, Evidence::Ack(ack), exclude).await;
}

async fn broadcast_reject(
    shared: Arc<RwLock<SharedState>>,
    reject: FairBatchReject,
    exclude: Option<(usize, PubkeyBytes)>,
) {
    broadcast_evidence(shared, Evidence::Reject(reject), exclude).await;
}

enum Evidence {
    Commit(FairBatchCommit),
    Ack(FairBatchReceiptCommit),
    Reject(FairBatchReject),
}

async fn broadcast_evidence(
    shared: Arc<RwLock<SharedState>>,
    evidence: Evidence,
    exclude: Option<(usize, PubkeyBytes)>,
) {
    let (msg, predicate): (PopToAgent, fn(&Subscriptions) -> bool) = match &evidence {
        Evidence::Commit(c) => (PopToAgent::FairBatchCommit(c.clone()), |s| s.fair_commits),
        Evidence::Ack(a) => (PopToAgent::FairBatchAck(a.clone()), |s| s.fair_acks),
        Evidence::Reject(r) => (PopToAgent::FairBatchReject(r.clone()), |s| s.fair_rejects),
    };

    let targets: Vec<mpsc::Sender<PopToAgent>> = {
        let s = shared.read().await;
        let mut out = Vec::new();
        for (pop_index, map) in s.sessions_by_pop.iter().enumerate() {
            for (pk, sess) in map.iter() {
                if let Some((ex_pop, ex_pk)) = exclude {
                    if pop_index == ex_pop && *pk == ex_pk {
                        continue;
                    }
                }
                if predicate(&sess.subs) {
                    out.push(sess.out_tx.clone());
                }
            }
        }
        out
    };

    for tx in targets {
        let _ = tx.send(msg.clone()).await;
    }
}

async fn batch_loop(
    cfg: Config,
    pops: Arc<Vec<PopNode>>,
    shared: Arc<RwLock<SharedState>>,
) -> Result<()> {
    let rpc_client = cfg.rpc_url.as_ref().map(|_| Client::new());

    let payer = Keypair::new();
    let mut payer_funded = cfg.airdrop_lamports == 0 || cfg.rpc_url.is_none();

    let ingress_pop = pops
        .get(0)
        .ok_or_else(|| anyhow!("no ingress pop configured"))?
        .clone();

    let mut batches_sent: u64 = 0;
    let mut tx_seq_start: u64 = 0;
    loop {
        if cfg.batches != 0 && batches_sent >= cfg.batches {
            break;
        }

        let have_sessions = {
            let s = shared.read().await;
            s.sessions_by_pop
                .get(ingress_pop.index)
                .map(|m| !m.is_empty())
                .unwrap_or(false)
        };
        if !have_sessions {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        if !payer_funded {
            if let (Some(url), Some(client)) = (cfg.rpc_url.as_ref(), rpc_client.as_ref()) {
                match request_airdrop_and_wait(client, url, payer.pubkey(), cfg.airdrop_lamports)
                    .await
                {
                    Ok(()) => payer_funded = true,
                    Err(err) => {
                        eprintln!("solanacdn-pop-stub: airdrop failed (retrying): {err:#}");
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                }
            }
        }

        let batch_id = random::<u128>();
        let (blockhash, target_slot, leader_pubkey) = if let (Some(url), Some(client)) =
            (cfg.rpc_url.as_ref(), rpc_client.as_ref())
        {
            let blockhash = fetch_latest_blockhash(client, url)
                .await
                .unwrap_or_else(|err| {
                    eprintln!("solanacdn-pop-stub: failed to fetch blockhash from {url}: {err:#}");
                    Hash::new_unique()
                });
            let slot = if let Some(slot) = cfg.target_slot {
                slot
            } else {
                let cur = fetch_current_slot(client, url).await.unwrap_or_else(|err| {
                    eprintln!("solanacdn-pop-stub: failed to fetch slot from {url}: {err:#}");
                    0
                });
                cur.saturating_add(cfg.target_slot_offset)
            };
            let leader = fetch_slot_leader(client, url, slot).await.ok();
            (blockhash, Some(slot), leader)
        } else {
            (Hash::new_unique(), cfg.target_slot, None)
        };

        let batch = build_fair_batch(
            &cfg,
            batch_id,
            tx_seq_start,
            blockhash,
            target_slot,
            &payer,
            &ingress_pop.signing_key,
        );

        let sent = send_fair_batch_to_leader(
            shared.clone(),
            ingress_pop.index,
            leader_pubkey,
            batch.clone(),
        )
        .await;
        if sent == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }

        if cfg.emit_witnesses {
            if let (Some(slot), Some(leader_pubkey)) = (target_slot, leader_pubkey) {
                send_witnesses(shared.clone(), pops.clone(), leader_pubkey, slot, &batch).await;
            } else {
                eprintln!(
                    "solanacdn-pop-stub: witnesses enabled but missing target_slot/leader_pubkey; set --rpc-url (and optionally --target-slot/--target-slot-offset) or pass --target-slot explicitly"
                );
            }
        }

        batches_sent = batches_sent.saturating_add(1);
        tx_seq_start = tx_seq_start.saturating_add(cfg.txs_per_batch as u64);
        if cfg.batches != 0 && batches_sent >= cfg.batches {
            break;
        }
        tokio::time::sleep(Duration::from_millis(cfg.interval_ms)).await;
    }

    Ok(())
}

async fn send_fair_batch_to_leader(
    shared: Arc<RwLock<SharedState>>,
    ingress_pop_index: usize,
    leader_pubkey: Option<PubkeyBytes>,
    batch: FairBatch,
) -> usize {
    let targets: Vec<mpsc::Sender<PopToAgent>> = {
        let s = shared.read().await;
        match s.sessions_by_pop.get(ingress_pop_index) {
            Some(map) => {
                if let Some(leader) = leader_pubkey {
                    map.get(&leader)
                        .map(|sess| vec![sess.out_tx.clone()])
                        .unwrap_or_else(Vec::new)
                } else {
                    map.values().map(|sess| sess.out_tx.clone()).collect()
                }
            }
            None => Vec::new(),
        }
    };

    let mut sent = 0usize;
    for tx in targets {
        if tx.send(PopToAgent::FairBatch(batch.clone())).await.is_ok() {
            sent = sent.saturating_add(1);
        }
    }
    sent
}

async fn send_witnesses(
    shared: Arc<RwLock<SharedState>>,
    pops: Arc<Vec<PopNode>>,
    leader_pubkey: PubkeyBytes,
    target_slot: u64,
    batch: &FairBatch,
) {
    if batch.attestation.payload.target_slot != Some(target_slot) {
        eprintln!(
            "solanacdn-pop-stub: unexpected witness target_slot mismatch: witness_target_slot={target_slot} batch_target_slot={:?}",
            batch.attestation.payload.target_slot
        );
    }
    for pop in pops.iter() {
        let witness_payload = FairBatchWitnessPayload {
            attestation: batch.attestation.payload.clone(),
            leader_pubkey,
            pop_time_ms: now_ms(),
        };
        let witness = match FairBatchWitness::sign(witness_payload, &pop.signing_key) {
            Ok(w) => w,
            Err(err) => {
                eprintln!(
                    "solanacdn-pop-stub: {} failed to sign witness: {err:#}",
                    pop.label()
                );
                continue;
            }
        };
        let msg = PopToAgent::FairBatchWitness(witness);

        let targets: Vec<mpsc::Sender<PopToAgent>> = {
            let s = shared.read().await;
            s.sessions_by_pop
                .get(pop.index)
                .map(|map| {
                    map.values()
                        .filter(|sess| sess.subs.fair_witnesses)
                        .map(|sess| sess.out_tx.clone())
                        .collect()
                })
                .unwrap_or_default()
        };
        for tx in targets {
            let _ = tx.send(msg.clone()).await;
        }
    }
}

fn build_fair_batch(
    cfg: &Config,
    batch_id: u128,
    tx_seq_start: u64,
    recent_blockhash: Hash,
    target_slot: Option<u64>,
    payer: &Keypair,
    pop_signing_key: &ed25519_dalek_v2::SigningKey,
) -> FairBatch {
    let mut txs = Vec::with_capacity(cfg.txs_per_batch);

    for idx in 0..cfg.txs_per_batch {
        let mut ixs = Vec::with_capacity(2);
        ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(
            (idx as u32).saturating_add(1),
        ));
        // Give the fair flow a non-zero CU price so under load it stays competitive.
        ixs.push(ComputeBudgetInstruction::set_compute_unit_price(1));
        let message = Message::new(ixs.as_slice(), Some(&payer.pubkey()));
        let tx = Transaction::new(&[payer], message, recent_blockhash);
        let vtx = VersionedTransaction::from(tx);
        let payload = bincode::serialize(&vtx).expect("serialize tx");
        let sig = SignatureBytes(*vtx.signatures[0].as_array());
        txs.push(FairTx { sig, payload });
    }

    let created_at_ms = now_ms();
    let batch_ms: u16 = 50;
    let tx_count: u32 = txs.len().try_into().unwrap_or(0);
    let tx_merkle_root = fair_merkle_root(txs.as_slice());
    let attestation_payload = FairBatchAttestationPayload {
        origin_pop_id: cfg.origin_pop_id.clone(),
        flow_id: cfg.flow_id,
        batch_id,
        tx_seq_start,
        tx_count,
        tx_merkle_root,
        created_at_ms,
        batch_ms,
        target_slot,
    };
    let attestation = FairBatchAttestation::sign(attestation_payload, pop_signing_key)
        .unwrap_or_else(|e| panic!("failed to sign FairBatchAttestation: {e}"));

    FairBatch {
        origin_pop_id: cfg.origin_pop_id.clone(),
        flow_id: cfg.flow_id,
        batch_id,
        tx_seq_start,
        created_at_ms,
        batch_ms,
        target_slot,
        attestation,
        txs,
    }
}

const FAIR_MERKLE_LEAF_DOMAIN: &[u8] = b"SCDNFAIRLEAFv1";
const FAIR_MERKLE_NODE_DOMAIN: &[u8] = b"SCDNFAIRNODEv1";

fn sha256_bytes(data: &[u8]) -> [u8; 32] {
    let digest = sha256_hasher::hash(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(digest.as_ref());
    out
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

fn fair_merkle_root(txs: &[FairTx]) -> [u8; 32] {
    if txs.is_empty() {
        return [0u8; 32];
    }
    let mut level: Vec<[u8; 32]> = txs
        .iter()
        .enumerate()
        .map(|(idx, tx)| fair_merkle_leaf_hash(idx as u32, &tx.sig.0))
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

async fn read_agent_msg<R: AsyncRead + Unpin>(reader: &mut R) -> Result<AgentToPop> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    Ok(decode_envelope::<AgentToPop>(&payload)?)
}

async fn write_pop_msg<W: AsyncWrite + Unpin>(writer: &mut W, msg: &PopToAgent) -> Result<()> {
    let payload = encode_envelope(msg)?;
    let len = u32::try_from(payload.len()).context("frame too large")?;
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(&payload).await?;
    writer.flush().await?;
    Ok(())
}

async fn drain_stream(mut recv: quinn::RecvStream) {
    loop {
        if read_agent_msg(&mut recv).await.is_err() {
            break;
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_secs(0))
        .as_millis() as u64
}

async fn fetch_latest_blockhash(client: &Client, rpc_url: &str) -> Result<Hash> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getLatestBlockhash",
        "params": []
    });
    let resp = client
        .post(rpc_url)
        .json(&request)
        .send()
        .await?
        .error_for_status()?;
    let value: serde_json::Value = resp.json().await?;
    let hash_str = value
        .get("result")
        .and_then(|v| v.get("value"))
        .and_then(|v| v.get("blockhash"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing blockhash in RPC response"))?;
    Hash::from_str(hash_str).map_err(|err| anyhow!("invalid blockhash: {err}"))
}

async fn fetch_current_slot(client: &Client, rpc_url: &str) -> Result<u64> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlot",
        "params": []
    });
    let resp = client
        .post(rpc_url)
        .json(&request)
        .send()
        .await?
        .error_for_status()?;
    let value: serde_json::Value = resp.json().await?;
    let slot = value
        .get("result")
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing slot in RPC response"))?;
    Ok(slot)
}

async fn fetch_slot_leader(client: &Client, rpc_url: &str, slot: u64) -> Result<PubkeyBytes> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getSlotLeaders",
        "params": [slot, 1]
    });
    let resp = client
        .post(rpc_url)
        .json(&request)
        .send()
        .await?
        .error_for_status()?;
    let value: serde_json::Value = resp.json().await?;
    let leader = value
        .get("result")
        .and_then(|v| v.as_array())
        .and_then(|v| v.get(0))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("missing leader pubkey in RPC response"))?;
    let leader = Pubkey::from_str(leader).map_err(|e| anyhow!("invalid leader pubkey: {e}"))?;
    Ok(PubkeyBytes(leader.to_bytes()))
}

async fn request_airdrop_and_wait(
    client: &Client,
    rpc_url: &str,
    pubkey: Pubkey,
    lamports: u64,
) -> Result<()> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "requestAirdrop",
        "params": [pubkey.to_string(), lamports]
    });
    let resp = client
        .post(rpc_url)
        .json(&request)
        .send()
        .await?
        .error_for_status()?;
    let value: serde_json::Value = resp.json().await?;
    let sig = value
        .get("result")
        .and_then(|v| v.as_str())
        .unwrap_or("<unknown>");
    eprintln!("solanacdn-pop-stub: requested airdrop sig={sig}");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if std::time::Instant::now() >= deadline {
            bail!("timed out waiting for airdrop balance");
        }
        match fetch_balance(client, rpc_url, pubkey).await {
            Ok(balance) if balance > 0 => return Ok(()),
            Ok(_) => {}
            Err(err) => eprintln!("solanacdn-pop-stub: getBalance error: {err:#}"),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

async fn fetch_balance(client: &Client, rpc_url: &str, pubkey: Pubkey) -> Result<u64> {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getBalance",
        "params": [pubkey.to_string()]
    });
    let resp = client
        .post(rpc_url)
        .json(&request)
        .send()
        .await?
        .error_for_status()?;
    let value: serde_json::Value = resp.json().await?;
    let balance = value
        .get("result")
        .and_then(|v| v.get("value"))
        .and_then(|v| v.as_u64())
        .ok_or_else(|| anyhow!("missing balance in RPC response"))?;
    Ok(balance)
}

fn make_server_config() -> Result<ServerConfig> {
    let keypair = Keypair::new();
    let (cert, key) = new_dummy_x509_certificate(&keypair);
    let server_tls_config =
        rustls::ServerConfig::builder_with_provider(Arc::new(crypto_provider()))
            .with_safe_default_protocol_versions()?
            .with_no_client_auth()
            .with_single_cert(vec![cert], key)?;
    let quic_config = QuicServerConfig::try_from(server_tls_config)?;
    Ok(ServerConfig::with_crypto(Arc::new(quic_config)))
}
