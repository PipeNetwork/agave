use {
    anyhow::{anyhow, bail, Context, Result},
    quinn::{
        crypto::rustls::QuicServerConfig,
        Endpoint, ServerConfig,
    },
    rand::random,
    reqwest::Client,
    serde_json::json,
    solanacdn_protocol::{
        crypto::{PubkeyBytes, random_nonce_16, SignatureBytes},
        frame::{decode_envelope, encode_envelope},
        messages::{
            AgentToPop, AuthOk, FairBatch, FairBatchAttestation, FairBatchAttestationPayload, FairTx, PopToAgent,
        },
    },
    solana_compute_budget_interface::ComputeBudgetInstruction,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_message::Message,
    solana_signer::Signer,
    solana_sha256_hasher as sha256_hasher,
    solana_tls_utils::{crypto_provider, new_dummy_x509_certificate},
    solana_transaction::{versioned::VersionedTransaction, Transaction},
    std::{
        net::SocketAddr,
        str::FromStr,
        sync::Arc,
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
        sync::mpsc,
    },
};

#[derive(Clone, Debug)]
struct Config {
    listen: SocketAddr,
    batches: u64,
    txs_per_batch: usize,
    interval_ms: u64,
    exit_after_ms: u64,
    origin_pop_id: String,
    target_slot: Option<u64>,
    rpc_url: Option<String>,
    echo_commits: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let cfg = parse_args()?;
    eprintln!(
        "solanacdn-pop-stub: listen={}, batches={}, txs_per_batch={}, interval_ms={}, exit_after_ms={}",
        cfg.listen, cfg.batches, cfg.txs_per_batch, cfg.interval_ms, cfg.exit_after_ms
    );
    eprintln!(
        "solanacdn-pop-stub: start validator with --solanacdn-pop {} --solanacdn-tls-insecure-skip-verify --fair",
        cfg.listen
    );

    let server_config = make_server_config()?;
    let endpoint = Endpoint::server(server_config, cfg.listen)?;

    if cfg.exit_after_ms > 0 {
        let endpoint = endpoint.clone();
        let exit_after_ms = cfg.exit_after_ms;
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(exit_after_ms)).await;
            endpoint.close(0u32.into(), b"done");
        });
    }

    loop {
        let incoming = endpoint.accept().await;
        let Some(connecting) = incoming else {
            break;
        };
        let cfg = cfg.clone();
        tokio::spawn(async move {
            match connecting.await {
                Ok(conn) => {
                    if let Err(err) = handle_connection(conn, cfg).await {
                        eprintln!("solanacdn-pop-stub: connection error: {err:#}");
                    }
                }
                Err(err) => eprintln!("solanacdn-pop-stub: failed to accept connection: {err}"),
            }
        });
    }

    Ok(())
}

fn usage() -> &'static str {
    "solanacdn-pop-stub [--listen HOST:PORT] [--batches N] [--txs-per-batch N] [--interval-ms MS] [--exit-after-ms MS] [--origin-pop-id ID] [--target-slot SLOT] [--rpc-url URL] [--echo-commits]"
}

fn parse_args() -> Result<Config> {
    let mut cfg = Config {
        listen: "127.0.0.1:9002".parse().expect("default listen"),
        batches: 1,
        txs_per_batch: 1,
        interval_ms: 500,
        exit_after_ms: 1_000,
        origin_pop_id: "local-pop".to_string(),
        target_slot: None,
        rpc_url: None,
        echo_commits: false,
    };

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                let value = next_arg(&mut args, "--listen")?;
                cfg.listen = value.parse().with_context(|| format!("invalid --listen: {value}"))?;
            }
            "--batches" => {
                let value = next_arg(&mut args, "--batches")?;
                cfg.batches = value.parse().with_context(|| format!("invalid --batches: {value}"))?;
            }
            "--txs-per-batch" => {
                let value = next_arg(&mut args, "--txs-per-batch")?;
                cfg.txs_per_batch = value
                    .parse()
                    .with_context(|| format!("invalid --txs-per-batch: {value}"))?;
            }
            "--interval-ms" => {
                let value = next_arg(&mut args, "--interval-ms")?;
                cfg.interval_ms =
                    value.parse().with_context(|| format!("invalid --interval-ms: {value}"))?;
            }
            "--exit-after-ms" => {
                let value = next_arg(&mut args, "--exit-after-ms")?;
                cfg.exit_after_ms =
                    value.parse().with_context(|| format!("invalid --exit-after-ms: {value}"))?;
            }
            "--origin-pop-id" => {
                cfg.origin_pop_id = next_arg(&mut args, "--origin-pop-id")?;
            }
            "--target-slot" => {
                let value = next_arg(&mut args, "--target-slot")?;
                cfg.target_slot =
                    Some(value.parse().with_context(|| format!("invalid --target-slot: {value}"))?);
            }
            "--rpc-url" => {
                cfg.rpc_url = Some(next_arg(&mut args, "--rpc-url")?);
            }
            "--echo-commits" => {
                cfg.echo_commits = true;
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

    Ok(cfg)
}

fn next_arg<I: Iterator<Item = String>>(args: &mut I, name: &str) -> Result<String> {
    args.next()
        .ok_or_else(|| anyhow::anyhow!("missing value for {name}"))
}

async fn handle_connection(conn: quinn::Connection, cfg: Config) -> Result<()> {
    loop {
        let (send, mut recv) = match conn.accept_bi().await {
            Ok(stream) => stream,
            Err(err) => {
                eprintln!("solanacdn-pop-stub: accept_bi ended: {err}");
                return Ok(());
            }
        };

        let msg = match read_agent_msg(&mut recv).await {
            Ok(msg) => msg,
            Err(err) => {
                eprintln!("solanacdn-pop-stub: failed to read first msg: {err:#}");
                continue;
            }
        };

        match msg {
            AgentToPop::Auth(_) | AgentToPop::AuthWithSessionToken(_) => {
                eprintln!("solanacdn-pop-stub: received auth");
                let drain_conn = conn.clone();
                let drain_task = tokio::spawn(async move {
                    loop {
                        let stream = drain_conn.accept_bi().await;
                        let Ok((_send, recv)) = stream else {
                            break;
                        };
                        tokio::spawn(async move {
                            drain_stream(recv).await;
                        });
                    }
                });

                let res = handle_control_stream(conn.clone(), send, recv, cfg.clone()).await;
                let _ = drain_task.await;
                return res;
            }
            other => {
                eprintln!("solanacdn-pop-stub: ignoring stream message: {other:?}");
                tokio::spawn(async move {
                    drain_stream(recv).await;
                });
            }
        }
    }
}

async fn handle_control_stream(
    conn: quinn::Connection,
    mut send: quinn::SendStream,
    mut recv: quinn::RecvStream,
    cfg: Config,
) -> Result<()> {
    let (out_tx, mut out_rx) = mpsc::channel::<PopToAgent>(64);
    let writer = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if write_pop_msg(&mut send, &msg).await.is_err() {
                return;
            }
        }
    });

    let pop_signing_key = {
        let mut rng = rand::rngs::OsRng;
        ed25519_dalek_v2::SigningKey::generate(&mut rng)
    };
    let pop_pubkey = PubkeyBytes(pop_signing_key.verifying_key().to_bytes());

    let auth_ok = AuthOk {
        pop_id: cfg.origin_pop_id.clone(),
        pop_pubkey,
        server_time_ms: now_ms(),
        udp_token: random_nonce_16(),
        udp_shreds_port: 0,
        udp_votes_port: 0,
    };
    if out_tx.send(PopToAgent::AuthOk(auth_ok)).await.is_err() {
        return Ok(());
    }

    let read_task = {
        let out_tx = out_tx.clone();
        let echo_commits = cfg.echo_commits;
        tokio::spawn(async move {
            loop {
                match read_agent_msg(&mut recv).await {
                    Ok(msg) => match msg {
                        AgentToPop::FairBatchCommit(commit) => {
                            eprintln!(
                                "solanacdn-pop-stub: received FairBatchCommit batch_id={} order_start={} txs={}",
                                commit.payload.batch_id,
                                commit.payload.order_start,
                                commit.payload.tx_sigs.len()
                            );
                            if echo_commits {
                                let _ = out_tx
                                    .send(PopToAgent::FairBatchCommit(commit))
                                    .await;
                            }
                        }
                        AgentToPop::Heartbeat(_) => {}
                        AgentToPop::Capabilities(cap) => {
                            eprintln!(
                                "solanacdn-pop-stub: capabilities tx_fair_ordering={}",
                                cap.tx_fair_ordering
                            );
                        }
                        other => {
                            eprintln!("solanacdn-pop-stub: ctrl msg {other:?}");
                        }
                    },
                    Err(_) => break,
                }
            }
        })
    };

    let rpc_client = cfg.rpc_url.as_ref().map(|_| Client::new());

    let mut batches_sent: u64 = 0;
    let mut tx_seq_start: u64 = 0;
    loop {
        if cfg.batches != 0 && batches_sent >= cfg.batches {
            break;
        }
        let batch_id = random::<u128>();
        let blockhash = if let (Some(url), Some(client)) =
            (cfg.rpc_url.as_ref(), rpc_client.as_ref())
        {
            match fetch_latest_blockhash(client, url).await {
                Ok(hash) => hash,
                Err(err) => {
                    eprintln!(
                        "solanacdn-pop-stub: failed to fetch blockhash from {url}: {err:#}"
                    );
                    Hash::new_unique()
                }
            }
        } else {
            Hash::new_unique()
        };
        let batch = build_fair_batch(&cfg, batch_id, tx_seq_start, blockhash, &pop_signing_key);
        if out_tx.send(PopToAgent::FairBatch(batch)).await.is_err() {
            break;
        }
        batches_sent = batches_sent.saturating_add(1);
        tx_seq_start = tx_seq_start.saturating_add(cfg.txs_per_batch as u64);
        if cfg.batches != 0 && batches_sent >= cfg.batches {
            break;
        }
        tokio::time::sleep(Duration::from_millis(cfg.interval_ms)).await;
    }

    if cfg.exit_after_ms > 0 {
        tokio::time::sleep(Duration::from_millis(cfg.exit_after_ms)).await;
        conn.close(0u32.into(), b"done");
        drop(out_tx);
    }

    let _ = read_task.await;
    let _ = writer.await;
    Ok(())
}

fn build_fair_batch(
    cfg: &Config,
    batch_id: u128,
    tx_seq_start: u64,
    recent_blockhash: Hash,
    pop_signing_key: &ed25519_dalek_v2::SigningKey,
) -> FairBatch {
    let signer = Keypair::new();
    let mut txs = Vec::with_capacity(cfg.txs_per_batch);

    for idx in 0..cfg.txs_per_batch {
        let ix = ComputeBudgetInstruction::set_compute_unit_limit((idx as u32).saturating_add(1));
        let message = Message::new(&[ix], Some(&signer.pubkey()));
        let tx = Transaction::new(&[&signer], message, recent_blockhash);
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
        flow_id: 0,
        batch_id,
        tx_seq_start,
        tx_count,
        tx_merkle_root,
        created_at_ms,
        batch_ms,
        target_slot: cfg.target_slot,
    };
    let attestation = FairBatchAttestation::sign(attestation_payload, pop_signing_key)
        .unwrap_or_else(|e| panic!("failed to sign FairBatchAttestation: {e}"));

    FairBatch {
        origin_pop_id: cfg.origin_pop_id.clone(),
        flow_id: 0,
        batch_id,
        tx_seq_start,
        created_at_ms,
        batch_ms,
        target_slot: cfg.target_slot,
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

fn make_server_config() -> Result<ServerConfig> {
    let keypair = Keypair::new();
    let (cert, key) = new_dummy_x509_certificate(&keypair);
    let server_tls_config = rustls::ServerConfig::builder_with_provider(Arc::new(crypto_provider()))
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    let quic_config = QuicServerConfig::try_from(server_tls_config)?;
    Ok(ServerConfig::with_crypto(Arc::new(quic_config)))
}
