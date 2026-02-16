use {
    anyhow::{bail, Context, Result},
    quinn::{Endpoint, ServerConfig},
    rand::random,
    solanacdn_protocol::{
        crypto::{random_nonce_16, SignatureBytes},
        frame::{decode_envelope, encode_envelope},
        messages::{AgentToPop, AuthOk, FairBatch, FairTx, PopToAgent},
    },
    solana_compute_budget_interface::ComputeBudgetInstruction,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_message::Message,
    solana_signer::Signer,
    solana_transaction::{versioned::VersionedTransaction, Transaction},
    std::{
        net::SocketAddr,
        sync::{Arc, Once},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
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

    loop {
        let incoming = endpoint.accept().await;
        let Some(connecting) = incoming else {
            continue;
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
}

fn usage() -> &'static str {
    "solanacdn-pop-stub [--listen HOST:PORT] [--batches N] [--txs-per-batch N] [--interval-ms MS] [--exit-after-ms MS] [--origin-pop-id ID] [--target-slot SLOT]"
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
        let (mut send, mut recv) = match conn.accept_bi().await {
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
                handle_control_stream(&mut send, recv, cfg.clone()).await?;
                return Ok(());
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
    send: &mut quinn::SendStream,
    mut recv: quinn::RecvStream,
    cfg: Config,
) -> Result<()> {
    let auth_ok = AuthOk {
        pop_id: cfg.origin_pop_id.clone(),
        server_time_ms: now_ms(),
        udp_token: random_nonce_16(),
        udp_shreds_port: 0,
        udp_votes_port: 0,
    };
    write_pop_msg(send, &PopToAgent::AuthOk(auth_ok)).await?;

    let read_task = tokio::spawn(async move {
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
    });

    let mut batches_sent: u64 = 0;
    loop {
        if cfg.batches != 0 && batches_sent >= cfg.batches {
            break;
        }
        let batch_id = random::<u128>();
        let batch = build_fair_batch(&cfg, batch_id);
        write_pop_msg(send, &PopToAgent::FairBatch(batch)).await?;
        batches_sent = batches_sent.saturating_add(1);
        if cfg.batches != 0 && batches_sent >= cfg.batches {
            break;
        }
        tokio::time::sleep(Duration::from_millis(cfg.interval_ms)).await;
    }

    if cfg.exit_after_ms > 0 {
        tokio::time::sleep(Duration::from_millis(cfg.exit_after_ms)).await;
    }

    let _ = read_task.await;
    Ok(())
}

fn build_fair_batch(cfg: &Config, batch_id: u128) -> FairBatch {
    let signer = Keypair::new();
    let recent_blockhash = Hash::new_unique();
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

    FairBatch {
        origin_pop_id: cfg.origin_pop_id.clone(),
        batch_id,
        created_at_ms: now_ms(),
        batch_ms: 50,
        target_slot: cfg.target_slot,
        txs,
    }
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

fn make_server_config() -> Result<ServerConfig> {
    init_rustls();
    let keypair = Keypair::new();
    let (cert, key) = new_dummy_x509_certificate(&keypair);
    let tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)?;
    let quic = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
        .map_err(|_| rustls::Error::InvalidCertificate(rustls::CertificateError::BadSignature))?;
    Ok(ServerConfig::with_crypto(Arc::new(quic)))
}

fn init_rustls() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

fn new_dummy_x509_certificate(
    keypair: &Keypair,
) -> (
    rustls::pki_types::CertificateDer<'static>,
    rustls::pki_types::PrivateKeyDer<'static>,
) {
    const PKCS8_PREFIX: [u8; 16] = [
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22,
        0x04, 0x20,
    ];

    let key_pkcs8_der = {
        let keypair_secret_bytes = keypair.secret_bytes();
        let keypair_secret_len = keypair_secret_bytes.len();
        if keypair_secret_len != 32 {
            panic!("Unexpected secret key length!");
        }
        let buffer_size = PKCS8_PREFIX
            .len()
            .checked_add(keypair_secret_len)
            .expect("Unexpected secret key length!");
        let mut key_pkcs8_der = Vec::<u8>::with_capacity(buffer_size);
        key_pkcs8_der.extend_from_slice(&PKCS8_PREFIX);
        key_pkcs8_der.extend_from_slice(keypair_secret_bytes);
        key_pkcs8_der
    };

    let mut cert_der = Vec::<u8>::with_capacity(0xf4);
    cert_der.extend_from_slice(&[
        0x30, 0x81, 0xf6, 0x30, 0x81, 0xa9, 0xa0, 0x03, 0x02, 0x01, 0x02, 0x02, 0x08, 0x01,
        0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70,
        0x30, 0x16, 0x31, 0x14, 0x30, 0x12, 0x06, 0x03, 0x55, 0x04, 0x03, 0x0c, 0x0b, 0x53,
        0x6f, 0x6c, 0x61, 0x6e, 0x61, 0x20, 0x6e, 0x6f, 0x64, 0x65, 0x30, 0x20, 0x17, 0x0d,
        0x37, 0x30, 0x30, 0x31, 0x30, 0x31, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x5a, 0x18,
        0x0f, 0x34, 0x30, 0x39, 0x36, 0x30, 0x31, 0x30, 0x31, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x30, 0x5a, 0x30, 0x00, 0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03,
        0x21, 0x00,
    ]);
    cert_der.extend_from_slice(&keypair.pubkey().to_bytes());
    cert_der.extend_from_slice(&[
        0xa3, 0x29, 0x30, 0x27, 0x30, 0x17, 0x06, 0x03, 0x55, 0x1d, 0x11, 0x01, 0x01, 0xff,
        0x04, 0x0d, 0x30, 0x0b, 0x82, 0x09, 0x6c, 0x6f, 0x63, 0x61, 0x6c, 0x68, 0x6f, 0x73,
        0x74, 0x30, 0x0c, 0x06, 0x03, 0x55, 0x1d, 0x13, 0x01, 0x01, 0xff, 0x04, 0x02, 0x30,
        0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x41, 0x00, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        0xff, 0xff, 0xff,
    ]);

    (
        rustls::pki_types::CertificateDer::from(cert_der),
        rustls::pki_types::PrivateKeyDer::try_from(key_pkcs8_der).expect("pkcs8"),
    )
}
