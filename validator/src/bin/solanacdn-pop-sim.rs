use {
    agave_logger::setup_with_default,
    clap::{App, Arg},
    log::{debug, info, warn},
    quinn::Endpoint,
    solana_keypair::Keypair,
    solana_tls_utils::new_dummy_x509_certificate,
    solanacdn_protocol::{
        crypto::random_nonce_16,
        frame::{DEFAULT_MAX_FRAME_BYTES, decode_envelope, encode_envelope},
        messages::{AgentToPop, AuthOk, PopHeartbeatAck, PopStatsSnapshot, PopToAgent},
    },
    std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
        sync::{RwLock, mpsc},
    },
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct McpDaKey {
    epoch: u64,
    slot: u64,
    lane_id: u8,
    checkpoint_ix: u16,
    checkpoint_id: [u8; 32],
}

struct SharedState {
    next_session_id: AtomicU64,
    sessions: RwLock<HashMap<u64, mpsc::UnboundedSender<PopToAgent>>>,
    da_requesters: RwLock<HashMap<McpDaKey, u64>>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis()
        .try_into()
        .unwrap_or(0)
}

async fn read_len_prefixed<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<Vec<u8>> {
    let mut header = [0u8; 4];
    reader.read_exact(&mut header).await?;
    let len_u32 = u32::from_be_bytes(header);
    let len: usize = usize::try_from(len_u32)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "frame length"))?;
    if len > DEFAULT_MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    let mut payload = vec![0u8; len];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

async fn write_len_prefixed<W: AsyncWrite + Unpin>(writer: &mut W, payload: &[u8]) -> std::io::Result<()> {
    let len: u32 = payload
        .len()
        .try_into()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "frame length"))?;
    if payload.len() > DEFAULT_MAX_FRAME_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "frame too large",
        ));
    }
    writer.write_all(&len.to_be_bytes()).await?;
    writer.write_all(payload).await?;
    Ok(())
}

async fn read_agent_msg<R: AsyncRead + Unpin>(reader: &mut R) -> std::io::Result<AgentToPop> {
    let bytes = read_len_prefixed(reader).await?;
    decode_envelope(&bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

async fn write_pop_msg<W: AsyncWrite + Unpin>(writer: &mut W, msg: &PopToAgent) -> std::io::Result<()> {
    let bytes =
        encode_envelope(msg).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    write_len_prefixed(writer, &bytes).await
}

async fn broadcast(state: &SharedState, msg: PopToAgent) {
    let sessions = state.sessions.read().await;
    for tx in sessions.values() {
        let _ = tx.send(msg.clone());
    }
}

async fn handle_ctrl_msg(
    state: &SharedState,
    session_id: u64,
    out_tx: &mpsc::UnboundedSender<PopToAgent>,
    msg: AgentToPop,
) {
    match msg {
        AgentToPop::Heartbeat(_hb) => {
            let ack = PopToAgent::HeartbeatAck(PopHeartbeatAck {
                pop_now_ms: now_ms(),
                stats: PopStatsSnapshot::default(),
                direct_shreds: None,
            });
            let _ = out_tx.send(ack);
        }
        AgentToPop::McpDaRequest(req) => {
            let key = McpDaKey {
                epoch: req.epoch,
                slot: req.slot,
                lane_id: req.lane_id,
                checkpoint_ix: req.checkpoint_ix,
                checkpoint_id: req.checkpoint_id,
            };
            {
                let mut reqs = state.da_requesters.write().await;
                reqs.insert(key, session_id);
            }
            broadcast(state, PopToAgent::McpDaRequest(req)).await;
        }
        AgentToPop::McpDaAttest(att) => {
            let key = McpDaKey {
                epoch: att.epoch,
                slot: att.slot,
                lane_id: att.lane_id,
                checkpoint_ix: att.checkpoint_ix,
                checkpoint_id: att.checkpoint_id,
            };
            let requester = {
                let reqs = state.da_requesters.read().await;
                reqs.get(&key).copied()
            };
            if let Some(requester_id) = requester {
                let sessions = state.sessions.read().await;
                if let Some(tx) = sessions.get(&requester_id) {
                    let _ = tx.send(PopToAgent::McpDaAttest(att));
                }
            } else {
                broadcast(state, PopToAgent::McpDaAttest(att)).await;
            }
        }
        _ => {}
    }
}

async fn handle_connection(
    endpoint_addr: SocketAddr,
    state: Arc<SharedState>,
    connecting: quinn::Incoming,
) {
    let conn = match connecting.await {
        Ok(v) => v,
        Err(e) => {
            debug!("pop-sim: connect failed: {e}");
            return;
        }
    };
    let peer = conn.remote_address();
    let session_id = state.next_session_id.fetch_add(1, Ordering::Relaxed);

    let (mut ctrl_send, mut ctrl_recv) = match conn.accept_bi().await {
        Ok(v) => v,
        Err(e) => {
            debug!("pop-sim: accept_bi(control) failed: {e}");
            return;
        }
    };

    match read_agent_msg(&mut ctrl_recv).await {
        Ok(AgentToPop::Auth(_)) | Ok(AgentToPop::AuthWithSessionToken(_)) => {}
        Ok(other) => {
            debug!("pop-sim: unexpected first ctrl msg from {peer}: {other:?}");
            return;
        }
        Err(e) => {
            debug!("pop-sim: failed reading auth from {peer}: {e}");
            return;
        }
    }

    // Insert session before responding so we can immediately broadcast MCP requests.
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<PopToAgent>();
    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(session_id, out_tx.clone());
    }

    let auth_ok = PopToAgent::AuthOk(AuthOk {
        pop_id: format!("pop-sim@{endpoint_addr}"),
        server_time_ms: now_ms(),
        udp_token: random_nonce_16(),
        udp_shreds_port: 0,
        udp_votes_port: 0,
    });
    if write_pop_msg(&mut ctrl_send, &auth_ok).await.is_err() {
        return;
    }

    // Drain any additional streams (shreds/votes), best-effort.
    let conn_for_extra = conn.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut _send, mut recv)) = conn_for_extra.accept_bi().await else {
                return;
            };
            let _ = read_agent_msg(&mut recv).await;
        }
    });

    info!("pop-sim: connected peer={peer} session_id={session_id}");

    let mut send_stream = ctrl_send;
    let writer_task = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            if write_pop_msg(&mut send_stream, &msg).await.is_err() {
                break;
            }
        }
    });

    loop {
        let msg = match read_agent_msg(&mut ctrl_recv).await {
            Ok(v) => v,
            Err(_) => break,
        };
        handle_ctrl_msg(state.as_ref(), session_id, &out_tx, msg).await;
    }

    writer_task.abort();

    {
        let mut sessions = state.sessions.write().await;
        sessions.remove(&session_id);
    }
    {
        let mut reqs = state.da_requesters.write().await;
        reqs.retain(|_k, v| *v != session_id);
    }

    info!("pop-sim: disconnected peer={peer} session_id={session_id}");
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    setup_with_default("info");

    let matches = App::new("solanacdn-pop-sim")
        .about("Local SolanaCDN POP simulator (MCP DA transport)")
        .arg(
            Arg::with_name("listen")
                .long("listen")
                .value_name("HOST:PORT")
                .takes_value(true)
                .default_value("127.0.0.1:10020")
                .help("QUIC listen address"),
        )
        .get_matches();

    let listen: SocketAddr = matches
        .value_of("listen")
        .unwrap_or("127.0.0.1:10020")
        .parse()
        .expect("valid --listen HOST:PORT");

    let pop_keypair = Keypair::new();
    let (cert, key) = new_dummy_x509_certificate(&pop_keypair);
    let server_cfg = quinn::ServerConfig::with_single_cert(vec![cert], key).expect("server config");
    let endpoint = Endpoint::server(server_cfg, listen).expect("bind QUIC endpoint");

    let addr = endpoint.local_addr().expect("local addr");
    info!("pop-sim: listening on {addr}");

    let state = Arc::new(SharedState {
        next_session_id: AtomicU64::new(1),
        sessions: RwLock::new(HashMap::new()),
        da_requesters: RwLock::new(HashMap::new()),
    });

    loop {
        let Some(connecting) = endpoint.accept().await else {
            warn!("pop-sim: endpoint accept stream ended");
            break;
        };
        let state = state.clone();
        tokio::spawn(handle_connection(addr, state, connecting));
    }
}
