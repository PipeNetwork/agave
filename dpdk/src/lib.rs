#![cfg_attr(
    not(feature = "agave-unstable-api"),
    deprecated(
        since = "3.1.0",
        note = "This crate has been marked for formal inclusion in the Agave Unstable API. From \
                v4.0.0 onward, the `agave-unstable-api` crate feature must be specified to \
                acknowledge use of an interface that may break without warning."
    )
)]
#![allow(clippy::arithmetic_side_effects)]

#[cfg(all(target_os = "linux", feature = "dpdk"))]
#[macro_use]
extern crate solana_metrics;

use std::{
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
};

use bytes::Bytes;
use crossbeam_channel::{Receiver, Sender};
use futures::task::AtomicWaker;
use quinn::{AsyncUdpSocket, UdpPoller};
#[cfg(all(target_os = "linux", feature = "dpdk"))]
use solana_packet::{Packet, PACKET_DATA_SIZE};
use solana_perf::packet::{PacketBatch, PacketBatchRecycler};
#[cfg(all(target_os = "linux", feature = "dpdk"))]
use solana_perf::packet::{PinnedPacketBatch, PACKETS_PER_BATCH};
use solana_streamer::streamer::{ChannelSend, StreamerReceiveStats};
use thiserror::Error;

#[derive(Clone, Debug)]
pub struct DpdkNetConfig {
    pub devargs: String,
    pub local_ip: Ipv4Addr,
    pub prefix_len: u8,
    pub gateway_ip: Option<Ipv4Addr>,
    /// Optional gateway MAC address to avoid ARP (useful on networks with restricted ARP).
    /// Requires `gateway_ip`.
    pub gateway_mac: Option<DpdkMacAddr>,
    /// Additional EAL args (excluding the implicit `-a <devargs>`).
    pub eal_args: Vec<String>,
    /// Time to wait for link-up during initialization.
    pub link_up_timeout_secs: u64,
    /// Optional CPU pinning for DPDK I/O threads. If provided, must include at least
    /// `io_threads` CPU IDs; queue `N` will be pinned to `io_thread_cpus[N]`.
    pub io_thread_cpus: Option<Vec<usize>>,
    /// Number of DPDK RX/TX queues (and I/O threads) to use.
    pub io_threads: u16,
    pub rx_desc: u16,
    pub tx_desc: u16,
    pub mbuf_count: u32,
    pub mbuf_data_size: u16,
    pub shred_tx_channel_cap: usize,
    pub quic_tx_channel_cap: usize,
    pub quic_rx_channel_cap: usize,
}

impl Default for DpdkNetConfig {
    fn default() -> Self {
        Self {
            devargs: String::new(),
            local_ip: Ipv4Addr::UNSPECIFIED,
            prefix_len: 32,
            gateway_ip: None,
            gateway_mac: None,
            eal_args: Vec::new(),
            link_up_timeout_secs: 10,
            io_thread_cpus: None,
            io_threads: 1,
            rx_desc: 1024,
            tx_desc: 1024,
            mbuf_count: 8192,
            // DPDK default mbuf buf size is typically 2176; this is a safe round number.
            mbuf_data_size: 2304,
            shred_tx_channel_cap: 1_000_000,
            quic_tx_channel_cap: 65_536,
            quic_rx_channel_cap: 65_536,
        }
    }
}

#[derive(Debug, Error, Clone)]
pub enum DpdkError {
    #[error("DPDK support not enabled (requires Linux + `agave-dpdk` crate feature `dpdk`)")]
    NotSupported,
    #[error("invalid DPDK config: {0}")]
    InvalidConfig(String),
    #[error("DPDK EAL init failed (rc={0})")]
    EalInitFailed(i32),
    #[error("no DPDK ports available")]
    NoPortsAvailable,
    #[error("DPDK port init failed (rc={0})")]
    PortInitFailed(i32),
}

#[derive(Clone, Copy, Debug)]
pub struct DpdkMacAddr(pub [u8; 6]);

impl std::fmt::Display for DpdkMacAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let [b0, b1, b2, b3, b4, b5] = self.0;
        write!(
            f,
            "{b0:02x}:{b1:02x}:{b2:02x}:{b3:02x}:{b4:02x}:{b5:02x}"
        )
    }
}

impl std::str::FromStr for DpdkMacAddr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        if s.is_empty() {
            return Err("mac address is empty".to_string());
        }

        let sep = if s.contains(':') {
            ':'
        } else if s.contains('-') {
            '-'
        } else {
            return Err("mac address must use ':' or '-' separators".to_string());
        };

        let mut bytes = [0u8; 6];
        let parts: Vec<&str> = s.split(sep).collect();
        if parts.len() != 6 {
            return Err(format!(
                "mac address must have 6 octets (got {})",
                parts.len()
            ));
        }
        for (i, part) in parts.into_iter().enumerate() {
            if part.len() != 2 {
                return Err(format!("mac octet must be 2 hex chars (got '{part}')"));
            }
            let v = u8::from_str_radix(part, 16)
                .map_err(|_| format!("invalid mac octet (not hex): '{part}'"))?;
            bytes[i] = v;
        }

        Ok(DpdkMacAddr(bytes))
    }
}

#[derive(Clone, Debug)]
pub struct DpdkProbeInfo {
    pub port_id: u16,
    pub port_name: String,
    pub mac: DpdkMacAddr,
    pub local_ip: Ipv4Addr,
    pub tx_queues: u16,
    pub link_up: bool,
    pub link_speed_mbps: u32,
}

#[derive(Clone)]
pub struct PacketRoute {
    pub dst_port: u16,
    pub sender: Arc<dyn ChannelSend<PacketBatch> + Sync>,
    pub recycler: PacketBatchRecycler,
    pub stats: Arc<StreamerReceiveStats>,
    pub in_vote_only_mode: Option<Arc<AtomicBool>>,
    pub is_staked_service: bool,
}

impl std::fmt::Debug for PacketRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketRoute")
            .field("dst_port", &self.dst_port)
            .field("stats_name", &self.stats.name)
            .field("is_staked_service", &self.is_staked_service)
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct DpdkUdpSender {
    src_port: u16,
    tx: Sender<TxUdpDatagram>,
}

#[derive(Debug, Error)]
pub enum DpdkSendError {
    #[error("dpdk tx channel full")]
    Full,
    #[error("dpdk tx channel disconnected")]
    Disconnected,
}

impl DpdkUdpSender {
    pub fn try_send_to(&self, dst: SocketAddrV4, payload: Bytes) -> Result<(), DpdkSendError> {
        self.tx
            .try_send(TxUdpDatagram {
                src_port: self.src_port,
                dst,
                payload,
            })
            .map_err(|e| match e {
                crossbeam_channel::TrySendError::Full(_) => DpdkSendError::Full,
                crossbeam_channel::TrySendError::Disconnected(_) => DpdkSendError::Disconnected,
            })
    }
}

#[cfg_attr(not(all(target_os = "linux", feature = "dpdk")), allow(dead_code))]
#[derive(Debug, Clone)]
struct TxUdpDatagram {
    src_port: u16,
    dst: SocketAddrV4,
    payload: Bytes,
}

#[cfg_attr(not(all(target_os = "linux", feature = "dpdk")), allow(dead_code))]
#[derive(Debug)]
struct RxUdpDatagram {
    src: SocketAddrV4,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct QuicTxQueue {
    tx: Sender<TxUdpDatagram>,
    cap: usize,
    connected: AtomicBool,
    waiters: Mutex<Vec<std::task::Waker>>,
}

impl QuicTxQueue {
    fn new(cap: usize) -> (Arc<Self>, Receiver<TxUdpDatagram>) {
        let (tx, rx) = crossbeam_channel::bounded(cap);
        (
            Arc::new(Self {
                tx,
                cap,
                connected: AtomicBool::new(true),
                waiters: Mutex::new(Vec::new()),
            }),
            rx,
        )
    }

    fn try_send(
        &self,
        item: TxUdpDatagram,
    ) -> Result<(), crossbeam_channel::TrySendError<TxUdpDatagram>> {
        if !self.connected.load(Ordering::Relaxed) {
            return Err(crossbeam_channel::TrySendError::Disconnected(item));
        }
        self.tx.try_send(item)
    }

    fn poll_writable(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if !self.connected.load(Ordering::Relaxed) {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "dpdk tx queue disconnected",
            )));
        }

        if self.tx.len() < self.cap {
            return std::task::Poll::Ready(Ok(()));
        }

        let waker = cx.waker();
        {
            let mut waiters = self
                .waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if let Some(existing) = waiters.iter_mut().find(|w| w.will_wake(waker)) {
                *existing = waker.clone();
            } else {
                waiters.push(waker.clone());
            }
        }

        // Re-check after registering to avoid missed wake-ups.
        if !self.connected.load(Ordering::Relaxed) {
            return std::task::Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "dpdk tx queue disconnected",
            )));
        }
        if self.tx.len() < self.cap {
            return std::task::Poll::Ready(Ok(()));
        }
        std::task::Poll::Pending
    }

    fn wake_writers(&self) {
        let waiters = {
            let mut waiters = self
                .waiters
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            std::mem::take(&mut *waiters)
        };
        for w in waiters {
            w.wake();
        }
    }

    fn disconnect(&self) {
        self.connected.store(false, Ordering::Relaxed);
        self.wake_writers();
    }
}

pub struct DpdkQuicUdpSocket {
    local_addr: SocketAddr,
    tx: Arc<QuicTxQueue>,
    #[cfg_attr(not(all(target_os = "linux", feature = "dpdk")), allow(dead_code))]
    rx_sender: Sender<RxUdpDatagram>,
    rx_receiver: Receiver<RxUdpDatagram>,
    rx_buf_pool_tx: Sender<Vec<u8>>,
    rx_buf_pool_rx: Receiver<Vec<u8>>,
    rx_waker: AtomicWaker,
}

impl std::fmt::Debug for DpdkQuicUdpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DpdkQuicUdpSocket")
            .field("local_addr", &self.local_addr)
            .finish()
    }
}

impl DpdkQuicUdpSocket {
    #[cfg_attr(not(all(target_os = "linux", feature = "dpdk")), allow(dead_code))]
    fn enqueue(&self, src: SocketAddrV4, payload: &[u8]) -> bool {
        fn return_buf(pool: &Sender<Vec<u8>>, mut buf: Vec<u8>) {
            buf.clear();
            let _ = pool.try_send(buf);
        }

        let mut buf = match self.rx_buf_pool_rx.try_recv() {
            Ok(buf) => buf,
            Err(_) => Vec::with_capacity(payload.len()),
        };
        buf.clear();
        buf.extend_from_slice(payload);

        match self.rx_sender.try_send(RxUdpDatagram { src, payload: buf }) {
            Ok(()) => {
                self.rx_waker.wake();
                true
            }
            Err(crossbeam_channel::TrySendError::Full(item)) => {
                return_buf(&self.rx_buf_pool_tx, item.payload);
                false
            }
            Err(crossbeam_channel::TrySendError::Disconnected(item)) => {
                return_buf(&self.rx_buf_pool_tx, item.payload);
                false
            }
        }
    }
}

#[derive(Debug)]
struct QuicTxPoller {
    tx: Arc<QuicTxQueue>,
}

impl UdpPoller for QuicTxPoller {
    fn poll_writable(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        self.get_mut().tx.poll_writable(cx)
    }
}

impl AsyncUdpSocket for DpdkQuicUdpSocket {
    fn create_io_poller(self: Arc<Self>) -> std::pin::Pin<Box<dyn UdpPoller>> {
        Box::pin(QuicTxPoller { tx: self.tx.clone() })
    }

    fn try_send(&self, transmit: &quinn::udp::Transmit<'_>) -> std::io::Result<()> {
        if transmit.segment_size.is_some() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "DPDK QUIC socket does not support GSO",
            ));
        }
        let dst = match transmit.destination {
            SocketAddr::V4(v4) => v4,
            SocketAddr::V6(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "IPv6 not supported",
                ))
            }
        };
        // NOTE: Quinn may set src_ip; we currently ignore it and always use the interface IP.
        let _ = transmit.src_ip;

        self.tx
            .try_send(TxUdpDatagram {
                src_port: self.local_addr.port(),
                dst,
                payload: Bytes::copy_from_slice(transmit.contents),
            })
            .map_err(|e| match e {
                crossbeam_channel::TrySendError::Full(_) => {
                    std::io::Error::new(std::io::ErrorKind::WouldBlock, "dpdk tx queue full")
                }
                crossbeam_channel::TrySendError::Disconnected(_) => std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "dpdk tx queue disconnected",
                ),
            })?;
        Ok(())
    }

    fn poll_recv(
        &self,
        cx: &mut std::task::Context<'_>,
        bufs: &mut [std::io::IoSliceMut<'_>],
        meta: &mut [quinn::udp::RecvMeta],
    ) -> std::task::Poll<std::io::Result<usize>> {
        let mut count: usize = 0;
        while count < bufs.len() && count < meta.len() {
            match self.rx_receiver.try_recv() {
                Ok(datagram) => {
                    let len = datagram.payload.len();
                    let dst = &mut bufs[count];
                    if len > dst.len() {
                        // Drop oversized datagram.
                        let mut payload = datagram.payload;
                        payload.clear();
                        let _ = self.rx_buf_pool_tx.try_send(payload);
                        continue;
                    }
                    dst[..len].copy_from_slice(&datagram.payload);
                    meta[count].addr = SocketAddr::V4(datagram.src);
                    meta[count].len = len;
                    meta[count].stride = 0;
                    meta[count].ecn = None;
                    meta[count].dst_ip = Some(self.local_addr.ip());
                    let mut payload = datagram.payload;
                    payload.clear();
                    let _ = self.rx_buf_pool_tx.try_send(payload);
                    count += 1;
                }
                Err(crossbeam_channel::TryRecvError::Empty) => break,
                Err(crossbeam_channel::TryRecvError::Disconnected) => {
                    return std::task::Poll::Ready(Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "dpdk rx queue disconnected",
                    )))
                }
            }
        }
        if count > 0 {
            return std::task::Poll::Ready(Ok(count));
        }

        self.rx_waker.register(cx.waker());
        // Re-check after registering to avoid missed wake-ups.
        match self.rx_receiver.try_recv() {
            Ok(datagram) => {
                let len = datagram.payload.len();
                if bufs.is_empty() || meta.is_empty() || len > bufs[0].len() {
                    let mut payload = datagram.payload;
                    payload.clear();
                    let _ = self.rx_buf_pool_tx.try_send(payload);
                    return std::task::Poll::Ready(Ok(0));
                }
                bufs[0][..len].copy_from_slice(&datagram.payload);
                meta[0].addr = SocketAddr::V4(datagram.src);
                meta[0].len = len;
                meta[0].stride = 0;
                meta[0].ecn = None;
                meta[0].dst_ip = Some(self.local_addr.ip());
                let mut payload = datagram.payload;
                payload.clear();
                let _ = self.rx_buf_pool_tx.try_send(payload);
                std::task::Poll::Ready(Ok(1))
            }
            Err(crossbeam_channel::TryRecvError::Empty) => std::task::Poll::Pending,
            Err(crossbeam_channel::TryRecvError::Disconnected) => std::task::Poll::Ready(Err(
                std::io::Error::new(std::io::ErrorKind::BrokenPipe, "dpdk rx queue disconnected"),
            )),
        }
    }

    fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.local_addr)
    }

    fn may_fragment(&self) -> bool {
        false
    }
}

#[derive(Debug)]
pub struct Dpdk {
    local_ip: Ipv4Addr,
    local_mac: DpdkMacAddr,
    port_id: u16,
    port_name: String,
    tx_queues: u16,
    shred_tx: Sender<TxUdpDatagram>,
    stop: Arc<AtomicBool>,
    threads: Vec<JoinHandle<()>>,
}

impl Dpdk {
    pub fn local_ip(&self) -> Ipv4Addr {
        self.local_ip
    }

    pub fn local_mac(&self) -> DpdkMacAddr {
        self.local_mac
    }

    pub fn port_id(&self) -> u16 {
        self.port_id
    }

    pub fn port_name(&self) -> &str {
        &self.port_name
    }

    pub fn tx_queues(&self) -> u16 {
        self.tx_queues
    }

    pub fn udp_sender(&self, src_port: u16) -> DpdkUdpSender {
        DpdkUdpSender {
            src_port,
            tx: self.shred_tx.clone(),
        }
    }
}

impl Drop for Dpdk {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads.drain(..) {
            let _ = thread.join();
        }
    }
}

pub struct DpdkBuilder {
    config: DpdkNetConfig,
    routes: Vec<PacketRoute>,
    quic_ports: Vec<(u16, Vec<Arc<DpdkQuicUdpSocket>>)>,
    quic_tx: Arc<QuicTxQueue>,
    #[cfg_attr(not(all(target_os = "linux", feature = "dpdk")), allow(dead_code))]
    quic_tx_rx: Receiver<TxUdpDatagram>,
}

impl DpdkBuilder {
    pub fn new(config: DpdkNetConfig) -> Self {
        let (quic_tx, quic_tx_rx) = QuicTxQueue::new(config.quic_tx_channel_cap);
        Self {
            config,
            routes: Vec::new(),
            quic_ports: Vec::new(),
            quic_tx,
            quic_tx_rx,
        }
    }

    pub fn add_packet_route(mut self, route: PacketRoute) -> Self {
        self.routes.push(route);
        self
    }

    /// Returns `count` receivers, one per QUIC endpoint, and configures the
    /// DPDK RX loop to distribute datagrams for `dst_port` across them.
    pub fn add_quic_port(
        mut self,
        dst_port: u16,
        count: usize,
    ) -> (Self, Vec<Arc<DpdkQuicUdpSocket>>) {
        let mut sockets = Vec::with_capacity(count);
        for _ in 0..count {
            let (rx_sender, rx_receiver) =
                crossbeam_channel::bounded(self.config.quic_rx_channel_cap);
            let pool_cap = std::cmp::min(1024usize, self.config.quic_rx_channel_cap);
            let (rx_buf_pool_tx, rx_buf_pool_rx) = crossbeam_channel::bounded(pool_cap);
            sockets.push(Arc::new(DpdkQuicUdpSocket {
                local_addr: SocketAddr::V4(SocketAddrV4::new(self.config.local_ip, dst_port)),
                tx: self.quic_tx.clone(),
                rx_sender,
                rx_receiver,
                rx_buf_pool_tx,
                rx_buf_pool_rx,
                rx_waker: AtomicWaker::new(),
            }));
        }
        self.quic_ports.push((dst_port, sockets.clone()));
        (self, sockets)
    }

    pub fn build(self, exit: Arc<AtomicBool>) -> Result<Dpdk, DpdkError> {
        #[cfg(not(all(target_os = "linux", feature = "dpdk")))]
        {
            let _ = exit;
            self.quic_tx.disconnect();
            return Err(DpdkError::NotSupported);
        }

        #[cfg(all(target_os = "linux", feature = "dpdk"))]
        {
            struct DisconnectGuard {
                queue: Arc<QuicTxQueue>,
                armed: bool,
            }

            impl DisconnectGuard {
                fn new(queue: Arc<QuicTxQueue>) -> Self {
                    Self { queue, armed: true }
                }

                fn disarm(&mut self) {
                    self.armed = false;
                }
            }

            impl Drop for DisconnectGuard {
                fn drop(&mut self) {
                    if self.armed {
                        self.queue.disconnect();
                    }
                }
            }

            let mut disconnect_guard = DisconnectGuard::new(self.quic_tx.clone());

            let mut config = self.config.clone();

            if config.devargs.is_empty() {
                return Err(DpdkError::InvalidConfig(
                    "devargs must be provided".to_string(),
                ));
            }
            if config.local_ip.is_unspecified() {
                return Err(DpdkError::InvalidConfig(
                    "local_ip must be provided".to_string(),
                ));
            }
            if config.prefix_len > 32 {
                return Err(DpdkError::InvalidConfig(
                    "prefix_len must be <= 32".to_string(),
                ));
            }
            if config.gateway_ip.is_none() {
                if let Some((gw, source)) = infer_gateway_from_devargs(&config.devargs) {
                    log::info!("DPDK gateway_ip inferred as {gw} ({source})");
                    config.gateway_ip = Some(gw);
                }
            }
            if config.gateway_mac.is_some() && config.gateway_ip.is_none() {
                return Err(DpdkError::InvalidConfig(
                    "gateway_mac requires gateway_ip (set --experimental-dpdk-gateway or ensure it \
                     can be inferred from the devargs)"
                        .to_string(),
                ));
            }
            if config.gateway_ip.is_none() {
                log::warn!(
                    "DPDK gateway_ip not set; off-subnet peers will be unreachable. \
                     Consider --experimental-dpdk-gateway"
                );
            }
            if config.prefix_len >= 31 && config.gateway_ip.is_none() {
                return Err(DpdkError::InvalidConfig(
                    "gateway_ip must be provided when prefix_len is 31 or 32".to_string(),
                ));
            }
            if config.shred_tx_channel_cap == 0 {
                return Err(DpdkError::InvalidConfig(
                    "shred_tx_channel_cap must be > 0".to_string(),
                ));
            }
            if config.io_threads == 0 {
                return Err(DpdkError::InvalidConfig(
                    "io_threads must be > 0".to_string(),
                ));
            }
            if config.quic_tx_channel_cap == 0 {
                return Err(DpdkError::InvalidConfig(
                    "quic_tx_channel_cap must be > 0".to_string(),
                ));
            }
            if config.quic_rx_channel_cap == 0 {
                return Err(DpdkError::InvalidConfig(
                    "quic_rx_channel_cap must be > 0".to_string(),
                ));
            }
            if config.link_up_timeout_secs == 0 {
                return Err(DpdkError::InvalidConfig(
                    "link_up_timeout_secs must be > 0".to_string(),
                ));
            }
            if let Some(ref cpus) = config.io_thread_cpus {
                if cpus.len() < usize::from(config.io_threads) {
                    return Err(DpdkError::InvalidConfig(format!(
                        "io_thread_cpus must include at least {} CPU(s) (got {})",
                        config.io_threads,
                        cpus.len()
                    )));
                }
            }

            let rx_needed =
                u64::from(config.rx_desc).saturating_mul(u64::from(config.io_threads));
            // We request TX queues to match `io_threads` for better TX scaling; the NIC/PMD may
            // clamp the actual TX queue count.
            let tx_needed =
                u64::from(config.tx_desc).saturating_mul(u64::from(config.io_threads));
            let headroom = 2048u64;
            let min_mbuf_count = rx_needed
                .saturating_add(tx_needed)
                .saturating_add(headroom)
                .min(u64::from(u32::MAX));
            if u64::from(config.mbuf_count) < min_mbuf_count {
                log::warn!(
                    "DPDK mbuf_count ({}) may be too small (rx_desc={} io_threads={} tx_desc={}). \
                     Recommended >= {} to avoid RX starvation / port init failures; adjust via \
                     --experimental-dpdk-mbuf-count",
                    config.mbuf_count,
                    config.rx_desc,
                    config.io_threads,
                    config.tx_desc,
                    min_mbuf_count
                );
            }

            #[cfg(target_os = "linux")]
            {
                let conflicts = kernel_ipv4_conflicts(config.local_ip)
                    .map_err(DpdkError::InvalidConfig)?;
                if !conflicts.is_empty() {
                    return Err(DpdkError::InvalidConfig(format!(
                        "local_ip {} is already configured on kernel interface(s): {}; remove it \
                         to avoid ARP/route conflicts when using DPDK",
                        config.local_ip,
                        conflicts.join(", ")
                    )));
                }
            }

            #[cfg(target_os = "linux")]
            if let Some(pci_bdf) = device_name_from_devargs(&config.devargs)
                .and_then(normalize_pci_bdf)
            {
                if let Err(e) = validate_pci_driver_for_dpdk(&pci_bdf) {
                    return Err(DpdkError::InvalidConfig(e));
                }
            }

            let (shred_tx, shred_tx_rx) =
                crossbeam_channel::bounded(config.shred_tx_channel_cap);
            let stop = Arc::new(AtomicBool::new(false));

            let routes = self.routes.clone();
            let quic_ports = self.quic_ports.clone();
            let quic_tx = self.quic_tx.clone();
            let quic_tx_rx = self.quic_tx_rx;

            let (local_mac, local_ip, port_id, port_name, tx_queues) =
                unsafe { imp::init_and_get_addr(&config)? };
            unsafe {
                imp::wait_for_link_up(std::time::Duration::from_secs(
                    config.link_up_timeout_secs,
                ))?;
            }

            if tx_queues < config.io_threads {
                log::warn!(
                    "DPDK TX queue count clamped: requested {} (io_threads), using {} (max supported by NIC/PMD)",
                    config.io_threads,
                    tx_queues
                );
            } else if tx_queues > 1 {
                log::info!("DPDK TX queues enabled: {tx_queues}");
            }

            let io_counters = Arc::new(imp::DpdkIoCounters::default());

            let (arp_event_tx, arp_event_rx0) =
                crossbeam_channel::bounded::<imp::ArpEvent>(1024);

            // Fail fast if the configured gateway cannot be resolved via ARP. For most validator
            // traffic, especially public /31 and /32 deployments, this is a hard requirement.
            if let Some(gw) = config.gateway_ip {
                let (gw_mac, is_static) = if let Some(mac) = config.gateway_mac {
                    log::info!("DPDK gateway MAC configured: {gw} is at {mac}");
                    (mac, true)
                } else {
                    let timeout = std::time::Duration::from_secs(2);
                    let mac = unsafe {
                        imp::arp_probe(
                            local_mac,
                            local_ip,
                            gw,
                            config.io_threads,
                            timeout,
                            io_counters.as_ref(),
                        )
                    }?;
                    log::info!("DPDK gateway ARP: {gw} is at {mac}");
                    (mac, false)
                };
                // Seed the TX thread ARP cache so first packets don't get dropped while ARP warms.
                let _ = arp_event_tx.try_send(imp::ArpEvent::Rx {
                    opcode: 2,
                    sender_mac: gw_mac,
                    sender_ip: gw,
                    target_ip: local_ip,
                    is_static,
                });
            }
            let mut arp_event_rx0 = Some(arp_event_rx0);

            // When TX uses multiple queues/threads, ARP replies may arrive on any RX queue due to
            // RSS. Keep ARP handling centralized on queue 0, but broadcast resolved neighbors to
            // the other TX threads so they can transmit without re-ARPing.
            let mut arp_update_txs: Vec<Option<Sender<imp::ArpCacheUpdate>>> =
                vec![None; usize::from(tx_queues)];
            let mut arp_update_rxs: Vec<Option<Receiver<imp::ArpCacheUpdate>>> =
                vec![None; usize::from(tx_queues)];
            for q in 1..tx_queues {
                let (tx, rx) = crossbeam_channel::bounded::<imp::ArpCacheUpdate>(1024);
                arp_update_txs[usize::from(q)] = Some(tx);
                arp_update_rxs[usize::from(q)] = Some(rx);
            }
            let mut arp_update_txs0 = Some(arp_update_txs);

            let (io_start_tx, io_start_rx) = crossbeam_channel::bounded::<imp::IoThreadStart>(
                usize::from(config.io_threads),
            );

            let mut threads: Vec<JoinHandle<()>> =
                Vec::with_capacity(usize::from(config.io_threads));
            for queue_id in 0..config.io_threads {
                let arp_event_rx = if queue_id == 0 {
                    arp_event_rx0.take()
                } else {
                    None
                };
                let arp_update_txs = if queue_id == 0 {
                    arp_update_txs0.take()
                } else {
                    None
                };
                let arp_update_rx = if queue_id < tx_queues {
                    arp_update_rxs[usize::from(queue_id)].take()
                } else {
                    None
                };
                let thread = match std::thread::Builder::new()
                    .name(format!("agaveDpdkIo{queue_id}"))
                    .spawn({
                        let config = config.clone();
                        let routes = routes.clone();
                        let quic_ports = quic_ports.clone();
                        let shred_tx_rx = shred_tx_rx.clone();
                        let quic_tx = quic_tx.clone();
                        let quic_tx_rx = quic_tx_rx.clone();
                        let exit = exit.clone();
                        let stop = stop.clone();
                        let arp_event_tx = arp_event_tx.clone();
                        let io_start_tx = io_start_tx.clone();
                        let io_counters = io_counters.clone();
                        move || {
                            let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                                imp::run_dpdk_io_loop(
                                    config,
                                    routes,
                                    quic_ports,
                                    shred_tx_rx,
                                    quic_tx,
                                    quic_tx_rx,
                                    io_counters,
                                    exit.clone(),
                                    stop.clone(),
                                    queue_id,
                                    tx_queues,
                                    arp_event_tx,
                                    arp_event_rx,
                                    arp_update_txs,
                                    arp_update_rx,
                                    io_start_tx,
                                )
                            }));
                            match res {
                                Ok(Ok(())) => {}
                                Ok(Err(e)) => {
                                    log::error!(
                                        "dpdk io thread {queue_id} exited with error: {e}"
                                    );
                                    stop.store(true, Ordering::Relaxed);
                                    exit.store(true, Ordering::Relaxed);
                                }
                                Err(panic) => {
                                    let msg = if let Some(s) = panic.downcast_ref::<&str>() {
                                        (*s).to_string()
                                    } else if let Some(s) = panic.downcast_ref::<String>() {
                                        s.clone()
                                    } else {
                                        "unknown panic".to_string()
                                    };
                                    log::error!("dpdk io thread {queue_id} panicked: {msg}");
                                    stop.store(true, Ordering::Relaxed);
                                    exit.store(true, Ordering::Relaxed);
                                }
                            }
                        }
                    })
                {
                    Ok(thread) => thread,
                    Err(e) => {
                        stop.store(true, Ordering::Relaxed);
                        for thread in threads.drain(..) {
                            let _ = thread.join();
                        }
                        return Err(DpdkError::InvalidConfig(format!(
                            "failed to spawn dpdk io thread {queue_id}: {e}"
                        )));
                    }
                };
                threads.push(thread);
            }

            drop(io_start_tx);
            let start_timeout = std::time::Duration::from_secs(5);
            let mut started: usize = 0;
            while started < usize::from(config.io_threads) {
                match io_start_rx.recv_timeout(start_timeout) {
                    Ok(imp::IoThreadStart::Ready(_queue_id)) => {
                        started += 1;
                    }
                    Ok(imp::IoThreadStart::Error(queue_id, err)) => {
                        stop.store(true, Ordering::Relaxed);
                        for thread in threads.drain(..) {
                            let _ = thread.join();
                        }
                        return Err(DpdkError::InvalidConfig(format!(
                            "dpdk io thread {queue_id} failed to start: {err}"
                        )));
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        stop.store(true, Ordering::Relaxed);
                        for thread in threads.drain(..) {
                            let _ = thread.join();
                        }
                        return Err(DpdkError::InvalidConfig(format!(
                            "timed out waiting for {}/{} dpdk io threads to start",
                            started,
                            config.io_threads
                        )));
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                        stop.store(true, Ordering::Relaxed);
                        for thread in threads.drain(..) {
                            let _ = thread.join();
                        }
                        return Err(DpdkError::InvalidConfig(
                            "dpdk io thread startup channel disconnected".to_string(),
                        ));
                    }
                }
            }

            disconnect_guard.disarm();
            Ok(Dpdk {
                local_ip,
                local_mac,
                port_id,
                port_name,
                tx_queues,
                shred_tx,
                stop,
                threads,
            })
        }
    }
}

pub fn probe(config: &DpdkNetConfig) -> Result<DpdkProbeInfo, DpdkError> {
    #[cfg(not(all(target_os = "linux", feature = "dpdk")))]
    {
        let _ = config;
        Err(DpdkError::NotSupported)
    }

    #[cfg(all(target_os = "linux", feature = "dpdk"))]
    unsafe {
        imp::probe(config)
    }
}

pub fn infer_gateway_from_devargs(devargs: &str) -> Option<(Ipv4Addr, String)> {
    #[cfg(all(target_os = "linux", feature = "dpdk"))]
    {
        infer_gateway_from_devargs_inner(devargs)
    }
    #[cfg(not(all(target_os = "linux", feature = "dpdk")))]
    {
        let _ = devargs;
        None
    }
}

#[cfg(any(test, all(target_os = "linux", feature = "dpdk")))]
fn device_name_from_devargs(devargs: &str) -> Option<&str> {
    let first = devargs.split(',').next()?.trim();
    if first.is_empty() {
        return None;
    }
    Some(first.strip_prefix("pci:").unwrap_or(first))
}

#[cfg(all(target_os = "linux", feature = "dpdk"))]
fn infer_gateway_from_devargs_inner(devargs: &str) -> Option<(Ipv4Addr, String)> {
    let pci_bdf = device_name_from_devargs(devargs).and_then(normalize_pci_bdf)?;
    infer_gateway_from_pci_bdf(&pci_bdf)
}

#[cfg(all(target_os = "linux", feature = "dpdk"))]
fn infer_gateway_from_pci_bdf(pci_bdf: &str) -> Option<(Ipv4Addr, String)> {
    fn gateway_for_iface(iface: &str) -> Option<(Ipv4Addr, u32)> {
        let contents = std::fs::read_to_string("/proc/net/route").ok()?;
        parse_proc_net_route_default_gateway(&contents, iface)
    }

    fn net_ifaces_for_pci(pci_bdf: &str) -> Vec<String> {
        let net_dir = std::path::Path::new("/sys/bus/pci/devices")
            .join(pci_bdf)
            .join("net");
        let Ok(rd) = std::fs::read_dir(net_dir) else {
            return Vec::new();
        };
        let mut out: Vec<String> = rd
            .filter_map(|e| {
                let e = e.ok()?;
                e.file_name().to_str().map(|s| s.to_string())
            })
            .collect();
        out.sort();
        out
    }

    fn physfn_pci(pci_bdf: &str) -> Option<String> {
        let physfn = std::path::Path::new("/sys/bus/pci/devices")
            .join(pci_bdf)
            .join("physfn");
        let target = std::fs::read_link(physfn).ok()?;
        target
            .file_name()
            .and_then(|s| s.to_str())
            .map(|s| s.to_string())
    }

    // If this is a VF, infer the gateway from the PF default route.
    let pf_bdf = physfn_pci(pci_bdf).unwrap_or_else(|| pci_bdf.to_string());
    let ifaces = net_ifaces_for_pci(&pf_bdf);

    let mut best: Option<(Ipv4Addr, u32, String)> = None;
    for iface in ifaces {
        let Some((gw, metric)) = gateway_for_iface(&iface) else {
            continue;
        };
        let pick = match best {
            None => true,
            Some((_, best_metric, _)) => metric < best_metric,
        };
        if pick {
            best = Some((
                gw,
                metric,
                format!("kernel default route via interface '{iface}' (PF {pf_bdf})"),
            ));
        }
    }
    best.map(|(gw, _metric, src)| (gw, src))
}

#[cfg(all(target_os = "linux", feature = "dpdk"))]
fn parse_proc_net_route_default_gateway(contents: &str, iface: &str) -> Option<(Ipv4Addr, u32)> {
    fn parse_hex_u32(s: &str) -> Option<u32> {
        if s.is_empty() || s.len() > 8 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return None;
        }
        u32::from_str_radix(s, 16).ok()
    }

    // Route flags from <linux/route.h>.
    const RTF_UP: u32 = 0x0001;

    let mut best: Option<(Ipv4Addr, u32)> = None;
    for (idx, line) in contents.lines().enumerate() {
        if idx == 0 {
            continue; // header
        }
        let mut it = line.split_whitespace();
        let Some(ifname) = it.next() else {
            continue;
        };
        if ifname != iface {
            continue;
        }
        let (Some(dst), Some(gw), Some(flags)) = (it.next(), it.next(), it.next()) else {
            continue;
        };
        // Skip RefCnt/Use
        let _refcnt = it.next();
        let _use = it.next();
        let Some(metric) = it.next() else {
            continue;
        };

        let dst = match parse_hex_u32(dst) {
            Some(v) => v,
            None => continue,
        };
        if dst != 0 {
            continue;
        }
        let Some(flags) = parse_hex_u32(flags) else {
            continue;
        };
        if (flags & RTF_UP) == 0 {
            continue;
        }
        let Some(gw) = parse_hex_u32(gw) else {
            continue;
        };
        if gw == 0 {
            continue;
        }
        let Ok(metric) = metric.parse::<u32>() else {
            continue;
        };
        let octets = gw.to_le_bytes();
        let gw_ip = Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3]);

        let pick = match best {
            None => true,
            Some((_, best_metric)) => metric < best_metric,
        };
        if pick {
            best = Some((gw_ip, metric));
        }
    }
    best
}

#[cfg(all(target_os = "linux", any(test, feature = "dpdk")))]
fn kernel_ipv4_conflicts(ip: Ipv4Addr) -> Result<Vec<String>, String> {
    use std::collections::HashSet;

    unsafe {
        let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut ifap as *mut *mut libc::ifaddrs) != 0 {
            return Err(std::io::Error::last_os_error().to_string());
        }
        struct FreeOnDrop(*mut libc::ifaddrs);
        impl Drop for FreeOnDrop {
            fn drop(&mut self) {
                unsafe { libc::freeifaddrs(self.0) };
            }
        }
        let _guard = FreeOnDrop(ifap);

        let mut ifnames: HashSet<String> = HashSet::new();
        let mut cur = ifap;
        while !cur.is_null() {
            let ifa = &*cur;
            if !ifa.ifa_addr.is_null() && (*ifa.ifa_addr).sa_family as i32 == libc::AF_INET {
                let addr = &*(ifa.ifa_addr as *const libc::sockaddr_in);
                let octets = addr.sin_addr.s_addr.to_ne_bytes();
                let addr_ip = Ipv4Addr::new(octets[0], octets[1], octets[2], octets[3]);
                if addr_ip == ip {
                    if !ifa.ifa_name.is_null() {
                        let name = std::ffi::CStr::from_ptr(ifa.ifa_name)
                            .to_string_lossy()
                            .into_owned();
                        ifnames.insert(name);
                    } else {
                        ifnames.insert("(unknown)".to_string());
                    }
                }
            }
            cur = ifa.ifa_next;
        }

        let mut out: Vec<String> = ifnames.into_iter().collect();
        out.sort();
        Ok(out)
    }
}

#[cfg(any(test, all(target_os = "linux", feature = "dpdk")))]
fn normalize_pci_bdf(pci: &str) -> Option<String> {
    fn is_hex(s: &str) -> bool {
        !s.is_empty() && s.bytes().all(|b| b.is_ascii_hexdigit())
    }

    let s = pci.trim();
    if s.is_empty() {
        return None;
    }
    let s = if s.len() == "01:00.0".len() && s.as_bytes().get(2) == Some(&b':') {
        format!("0000:{s}")
    } else {
        s.to_string()
    };
    let mut parts = s.split(':');
    let domain = parts.next()?;
    let bus = parts.next()?;
    let devfn = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    let (dev, func) = devfn.split_once('.')?;
    if domain.len() != 4 || !is_hex(domain) || bus.len() != 2 || !is_hex(bus) || dev.len() != 2 {
        return None;
    }
    if !is_hex(dev) || func.len() != 1 || !is_hex(func) {
        return None;
    }
    Some(format!("{domain}:{bus}:{dev}.{func}"))
}

#[cfg(all(target_os = "linux", feature = "dpdk"))]
fn validate_pci_driver_for_dpdk(pci_bdf: &str) -> Result<(), String> {
    let sysfs_dev = format!("/sys/bus/pci/devices/{pci_bdf}");
    let driver_link = std::path::Path::new(&sysfs_dev).join("driver");
    let driver = match std::fs::read_link(&driver_link) {
        Ok(path) => path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("(unknown)")
            .to_string(),
        Err(_) => {
            return Err(format!(
                "PCI device {pci_bdf} has no kernel driver bound; bind it to vfio-pci (or a \
                 supported UIO PMD) before enabling DPDK"
            ));
        }
    };

    let ok = matches!(
        driver.as_str(),
        "vfio-pci" | "uio_pci_generic" | "igb_uio"
    );
    if !ok {
        return Err(format!(
            "PCI device {pci_bdf} is bound to '{driver}', not vfio/uio. Bind it to vfio-pci (or a \
             supported UIO PMD) before enabling DPDK"
        ));
    }

    if driver == "vfio-pci" {
        use std::fs::OpenOptions;

        let iommu_group = std::path::Path::new(&sysfs_dev).join("iommu_group");
        if !iommu_group.exists() {
            return Err(format!(
                "PCI device {pci_bdf} is bound to vfio-pci but has no iommu_group; ensure IOMMU \
                 is enabled in BIOS/kernel (intel_iommu=on or amd_iommu=on)"
            ));
        }

        let group_path = std::fs::canonicalize(&iommu_group).map_err(|e| {
            format!("failed to resolve iommu_group for PCI device {pci_bdf}: {e}")
        })?;
        let group_id = group_path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| format!("failed to parse iommu_group id for PCI device {pci_bdf}"))?;

        let vfio_group_dev = std::path::Path::new("/dev/vfio").join(group_id);
        if !vfio_group_dev.exists() {
            return Err(format!(
                "vfio-pci is bound but {vfio_group_dev:?} does not exist; ensure the vfio \
                 subsystem is loaded and you have permission to access VFIO"
            ));
        }
        OpenOptions::new()
            .read(true)
            .write(true)
            .open(&vfio_group_dev)
            .map_err(|e| {
                format!(
                    "cannot open {vfio_group_dev:?} ({e}). Run the validator as root or grant \
                     access to this VFIO group device (udev permissions / vfio group)."
                )
            })?;

        // For VFIO, all devices in the IOMMU group must be managed by vfio-pci. Fail fast with a
        // clear error if the group is not fully bound.
        let devices_dir = group_path.join("devices");
        if let Ok(entries) = std::fs::read_dir(&devices_dir) {
            for entry in entries.flatten() {
                let dev = entry.file_name().to_string_lossy().into_owned();
                if dev == pci_bdf {
                    continue;
                }
                let other_sysfs = std::path::Path::new("/sys/bus/pci/devices").join(&dev);
                let other_driver_link = other_sysfs.join("driver");
                let other_driver = match std::fs::read_link(&other_driver_link) {
                    Ok(path) => path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("(unknown)")
                        .to_string(),
                    Err(_) => "(none)".to_string(),
                };
                if other_driver != "vfio-pci" {
                    return Err(format!(
                        "PCI device {pci_bdf} is in IOMMU group {group_id}, but group member \
                         {dev} is bound to '{other_driver}'. For VFIO, all devices in the IOMMU \
                         group must be bound to vfio-pci (or move to a server/NIC with better \
                         IOMMU isolation)."
                    ));
                }
            }
        }
    }

    Ok(())
}

#[cfg(any(test, all(target_os = "linux", feature = "dpdk")))]
fn ethertype_and_l3_offset(packet: &[u8]) -> Option<(u16, usize)> {
    if packet.len() < 14 {
        return None;
    }

    let mut ethertype = u16::from_be_bytes([packet[12], packet[13]]);
    let mut offset: usize = 14;

    // Handle up to 2 VLAN tags (802.1Q / 802.1ad).
    for _ in 0..2 {
        if ethertype == 0x8100 || ethertype == 0x88a8 {
            if packet.len() < offset + 4 {
                return None;
            }
            ethertype = u16::from_be_bytes([packet[offset + 2], packet[offset + 3]]);
            offset = offset.checked_add(4)?;
            continue;
        }
        break;
    }

    Some((ethertype, offset))
}

#[cfg(all(target_os = "linux", feature = "dpdk"))]
mod imp {
    use super::*;
    use libc::{c_char, c_int};
    use std::{
        ffi::{CStr, CString},
        ptr,
        sync::{atomic::AtomicUsize, OnceLock},
        time::{Duration, Instant},
    };

    #[derive(Default)]
    pub(super) struct DpdkIoCounters {
        pub(super) quic_rx_enqueued: AtomicUsize,
        pub(super) quic_rx_dropped: AtomicUsize,
        pub(super) arp_event_dropped: AtomicUsize,
        pub(super) tx_mbuf_alloc_fail: AtomicUsize,
        pub(super) tx_mbuf_append_fail: AtomicUsize,
        pub(super) tx_build_fail: AtomicUsize,
        pub(super) tx_dropped_no_arp: AtomicUsize,
        pub(super) tx_dropped_no_gateway: AtomicUsize,
        pub(super) tx_burst_unsent: AtomicUsize,
    }

    impl DpdkIoCounters {
        pub(super) fn report(&self) {
            datapoint_info!(
                "dpdk_io",
                (
                    "quic_rx_enqueued",
                    self.quic_rx_enqueued.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "quic_rx_dropped",
                    self.quic_rx_dropped.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "arp_event_dropped",
                    self.arp_event_dropped.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "tx_mbuf_alloc_fail",
                    self.tx_mbuf_alloc_fail.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "tx_mbuf_append_fail",
                    self.tx_mbuf_append_fail.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "tx_build_fail",
                    self.tx_build_fail.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "tx_dropped_no_arp",
                    self.tx_dropped_no_arp.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "tx_dropped_no_gateway",
                    self.tx_dropped_no_gateway.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
                (
                    "tx_burst_unsent",
                    self.tx_burst_unsent.swap(0, Ordering::Relaxed) as i64,
                    i64
                ),
            );
        }
    }

    #[repr(C)]
    struct AgaveDpdkPort {
        _private: [u8; 0],
    }

    #[repr(C)]
    struct RteMbuf {
        _private: [u8; 0],
    }

    extern "C" {
        fn rte_eal_init(argc: c_int, argv: *mut *mut c_char) -> c_int;
        fn rte_eth_dev_count_avail() -> u16;
        fn rte_eth_dev_get_port_by_name(name: *const c_char, port_id: *mut u16) -> c_int;
        fn rte_eth_dev_get_name_by_port(port_id: u16, name: *mut c_char) -> c_int;
        fn rte_thread_register() -> c_int;
        fn rte_thread_unregister();

        fn agave_dpdk_port_open(
            port_id: u16,
            rx_queues: u16,
            tx_queues: u16,
            rx_desc: u16,
            tx_desc: u16,
            mbuf_count: u32,
            mbuf_data_size: u16,
            out_port: *mut *mut AgaveDpdkPort,
        ) -> c_int;
        fn agave_dpdk_port_get_mac(port: *const AgaveDpdkPort, out_mac: *mut u8);
        fn agave_dpdk_port_get_link(
            port: *const AgaveDpdkPort,
            out_up: *mut u8,
            out_speed_mbps: *mut u32,
        ) -> c_int;
        fn agave_dpdk_port_get_tx_queues(port: *const AgaveDpdkPort) -> u16;

        fn agave_dpdk_rx_burst(
            port: *const AgaveDpdkPort,
            queue_id: u16,
            rx_pkts: *mut *mut RteMbuf,
            nb_pkts: u16,
        ) -> u16;
        fn agave_dpdk_tx_burst(
            port: *const AgaveDpdkPort,
            queue_id: u16,
            tx_pkts: *mut *mut RteMbuf,
            nb_pkts: u16,
        ) -> u16;

        fn agave_dpdk_pktmbuf_alloc(port: *const AgaveDpdkPort) -> *mut RteMbuf;
        fn agave_dpdk_pktmbuf_free(m: *mut RteMbuf);
        fn agave_dpdk_pktmbuf_append(m: *mut RteMbuf, len: u16) -> *mut u8;
        fn agave_dpdk_pktmbuf_mtod(m: *const RteMbuf) -> *const u8;
        fn agave_dpdk_pktmbuf_pkt_len(m: *const RteMbuf) -> u32;
        fn agave_dpdk_pktmbuf_is_contiguous(m: *const RteMbuf) -> c_int;
        fn agave_dpdk_pktmbuf_linearize(m: *mut RteMbuf) -> c_int;
    }

    static EAL_INIT: OnceLock<Result<(), DpdkError>> = OnceLock::new();
    static PORT_HANDLE: OnceLock<Result<usize, DpdkError>> = OnceLock::new();

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct InitConfig {
        devargs: String,
        eal_args: Vec<String>,
        io_thread_cpus: Option<Vec<usize>>,
        io_threads: u16,
        rx_desc: u16,
        tx_desc: u16,
        mbuf_count: u32,
        mbuf_data_size: u16,
    }

    impl InitConfig {
        fn from_net_config(config: &DpdkNetConfig) -> Self {
            Self {
                devargs: config.devargs.clone(),
                eal_args: config.eal_args.clone(),
                io_thread_cpus: config.io_thread_cpus.clone(),
                io_threads: config.io_threads,
                rx_desc: config.rx_desc,
                tx_desc: config.tx_desc,
                mbuf_count: config.mbuf_count,
                mbuf_data_size: config.mbuf_data_size,
            }
        }
    }

    static INIT_CONFIG: OnceLock<InitConfig> = OnceLock::new();

    fn ensure_init_config(config: &DpdkNetConfig) -> Result<(), DpdkError> {
        let requested = InitConfig::from_net_config(config);
        let existing = INIT_CONFIG.get_or_init(|| requested.clone());
        if existing != &requested {
            return Err(DpdkError::InvalidConfig(format!(
                "dpdk already initialized with a different config; run in a fresh process \
                 (existing devargs='{}' io_threads={} rx_desc={} tx_desc={} mbuf_count={} \
                 mbuf_data_size={} eal_args={:?} io_thread_cpus={:?}; requested devargs='{}' \
                 io_threads={} rx_desc={} tx_desc={} mbuf_count={} mbuf_data_size={} eal_args={:?} \
                 io_thread_cpus={:?})",
                existing.devargs,
                existing.io_threads,
                existing.rx_desc,
                existing.tx_desc,
                existing.mbuf_count,
                existing.mbuf_data_size,
                existing.eal_args,
                existing.io_thread_cpus,
                requested.devargs,
                requested.io_threads,
                requested.rx_desc,
                requested.tx_desc,
                requested.mbuf_count,
                requested.mbuf_data_size,
                requested.eal_args,
                requested.io_thread_cpus,
            )));
        }
        Ok(())
    }

    fn ensure_eal(config: &DpdkNetConfig) -> Result<(), DpdkError> {
        let init = EAL_INIT.get_or_init(|| {
            fn read_online_cpus() -> Vec<usize> {
                if let Ok(s) = std::fs::read_to_string("/sys/devices/system/cpu/online") {
                    let mut out: Vec<usize> = Vec::new();
                    for part in s.trim().split(',') {
                        let part = part.trim();
                        if part.is_empty() {
                            continue;
                        }
                        if let Some((a, b)) = part.split_once('-') {
                            let (Ok(start), Ok(end)) = (a.parse::<usize>(), b.parse::<usize>())
                            else {
                                continue;
                            };
                            if start > end {
                                continue;
                            }
                            out.extend(start..=end);
                        } else if let Ok(cpu) = part.parse::<usize>() {
                            out.push(cpu);
                        }
                    }
                    out.sort_unstable();
                    out.dedup();
                    if !out.is_empty() {
                        return out;
                    }
                }
                let n = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
                if n > 0 {
                    return (0..(n as usize)).collect();
                }
                vec![0]
            }

            // Minimal default: single core, primary, no telemetry, and a file-prefix to
            // reduce collisions with other DPDK apps.
            let mut args: Vec<String> = vec!["agave-dpdk".to_string()];

            let has_proc_type_arg = config
                .eal_args
                .iter()
                .any(|a| a == "--proc-type" || a.starts_with("--proc-type="));
            if !has_proc_type_arg {
                args.push("--proc-type=primary".to_string());
            }

            // DPDK "primary" mode typically creates a runtime directory (e.g. `/var/run/dpdk` or
            // `$XDG_RUNTIME_DIR/dpdk`) for multiprocess coordination. When running as a systemd
            // system service as a non-root user, `XDG_RUNTIME_DIR` is often unset/unwritable,
            // causing EAL init to fail. `--in-memory` avoids the runtime dir entirely and is
            // sufficient for Agave (single-process).
            //
            // We only add this when the operator hasn't set it explicitly.
            let has_in_memory_arg = config.eal_args.iter().any(|a| a == "--in-memory");
            if !has_in_memory_arg {
                let euid = unsafe { libc::geteuid() };
                if euid != 0 {
                    let xdg = std::env::var_os("XDG_RUNTIME_DIR");
                    let xdg_writable = xdg.as_ref().is_some_and(|dir| {
                        let p = std::path::Path::new(dir);
                        if !p.is_dir() {
                            return false;
                        }
                        use std::os::unix::ffi::OsStrExt;
                        let Ok(dir_c) = CString::new(p.as_os_str().as_bytes()) else {
                            return false;
                        };
                        unsafe { libc::access(dir_c.as_ptr(), libc::W_OK) == 0 }
                    });
                    if !xdg_writable {
                        log::info!(
                            "DPDK: enabling EAL --in-memory (no writable XDG_RUNTIME_DIR for non-root user)"
                        );
                        args.push("--in-memory".to_string());
                    }
                }
            }

            let has_telemetry_arg = config.eal_args.iter().any(|a| {
                a == "--no-telemetry" || a == "--telemetry" || a.starts_with("--telemetry=")
            });
            if !has_telemetry_arg {
                args.push("--no-telemetry".to_string());
            }

            let has_file_prefix_arg = config
                .eal_args
                .iter()
                .any(|a| a == "--file-prefix" || a.starts_with("--file-prefix="));
            if !has_file_prefix_arg {
                args.push(format!("--file-prefix=agave-{}", std::process::id()));
            }

            // Ensure lcore selection exists unless the user provided one.
            let has_lcore_arg = config
                .eal_args
                .iter()
                .any(|a| a == "-l" || a == "-c" || a == "--lcores" || a.starts_with("--lcores="));
            if !has_lcore_arg {
                let mut lcores: std::collections::BTreeSet<usize> =
                    std::collections::BTreeSet::new();
                let online = read_online_cpus();
                if let Some(ref pinned) = config.io_thread_cpus {
                    for &cpu in pinned {
                        if !online.binary_search(&cpu).is_ok() {
                            return Err(DpdkError::InvalidConfig(format!(
                                "dpdk cpu pinning requested, but CPU {cpu} is not online"
                            )));
                        }
                        lcores.insert(cpu);
                    }
                }
                // Always include CPU 0 as a conservative default.
                lcores.insert(0);
                // Prefer including the current CPU for the init thread.
                let init_cpu = unsafe { libc::sched_getcpu() };
                if init_cpu >= 0 {
                    lcores.insert(init_cpu as usize);
                }
                // We spawn `io_threads` DPDK I/O threads in addition to the init thread which
                // calls `rte_eal_init()`. Ensure enough lcores exist for both.
                let needed = usize::from(config.io_threads).saturating_add(1);
                for cpu in online {
                    if lcores.len() < needed {
                        lcores.insert(cpu);
                    }
                }
                if lcores.len() < needed {
                    return Err(DpdkError::InvalidConfig(format!(
                        "not enough online CPUs for DPDK io_threads={} (need >= {needed})",
                        config.io_threads
                    )));
                }

                args.push("-l".to_string());
                args.push(
                    lcores
                        .iter()
                        .map(|c| c.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }

            args.push("-a".to_string());
            args.push(config.devargs.clone());
            args.extend(config.eal_args.clone());

            let cstrs: Vec<CString> = args
                .into_iter()
                .map(|s| CString::new(s).map_err(|e| DpdkError::InvalidConfig(e.to_string())))
                .collect::<Result<Vec<_>, _>>()?;
            let mut argv: Vec<*mut c_char> =
                cstrs.iter().map(|s| s.as_ptr() as *mut c_char).collect();
            let argc: c_int = argv
                .len()
                .try_into()
                .map_err(|_| DpdkError::InvalidConfig("too many eal args".to_string()))?;

            let rc = unsafe { rte_eal_init(argc, argv.as_mut_ptr()) };
            if rc < 0 {
                return Err(DpdkError::EalInitFailed(rc));
            }
            Ok(())
        });

        init.clone()
    }

    fn host_preflight(config: &DpdkNetConfig) -> Result<(), DpdkError> {
        let no_huge = config
            .eal_args
            .iter()
            .any(|a| a == "--no-huge" || a == "--no-hugepages");

        if no_huge {
            return Ok(());
        }

        fn huge_dir_from_eal_args(eal_args: &[String]) -> Option<&str> {
            let mut iter = eal_args.iter().map(String::as_str).peekable();
            while let Some(arg) = iter.next() {
                if arg == "--huge-dir" {
                    return iter.next();
                }
                if let Some(v) = arg.strip_prefix("--huge-dir=") {
                    return Some(v);
                }
            }
            None
        }

        let huge_dir = huge_dir_from_eal_args(&config.eal_args).unwrap_or("/dev/hugepages");

        let meminfo =
            std::fs::read_to_string("/proc/meminfo").map_err(|e| DpdkError::InvalidConfig(format!(
                "failed to read /proc/meminfo for hugepage preflight: {e}"
            )))?;
        let mut huge_total: Option<u64> = None;
        let mut huge_free: Option<u64> = None;
        let mut huge_size_kb: Option<u64> = None;
        for line in meminfo.lines() {
            let mut parts = line.split_whitespace();
            let key = parts.next().unwrap_or("");
            let val = parts.next().and_then(|v| v.parse::<u64>().ok());
            match key {
                "HugePages_Total:" => huge_total = val,
                "HugePages_Free:" => huge_free = val,
                "Hugepagesize:" => huge_size_kb = val,
                _ => {}
            }
        }
        if matches!(huge_total, Some(0)) {
            return Err(DpdkError::InvalidConfig(
                "no hugepages configured (HugePages_Total=0). Configure hugepages and mount \
                 hugetlbfs (typically /dev/hugepages) before enabling DPDK"
                    .to_string(),
            ));
        }
        if matches!(huge_free, Some(0)) {
            return Err(DpdkError::InvalidConfig(
                "no free hugepages available (HugePages_Free=0). Free hugepages (stop other DPDK \
                 apps) or increase hugepage reservation before enabling DPDK"
                    .to_string(),
            ));
        }

        if !std::path::Path::new(huge_dir).exists() {
            log::warn!(
                "DPDK hugepage dir '{huge_dir}' does not exist; mount hugetlbfs there or pass \
                 --experimental-dpdk-eal-arg --huge-dir=<path>"
            );
        } else if let Ok(mounts) = std::fs::read_to_string("/proc/mounts") {
            let mounted = mounts.lines().any(|line| {
                let mut it = line.split_whitespace();
                let _src = it.next();
                let mnt = it.next();
                let fstype = it.next();
                matches!((mnt, fstype), (Some(mnt), Some("hugetlbfs")) if mnt == huge_dir)
            });
            if !mounted {
                log::warn!(
                    "DPDK hugepage dir '{huge_dir}' is not a hugetlbfs mount; mount hugetlbfs \
                     there (e.g. /dev/hugepages)"
                );
            }
        }

        unsafe {
            let mut lim = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            if libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut lim) == 0 {
                let cur = lim.rlim_cur as u64;
                let inf = libc::RLIM_INFINITY as u64;
                if cur != inf {
                    let warn_below: u64 = 64 * 1024 * 1024;
                    if cur < warn_below {
                        let size_kb = huge_size_kb.unwrap_or(0);
                        log::warn!(
                            "RLIMIT_MEMLOCK is low ({cur} bytes). DPDK often requires a high/unlimited \
                             memlock limit (systemd: LimitMEMLOCK=infinity; shell: ulimit -l unlimited). \
                             Hugepagesize={size_kb}kB"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    pub(super) unsafe fn probe(config: &DpdkNetConfig) -> Result<DpdkProbeInfo, DpdkError> {
        if let Some(pci_bdf) = device_name_from_devargs(&config.devargs).and_then(normalize_pci_bdf)
        {
            if let Err(e) = validate_pci_driver_for_dpdk(&pci_bdf) {
                return Err(DpdkError::InvalidConfig(e));
            }
        }

        let (mac, local_ip, port_id, port_name, tx_queues) = unsafe { init_and_get_addr(config)? };
        let port = *PORT_HANDLE
            .get()
            .expect("port initialized")
            .as_ref()
            .map_err(|e| e.clone())? as *mut AgaveDpdkPort;
        let (link_up, link_speed_mbps) = unsafe { get_link_info(port) }.unwrap_or((false, 0));
        Ok(DpdkProbeInfo {
            port_id,
            port_name,
            mac,
            local_ip,
            tx_queues,
            link_up,
            link_speed_mbps,
        })
    }

    pub(super) unsafe fn init_and_get_addr(
        config: &DpdkNetConfig,
    ) -> Result<(DpdkMacAddr, Ipv4Addr, u16, String, u16), DpdkError> {
        fn port_init_failure(rc: i32, config: &DpdkNetConfig) -> DpdkError {
            let errno = if rc < 0 {
                std::io::Error::from_raw_os_error(-rc).to_string()
            } else {
                "unknown error".to_string()
            };
            let msg = match rc {
                x if x == -libc::ENOTSUP && config.io_threads > 1 => format!(
                    "DPDK port init failed (rc={rc}: {errno}). RSS is not supported; re-run with \
                     --experimental-dpdk-io-threads 1 or use an RSS-capable NIC/PMD",
                ),
                x if x == -libc::ENOTSUP => format!(
                    "DPDK port init failed (rc={rc}: {errno}). Operation not supported by the \
                     NIC/PMD"
                ),
                x if x == -libc::ENOMEM => format!(
                    "DPDK port init failed (rc={rc}: {errno}). Out of memory; check hugepages and \
                     DPDK mempool sizing (mbuf_count/mbuf_data_size)"
                ),
                x if x == -libc::EINVAL => format!(
                    "DPDK port init failed (rc={rc}: {errno}). Invalid argument; check ring sizes \
                     (rx_desc/tx_desc) and mbuf sizing"
                ),
                _ => format!("DPDK port init failed (rc={rc}: {errno})"),
            };
            DpdkError::InvalidConfig(msg)
        }

        if config.io_threads == 0 {
            return Err(DpdkError::InvalidConfig(
                "io_threads must be > 0".to_string(),
            ));
        }

        ensure_init_config(config)?;

        host_preflight(config)?;

        ensure_eal(config)?;
        let count = unsafe { rte_eth_dev_count_avail() };
        if count == 0 {
            return Err(DpdkError::NoPortsAvailable);
        }
        let port_id: u16 = {
            let port_name = device_name_from_devargs(&config.devargs).ok_or_else(|| {
                DpdkError::InvalidConfig("devargs must include a device name".to_string())
            })?;

            let mut port_id: u16 = 0;
            let port_name_c = CString::new(port_name)
                .map_err(|e| DpdkError::InvalidConfig(e.to_string()))?;
            let rc =
                unsafe { rte_eth_dev_get_port_by_name(port_name_c.as_ptr(), &mut port_id) };
            if rc == 0 {
                port_id
            } else if count == 1 {
                0
            } else {
                return Err(DpdkError::InvalidConfig(format!(
                    "failed to resolve DPDK port id for device name '{port_name}' (rc={rc})"
                )));
            }
        };
        let port = *PORT_HANDLE
            .get_or_init(|| {
                let mut out: *mut AgaveDpdkPort = ptr::null_mut();
                let rc = unsafe {
                    agave_dpdk_port_open(
                        port_id,
                        /*rx_queues=*/ config.io_threads,
                        /*tx_queues=*/ config.io_threads,
                        config.rx_desc,
                        config.tx_desc,
                        config.mbuf_count,
                        config.mbuf_data_size,
                        &mut out as *mut *mut AgaveDpdkPort,
                    )
                };
                if rc < 0 {
                    return Err(port_init_failure(rc, config));
                }
                if out.is_null() {
                    return Err(DpdkError::InvalidConfig(
                        "dpdk port init returned a null handle".to_string(),
                    ));
                }
                Ok(out as usize)
            })
            .as_ref()
            .map_err(|e| e.clone())? as *mut AgaveDpdkPort;

        let mut mac = [0u8; 6];
        unsafe { agave_dpdk_port_get_mac(port, mac.as_mut_ptr()) };
        let tx_queues = unsafe { agave_dpdk_port_get_tx_queues(port) };
        if tx_queues == 0 {
            return Err(DpdkError::InvalidConfig(
                "dpdk port reports 0 tx queues".to_string(),
            ));
        }
        let port_name = {
            let mut buf = [0u8; 256];
            buf[buf.len() - 1] = 0;
            let rc =
                unsafe { rte_eth_dev_get_name_by_port(port_id, buf.as_mut_ptr() as *mut c_char) };
            if rc == 0 {
                unsafe { CStr::from_ptr(buf.as_ptr() as *const c_char) }
                    .to_string_lossy()
                    .into_owned()
            } else {
                device_name_from_devargs(&config.devargs)
                    .unwrap_or_default()
                    .to_string()
            }
        };
        Ok((DpdkMacAddr(mac), config.local_ip, port_id, port_name, tx_queues))
    }

    unsafe fn get_link_info(port: *const AgaveDpdkPort) -> Option<(bool, u32)> {
        let mut up: u8 = 0;
        let mut speed_mbps: u32 = 0;
        let rc = unsafe { agave_dpdk_port_get_link(port, &mut up, &mut speed_mbps) };
        if rc < 0 {
            None
        } else {
            Some((up != 0, speed_mbps))
        }
    }

    pub(super) unsafe fn wait_for_link_up(timeout: Duration) -> Result<(), DpdkError> {
        let port = *PORT_HANDLE
            .get()
            .expect("port initialized")
            .as_ref()
            .map_err(|e| e.clone())? as *const AgaveDpdkPort;

        let start = Instant::now();
        let poll = Duration::from_millis(100);
        while start.elapsed() < timeout {
            if let Some((up, _speed_mbps)) = unsafe { get_link_info(port) } {
                if up {
                    return Ok(());
                }
            } else {
                return Err(DpdkError::InvalidConfig(
                    "failed to query DPDK link state".to_string(),
                ));
            }
            std::thread::sleep(poll);
        }

        Err(DpdkError::InvalidConfig(format!(
            "DPDK link did not come up within {}s",
            timeout.as_secs()
        )))
    }

    #[derive(Clone, Copy, Debug)]
    struct ArpEntry {
        mac: DpdkMacAddr,
        updated_at: Instant,
        is_static: bool,
    }

    #[derive(Clone, Copy, Debug)]
    pub(super) enum ArpEvent {
        Rx {
            opcode: u16,
            sender_mac: DpdkMacAddr,
            sender_ip: Ipv4Addr,
            target_ip: Ipv4Addr,
            is_static: bool,
        },
        Resolve {
            target_ip: Ipv4Addr,
        },
    }

    #[derive(Clone, Copy, Debug)]
    pub(super) struct ArpCacheUpdate {
        pub ip: Ipv4Addr,
        pub mac: DpdkMacAddr,
        pub is_static: bool,
    }

    #[derive(Clone, Debug)]
    pub(super) enum IoThreadStart {
        Ready(u16),
        Error(u16, DpdkError),
    }

    fn ipv4_mask(prefix_len: u8) -> u32 {
        if prefix_len == 0 {
            0
        } else {
            u32::MAX << (32u32.saturating_sub(prefix_len as u32))
        }
    }

    fn on_link(dst: Ipv4Addr, local: Ipv4Addr, prefix_len: u8) -> bool {
        let mask = ipv4_mask(prefix_len);
        let dst_u = u32::from_be_bytes(dst.octets());
        let local_u = u32::from_be_bytes(local.octets());
        (dst_u & mask) == (local_u & mask)
    }

    fn checksum16(data: &[u8]) -> u16 {
        let mut sum: u32 = 0;
        let mut i: usize = 0;
        while i + 1 < data.len() {
            let w = u16::from_be_bytes([data[i], data[i + 1]]) as u32;
            sum = sum.wrapping_add(w);
            i += 2;
        }
        if i < data.len() {
            sum = sum.wrapping_add((data[i] as u32) << 8);
        }
        while (sum >> 16) != 0 {
            sum = (sum & 0xFFFF).wrapping_add(sum >> 16);
        }
        !(sum as u16)
    }

    fn udp_checksum(src: Ipv4Addr, dst: Ipv4Addr, udp_header_and_payload: &[u8]) -> u16 {
        let mut pseudo = [0u8; 12];
        pseudo[0..4].copy_from_slice(&src.octets());
        pseudo[4..8].copy_from_slice(&dst.octets());
        pseudo[8] = 0;
        pseudo[9] = 17; // UDP
        let udp_len: u16 = udp_header_and_payload.len().try_into().unwrap_or(u16::MAX);
        pseudo[10..12].copy_from_slice(&udp_len.to_be_bytes());

        let mut sum: u32 = 0;
        for chunk in pseudo.chunks_exact(2) {
            sum = sum.wrapping_add(u16::from_be_bytes([chunk[0], chunk[1]]) as u32);
        }
        let mut i: usize = 0;
        while i + 1 < udp_header_and_payload.len() {
            sum = sum.wrapping_add(u16::from_be_bytes([
                udp_header_and_payload[i],
                udp_header_and_payload[i + 1],
            ]) as u32);
            i += 2;
        }
        if i < udp_header_and_payload.len() {
            sum = sum.wrapping_add((udp_header_and_payload[i] as u32) << 8);
        }
        while (sum >> 16) != 0 {
            sum = (sum & 0xFFFF).wrapping_add(sum >> 16);
        }
        let csum = !(sum as u16);
        if csum == 0 {
            0xFFFF
        } else {
            csum
        }
    }

    fn build_arp_reply(
        buf: &mut [u8],
        dst_mac: DpdkMacAddr,
        src_mac: DpdkMacAddr,
        sender_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
    ) -> usize {
        // Ethernet header: 14 bytes
        buf[0..6].copy_from_slice(&dst_mac.0);
        buf[6..12].copy_from_slice(&src_mac.0);
        buf[12..14].copy_from_slice(&0x0806u16.to_be_bytes()); // ARP

        // ARP payload (Ethernet/IPv4): 28 bytes
        let base = 14;
        buf[base..base + 2].copy_from_slice(&1u16.to_be_bytes()); // htype Ethernet
        buf[base + 2..base + 4].copy_from_slice(&0x0800u16.to_be_bytes()); // ptype IPv4
        buf[base + 4] = 6;
        buf[base + 5] = 4;
        buf[base + 6..base + 8].copy_from_slice(&2u16.to_be_bytes()); // reply
        buf[base + 8..base + 14].copy_from_slice(&src_mac.0);
        buf[base + 14..base + 18].copy_from_slice(&sender_ip.octets());
        buf[base + 18..base + 24].copy_from_slice(&dst_mac.0);
        buf[base + 24..base + 28].copy_from_slice(&target_ip.octets());
        14 + 28
    }

    fn build_arp_request(
        buf: &mut [u8],
        src_mac: DpdkMacAddr,
        sender_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
    ) -> usize {
        buf[0..6].copy_from_slice(&[0xFFu8; 6]);
        buf[6..12].copy_from_slice(&src_mac.0);
        buf[12..14].copy_from_slice(&0x0806u16.to_be_bytes());

        let base = 14;
        buf[base..base + 2].copy_from_slice(&1u16.to_be_bytes());
        buf[base + 2..base + 4].copy_from_slice(&0x0800u16.to_be_bytes());
        buf[base + 4] = 6;
        buf[base + 5] = 4;
        buf[base + 6..base + 8].copy_from_slice(&1u16.to_be_bytes()); // request
        buf[base + 8..base + 14].copy_from_slice(&src_mac.0);
        buf[base + 14..base + 18].copy_from_slice(&sender_ip.octets());
        buf[base + 18..base + 24].copy_from_slice(&[0u8; 6]);
        buf[base + 24..base + 28].copy_from_slice(&target_ip.octets());
        14 + 28
    }

    fn build_udp_ipv4_frame(
        buf: &mut [u8],
        dst_mac: DpdkMacAddr,
        src_mac: DpdkMacAddr,
        src_ip: Ipv4Addr,
        src_port: u16,
        dst_ip: Ipv4Addr,
        dst_port: u16,
        payload: &[u8],
    ) -> Option<usize> {
        let total_len = 14usize
            .checked_add(20)?
            .checked_add(8)?
            .checked_add(payload.len())?;
        if total_len > buf.len() {
            return None;
        }

        // Ethernet
        buf[0..6].copy_from_slice(&dst_mac.0);
        buf[6..12].copy_from_slice(&src_mac.0);
        buf[12..14].copy_from_slice(&0x0800u16.to_be_bytes()); // IPv4

        // IPv4 header
        let ip_base = 14;
        buf[ip_base] = 0x45; // v4, IHL=5
        buf[ip_base + 1] = 0;
        let ip_total: u16 = (20usize + 8usize + payload.len()).try_into().ok()?;
        buf[ip_base + 2..ip_base + 4].copy_from_slice(&ip_total.to_be_bytes());
        buf[ip_base + 4..ip_base + 6].copy_from_slice(&0u16.to_be_bytes()); // id
        buf[ip_base + 6..ip_base + 8].copy_from_slice(&0x4000u16.to_be_bytes()); // DF
        buf[ip_base + 8] = 64; // ttl
        buf[ip_base + 9] = 17; // UDP
        buf[ip_base + 10..ip_base + 12].copy_from_slice(&0u16.to_be_bytes()); // checksum placeholder
        buf[ip_base + 12..ip_base + 16].copy_from_slice(&src_ip.octets());
        buf[ip_base + 16..ip_base + 20].copy_from_slice(&dst_ip.octets());
        let csum = checksum16(&buf[ip_base..ip_base + 20]);
        buf[ip_base + 10..ip_base + 12].copy_from_slice(&csum.to_be_bytes());

        // UDP header + payload
        let udp_base = ip_base + 20;
        buf[udp_base..udp_base + 2].copy_from_slice(&src_port.to_be_bytes());
        buf[udp_base + 2..udp_base + 4].copy_from_slice(&dst_port.to_be_bytes());
        let udp_len: u16 = (8usize + payload.len()).try_into().ok()?;
        buf[udp_base + 4..udp_base + 6].copy_from_slice(&udp_len.to_be_bytes());
        buf[udp_base + 6..udp_base + 8].copy_from_slice(&0u16.to_be_bytes()); // checksum placeholder
        buf[udp_base + 8..udp_base + 8 + payload.len()].copy_from_slice(payload);
        let udp_csum = udp_checksum(src_ip, dst_ip, &buf[udp_base..udp_base + 8 + payload.len()]);
        buf[udp_base + 6..udp_base + 8].copy_from_slice(&udp_csum.to_be_bytes());

        Some(total_len)
    }

    fn parse_arp(
        packet: &[u8],
    ) -> Option<(
        /*opcode*/ u16,
        DpdkMacAddr,
        Ipv4Addr,
        DpdkMacAddr,
        Ipv4Addr,
    )> {
        let (ethertype, base) = ethertype_and_l3_offset(packet)?;
        if ethertype != 0x0806 {
            return None;
        }
        if packet.len() < base + 28 {
            return None;
        }
        let htype = u16::from_be_bytes([packet[base], packet[base + 1]]);
        let ptype = u16::from_be_bytes([packet[base + 2], packet[base + 3]]);
        let hlen = packet[base + 4];
        let plen = packet[base + 5];
        if htype != 1 || ptype != 0x0800 || hlen != 6 || plen != 4 {
            return None;
        }
        let opcode = u16::from_be_bytes([packet[base + 6], packet[base + 7]]);
        let sender_mac = DpdkMacAddr([
            packet[base + 8],
            packet[base + 9],
            packet[base + 10],
            packet[base + 11],
            packet[base + 12],
            packet[base + 13],
        ]);
        let sender_ip = Ipv4Addr::new(
            packet[base + 14],
            packet[base + 15],
            packet[base + 16],
            packet[base + 17],
        );
        let target_mac = DpdkMacAddr([
            packet[base + 18],
            packet[base + 19],
            packet[base + 20],
            packet[base + 21],
            packet[base + 22],
            packet[base + 23],
        ]);
        let target_ip = Ipv4Addr::new(
            packet[base + 24],
            packet[base + 25],
            packet[base + 26],
            packet[base + 27],
        );
        Some((opcode, sender_mac, sender_ip, target_mac, target_ip))
    }

    struct RxUdpDatagramView<'a> {
        src: SocketAddrV4,
        dst_port: u16,
        payload: &'a [u8],
    }

    fn parse_ipv4_udp_view<'a>(
        packet: &'a [u8],
        local_ip: Ipv4Addr,
    ) -> Option<RxUdpDatagramView<'a>> {
        let (ethertype, ip_base) = ethertype_and_l3_offset(packet)?;
        if ethertype != 0x0800 {
            return None;
        }
        if packet.len() < ip_base + 20 {
            return None;
        }
        let ver_ihl = packet[ip_base];
        if (ver_ihl >> 4) != 4 {
            return None;
        }
        let ihl_words = (ver_ihl & 0x0F) as usize;
        let ip_hlen = ihl_words.checked_mul(4)?;
        if ip_hlen < 20 {
            return None;
        }
        if packet.len() < ip_base + ip_hlen {
            return None;
        }
        let ip_total = u16::from_be_bytes([packet[ip_base + 2], packet[ip_base + 3]]) as usize;
        if ip_total < ip_hlen {
            return None;
        }
        let ip_end = ip_base.checked_add(ip_total)?;
        if ip_end > packet.len() {
            return None;
        }
        // Drop IPv4 fragments (we do not reassemble).
        let flags_frag = u16::from_be_bytes([packet[ip_base + 6], packet[ip_base + 7]]);
        let frag_offset = flags_frag & 0x1FFF;
        let more_frags = (flags_frag & 0x2000) != 0;
        if frag_offset != 0 || more_frags {
            return None;
        }
        let proto = packet[ip_base + 9];
        if proto != 17 {
            return None;
        }
        let src_ip = Ipv4Addr::new(
            packet[ip_base + 12],
            packet[ip_base + 13],
            packet[ip_base + 14],
            packet[ip_base + 15],
        );
        let dst_ip = Ipv4Addr::new(
            packet[ip_base + 16],
            packet[ip_base + 17],
            packet[ip_base + 18],
            packet[ip_base + 19],
        );
        if dst_ip != local_ip {
            return None;
        }
        let udp_base = ip_base + ip_hlen;
        if udp_base + 8 > ip_end {
            return None;
        }
        let src_port = u16::from_be_bytes([packet[udp_base], packet[udp_base + 1]]);
        let dst_port = u16::from_be_bytes([packet[udp_base + 2], packet[udp_base + 3]]);
        let udp_len = u16::from_be_bytes([packet[udp_base + 4], packet[udp_base + 5]]) as usize;
        if udp_len < 8 {
            return None;
        }
        let udp_end = udp_base.checked_add(udp_len)?;
        if udp_end > ip_end {
            return None;
        }
        let payload_len = udp_len - 8;
        let payload_base = udp_base + 8;
        let payload_end = payload_base.checked_add(payload_len)?;
        if payload_end > ip_end {
            return None;
        }
        let payload = &packet[payload_base..payload_end];
        Some(RxUdpDatagramView {
            src: SocketAddrV4::new(src_ip, src_port),
            dst_port,
            payload,
        })
    }

    fn send_frame(
        port: *mut AgaveDpdkPort,
        queue_id: u16,
        frame: &[u8],
        io_counters: &DpdkIoCounters,
    ) {
        unsafe {
            let m = agave_dpdk_pktmbuf_alloc(port);
            if m.is_null() {
                io_counters
                    .tx_mbuf_alloc_fail
                    .fetch_add(1, Ordering::Relaxed);
                return;
            }
            let dst = agave_dpdk_pktmbuf_append(m, frame.len() as u16);
            if dst.is_null() {
                io_counters
                    .tx_mbuf_append_fail
                    .fetch_add(1, Ordering::Relaxed);
                agave_dpdk_pktmbuf_free(m);
                return;
            }
            ptr::copy_nonoverlapping(frame.as_ptr(), dst, frame.len());
            let mut arr: [*mut RteMbuf; 1] = [m];
            let sent = agave_dpdk_tx_burst(port, queue_id, arr.as_mut_ptr(), 1);
            if sent < 1 {
                io_counters.tx_burst_unsent.fetch_add(1, Ordering::Relaxed);
                agave_dpdk_pktmbuf_free(m);
            }
        }
    }

    pub(super) unsafe fn arp_probe(
        local_mac: DpdkMacAddr,
        local_ip: Ipv4Addr,
        target_ip: Ipv4Addr,
        rx_queues: u16,
        timeout: Duration,
        io_counters: &DpdkIoCounters,
    ) -> Result<DpdkMacAddr, DpdkError> {
        let port = *PORT_HANDLE
            .get()
            .expect("port initialized")
            .as_ref()
            .map_err(|e| e.clone())? as *mut AgaveDpdkPort;

        if rx_queues == 0 {
            return Err(DpdkError::InvalidConfig(
                "arp_probe rx_queues must be > 0".to_string(),
            ));
        }

        let mut request = [0u8; 64];
        let req_len = build_arp_request(&mut request, local_mac, local_ip, target_ip);
        if req_len == 0 || req_len > request.len() {
            return Err(DpdkError::InvalidConfig(
                "failed to build ARP probe request".to_string(),
            ));
        }

        let deadline = Instant::now() + timeout;
        let req_interval = Duration::from_millis(200);
        let mut last_req: Option<Instant> = None;

        let mut rx_pkts: [*mut RteMbuf; 32] = [ptr::null_mut(); 32];

        loop {
            let now = Instant::now();
            if now >= deadline {
                return Err(DpdkError::InvalidConfig(format!(
                    "failed to resolve {target_ip} via ARP within {}ms; check L2 connectivity, VF \
                     trust/spoofchk, and --experimental-dpdk-gateway",
                    timeout.as_millis()
                )));
            }

            let do_req = match last_req {
                None => true,
                Some(ts) => now.duration_since(ts) >= req_interval,
            };
            if do_req {
                send_frame(port, /*queue_id=*/ 0, &request[..req_len], io_counters);
                last_req = Some(now);
            }

            let mut found: Option<DpdkMacAddr> = None;
            for q in 0..rx_queues {
                let n = unsafe {
                    agave_dpdk_rx_burst(
                        port,
                        /*queue_id=*/ q,
                        rx_pkts.as_mut_ptr(),
                        rx_pkts.len() as u16,
                    )
                } as usize;

                for slot in rx_pkts.iter_mut().take(n) {
                    let m = *slot;
                    *slot = ptr::null_mut();
                    if m.is_null() {
                        continue;
                    }

                    let contiguous = unsafe { agave_dpdk_pktmbuf_is_contiguous(m) } != 0;
                    if !contiguous {
                        let rc = unsafe { agave_dpdk_pktmbuf_linearize(m) };
                        if rc < 0 {
                            unsafe { agave_dpdk_pktmbuf_free(m) };
                            continue;
                        }
                    }

                    let len = unsafe { agave_dpdk_pktmbuf_pkt_len(m) } as usize;
                    if len > 0 {
                        let data = unsafe { agave_dpdk_pktmbuf_mtod(m) } as *const u8;
                        if !data.is_null() && found.is_none() {
                            let packet = unsafe { std::slice::from_raw_parts(data, len) };
                            if let Some((opcode, sender_mac, sender_ip, _target_mac, target)) =
                                parse_arp(packet)
                            {
                                if opcode == 2 && sender_ip == target_ip && target == local_ip {
                                    found = Some(sender_mac);
                                }
                            }
                        }
                    }

                    unsafe { agave_dpdk_pktmbuf_free(m) };
                }
            }

            if let Some(mac) = found {
                return Ok(mac);
            }

            std::thread::yield_now();
        }
    }

    fn set_current_thread_affinity(cpu: usize) -> Result<(), DpdkError> {
        if cpu >= libc::CPU_SETSIZE as usize {
            return Err(DpdkError::InvalidConfig(format!(
                "cpu id {cpu} is out of range for sched_setaffinity (CPU_SETSIZE={})",
                libc::CPU_SETSIZE
            )));
        }
        unsafe {
            let mut set: libc::cpu_set_t = std::mem::zeroed();
            libc::CPU_ZERO(&mut set);
            libc::CPU_SET(cpu, &mut set);
            let rc = libc::sched_setaffinity(
                /*pid=*/ 0,
                std::mem::size_of::<libc::cpu_set_t>(),
                &set,
            );
            if rc != 0 {
                return Err(DpdkError::InvalidConfig(format!(
                    "failed to set DPDK I/O thread CPU affinity to {cpu}: {}",
                    std::io::Error::last_os_error()
                )));
            }
        }
        Ok(())
    }

    pub(super) fn run_dpdk_io_loop(
        config: DpdkNetConfig,
        routes: Vec<PacketRoute>,
        quic_ports: Vec<(u16, Vec<Arc<DpdkQuicUdpSocket>>)>,
        shred_tx_rx: Receiver<TxUdpDatagram>,
        quic_tx: Arc<QuicTxQueue>,
        quic_tx_rx: Receiver<TxUdpDatagram>,
        io_counters: Arc<DpdkIoCounters>,
        exit: Arc<AtomicBool>,
        stop: Arc<AtomicBool>,
        queue_id: u16,
        tx_queues: u16,
        arp_event_tx: Sender<ArpEvent>,
        arp_event_rx: Option<Receiver<ArpEvent>>,
        arp_update_txs: Option<Vec<Option<Sender<ArpCacheUpdate>>>>,
        arp_update_rx: Option<Receiver<ArpCacheUpdate>>,
        io_start_tx: Sender<IoThreadStart>,
    ) -> Result<(), DpdkError> {
        if let Some(ref cpus) = config.io_thread_cpus {
            let idx = usize::from(queue_id);
            if let Some(&cpu) = cpus.get(idx) {
                set_current_thread_affinity(cpu)?;
                log::info!("DPDK I/O thread pinned: queue={queue_id} cpu={cpu}");
            }
        }

        // Ensure EAL + port are ready.
        if let Err(e) = unsafe { init_and_get_addr(&config) } {
            let _ = io_start_tx.try_send(IoThreadStart::Error(queue_id, e.clone()));
            return Err(e);
        }
        let port = *PORT_HANDLE
            .get()
            .expect("port initialized")
            .as_ref()
            .expect("port initialized") as *mut AgaveDpdkPort;

        struct DisconnectOnDrop {
            queue: Arc<QuicTxQueue>,
        }

        impl Drop for DisconnectOnDrop {
            fn drop(&mut self) {
                self.queue.disconnect();
            }
        }

        let _disconnect_guard = DisconnectOnDrop {
            queue: quic_tx.clone(),
        };

        // Register this thread with EAL.
        if unsafe { rte_thread_register() } != 0 {
            let err = DpdkError::InvalidConfig(
                "failed to register DPDK I/O thread with EAL (check EAL lcore args)".to_string(),
            );
            let _ = io_start_tx.try_send(IoThreadStart::Error(queue_id, err.clone()));
            return Err(err);
        }

        let local_ip = config.local_ip;
        let local_mac = unsafe {
            let mut mac = [0u8; 6];
            agave_dpdk_port_get_mac(port, mac.as_mut_ptr());
            DpdkMacAddr(mac)
        };
        const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(1);
        let mut last_stats_report = Instant::now();
        let mut last_link: Option<(bool, u32)> = None;
        if queue_id == 0 {
            if let Some((up, speed_mbps)) = unsafe { get_link_info(port) } {
                if up {
                    log::info!("DPDK link up ({speed_mbps} Mbps)");
                } else {
                    log::warn!("DPDK link down");
                }
                last_link = Some((up, speed_mbps));
            }
        }

        const ARP_REQUEST_INTERVAL: Duration = Duration::from_secs(1);
        const ARP_STALE_AFTER: Duration = Duration::from_secs(60);
        const ARP_CACHE_MAX_ENTRIES: usize = 16_384;
        const ARP_CACHE_EVICT_AFTER: Duration = Duration::from_secs(10 * 60);
        const LAST_ARP_REQUEST_MAX_ENTRIES: usize = 16_384;
        const LAST_ARP_REQUEST_EVICT_AFTER: Duration = Duration::from_secs(30);
        const ARP_PRUNE_INTERVAL: Duration = Duration::from_secs(5);
        const TX_DRAIN_MAX: usize = 64;

        struct TxState {
            arp_cache: std::collections::HashMap<Ipv4Addr, ArpEntry>,
            last_arp_request: std::collections::HashMap<Ipv4Addr, Instant>,
            scratch_frame: Vec<u8>,
            tx_items: Vec<TxUdpDatagram>,
            arp_event_rx: Option<Receiver<ArpEvent>>,
            arp_update_rx: Option<Receiver<ArpCacheUpdate>>,
            arp_update_txs: Option<Vec<Option<Sender<ArpCacheUpdate>>>>,
        }

        struct RouteState {
            route: PacketRoute,
            batch: PinnedPacketBatch,
            filled: usize,
        }

        let mut route_states: Vec<RouteState> = routes
            .into_iter()
            .map(|route| {
                let mut batch = PinnedPacketBatch::new_with_recycler(
                    &route.recycler,
                    PACKETS_PER_BATCH,
                    route.stats.name,
                );
                batch.resize(PACKETS_PER_BATCH, Packet::default());
                RouteState {
                    route,
                    batch,
                    filled: 0,
                }
            })
            .collect();

        let mut rx_buf: [*mut RteMbuf; 64] = [ptr::null_mut(); 64];
        if tx_queues == 0 {
            let err = DpdkError::InvalidConfig(
                "internal error: tx_queues must be > 0".to_string(),
            );
            let _ = io_start_tx.try_send(IoThreadStart::Error(queue_id, err.clone()));
            return Err(err);
        }
        let is_tx_thread = queue_id < tx_queues;
        let mut tx_state: Option<TxState> = if is_tx_thread {
            let arp_event_rx = if queue_id == 0 {
                match arp_event_rx {
                    Some(rx) => Some(rx),
                    None => {
                        let err = DpdkError::InvalidConfig(
                            "internal error: missing arp_event_rx for queue 0".to_string(),
                        );
                        let _ = io_start_tx.try_send(IoThreadStart::Error(queue_id, err.clone()));
                        return Err(err);
                    }
                }
            } else {
                None
            };

            let scratch_len = std::cmp::max(2048usize, usize::from(config.mbuf_data_size));
            let mut scratch_frame: Vec<u8> = vec![0u8; scratch_len];
            let mut last_arp_request: std::collections::HashMap<Ipv4Addr, Instant> =
                std::collections::HashMap::new();

            if queue_id == 0 {
                // Best-effort ARP priming: announce our IP and resolve the gateway early.
                let len = build_arp_request(&mut scratch_frame, local_mac, local_ip, local_ip);
                send_frame(port, /*queue_id=*/ 0, &scratch_frame[..len], io_counters.as_ref());
                if let Some(gw) = config.gateway_ip {
                    // Only pre-resolve via ARP when the operator didn't provide a static gateway MAC.
                    if config.gateway_mac.is_none() {
                        let len = build_arp_request(&mut scratch_frame, local_mac, local_ip, gw);
                        send_frame(port, /*queue_id=*/ 0, &scratch_frame[..len], io_counters.as_ref());
                        last_arp_request.insert(gw, Instant::now());
                    }
                }
            }

            let mut arp_cache: std::collections::HashMap<Ipv4Addr, ArpEntry> =
                std::collections::HashMap::new();
            if let (Some(gw), Some(mac)) = (config.gateway_ip, config.gateway_mac) {
                arp_cache.insert(
                    gw,
                    ArpEntry {
                        mac,
                        updated_at: Instant::now(),
                        is_static: true,
                    },
                );
            }

            Some(TxState {
                arp_cache,
                last_arp_request,
                scratch_frame,
                tx_items: Vec::with_capacity(TX_DRAIN_MAX),
                arp_event_rx,
                arp_update_rx,
                arp_update_txs,
            })
        } else {
            None
        };

        let _ = io_start_tx.try_send(IoThreadStart::Ready(queue_id));

        let flush_state = |state: &mut RouteState| {
            let len = state.filled;
            if len == 0 {
                return;
            }

            state.batch.truncate(len);
            let mut next_batch = PinnedPacketBatch::new_with_recycler(
                &state.route.recycler,
                PACKETS_PER_BATCH,
                state.route.stats.name,
            );
            next_batch.resize(PACKETS_PER_BATCH, Packet::default());
            let send_batch = std::mem::replace(&mut state.batch, next_batch);
            state.filled = 0;

            state
                .route
                .stats
                .packets_count
                .fetch_add(len, Ordering::Relaxed);
            state
                .route
                .stats
                .packet_batches_count
                .fetch_add(1, Ordering::Relaxed);
            state
                .route
                .stats
                .max_channel_len
                .fetch_max(state.route.sender.len(), Ordering::Relaxed);
            if len == PACKETS_PER_BATCH {
                state
                    .route
                    .stats
                    .full_packet_batches_count
                    .fetch_add(1, Ordering::Relaxed);
            }

            match state.route.sender.try_send(send_batch.into()) {
                Ok(()) => {}
                Err(crossbeam_channel::TrySendError::Full(_)) => {
                    state
                        .route
                        .stats
                        .num_packets_dropped
                        .fetch_add(len, Ordering::Relaxed);
                }
                Err(crossbeam_channel::TrySendError::Disconnected(_)) => {}
            }
        };

        let mut last_partial_flush = Instant::now();
        const PARTIAL_FLUSH_INTERVAL: Duration = Duration::from_millis(1);

        let upsert_arp_cache = |arp_cache: &mut std::collections::HashMap<Ipv4Addr, ArpEntry>,
                                ip: Ipv4Addr,
                                mac: DpdkMacAddr,
                                now: Instant,
                                is_static: bool| {
            let keep_static = arp_cache.get(&ip).map(|e| e.is_static).unwrap_or(false);
            if !arp_cache.contains_key(&ip) && arp_cache.len() >= ARP_CACHE_MAX_ENTRIES {
                if let Some(evict_ip) = arp_cache
                    .iter()
                    .filter(|(_, e)| !e.is_static)
                    .min_by_key(|(_, e)| e.updated_at)
                    .map(|(ip, _)| *ip)
                {
                    arp_cache.remove(&evict_ip);
                } else {
                    return;
                }
            }
            arp_cache.insert(
                ip,
                ArpEntry {
                    mac,
                    updated_at: now,
                    is_static: is_static || keep_static,
                },
            );
        };

        let prune_arp_state = |tx_state: &mut TxState, now: Instant| {
            tx_state
                .last_arp_request
                .retain(|_, ts| now.duration_since(*ts) <= LAST_ARP_REQUEST_EVICT_AFTER);
            if tx_state.last_arp_request.len() > LAST_ARP_REQUEST_MAX_ENTRIES {
                let excess = tx_state
                    .last_arp_request
                    .len()
                    .saturating_sub(LAST_ARP_REQUEST_MAX_ENTRIES);
                let mut entries: Vec<(Ipv4Addr, Instant)> = tx_state
                    .last_arp_request
                    .iter()
                    .map(|(ip, ts)| (*ip, *ts))
                    .collect();
                entries.sort_by_key(|(_, ts)| *ts);
                for (ip, _) in entries.into_iter().take(excess) {
                    tx_state.last_arp_request.remove(&ip);
                }
            }

            tx_state.arp_cache.retain(|_, e| {
                e.is_static || now.duration_since(e.updated_at) <= ARP_CACHE_EVICT_AFTER
            });
            if tx_state.arp_cache.len() > ARP_CACHE_MAX_ENTRIES {
                let excess = tx_state
                    .arp_cache
                    .len()
                    .saturating_sub(ARP_CACHE_MAX_ENTRIES);
                let mut entries: Vec<(Ipv4Addr, Instant)> = tx_state
                    .arp_cache
                    .iter()
                    .filter(|(_, e)| !e.is_static)
                    .map(|(ip, e)| (*ip, e.updated_at))
                    .collect();
                entries.sort_by_key(|(_, ts)| *ts);
                for (ip, _) in entries.into_iter().take(excess) {
                    tx_state.arp_cache.remove(&ip);
                }
            }
        };

        let is_cacheable_arp_reply = |sender_ip: Ipv4Addr, sender_mac: DpdkMacAddr| -> bool {
            if sender_ip.is_unspecified()
                || sender_ip == local_ip
                || sender_ip.is_broadcast()
                || sender_ip.is_multicast()
            {
                return false;
            }
            let [b0, b1, b2, b3, b4, b5] = sender_mac.0;
            if [b0, b1, b2, b3, b4, b5] == [0u8; 6] || [b0, b1, b2, b3, b4, b5] == [0xff; 6]
            {
                return false;
            }
            if (b0 & 1) != 0 {
                return false;
            }

            let is_gateway = config.gateway_ip.is_some_and(|gw| gw == sender_ip);
            is_gateway || on_link(sender_ip, local_ip, config.prefix_len)
        };

        let mut last_arp_prune = Instant::now();

        while !stop.load(Ordering::Relaxed) && !exit.load(Ordering::Relaxed) {
            let n =
                unsafe { agave_dpdk_rx_burst(port, queue_id, rx_buf.as_mut_ptr(), 64) } as usize;
            for i in 0..n {
                let m = rx_buf[i];
                if m.is_null() {
                    continue;
                }
                let pkt_len = unsafe { agave_dpdk_pktmbuf_pkt_len(m) } as usize;
                if unsafe { agave_dpdk_pktmbuf_is_contiguous(m) } == 0 {
                    let rc = unsafe { agave_dpdk_pktmbuf_linearize(m) };
                    if rc != 0 {
                        unsafe { agave_dpdk_pktmbuf_free(m) };
                        continue;
                    }
                }
                let ptr = unsafe { agave_dpdk_pktmbuf_mtod(m) };
                if ptr.is_null() {
                    unsafe { agave_dpdk_pktmbuf_free(m) };
                    continue;
                }
                let packet = unsafe { std::slice::from_raw_parts(ptr, pkt_len) };

                if let Some((opcode, sender_mac, sender_ip, _target_mac, target_ip)) =
                    parse_arp(packet)
                {
                    if target_ip == local_ip {
                        let cacheable = opcode == 2 && is_cacheable_arp_reply(sender_ip, sender_mac);
                        if queue_id == 0 {
                            let now = Instant::now();
                            let tx_state = tx_state
                                .as_mut()
                                .expect("tx_state must exist for queue 0");
                            match opcode {
                                1 => {
                                    // ARP request for us: reply and cache sender.
                                    let len = build_arp_reply(
                                        &mut tx_state.scratch_frame,
                                        sender_mac,
                                        local_mac,
                                        local_ip,
                                        sender_ip,
                                    );
                                    send_frame(
                                        port,
                                        queue_id,
                                        &tx_state.scratch_frame[..len],
                                        io_counters.as_ref(),
                                    );
                                }
                                2 => {
                                    if cacheable {
                                        // ARP reply: cache sender.
                                        upsert_arp_cache(
                                            &mut tx_state.arp_cache,
                                            sender_ip,
                                            sender_mac,
                                            now,
                                            /*is_static=*/ false,
                                        );
                                    }
                                }
                                _ => {}
                            }

                            // Broadcast resolved neighbors to other TX threads.
                            if cacheable {
                                if let Some(ref txs) = tx_state.arp_update_txs {
                                    for (idx, tx) in txs.iter().enumerate() {
                                        if idx == 0 {
                                            continue;
                                        }
                                        if let Some(tx) = tx {
                                            let _ = tx.try_send(ArpCacheUpdate {
                                                ip: sender_ip,
                                                mac: sender_mac,
                                                is_static: false,
                                            });
                                        }
                                    }
                                }
                            }
                        } else {
                            let forward = match opcode {
                                1 => true,
                                2 => cacheable,
                                _ => false,
                            };
                            if forward {
                                match arp_event_tx.try_send(ArpEvent::Rx {
                                    opcode,
                                    sender_mac,
                                    sender_ip,
                                    target_ip,
                                    is_static: false,
                                }) {
                                    Ok(()) => {}
                                    Err(crossbeam_channel::TrySendError::Full(_)) => {
                                        io_counters
                                            .arp_event_dropped
                                            .fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(crossbeam_channel::TrySendError::Disconnected(_)) => {}
                                }
                            }
                        }
                    }
                    unsafe { agave_dpdk_pktmbuf_free(m) };
                    continue;
                }

                if let Some(datagram) = parse_ipv4_udp_view(packet, local_ip) {
                    // QUIC routes first.
                    if let Some((_, sockets)) =
                        quic_ports.iter().find(|(p, _)| *p == datagram.dst_port)
                    {
                        if !sockets.is_empty() {
                            let key = u64::from(u32::from_be_bytes(datagram.src.ip().octets()))
                                ^ u64::from(datagram.src.port());
                            let idx = (key as usize) % sockets.len();
                            let ok = sockets[idx].enqueue(datagram.src, datagram.payload);
                            if ok {
                                io_counters
                                    .quic_rx_enqueued
                                    .fetch_add(1, Ordering::Relaxed);
                            } else {
                                io_counters
                                    .quic_rx_dropped
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        unsafe { agave_dpdk_pktmbuf_free(m) };
                        continue;
                    }

                    for state in &mut route_states {
                        if state.route.dst_port != datagram.dst_port {
                            continue;
                        }
                        if let Some(ref in_vote_only_mode) = state.route.in_vote_only_mode {
                            if in_vote_only_mode.load(Ordering::Relaxed) {
                                break;
                            }
                        }
                        if datagram.payload.len() > PACKET_DATA_SIZE {
                            break;
                        }

                        if state.filled >= PACKETS_PER_BATCH {
                            flush_state(state);
                        }
                        if state.filled < PACKETS_PER_BATCH {
                            let packet = state.batch.get_mut(state.filled).unwrap();
                            packet.meta_mut().size = datagram.payload.len();
                            packet
                                .meta_mut()
                                .set_socket_addr(&SocketAddr::V4(datagram.src));
                            packet
                                .meta_mut()
                                .set_from_staked_node(state.route.is_staked_service);
                            packet.buffer_mut()[..datagram.payload.len()]
                                .copy_from_slice(&datagram.payload);
                            state.filled += 1;
                        }
                        break;
                    }
                }

                unsafe { agave_dpdk_pktmbuf_free(m) };
            }

            // Flush partial batches periodically for latency, and also when idle.
            let now = Instant::now();
            if n == 0 || now.duration_since(last_partial_flush) >= PARTIAL_FLUSH_INTERVAL {
                for state in &mut route_states {
                    flush_state(state);
                }
                last_partial_flush = now;
            }

            if queue_id == 0 && now.duration_since(last_stats_report) >= STATS_REPORT_INTERVAL {
                for state in &route_states {
                    // TVU shred fetch stats are already reported by ShredFetchStage.
                    if state.route.stats.name.starts_with("shred_fetch_") {
                        continue;
                    }
                    state.route.stats.report();
                }
                io_counters.report();
                if let Some((up, speed_mbps)) = unsafe { get_link_info(port) } {
                    let cur = (up, speed_mbps);
                    if last_link != Some(cur) {
                        if up {
                            log::info!("DPDK link up ({speed_mbps} Mbps)");
                        } else {
                            log::warn!("DPDK link down");
                        }
                        last_link = Some(cur);
                    }
                }
                last_stats_report = now;
            }

            if let Some(tx_state) = tx_state.as_mut() {
                if now.duration_since(last_arp_prune) >= ARP_PRUNE_INTERVAL {
                    prune_arp_state(tx_state, now);
                    last_arp_prune = now;
                }

                // Drain ARP cache updates from queue 0 (when TX uses multiple queues).
                if let Some(ref rx) = tx_state.arp_update_rx {
                    while let Ok(upd) = rx.try_recv() {
                        upsert_arp_cache(
                            &mut tx_state.arp_cache,
                            upd.ip,
                            upd.mac,
                            now,
                            upd.is_static,
                        );
                    }
                }

                // Drain ARP events from other RX queues (queue 0 only).
                let mut arp_need_prune = false;
                if let Some(ref rx) = tx_state.arp_event_rx {
                    while let Ok(ev) = rx.try_recv() {
                        match ev {
                            ArpEvent::Rx {
                                opcode,
                                sender_mac,
                                sender_ip,
                                target_ip,
                                is_static,
                            } => {
                                if target_ip != local_ip {
                                    continue;
                                }
                                let cacheable =
                                    opcode == 2 && is_cacheable_arp_reply(sender_ip, sender_mac);
                                match opcode {
                                    1 => {
                                        let len = build_arp_reply(
                                            &mut tx_state.scratch_frame,
                                            sender_mac,
                                            local_mac,
                                            local_ip,
                                            sender_ip,
                                        );
                                        send_frame(
                                            port,
                                            /*queue_id=*/ 0,
                                            &tx_state.scratch_frame[..len],
                                            io_counters.as_ref(),
                                        );
                                    }
                                    2 => {
                                        if cacheable {
                                            upsert_arp_cache(
                                                &mut tx_state.arp_cache,
                                                sender_ip,
                                                sender_mac,
                                                now,
                                                is_static,
                                            );
                                        }
                                    }
                                    _ => {}
                                }

                                // Broadcast resolved neighbors to other TX threads.
                                if cacheable {
                                    if let Some(ref txs) = tx_state.arp_update_txs {
                                        for (idx, tx) in txs.iter().enumerate() {
                                            if idx == 0 {
                                                continue;
                                            }
                                            if let Some(tx) = tx {
                                                let _ = tx.try_send(ArpCacheUpdate {
                                                    ip: sender_ip,
                                                    mac: sender_mac,
                                                    is_static,
                                                });
                                            }
                                        }
                                    }
                                }
                            }
                            ArpEvent::Resolve { target_ip } => {
                                if target_ip.is_unspecified()
                                    || target_ip == local_ip
                                    || target_ip.is_broadcast()
                                    || target_ip.is_multicast()
                                {
                                    continue;
                                }
                                let is_gateway = config.gateway_ip.is_some_and(|gw| gw == target_ip);
                                if is_gateway && config.gateway_mac.is_some() {
                                    continue;
                                }
                                if !is_gateway && !on_link(target_ip, local_ip, config.prefix_len) {
                                    continue;
                                }
                                let already_ok = match tx_state.arp_cache.get(&target_ip).copied() {
                                    Some(entry)
                                        if entry.is_static
                                            || now.duration_since(entry.updated_at)
                                                <= ARP_STALE_AFTER =>
                                    {
                                        true
                                    }
                                    _ => false,
                                };
                                if already_ok {
                                    continue;
                                }
                                let do_req = match tx_state.last_arp_request.get(&target_ip) {
                                    None => true,
                                    Some(ts) => now.duration_since(*ts) > ARP_REQUEST_INTERVAL,
                                };
                                if do_req {
                                    let len = build_arp_request(
                                        &mut tx_state.scratch_frame,
                                        local_mac,
                                        local_ip,
                                        target_ip,
                                    );
                                    send_frame(
                                        port,
                                        /*queue_id=*/ 0,
                                        &tx_state.scratch_frame[..len],
                                        io_counters.as_ref(),
                                    );
                                    tx_state.last_arp_request.insert(target_ip, now);
                                    arp_need_prune |= tx_state.last_arp_request.len()
                                        > LAST_ARP_REQUEST_MAX_ENTRIES;
                                }
                            }
                        }
                    }
                }
                if arp_need_prune {
                    prune_arp_state(tx_state, now);
                }

                // TX: drain a small batch per loop (one TX queue per thread).
                tx_state.tx_items.clear();
                let mut drained_quic: usize = 0;
                for _ in 0..TX_DRAIN_MAX {
                    match quic_tx_rx.try_recv() {
                        Ok(item) => {
                            drained_quic += 1;
                            tx_state.tx_items.push(item);
                        }
                        Err(crossbeam_channel::TryRecvError::Empty) => break,
                        Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                    }
                }
                if drained_quic > 0 {
                    quic_tx.wake_writers();
                }
                while tx_state.tx_items.len() < TX_DRAIN_MAX {
                    match shred_tx_rx.try_recv() {
                        Ok(item) => tx_state.tx_items.push(item),
                        Err(crossbeam_channel::TryRecvError::Empty) => break,
                        Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                    }
                }
                if !tx_state.tx_items.is_empty() {
                    let mut tx_mbufs: [*mut RteMbuf; 64] = [ptr::null_mut(); 64];
                    let mut tx_count: usize = 0;
                    let mut arp_need_prune = false;
                    for item in tx_state.tx_items.drain(..) {
                        let dst_ip = *item.dst.ip();
                        let next_hop_ip = if on_link(dst_ip, local_ip, config.prefix_len) {
                            dst_ip
                        } else {
                            match config.gateway_ip {
                                Some(gw) => gw,
                                None => {
                                    io_counters
                                        .tx_dropped_no_gateway
                                        .fetch_add(1, Ordering::Relaxed);
                                    continue;
                                }
                            }
                        };
                        let entry = match tx_state.arp_cache.get(&next_hop_ip).copied() {
                            Some(entry)
                                if entry.is_static
                                    || now.duration_since(entry.updated_at) <= ARP_STALE_AFTER =>
                            {
                                entry
                            }
                            _ => {
                                let do_req = match tx_state.last_arp_request.get(&next_hop_ip) {
                                    None => true,
                                    Some(ts) => now.duration_since(*ts) > ARP_REQUEST_INTERVAL,
                                };
                                if do_req {
                                    if queue_id == 0 {
                                        let len = build_arp_request(
                                            &mut tx_state.scratch_frame,
                                            local_mac,
                                            local_ip,
                                            next_hop_ip,
                                        );
                                        send_frame(
                                            port,
                                            /*queue_id=*/ 0,
                                            &tx_state.scratch_frame[..len],
                                            io_counters.as_ref(),
                                        );
                                    } else {
                                        match arp_event_tx.try_send(ArpEvent::Resolve {
                                            target_ip: next_hop_ip,
                                        }) {
                                            Ok(()) => {}
                                            Err(crossbeam_channel::TrySendError::Full(_)) => {
                                                // Fall back to sending the ARP request on this
                                                // thread's TX queue to avoid stalling ARP
                                                // resolution if the centralized event channel is
                                                // saturated.
                                                let len = build_arp_request(
                                                    &mut tx_state.scratch_frame,
                                                    local_mac,
                                                    local_ip,
                                                    next_hop_ip,
                                                );
                                                send_frame(
                                                    port,
                                                    /*queue_id=*/ queue_id,
                                                    &tx_state.scratch_frame[..len],
                                                    io_counters.as_ref(),
                                                );
                                                io_counters
                                                    .arp_event_dropped
                                                    .fetch_add(1, Ordering::Relaxed);
                                            }
                                            Err(crossbeam_channel::TrySendError::Disconnected(
                                                _,
                                            )) => {
                                                // If queue 0 has exited, still attempt to send
                                                // the ARP request on this TX queue so we don't
                                                // silently stop resolving neighbors.
                                                let len = build_arp_request(
                                                    &mut tx_state.scratch_frame,
                                                    local_mac,
                                                    local_ip,
                                                    next_hop_ip,
                                                );
                                                send_frame(
                                                    port,
                                                    /*queue_id=*/ queue_id,
                                                    &tx_state.scratch_frame[..len],
                                                    io_counters.as_ref(),
                                                );
                                            }
                                        }
                                    }
                                    tx_state.last_arp_request.insert(next_hop_ip, now);
                                    arp_need_prune |= tx_state.last_arp_request.len()
                                        > LAST_ARP_REQUEST_MAX_ENTRIES;
                                }
                                io_counters
                                    .tx_dropped_no_arp
                                    .fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                        };

                        unsafe {
                            let m = agave_dpdk_pktmbuf_alloc(port);
                            if m.is_null() {
                                io_counters
                                    .tx_mbuf_alloc_fail
                                    .fetch_add(1, Ordering::Relaxed);
                                continue;
                            }
                            let frame_len = match 14usize
                                .checked_add(20)
                                .and_then(|v| v.checked_add(8))
                                .and_then(|v| v.checked_add(item.payload.len()))
                            {
                                Some(v) => v,
                                None => {
                                    io_counters
                                        .tx_build_fail
                                        .fetch_add(1, Ordering::Relaxed);
                                    agave_dpdk_pktmbuf_free(m);
                                    continue;
                                }
                            };
                            if frame_len > usize::from(u16::MAX) {
                                io_counters
                                    .tx_build_fail
                                    .fetch_add(1, Ordering::Relaxed);
                                agave_dpdk_pktmbuf_free(m);
                                continue;
                            }
                            let dst = agave_dpdk_pktmbuf_append(m, frame_len as u16);
                            if dst.is_null() {
                                io_counters
                                    .tx_mbuf_append_fail
                                    .fetch_add(1, Ordering::Relaxed);
                                agave_dpdk_pktmbuf_free(m);
                                continue;
                            }
                            let buf = std::slice::from_raw_parts_mut(dst, frame_len);
                            if build_udp_ipv4_frame(
                                buf,
                                entry.mac,
                                local_mac,
                                local_ip,
                                item.src_port,
                                dst_ip,
                                item.dst.port(),
                                &item.payload,
                            )
                            .is_none()
                            {
                                io_counters
                                    .tx_build_fail
                                    .fetch_add(1, Ordering::Relaxed);
                                agave_dpdk_pktmbuf_free(m);
                                continue;
                            }
                            tx_mbufs[tx_count] = m;
                            tx_count += 1;
                        }
                        if tx_count == tx_mbufs.len() {
                            tx_flush(
                                port,
                                queue_id,
                                &mut tx_mbufs,
                                tx_count,
                                io_counters.as_ref(),
                            );
                            tx_count = 0;
                        }
                    }
                    if tx_count > 0 {
                        tx_flush(port, queue_id, &mut tx_mbufs, tx_count, io_counters.as_ref());
                    }
                    if arp_need_prune {
                        prune_arp_state(tx_state, now);
                    }
                }
            }

            if n == 0 {
                std::hint::spin_loop();
            }
        }

        unsafe { rte_thread_unregister() };

        Ok(())
    }

    fn tx_flush(
        port: *mut AgaveDpdkPort,
        queue_id: u16,
        mbufs: &mut [*mut RteMbuf],
        count: usize,
        io_counters: &DpdkIoCounters,
    ) {
        if count == 0 {
            return;
        }
        debug_assert!(count <= mbufs.len());
        unsafe {
            let sent =
                agave_dpdk_tx_burst(port, queue_id, mbufs.as_mut_ptr(), count as u16) as usize;
            if sent < count {
                io_counters.tx_burst_unsent.fetch_add(
                    count.saturating_sub(sent),
                    Ordering::Relaxed,
                );
            }
            for i in sent..count {
                let m = mbufs[i];
                if !m.is_null() {
                    agave_dpdk_pktmbuf_free(m);
                }
            }
        }
        for m in mbufs.iter_mut().take(count) {
            *m = ptr::null_mut();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        device_name_from_devargs, ethertype_and_l3_offset, normalize_pci_bdf, DpdkMacAddr,
        QuicTxQueue, TxUdpDatagram,
    };
    use bytes::Bytes;
    use futures::task::noop_waker;
    use std::{net::SocketAddrV4, task::Context};

    #[test]
    fn device_name_from_devargs_parses_pci_names() {
        assert_eq!(
            device_name_from_devargs("0000:01:00.0"),
            Some("0000:01:00.0")
        );
        assert_eq!(
            device_name_from_devargs("pci:0000:01:00.0"),
            Some("0000:01:00.0")
        );
        assert_eq!(
            device_name_from_devargs("0000:01:00.0,representor=[1]"),
            Some("0000:01:00.0")
        );
        assert_eq!(device_name_from_devargs(""), None);
        assert_eq!(device_name_from_devargs("   "), None);
        assert_eq!(device_name_from_devargs(",foo=bar"), None);
    }

    #[test]
    fn normalize_pci_bdf_accepts_common_forms() {
        assert_eq!(
            normalize_pci_bdf("0000:01:00.0"),
            Some("0000:01:00.0".to_string())
        );
        assert_eq!(
            normalize_pci_bdf("01:00.0"),
            Some("0000:01:00.0".to_string())
        );
        assert_eq!(normalize_pci_bdf("pci:0000:01:00.0"), None);
        assert_eq!(normalize_pci_bdf(""), None);
        assert_eq!(normalize_pci_bdf("0000:01:00"), None);
        assert_eq!(normalize_pci_bdf("0000:xx:00.0"), None);
    }

    #[test]
    fn ethertype_and_l3_offset_handles_vlan() {
        // Untagged IPv4
        let mut pkt = [0u8; 14];
        pkt[12..14].copy_from_slice(&0x0800u16.to_be_bytes());
        assert_eq!(ethertype_and_l3_offset(&pkt), Some((0x0800, 14)));

        // 802.1Q VLAN + IPv4
        let mut pkt = [0u8; 18];
        pkt[12..14].copy_from_slice(&0x8100u16.to_be_bytes());
        pkt[16..18].copy_from_slice(&0x0800u16.to_be_bytes());
        assert_eq!(ethertype_and_l3_offset(&pkt), Some((0x0800, 18)));

        // Double-tag (802.1ad + 802.1Q) + IPv4
        let mut pkt = [0u8; 22];
        pkt[12..14].copy_from_slice(&0x88a8u16.to_be_bytes());
        pkt[16..18].copy_from_slice(&0x8100u16.to_be_bytes());
        pkt[20..22].copy_from_slice(&0x0800u16.to_be_bytes());
        assert_eq!(ethertype_and_l3_offset(&pkt), Some((0x0800, 22)));

        // Truncated VLAN header
        let mut pkt = [0u8; 16];
        pkt[12..14].copy_from_slice(&0x8100u16.to_be_bytes());
        assert_eq!(ethertype_and_l3_offset(&pkt), None);
    }

    #[cfg(all(target_os = "linux", feature = "dpdk"))]
    #[test]
    fn parse_proc_net_route_default_gateway_picks_lowest_metric() {
        let contents = "\
Iface\tDestination\tGateway\tFlags\tRefCnt\tUse\tMetric\tMask\tMTU\tWindow\tIRTT\n\
eno1\t00000000\t08552E48\t0003\t0\t0\t100\t00000000\t0\t0\t0\n\
eno1\t00000000\t01020304\t0003\t0\t0\t10\t00000000\t0\t0\t0\n\
eno1\t00112233\t0A000001\t0001\t0\t0\t1\t00000000\t0\t0\t0\n\
";
        let (gw, metric) = super::parse_proc_net_route_default_gateway(contents, "eno1").unwrap();
        assert_eq!(gw, std::net::Ipv4Addr::new(4, 3, 2, 1));
        assert_eq!(metric, 10);
    }

    #[test]
    fn quic_tx_queue_is_bounded_and_reports_writable() {
        let (queue, rx) = QuicTxQueue::new(1);

        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(matches!(
            queue.poll_writable(&mut cx),
            std::task::Poll::Ready(Ok(()))
        ));

        queue
            .try_send(TxUdpDatagram {
                src_port: 1234,
                dst: SocketAddrV4::new([127, 0, 0, 1].into(), 5678),
                payload: Bytes::from_static(b"hello"),
            })
            .unwrap();

        assert!(matches!(
            queue.poll_writable(&mut cx),
            std::task::Poll::Pending
        ));
        assert!(matches!(
            queue.try_send(TxUdpDatagram {
                src_port: 1234,
                dst: SocketAddrV4::new([127, 0, 0, 1].into(), 5678),
                payload: Bytes::from_static(b"world"),
            }),
            Err(crossbeam_channel::TrySendError::Full(_))
        ));

        let _ = rx.try_recv().unwrap();
        assert!(matches!(
            queue.poll_writable(&mut cx),
            std::task::Poll::Ready(Ok(()))
        ));

        // Ensure waiters are drained.
        queue.wake_writers();
        assert!(queue
            .waiters
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
    }

    #[test]
    fn quic_tx_queue_errors_when_disconnected() {
        let (queue, rx) = QuicTxQueue::new(1);
        drop(rx);
        queue.disconnect();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let err = match queue.poll_writable(&mut cx) {
            std::task::Poll::Ready(Err(e)) => e,
            _ => panic!("expected ready error"),
        };
        assert_eq!(err.kind(), std::io::ErrorKind::BrokenPipe);
    }

    #[test]
    fn dpdk_mac_addr_from_str_accepts_common_forms() {
        assert_eq!(
            "7c:c2:55:af:f4:d8".parse::<DpdkMacAddr>().unwrap().0,
            [0x7c, 0xc2, 0x55, 0xaf, 0xf4, 0xd8]
        );
        assert_eq!(
            "7C-C2-55-AF-F4-D8".parse::<DpdkMacAddr>().unwrap().0,
            [0x7c, 0xc2, 0x55, 0xaf, 0xf4, 0xd8]
        );
        assert!("".parse::<DpdkMacAddr>().is_err());
        assert!("7c:c2:55:af:f4".parse::<DpdkMacAddr>().is_err());
        assert!("7c:c2:55:af:f4:zz".parse::<DpdkMacAddr>().is_err());
    }
}
