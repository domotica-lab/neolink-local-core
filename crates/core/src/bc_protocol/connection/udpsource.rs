use super::DiscoveryResult;
use crate::bc::codex::BcCodex;
use crate::bc::model::*;
use crate::bc_protocol::errors::BcUdpDropReciverKind;
use crate::bcudp::codex::BcUdpCodex;
use crate::bcudp::{model::*, xml::*};
use crate::{Credentials, Error, Result};
use delegate::delegate;
use futures::{
    sink::{Sink, SinkExt},
    stream::{IntoAsyncRead, Stream, StreamExt, TryStreamExt},
};
use rand::{rng, seq::SliceRandom, RngExt};
use socket2::SockRef;
use std::collections::BTreeMap;
use std::io::{Error as IoError, Result as IoResult};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::{
    net::UdpSocket,
    sync::{
        mpsc::channel,
        watch::{channel as watch, Sender as WatchSender},
    },
    task::JoinSet,
    time::{interval, sleep, Duration, Instant, Interval},
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt};
use tokio_util::sync::{CancellationToken, PollSender};
use tokio_util::{
    codec::{Decoder, Encoder, Framed},
    udp::UdpFramed,
};

const MTU: usize = 1350;
const UDPDATA_HEADER_SIZE: usize = 20;
const SOCKET_IN_CAPACITY: usize = 4096;
const SOCKET_OUT_CAPACITY: usize = 32768;
/// See `UdpPayloadSource::new`: reassembled packets queued for the decoder.
const DELIVERY_CAPACITY: usize = 2048;
/// Most packets one ACK's selective bitmap covers (1 byte each), so the ACK
/// always fits one datagram (MTU 1350 - 28-byte header). A fragmented ACK on
/// Wi-Fi is far more likely to be lost, which the camera reads as loss.
const ACK_BITMAP_CAP: u32 = 1000;
/// How long the reassembler waits for a missing packet before skipping it.
///
/// A skipped packet desyncs the framing for one message; the BC control codec
/// resyncs (and the media codec drops to the next frame) rather than dropping
/// the connection, but we still prefer the gap to be filled by a retransmit.
/// Selective-ACK NAKs the missing packet on every received packet, so a larger
/// window lets several retransmit round-trips land before we give up. 120 ms
/// was too tight on remote/relay paths (one RTT); 500 ms covers several.
pub const DEFAULT_GAP_SKIP_WAIT_MS: u64 = 500;

/// Per-step timeout used while reconnecting the UDP socket (send/connect/handshake)
/// after a send failure, e.g. on a lossy cellular network.
const UDP_RECONNECT_STEP: Duration = Duration::from_millis(250);

pub(crate) type InnerFramed = Framed<Compat<IntoAsyncRead<UdpPayloadSource>>, BcCodex>;
pub(crate) struct UdpSource {
    inner: Pin<Box<InnerFramed>>,
}

impl UdpSource {
    pub(crate) async fn new_from_discovery<T: Into<String>, U: Into<String>>(
        discovery: DiscoveryResult,
        username: T,
        password: Option<U>,
        debug: bool,
        gap_skip_wait: Duration,
    ) -> Result<Self> {
        // Ensure that the discovery keep alive are all stopped here
        // We now handle all coms in UdpSource
        discovery.socket.set_broadcast(false)?;
        Self::new_from_socket(
            discovery.socket,
            discovery.addr,
            discovery.client_id,
            discovery.camera_id,
            username,
            password,
            debug,
            gap_skip_wait,
        )
        .await
    }

    pub(crate) async fn new_from_socket<T: Into<String>, U: Into<String>>(
        stream: Arc<UdpSocket>,
        addr: SocketAddr,
        client_id: i32,
        camera_id: i32,
        username: T,
        password: Option<U>,
        debug: bool,
        gap_skip_wait: Duration,
    ) -> Result<Self> {
        let bcudp_source = BcUdpSource::new_from_socket(stream, addr).await?;
        let payload_source = bcudp_source
            .into_payload_source(client_id, camera_id, gap_skip_wait)
            .await;
        let async_read = payload_source.into_async_read().compat();
        let codex = if debug {
            BcCodex::new_with_debug(Credentials::new(username, password))
        } else {
            BcCodex::new(Credentials::new(username, password))
        };
        let framed = Framed::new(async_read, codex);

        Ok(Self {
            inner: Box::pin(framed),
        })
    }

    // pub(crate) async fn send(&mut self, bc: Bc) -> Result<()> {
    //     self.inner.send(bc).await
    // }
    // pub(crate) async fn recv(&mut self) -> Result<Bc> {
    //     loop {
    //         if let Some(result) = self.inner.next().await {
    //             return result;
    //         }
    //     }
    // }
}

impl Stream for UdpSource {
    type Item = std::result::Result<<BcCodex as Decoder>::Item, <BcCodex as Decoder>::Error>;

    delegate! {
        to self.inner.as_mut() {
            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
        }
    }

    delegate! {
        to self.inner.as_ref().get_ref() {
            fn size_hint(&self) -> (usize, Option<usize>);
        }
    }
}

impl Sink<Bc> for UdpSource {
    type Error = <BcCodex as Encoder<Bc>>::Error;

    delegate! {
        to self.inner.as_mut() {
            fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
            fn start_send(mut self: Pin<&mut Self>, item: Bc) -> std::result::Result<(), Self::Error>;
            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
            fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
        }
    }
}

pub(crate) struct BcUdpSource {
    inner: Pin<Box<UdpFramed<BcUdpCodex, Arc<UdpSocket>>>>,
    addr: SocketAddr,
}

impl BcUdpSource {
    pub(crate) async fn new_from_socket(stream: Arc<UdpSocket>, addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            inner: Box::pin(UdpFramed::new(stream, BcUdpCodex::new())),
            addr,
        })
    }

    pub(crate) async fn into_payload_source(
        self,
        client_id: i32,
        camera_id: i32,
        gap_skip_wait: Duration,
    ) -> UdpPayloadSource {
        UdpPayloadSource::new(self, client_id, camera_id, gap_skip_wait).await
    }
}

impl Stream for BcUdpSource {
    type Item = Result<(BcUdp, SocketAddr)>;

    delegate! {
        to self.inner.as_mut() {
            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
        }
    }

    delegate! {
        to self.inner.as_ref().get_ref() {
            fn size_hint(&self) -> (usize, Option<usize>);
        }
    }
}

impl Sink<(BcUdp, SocketAddr)> for BcUdpSource {
    type Error = Error;

    delegate! {
        to self.inner.as_mut() {
            fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
            fn start_send(mut self: Pin<&mut Self>, item: (BcUdp, SocketAddr)) -> std::result::Result<(), Self::Error>;
            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
            fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
        }
    }
}

#[derive(Default)]
struct AckLatency {
    current_values: Vec<u32>,
    last_recieve_time: Option<Instant>,
    display_value: u32,
    last_display_time: Option<Instant>,
}

impl AckLatency {
    /// Used to get the current latency, in thd way that the official
    /// client does. This is a value that seems to be updated only every second
    /// Observed values are `0`,    `54785`,    `55062`,     `2528`,
    fn get_value(&self) -> u32 {
        self.display_value
    }

    /// Used to updaet the average latency calculation
    fn feed_ack(&mut self) {
        // Update the last recieve time
        let now = Instant::now();
        if let Some(last_recieve_time) = self.last_recieve_time {
            let diff = (now - last_recieve_time).as_micros();
            self.current_values.push(diff as u32);
            self.last_recieve_time = Some(now);
        } else {
            self.last_recieve_time = Some(now);
        }

        // Update the display_value
        // this is done only ever 1s
        if let Some(last_display_time) = self.last_display_time {
            if now - last_display_time > Duration::from_secs(1) {
                // A second has passed update this
                self.last_display_time = Some(now);
                let current_values_count = self.current_values.len() as u32;
                let current_value = self
                    .current_values
                    .iter()
                    .fold(0u32, |acc, value| acc + *value / current_values_count);
                self.current_values = vec![]; // Reset the average vec

                self.display_value = current_value;
            }
        } else {
            // First 1s is a zero value
            self.last_display_time = Some(now);
            self.display_value = 0;
        }
    }
}

pub(crate) struct UdpPayloadSource {
    inner_stream: Pin<Box<ReceiverStream<IoResult<Vec<u8>>>>>,
    inner_sink: PollSender<Vec<u8>>,
    set: JoinSet<Result<()>>,
    cancel_token: CancellationToken,
}

impl Drop for UdpPayloadSource {
    fn drop(&mut self) {
        log::trace!("Drop UdpPayloadSource");
        self.cancel_token.cancel();
        let _gt = tokio::runtime::Handle::current().enter();
        let mut set = std::mem::take(&mut self.set);
        tokio::task::spawn(async move {
            while set.join_next().await.is_some() {}
            log::trace!("Dropped UdpPayloadSource");
        });
    }
}

struct UdpPayloadInner {
    camera_addr: SocketAddr,
    ack_tx: WatchSender<UdpAck>,
    socket_in: PollSender<BcUdp>,
    socket_out: ReceiverStream<(BcUdp, SocketAddr)>,
    thread_stream: PollSender<IoResult<Vec<u8>>>,
    thread_sink: ReceiverStream<Vec<u8>>,
    client_id: i32,
    camera_id: i32,
    packets_sent: u32,
    packets_want: u32,
    sent: BTreeMap<u32, UdpData>,
    /// Out-of-order packets waiting for the gap before them to be filled,
    /// with the instant each one arrived (used to time the gap, see
    /// [`gap_to_skip`]).
    recieved: BTreeMap<u32, (Vec<u8>, Instant)>,
    last_delivery_at: Instant,
    /// Learns how late the camera's retransmits arrive and stretches the gap
    /// wait to match (see [`GapWait`]).
    gap_wait: GapWait,
    // --- debug transport stats (1/sec heartbeat) ---
    stat_last_report: Instant,
    stat_pkts_in: u64,
    stat_bytes_in: u64,
    stat_delivered: u64,
    stat_resends: u64,
    // reorder/false-loss diagnostics: packets arriving ahead of the contiguous
    // point force the selective-ACK to report "holes" the camera may read as loss
    stat_reorder_events: u64,
    stat_max_reorder_depth: u32,
    stat_max_pending: u32,
    // Receiver delivery-rate fed back to the camera in the ACK `recv_bytes_per_sec`
    // field (the field reverse-engineered upstream as `maybe_latency`): bytes received
    // in the trailing ~1s, latched once per second. The camera's CUBIC sender uses it
    // as its bandwidth estimate (see build_send_ack).
    ack_recv_rate: u32,
    /// Offical Client does ack every 10ms if we don't also do this the camera
    /// seems to think we have a poor connection and will abort
    /// This `ack_interval` controls how ofen we do this
    /// Offical Client does resend every 500ms
    /// This `resend_interval` controls how ofen we do this
    resend_interval: Interval,
    ack_latency: AckLatency,
    cancel: CancellationToken,
    set: JoinSet<Result<()>>,
}
impl UdpPayloadInner {
    fn new(
        mut inner: BcUdpSource,
        thread_stream: PollSender<IoResult<Vec<u8>>>,
        thread_sink: ReceiverStream<Vec<u8>>,
        client_id: i32,
        camera_id: i32,
        gap_skip_wait: Duration,
    ) -> Self {
        let mut set = JoinSet::new();
        let camera_addr = inner.addr;
        let cancel = CancellationToken::new();
        // Data in this needs to be passed into the socket regularly
        // especially the ACK packets on UDP. The thread must not lock
        // and MUST send ACK packets or else be dropped by the camera.
        // In order to achieve this we use dedicated threads for ACK
        // and the socket

        let (socket_in_tx, socket_in_rx) = channel::<BcUdp>(SOCKET_IN_CAPACITY);
        let (socket_out_tx, socket_out_rx) = channel::<(BcUdp, SocketAddr)>(SOCKET_OUT_CAPACITY);
        // let (mut socket_tx, mut socket_rx) = inner.split();

        // Send/Recv on the socket
        let send_cancel = cancel.clone();
        let mut socket_in_rx = ReceiverStream::new(socket_in_rx);
        let thread_camera_addr = camera_addr;
        let socket_out_tx = socket_out_tx.clone();
        let thread_client_id = client_id;
        let thread_camera_id = camera_id;
        const TIME_OUT: u64 = 10;
        let mut recv_timeout = Box::pin(sleep(Duration::from_secs(TIME_OUT)));
        set.spawn(async move {
            let mut dropped = 0u64;
            let mut last_drop_log = Instant::now();
            let mut bad_datagrams = 0u64;
            let mut last_bad_log = Instant::now() - Duration::from_secs(60);
            let result = tokio::select! {
                _ = send_cancel.cancelled() => {
                    Result::Ok(())
                },
                v = async {
                    loop {
                        break tokio::select!{
                            _ = recv_timeout.as_mut() => {
                                Err(Error::BcUdpTimeout)
                            }
                            packet = inner.next() => {
                                log::trace!("Cam->App");
                                let packet = match packet.ok_or(Error::BcUdpDropReciver(BcUdpDropReciverKind::NoneRecieved))? {
                                    Ok(packet) => packet,
                                    // One datagram we cannot parse (corrupt, or a
                                    // message type we do not model) must not tear
                                    // down a working stream: drop it and go on,
                                    // as discovery already does. The codec has
                                    // discarded it, so the next read is clean.
                                    Err(e) if super::discovery::is_unparseable_datagram(&e) => {
                                        bad_datagrams += 1;
                                        if last_bad_log.elapsed() >= Duration::from_secs(60) {
                                            log::info!(
                                                "UDP from {}: ignored {} unparseable datagram(s) (last: {e:?})",
                                                thread_camera_addr, bad_datagrams
                                            );
                                            bad_datagrams = 0;
                                            last_bad_log = Instant::now();
                                        }
                                        continue;
                                    }
                                    Err(e) => return Err(e),
                                };
                                recv_timeout.as_mut().reset(Instant::now() + Duration::from_secs(TIME_OUT));
                                // let packet = socket_rx.next().await.ok_or(Error::BcUdpDropReciver)??;
                                match socket_out_tx.try_send(packet) {
                                    Ok(()) => {}
                                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                        dropped += 1;
                                        let now = Instant::now();
                                        if now.duration_since(last_drop_log) >= Duration::from_secs(1) {
                                            log::warn!(
                                                "UDP recv queue full; dropped {} packets in last {:?}",
                                                dropped,
                                                now.duration_since(last_drop_log)
                                            );
                                            dropped = 0;
                                            last_drop_log = now;
                                        }
                                    }
                                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                        return Err(Error::BcUdpDropReciver(
                                            BcUdpDropReciverKind::SendFailed(
                                                "socket_out channel closed".to_string(),
                                            ),
                                        ));
                                    }
                                }
                                continue;
                            },
                            packet = socket_in_rx.next() => {
                                let packet = packet.ok_or(Error::BcUdpDropSender)?;
                                match tokio::time::timeout(UDP_RECONNECT_STEP, inner.send((packet, thread_camera_addr))).await {
                                    Ok(written) => {
                                        written?;
                                    }
                                    Err(_) => {
                                        log::trace!("Socket Error, attempting reconnect over a new one");
                                        // Socket is (maybe) broken
                                        // Seems to happen with network reconnects like over
                                        // a lossy cellular network
                                        let stream = Arc::new(tokio::time::timeout(UDP_RECONNECT_STEP, connect_try_port(inner.inner.get_ref().local_addr()?.port())).await.map_err(|_| Error::BcUdpReconnectTimeout)??);
                                        inner = tokio::time::timeout(UDP_RECONNECT_STEP, BcUdpSource::new_from_socket(stream, inner.addr)).await.map_err(|_| Error::BcUdpReconnectTimeout)??;

                                        // Inform the camera that we are the same client
                                        //
                                        // At least I think that is what this is for.
                                        // Might also have to do this for the relay but not sure
                                        let msg = BcUdp::Discovery(UdpDiscovery {
                                            tid: {
                                                let mut rng = rng();
                                                (rng.random::<u8>()) as u32
                                            },
                                            payload: UdpXml::C2dHb(C2dHb {
                                                    cid: thread_client_id,
                                                    did: thread_camera_id,
                                                }),
                                        });
                                        let _ = tokio::time::timeout(UDP_RECONNECT_STEP, inner.send((msg, thread_camera_addr))).await;
                                    }
                                }

                                log::trace!("Send Packet");
                                continue;
                            }
                        }
                    }?;
                    Ok(())
                } => v,
            };
            send_cancel.cancel();
            // The receive loop only sees this as its channel closing
            // (`NoneRecieved`), so name the real cause here.
            if let Err(e) = &result {
                log::warn!("UDP socket to {} closed: {e:?}", thread_camera_addr);
            }
            result
        });

        // Queue up ack packets
        let ack_cancel = cancel.clone();
        let mut ack_interval = interval(Duration::from_millis(10)); // Offical Client does ack every 10ms
        ack_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let (ack_tx, ack_rx) = watch(UdpAck::empty(camera_id));
        let ack_socket_in_tx = socket_in_tx.clone();
        set.spawn(async move {
            tokio::select! {
                _ = ack_cancel.cancelled() => {
                    Result::Ok(())
                },
                v = async {
                    loop {
                        ack_interval.tick().await;
                        // Send an ack packet
                        log::trace!("send ack");
                        let ack_packet = BcUdp::Ack(ack_rx.borrow().clone());
                        ack_socket_in_tx.send(ack_packet).await?;
                    }
                } => v,
            }
        });

        // Queue up Hb packets
        let thread_client_id = client_id;
        let thread_camera_id = camera_id;
        let thread_sender = socket_in_tx.clone();
        let mut thread_interval = interval(Duration::from_secs(1));
        let thread_cancel = cancel.clone();
        set.spawn(async move {
            tokio::select! {
                _ = thread_cancel.cancelled() => Result::Ok(()),
                v = async {
                    loop {
                        thread_interval.tick().await;
                        let msg = BcUdp::Discovery(UdpDiscovery {
                            tid: {
                                let mut rng = rng();
                                (rng.random::<u8>()) as u32
                            },
                            payload: UdpXml::C2dHb(C2dHb {
                                    cid: thread_client_id,
                                    did: thread_camera_id,
                                }),
                        });
                        if thread_sender.send(msg).await.is_err() {
                            break Result::Ok(());
                        }
                    }
                } => v,
            }
        });

        let gap_wait = GapWait::new(gap_skip_wait, camera_addr.ip(), ADAPTIVE_GAP_WAIT);
        if gap_wait.current() > gap_skip_wait {
            log::info!(
                "UDP gap skip wait set to {}ms (configured {}ms, learned from earlier late retransmits)",
                gap_wait.current().as_millis(),
                gap_skip_wait.as_millis()
            );
        } else {
            log::info!("UDP gap skip wait set to {}ms", gap_skip_wait.as_millis());
        }

        Self {
            camera_addr,
            ack_tx,
            socket_in: PollSender::new(socket_in_tx),
            socket_out: ReceiverStream::new(socket_out_rx),
            thread_stream,
            thread_sink,
            client_id,
            camera_id,
            packets_sent: 0,
            packets_want: 0,
            sent: Default::default(),
            recieved: Default::default(),
            last_delivery_at: Instant::now(),
            gap_wait,
            stat_last_report: Instant::now(),
            stat_pkts_in: 0,
            stat_bytes_in: 0,
            stat_delivered: 0,
            stat_resends: 0,
            stat_reorder_events: 0,
            stat_max_reorder_depth: 0,
            stat_max_pending: 0,
            ack_recv_rate: 0,
            resend_interval: interval(Duration::from_millis(500)), // Offical Client does resend every 500ms
            ack_latency: Default::default(),
            cancel,
            set,
        }
    }
    async fn run(&mut self) -> Result<()> {
        let camera_addr = self.camera_addr;
        tokio::select! {
            _ = self.resend_interval.tick() => {
                log::trace!("Resend Tick");
                for (_, resend) in self.sent.iter() {
                    self.stat_resends += 1;
                    self.socket_in.feed(BcUdp::Data(resend.clone())).await?;
                }
                self.ack_tx.send_replace(self.build_send_ack()); // Ensure we update the ack packet sometimes too
                Result::Ok(())
            },
            v = self.thread_sink.next() => {
                log::trace!("App->Camera");
                // Incomming from application
                // Outgoing on socket
                let item = v.ok_or(Error::BcUdpDropSender)?;

                for chunk in item.chunks(MTU - UDPDATA_HEADER_SIZE) {
                    let udp_data = UdpData {
                        connection_id: self.camera_id,
                        packet_id: self.packets_sent,
                        payload: chunk.to_vec(),
                    };
                    self.packets_sent += 1;
                    self.sent.insert(udp_data.packet_id, udp_data.clone());
                    self.socket_in.feed(BcUdp::Data(udp_data)).await?;
                }
                Ok(())
            }
            v = self.socket_out.next() => {
                log::trace!("Camera->App");
                // Incomming from socket
                // Outgoing to application
                let (item, addr) = v.ok_or(Error::BcUdpDropReciver(BcUdpDropReciverKind::NoneRecieved))?;
                if addr == camera_addr {
                    match item {
                        BcUdp::Discovery(UdpDiscovery{
                            payload: UdpXml::D2cHb(D2cHb {
                                cid,
                                did,
                            }),
                            ..
                        }) => {
                            if cid != self.client_id {
                                log::info!("Camera sent different client ID in HB");
                            }
                            if did != self.camera_id {
                                log::info!("Camera sent different device ID in HB");
                            }
                        },
                        BcUdp::Discovery(_disc) => {},
                        BcUdp::Ack(ack) => {
                            if ack.connection_id == self.client_id {
                                self.handle_ack(ack);
                            }
                        },
                        BcUdp::Data(data)  => {
                            if data.connection_id == self.client_id {
                                let packet_id = data.packet_id;
                                self.stat_pkts_in += 1;
                                self.stat_bytes_in += data.payload.len() as u64;
                                if packet_id >= self.packets_want {
                                    // error!("packets_want: {}", this.packets_want);
                                    // A packet arriving ahead of the contiguous point means
                                    // earlier packets haven't arrived yet (reorder or loss),
                                    // so the next selective-ACK will mark those as missing.
                                    if packet_id > self.packets_want {
                                        self.stat_reorder_events += 1;
                                        let depth = packet_id - self.packets_want;
                                        if depth > self.stat_max_reorder_depth {
                                            self.stat_max_reorder_depth = depth;
                                        }
                                    }
                                    self.recieved
                                        .entry(packet_id)
                                        .or_insert_with(|| (data.payload, Instant::now()));
                                    if self.recieved.len() as u32 > self.stat_max_pending {
                                        self.stat_max_pending = self.recieved.len() as u32;
                                    }
                                    self.ack_tx.send_replace(self.build_send_ack());
                                } else if let Some(late) = self.gap_wait.on_late(packet_id, Instant::now()) {
                                    // A retransmit of a packet we already skipped:
                                    // too late to use, but it tells us how long the
                                    // camera takes to resend.
                                    if let Some(new_wait) = late.raised_to {
                                        log::info!(
                                            "UDP retransmit from {} arrived {:?} after its gap; now waiting up to {}ms before skipping",
                                            self.camera_addr,
                                            late.after_gap,
                                            new_wait.as_millis()
                                        );
                                    }
                                }
                            }
                        },
                    }
                }
                log::trace!("Got packet");
                Ok(())
            },
        }?;
        log::trace!("Send");
        if let Some((next_id, wait)) = gap_to_skip(
            self.packets_want,
            &self.recieved,
            Instant::now(),
            self.gap_wait.current(),
        ) {
            let last_id = self.recieved.keys().max().copied().unwrap_or(next_id);
            let gap_len = next_id.saturating_sub(self.packets_want);
            log::warn!(
                "UDP payload gap from {}: skipping missing packets {}..{} (gap={}, wait={:?}, pending={}, max_id={})",
                self.camera_addr,
                self.packets_want,
                next_id.saturating_sub(1),
                gap_len,
                wait,
                self.recieved.len(),
                last_id,
            );
            self.gap_wait
                .on_skip(self.packets_want, next_id, Instant::now(), wait);
            self.packets_want = next_id;
            self.ack_tx.send_replace(self.build_send_ack());
        }
        while let Some((payload, _)) = self.recieved.remove(&self.packets_want) {
            log::trace!("  + {}", self.packets_want);
            self.packets_want += 1;
            self.stat_delivered += 1;
            let feed_started = Instant::now();
            self.thread_stream.feed(Ok(payload)).await?;
            self.gap_wait.on_delivery_wait(feed_started.elapsed());
            self.last_delivery_at = Instant::now();
        }
        if let Some(summary) = self.gap_wait.minute_summary(Instant::now()) {
            log::info!("UDP loss from {}: {}", self.camera_addr, summary);
        }
        if self.stat_last_report.elapsed() >= Duration::from_secs(1) {
            let win = self.stat_last_report.elapsed();
            let kbps = (self.stat_bytes_in as f64 * 8.0 / 1000.0) / win.as_secs_f64().max(0.001);
            // Latch the trailing ~1s received-byte count as the delivery-rate we report
            // to the camera in every ACK's `recv_bytes_per_sec` field (bytes/second).
            self.ack_recv_rate = self.stat_bytes_in.min(u32::MAX as u64) as u32;
            log::debug!(
                "UDP HB: in_pkts={} in_kbps={:.0} delivered={} resends={} packets_want={} sent_unacked={} recieved_pending={} reorder_events={} max_reorder_depth={} max_pending={} ack_recv_rate={} ack_latency_us={} since_delivery={:?} win={:?}",
                self.stat_pkts_in,
                kbps,
                self.stat_delivered,
                self.stat_resends,
                self.packets_want,
                self.sent.len(),
                self.recieved.len(),
                self.stat_reorder_events,
                self.stat_max_reorder_depth,
                self.stat_max_pending,
                self.ack_recv_rate,
                self.ack_latency.get_value(),
                self.last_delivery_at.elapsed(),
                win,
            );
            self.stat_pkts_in = 0;
            self.stat_bytes_in = 0;
            self.stat_delivered = 0;
            self.stat_resends = 0;
            self.stat_reorder_events = 0;
            self.stat_max_reorder_depth = 0;
            self.stat_max_pending = 0;
            self.stat_last_report = Instant::now();
        }
        log::trace!("recieved: {}", self.recieved.len());
        log::trace!("Flush");
        self.socket_in.flush().await?;
        self.thread_stream.flush().await?;
        log::trace!("Flushed");
        Ok(())
    }

    fn build_send_ack(&self) -> UdpAck {
        if self.packets_want > 0 {
            let mut first_missing: u32 = self.packets_want;
            while self.recieved.contains_key(&first_missing) {
                // Happens if we have recieved but not consumed yet
                first_missing += 1;
            }
            let missing_ids = ack_bitmap(first_missing, &self.recieved);

            UdpAck {
                connection_id: self.camera_id,
                packet_id: first_missing - 1, // Last we actually have is first_missing - 1
                group_id: 0,
                // Receiver delivery-rate feedback (this field is NOT latency).
                //
                // Proven from a packet capture of the official client: this carries the
                // receiver's measured throughput = bytes received in the trailing ~1s,
                // latched ~once per second. The camera's CUBIC rate controller uses it as
                // its bandwidth estimate:
                //   * 0                    -> no estimate; camera free-runs a conservative
                //                             default (~1.9 Mbps on some models)
                //   * a small/fixed value  -> camera believes the path is that slow and
                //                             paces down to it (~340 kbps)
                //   * the real measured rate -> estimate tracks reality and CUBIC ramps to
                //                             full bitrate (the official app sends
                //                             ~525 KB/s and sustains 4.2 Mbps)
                // An earlier attempt sent bytes-per-100ms here (10x too small), which the
                // camera read as a slow path and throttled — the unit must be bytes/SECOND.
                // We report our own measured received-bytes/s (`ack_recv_rate`).
                recv_bytes_per_sec: self.ack_recv_rate,
                payload: missing_ids,
            }
        } else {
            UdpAck::empty(self.camera_id)
        }
    }

    fn handle_ack(&mut self, ack: UdpAck) {
        let start = ack.packet_id;
        if start != 0xffffffff {
            // -1 means havent got anything yet
            self.sent.retain(|&k, _| k > start);

            for (idx, &value) in ack.payload.iter().enumerate() {
                let packet_id = (start + 1) + idx as u32;
                if value > 0 {
                    self.sent.remove(&packet_id);
                }
            }
        }
        self.ack_latency.feed_ack();
        log::trace!("sent: {}", self.sent.len());
    }
}

impl Drop for UdpPayloadInner {
    fn drop(&mut self) {
        log::trace!("Drop UdpPayloadInner");
        self.cancel.cancel();
        let _gt = tokio::runtime::Handle::current().enter();
        let mut set = std::mem::take(&mut self.set);
        tokio::task::spawn(async move {
            while set.join_next().await.is_some() {}
            log::trace!("Dropped UdpPayloadInner");
        });
    }
}
impl UdpPayloadSource {
    async fn new(
        inner: BcUdpSource,
        client_id: i32,
        camera_id: i32,
        gap_skip_wait: Duration,
    ) -> Self {
        let (inner_sink, thread_sink) = channel(100);
        // Reassembled packets handed to the BC decoder. Must hold several
        // main-stream keyframes (~200 packets each): while it is full,
        // `run()` blocks in `feed()`, so it stops processing arrivals and the
        // ACK it publishes goes stale — the camera reads that as a poor link
        // and throttles/resends (same stall Reoling measured on hardware).
        let (thread_stream, inner_stream) = channel(DELIVERY_CAPACITY);

        let mut payload_inner = UdpPayloadInner::new(
            inner,
            PollSender::new(thread_stream),
            ReceiverStream::new(thread_sink),
            client_id,
            camera_id,
            gap_skip_wait,
        );
        let cancel_token = tokio_util::sync::CancellationToken::new();

        let thread_cancel_token = cancel_token.clone();
        let mut set = JoinSet::new();
        set.spawn(async move {
            tokio::select! {
                v = async {
                    loop {
                        if payload_inner.thread_stream.is_closed() {
                            log::trace!("payload_inner.thread_stream.is_closed");
                            payload_inner.thread_sink.close();
                            return Err(Error::BcUdpPayloadDroppedInner);
                        }
                        log::trace!("Calling inner");
                        let res = payload_inner.run().await;
                        log::trace!("Called inner: {:?}", res);
                        match res {
                            Ok(()) => {}
                            Err(e) => {
                                log::trace!("UDP Error. Connection will Drop: {:?}", e);
                                // Pass error up
                                let _ = payload_inner
                                    .thread_stream
                                    .send(Err(IoError::other(e.clone())))
                                    .await;
                                return Result::<()>::Err(e);
                            }
                        }
                    }
                } => v,
                _ = thread_cancel_token.cancelled() => Ok(()),
            }
        });

        UdpPayloadSource {
            inner_stream: Box::pin(ReceiverStream::new(inner_stream)),
            inner_sink: PollSender::new(inner_sink),
            set,
            cancel_token,
        }
    }
}

impl Stream for UdpPayloadSource {
    type Item = IoResult<Vec<u8>>;

    delegate! {
        to self.inner_stream.as_mut() {
            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
        }
    }

    delegate! {
        to self.inner_stream.as_ref().get_ref() {
            fn size_hint(&self) -> (usize, Option<usize>);
        }
    }
}

impl Sink<Vec<u8>> for UdpPayloadSource {
    type Error = IoError;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.get_mut()
            .inner_sink
            .poll_ready_unpin(cx)
            .map_err(IoError::other)
    }
    fn start_send(self: Pin<&mut Self>, item: Vec<u8>) -> std::result::Result<(), Self::Error> {
        self.get_mut()
            .inner_sink
            .start_send_unpin(item)
            .map_err(IoError::other)
    }
    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.get_mut()
            .inner_sink
            .poll_flush_unpin(cx)
            .map_err(IoError::other)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), Self::Error>> {
        self.get_mut()
            .inner_sink
            .poll_close_unpin(cx)
            .map_err(IoError::other)
    }
}

impl futures::AsyncWrite for UdpPayloadSource {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
        let mut this = self.get_mut();
        match Pin::new(&mut this).poll_ready(cx) {
            Poll::Ready(Ok(())) => match Pin::new(&mut this).start_send(buf.to_vec()) {
                Ok(()) => Poll::Ready(Ok(buf.len())),
                Err(e) => Poll::Ready(Err(e)),
            },
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        Sink::poll_flush(self, cx)
    }
    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        Sink::poll_close(self, cx)
    }
}

/// Decide whether the reassembler should give up on the gap blocking delivery.
///
/// Returns `Some((next_id, waited))` when the packets before `next_id` should be
/// skipped. The gap is timed from when it was first *observed* — the arrival of
/// the earliest pending packet, since every pending packet lies beyond the
/// missing one — not from the last delivery. Timing from the last delivery made
/// a gap that appears after a quiet period (or after a previous skip) look
/// already "old", so it was skipped the instant it was seen, before the camera
/// had any chance to retransmit (`wait=3.2s, pending=1` in the logs). Every
/// such skip corrupts video until the next keyframe.
fn gap_to_skip<T>(
    packets_want: u32,
    recieved: &BTreeMap<u32, (T, Instant)>,
    now: Instant,
    gap_skip_wait: Duration,
) -> Option<(u32, Duration)> {
    let (&next_id, _) = recieved.iter().next()?;
    if next_id <= packets_want {
        return None;
    }
    let detected_at = recieved.values().map(|(_, at)| *at).min()?;
    let waited = now.saturating_duration_since(detected_at);
    (waited >= gap_skip_wait).then_some((next_id, waited))
}

/// Adaptive wait before skipping a UDP gap.
///
/// The camera does not resend a missing packet as soon as the selective-ACK
/// reports it; it resends on its own timer, so on a lossy link a retransmit
/// often lands just *after* the fixed `udp_gap_skip_ms` deadline and is thrown
/// away, while the skip has already corrupted the picture. `GapWait` remembers
/// recently skipped packet ids; when one of them arrives late it measures how
/// long after the gap it came and raises the wait to cover that (plus a
/// margin, capped at [`GapWait::MAX`]). Without late retransmits the wait never
/// grows, and it decays back to the configured value when they stop.
/// Whether the gap wait adapts to late retransmits (see [`GapWait`]).
///
/// Off: field use showed that waiting up to 1.2 s on every loss (a lossy
/// Wi-Fi camera loses 30–55 packets a minute) froze the picture almost
/// continuously and slowed stream start-up, while the fixed 500 ms skip only
/// smears a frame until the next keyframe and keeps the video moving. Late
/// retransmits are still counted for the per-minute summary.
const ADAPTIVE_GAP_WAIT: bool = false;

/// Gap wait learned per camera IP, kept across reconnects (on-demand cameras
/// reconnect often and would otherwise relearn it every time).
static LEARNED_GAP_WAIT: std::sync::LazyLock<
    std::sync::Mutex<std::collections::HashMap<std::net::IpAddr, Duration>>,
> = std::sync::LazyLock::new(Default::default);

struct GapWait {
    /// Raise the wait from late retransmits (`ADAPTIVE_GAP_WAIT`).
    adaptive: bool,
    camera_ip: Option<std::net::IpAddr>,
    configured: Duration,
    current: Duration,
    /// Skipped packet id -> (when the gap was first seen).
    skipped: BTreeMap<u32, Instant>,
    // per-minute summary
    window_start: Instant,
    win_skipped: u64,
    win_late: u64,
    win_max_late: Duration,
    /// Time `run()` spent blocked handing packets to the decoder this window.
    win_delivery_blocked: Duration,
}

struct LateRetransmit {
    /// Time from when the gap was first seen to the retransmit's arrival.
    after_gap: Duration,
    /// The new wait, if this arrival raised it.
    raised_to: Option<Duration>,
}

impl GapWait {
    /// Field data (two cameras, lossy Wi-Fi): the camera's first retransmit
    /// lands ~0.8 s after the gap; later ones (1.5–5 s) are its backed-off
    /// re-retransmits. Waiting for those costs a multi-second freeze per loss
    /// for little gain, so the wait is capped just above the first wave.
    const MAX: Duration = Duration::from_millis(1200);
    const MARGIN: Duration = Duration::from_millis(150);
    const DECAY: Duration = Duration::from_millis(100);
    const SUMMARY_EVERY: Duration = Duration::from_secs(60);
    /// Forget skipped ids after this long (a retransmit later than this is useless).
    const REMEMBER: Duration = Duration::from_secs(5);

    fn new(configured: Duration, camera_ip: std::net::IpAddr, adaptive: bool) -> Self {
        let mut me = Self::unlearned(configured);
        me.adaptive = adaptive;
        me.camera_ip = Some(camera_ip);
        if adaptive {
            let learned = LEARNED_GAP_WAIT
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&camera_ip)
                .copied()
                .unwrap_or(configured);
            me.current = learned.clamp(configured, Self::MAX.max(configured));
        }
        me
    }

    fn unlearned(configured: Duration) -> Self {
        Self {
            adaptive: true,
            camera_ip: None,
            configured,
            current: configured,
            skipped: BTreeMap::new(),
            window_start: Instant::now(),
            win_skipped: 0,
            win_late: 0,
            win_max_late: Duration::ZERO,
            win_delivery_blocked: Duration::ZERO,
        }
    }

    /// Record time spent waiting for room in the decoder channel. Only waits
    /// over 1 ms count (an immediate send takes microseconds).
    fn on_delivery_wait(&mut self, waited: Duration) {
        if waited > Duration::from_millis(1) {
            self.win_delivery_blocked += waited;
        }
    }

    fn current(&self) -> Duration {
        self.current
    }

    fn store(&self) {
        if let Some(ip) = self.camera_ip {
            LEARNED_GAP_WAIT
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(ip, self.current);
        }
    }

    /// Packets `from..to` were skipped at `now` after waiting `waited`.
    fn on_skip(&mut self, from: u32, to: u32, now: Instant, waited: Duration) {
        let gap_seen = now.checked_sub(waited).unwrap_or(now);
        for id in from..to {
            self.skipped.insert(id, gap_seen);
        }
        self.win_skipped += u64::from(to.saturating_sub(from));
        // Bound memory: drop old entries, and never keep more than a few thousand.
        self.skipped
            .retain(|_, seen| now.saturating_duration_since(*seen) < Self::REMEMBER);
        while self.skipped.len() > 4096 {
            self.skipped.pop_first();
        }
    }

    /// Packet `id` arrived behind the contiguous point. If it was skipped,
    /// record how late it was and maybe raise the wait.
    fn on_late(&mut self, id: u32, now: Instant) -> Option<LateRetransmit> {
        let seen = self.skipped.remove(&id)?;
        let after_gap = now.saturating_duration_since(seen);
        if after_gap >= Self::REMEMBER {
            return None;
        }
        self.win_late += 1;
        self.win_max_late = self.win_max_late.max(after_gap);
        let wanted = (after_gap + Self::MARGIN).min(Self::MAX);
        let raised_to = (self.adaptive && wanted > self.current).then(|| {
            self.current = wanted;
            self.store();
            wanted
        });
        Some(LateRetransmit {
            after_gap,
            raised_to,
        })
    }

    /// Once a minute: decay the wait if no late retransmits were seen, and
    /// return a summary line if anything was skipped.
    fn minute_summary(&mut self, now: Instant) -> Option<String> {
        if now.saturating_duration_since(self.window_start) < Self::SUMMARY_EVERY {
            return None;
        }
        if self.win_late == 0 && self.current > self.configured {
            self.current = self
                .current
                .saturating_sub(Self::DECAY)
                .max(self.configured);
            self.store();
        }
        let blocked = self.win_delivery_blocked;
        let summary = (self.win_skipped > 0 || blocked >= Duration::from_millis(100)).then(|| {
            format!(
                "last minute skipped {} packet(s); {} retransmit(s) arrived after their skip (latest {}ms after the gap); gap wait now {}ms (configured {}ms); delivery to decoder blocked {}ms",
                self.win_skipped,
                self.win_late,
                self.win_max_late.as_millis(),
                self.current.as_millis(),
                self.configured.as_millis(),
                blocked.as_millis()
            )
        });
        self.window_start = now;
        self.win_skipped = 0;
        self.win_late = 0;
        self.win_max_late = Duration::ZERO;
        self.win_delivery_blocked = Duration::ZERO;
        summary
    }
}

/// Selective-ACK bitmap: one byte per packet from `first_missing` (which we
/// lack) up to the highest buffered packet — `1` = received out of order,
/// `0` = still missing — capped at [`ACK_BITMAP_CAP`] entries so the ACK fits
/// one datagram.
fn ack_bitmap<T>(first_missing: u32, recieved: &BTreeMap<u32, T>) -> Vec<u8> {
    let Some(&highest) = recieved.keys().next_back() else {
        return vec![];
    };
    let end = highest.min(first_missing.saturating_add(ACK_BITMAP_CAP - 1));
    if end < first_missing {
        return vec![];
    }
    (first_missing..=end)
        .map(|id| u8::from(recieved.contains_key(&id)))
        .collect()
}

fn set_udp_buffer_sizes(socket: &std::net::UdpSocket, size: usize) {
    let sock_ref = SockRef::from(socket);
    if let Err(e) = sock_ref.set_recv_buffer_size(size) {
        log::warn!("UDP recv buffer size not set: {e:?}");
    }
    if let Err(e) = sock_ref.set_send_buffer_size(size) {
        log::warn!("UDP send buffer size not set: {e:?}");
    }
    match sock_ref.recv_buffer_size() {
        Ok(actual) => log::info!("UDP recv buffer size: {actual}"),
        Err(e) => log::warn!("UDP recv buffer size read failed: {e:?}"),
    }
    match sock_ref.send_buffer_size() {
        Ok(actual) => log::info!("UDP send buffer size: {actual}"),
        Err(e) => log::warn!("UDP send buffer size read failed: {e:?}"),
    }
}

/// Helper to create a UdpStream
async fn connect_try_port(port: u16) -> Result<UdpSocket> {
    let mut ports: Vec<u16> = (53500..54000).collect();
    {
        let mut rng = rng();
        ports.shuffle(&mut rng);
        drop(rng); // Do not hold RNG over an await
    }

    let addrs: Vec<_> = [port]
        .iter()
        .chain(ports.iter())
        .map(|&port| SocketAddr::from(([0, 0, 0, 0], port)))
        .collect();
    let socket = UdpSocket::bind(&addrs[..]).await?;
    let std_socket = socket.into_std()?;
    std_socket.set_nonblocking(true)?;
    // Larger UDP buffers help high-bitrate streams avoid packet loss.
    const UDP_BUFFER_SIZE: usize = 4 * 1024 * 1024;
    set_udp_buffer_sizes(&std_socket, UDP_BUFFER_SIZE);

    Ok(UdpSocket::from_std(std_socket)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    const WAIT: Duration = Duration::from_millis(500);

    #[test]
    fn gap_wait_grows_only_from_late_retransmits() {
        let t0 = Instant::now();
        let mut g = GapWait::unlearned(WAIT);
        // Skip 10..12 after waiting 500 ms.
        g.on_skip(10, 12, t0 + WAIT, WAIT);
        // Unknown / never-skipped id: ignored.
        assert!(g.on_late(99, t0 + WAIT).is_none());
        assert_eq!(g.current(), WAIT);
        // Retransmit of 10 arrives 700 ms after the gap was seen.
        let late = g.on_late(10, t0 + Duration::from_millis(700)).unwrap();
        assert_eq!(late.after_gap, Duration::from_millis(700));
        assert_eq!(late.raised_to, Some(Duration::from_millis(850)));
        assert_eq!(g.current(), Duration::from_millis(850));
        // Same id again: already consumed.
        assert!(g.on_late(10, t0 + Duration::from_millis(800)).is_none());
        // An earlier retransmit does not lower it.
        let late = g.on_late(11, t0 + Duration::from_millis(600)).unwrap();
        assert_eq!(late.raised_to, None);
        assert_eq!(g.current(), Duration::from_millis(850));
    }

    #[test]
    fn gap_wait_is_remembered_per_camera() {
        let ip: std::net::IpAddr = "10.255.0.7".parse().unwrap();
        let t0 = Instant::now();
        let mut g = GapWait::new(WAIT, ip, true);
        assert_eq!(g.current(), WAIT);
        g.on_skip(1, 2, t0 + WAIT, WAIT);
        g.on_late(1, t0 + Duration::from_millis(900)).unwrap();
        // A new connection to the same camera starts from the learned value.
        assert_eq!(
            GapWait::new(WAIT, ip, true).current(),
            Duration::from_millis(1050)
        );
        // Another camera is unaffected.
        assert_eq!(
            GapWait::new(WAIT, "10.255.0.8".parse().unwrap(), true).current(),
            WAIT
        );
    }

    #[test]
    fn fixed_gap_wait_never_grows_but_still_counts_late_retransmits() {
        let ip: std::net::IpAddr = "10.255.0.9".parse().unwrap();
        let t0 = Instant::now();
        let mut g = GapWait::new(WAIT, ip, false);
        g.window_start = t0;
        g.on_skip(1, 3, t0 + WAIT, WAIT);
        let late = g.on_late(1, t0 + Duration::from_millis(900)).unwrap();
        assert_eq!(late.raised_to, None);
        assert_eq!(g.current(), WAIT);
        let s = g.minute_summary(t0 + Duration::from_secs(61)).unwrap();
        assert!(s.contains("skipped 2 packet(s); 1 retransmit"), "{s}");
        assert_eq!(GapWait::new(WAIT, ip, false).current(), WAIT);
        assert!(!ADAPTIVE_GAP_WAIT, "production gap wait must stay fixed");
    }

    #[test]
    fn gap_wait_is_capped() {
        let t0 = Instant::now();
        let mut g = GapWait::unlearned(WAIT);
        g.on_skip(1, 2, t0 + WAIT, WAIT);
        g.on_late(1, t0 + Duration::from_millis(4000)).unwrap();
        assert_eq!(g.current(), GapWait::MAX);
    }

    #[test]
    fn gap_wait_decays_back_without_late_retransmits() {
        let t0 = Instant::now();
        let mut g = GapWait::unlearned(WAIT);
        g.on_skip(1, 2, t0 + WAIT, WAIT);
        g.on_late(1, t0 + Duration::from_millis(700)).unwrap();
        g.window_start = t0;
        // Minute with a late retransmit: summary, no decay.
        let s = g.minute_summary(t0 + Duration::from_secs(61)).unwrap();
        assert!(s.contains("1 retransmit"), "{s}");
        assert_eq!(g.current(), Duration::from_millis(850));
        // Quiet minute: decays by 100 ms, no summary (nothing skipped).
        assert!(g.minute_summary(t0 + Duration::from_secs(122)).is_none());
        assert_eq!(g.current(), Duration::from_millis(750));
        // Never below the configured value.
        for i in 3..10 {
            g.minute_summary(t0 + Duration::from_secs(61 * i));
        }
        assert_eq!(g.current(), WAIT);
    }

    #[test]
    fn ack_bitmap_marks_received_and_missing() {
        let mut r = BTreeMap::new();
        assert!(ack_bitmap(10, &r).is_empty());
        r.insert(11u32, ());
        r.insert(13u32, ());
        assert_eq!(ack_bitmap(10, &r), vec![0, 1, 0, 1]);
    }

    #[test]
    fn ack_bitmap_fits_one_datagram() {
        let mut r = BTreeMap::new();
        r.insert(5000u32, ());
        let bitmap = ack_bitmap(10, &r);
        assert_eq!(bitmap.len(), ACK_BITMAP_CAP as usize);
        assert!(bitmap.len() + 28 <= MTU);
    }

    #[test]
    fn delivery_blocking_is_reported() {
        let t0 = Instant::now();
        let mut g = GapWait::unlearned(WAIT);
        g.window_start = t0;
        g.on_delivery_wait(Duration::from_micros(50)); // immediate send: ignored
        g.on_delivery_wait(Duration::from_millis(300));
        let s = g.minute_summary(t0 + Duration::from_secs(61)).unwrap();
        assert!(s.contains("blocked 300ms"), "{s}");
    }

    #[test]
    fn no_skip_without_gap() {
        let t0 = Instant::now();
        let mut r = BTreeMap::new();
        assert_eq!(gap_to_skip(5, &r, t0 + WAIT * 10, WAIT), None);
        r.insert(5u32, ((), t0));
        assert_eq!(gap_to_skip(5, &r, t0 + WAIT * 10, WAIT), None);
    }

    #[test]
    fn fresh_gap_is_not_skipped_even_after_long_idle() {
        // Regression: the gap used to be timed from the last delivery, so the
        // first out-of-order packet after a quiet period skipped instantly.
        let t0 = Instant::now();
        let mut r = BTreeMap::new();
        r.insert(518u32, ((), t0));
        assert_eq!(
            gap_to_skip(510, &r, t0 + Duration::from_millis(1), WAIT),
            None
        );
        assert_eq!(gap_to_skip(510, &r, t0 + WAIT, WAIT), Some((518, WAIT)));
    }

    #[test]
    fn gap_is_timed_from_earliest_pending_arrival() {
        let t0 = Instant::now();
        let mut r = BTreeMap::new();
        r.insert(12u32, ((), t0 + Duration::from_millis(300)));
        r.insert(15u32, ((), t0)); // arrived first: gap observed at t0
        let now = t0 + WAIT;
        assert_eq!(gap_to_skip(10, &r, now, WAIT), Some((12, WAIT)));
        assert_eq!(
            gap_to_skip(10, &r, now - Duration::from_millis(1), WAIT),
            None
        );
    }
}
