use crate::bc;
use futures::stream::StreamExt;
use log::*;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, SocketAddr};
use std::{
    collections::{HashMap, HashSet},
    sync::atomic::{AtomicBool, AtomicU16, Ordering},
    time::Duration,
};
use tokio::sync::RwLock;

use Md5Trunc::*;

mod abilityinfo;
mod battery;
mod connection;
mod credentials;
mod email;
mod errors;
mod floodlight;
mod get_enc;
mod keepalive;
mod ledstate;
mod link;
mod login;
mod logout;
mod motion;
mod ping;
mod pirstate;
mod ptz;
mod reboot;
mod resolution;
mod services;
mod siren;
mod snap;
mod stream;
mod stream_info;
mod support;
mod talk;
mod time;
mod uid;
mod users;
mod version;

pub use connection::DEFAULT_GAP_SKIP_WAIT_MS;
pub(crate) use connection::*;
pub use connection::{last_local_address, remember_local_address};
pub use credentials::*;
pub use errors::Error;
pub use ledstate::LightState;
pub use login::MaxEncryption;
pub use motion::{MotionData, MotionStatus};
pub use pirstate::PirState;
pub use ptz::Direction;
pub use resolution::*;
use std::sync::Arc;
pub use stream::{StreamData, StreamKind};

pub(crate) type Result<T> = std::result::Result<T, Error>;

/// How long a one-shot command waits for the camera's reply message before
/// giving up. Shared by the floodlight/led/users/services/pir command helpers.
pub(crate) const CMD_REPLY_TIMEOUT: Duration = Duration::from_millis(500);

/// Ceiling for the exponential reconnect/registration backoff (seconds).
const MAX_BACKOFF_SECS: u64 = 60;

/// Default BC control port used when none is configured (and always tried as a fallback).
const BC_DEFAULT_PORT: u16 = 9000;

#[derive(Clone, Copy)]
enum ReadKind {
    ReadOnly,
    ReadWrite,
    None,
}

///
/// This is the primary struct of this library when interacting with the camera
///
pub struct BcCamera {
    channel_id: u8,
    connection: Arc<BcConnection>,
    logged_in: AtomicBool,
    message_num: AtomicU16,
    // Certain commands such as logout require the username/pass in plain text.... why....???
    credentials: Credentials,
    abilities: RwLock<HashMap<String, ReadKind>>,
    /// Features (e.g. "battery", "floodlight_tasks") the camera has rejected as
    /// unsupported, so the doomed request is not re-sent on this connection.
    unsupported: RwLock<HashSet<String>>,
    /// Set to the configured UID when the camera was reached by the automatic
    /// TCP attempt at its last known address: the caller must confirm with
    /// [`BcCamera::verify_auto_tcp`] after login that the IP still belongs to
    /// this camera.
    auto_tcp_uid: Option<String>,
}

/// Options used to construct a camera
#[derive(Debug)]
pub struct BcCameraOpt {
    /// Name, mostly used for message logs
    pub name: String,
    /// Channel the camera is on 0 unless using a NVR
    pub channel_id: u8,
    /// IPs of the camera
    pub addrs: Vec<IpAddr>,
    /// The UID of the camera
    pub uid: Option<String>,
    /// Port to try optional. When not given all known BC ports will be tried
    /// When given all known bc port AND the given port will be tried
    pub port: Option<u16>,
    /// Protocol decides if UDP/TCP are used for the camera
    pub protocol: ConnectionProtocol,
    /// Discovery method to allow
    pub discovery: DiscoveryMethods,
    /// Optional relay server region (maps to a specific p2p*.reolink.com)
    pub relay_server_region: Option<String>,
    /// Maximum number of retries for discovery
    pub max_discovery_retries: usize,
    /// Credentials for login
    pub credentials: Credentials,
    /// Toggle debug print of underlying data
    pub debug: bool,
    /// UDP gap skip wait in ms for packet reassembly (None uses default).
    pub udp_gap_skip_ms: Option<u64>,
    /// Do not try TCP automatically for this UID camera (it is known to be a
    /// battery camera, which has no TCP service): go straight to UDP.
    pub skip_auto_tcp: bool,
}

/// Used to choose the print format of various status messages like battery levels
///
/// Currently this is just the format of battery levels but if we ever got more status
/// messages then they will also use this information
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PrintFormat {
    /// None, don't print
    None,
    /// A human readable output
    Human,
    /// Xml formatted
    Xml,
}

/// Type of connection to try
#[derive(Default, Debug, Clone, Copy, Serialize, Deserialize)]
pub enum ConnectionProtocol {
    /// TCP and UDP
    #[default]
    TcpUdp,
    /// TCP only
    Tcp,
    /// Udp only
    Udp,
}

enum CameraLocation {
    Tcp(SocketAddr),
    Udp(DiscoveryResult),
}

/// Connect timeout of the automatic TCP attempt at a UID camera's last known
/// address. Kept short: a sleeping battery camera never answers.
const TCP_AUTO_CONNECT_WAIT: Duration = Duration::from_millis(1500);
/// Connect timeout of the automatic TCP attempt right after UDP local
/// discovery: the camera has just answered from that IP, so it is awake and a
/// LAN TCP connect completes in milliseconds. Short so the UDP session that
/// discovery opened is not left waiting.
const TCP_AUTO_CONNECT_WAIT_AFTER_DISCOVERY: Duration = Duration::from_millis(500);

impl BcCamera {
    /// Probe TCP at `socket`; `Err(true)` = refused (no TCP service),
    /// `Err(false)` = timeout or other failure.
    async fn try_auto_tcp(
        discovery: &Discovery,
        socket: SocketAddr,
        channel_id: u8,
        connect_wait: Duration,
    ) -> std::result::Result<(), bool> {
        match tokio::time::timeout(connect_wait, tokio::net::TcpStream::connect(socket)).await {
            Ok(Ok(stream)) => drop(stream),
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::ConnectionRefused => return Err(true),
            _ => return Err(false),
        }
        discovery
            .check_tcp(socket, channel_id)
            .await
            .map_err(|_| false)
    }

    /// Whether the automatic TCP attempt applies to this camera at all:
    /// UID-only, LAN discovery, TCP allowed. Returns the UID.
    fn auto_tcp_uid(options: &BcCameraOpt) -> Option<&String> {
        let uid = options.uid.as_ref()?;
        let eligible = !options.skip_auto_tcp
            && options.addrs.is_empty()
            && matches!(options.protocol, ConnectionProtocol::TcpUdp)
            && matches!(
                options.discovery,
                DiscoveryMethods::Local | DiscoveryMethods::Debug
            );
        eligible.then_some(uid)
    }

    /// Automatic TCP attempt at `ip`:9000 for a UID-only camera, recording the
    /// outcome. `Some(socket)` = TCP works, use it.
    async fn auto_tcp_attempt(
        discovery: &Discovery,
        options: &BcCameraOpt,
        uid: &str,
        ip: IpAddr,
        connect_wait: Duration,
        what: &str,
    ) -> Option<SocketAddr> {
        let socket = SocketAddr::new(ip, BC_DEFAULT_PORT);
        match Self::try_auto_tcp(discovery, socket, options.channel_id, connect_wait).await {
            Ok(()) => {
                tcp_auto_result(uid, true, false);
                info!("{}: TCP at {what} {socket} works; using TCP", options.name);
                info!("MEDIA PATH: TCP over LAN to {} (NOT UDP P2P)", socket);
                Some(socket)
            }
            Err(refused) => {
                tcp_auto_result(uid, false, refused);
                info!(
                    "{}: no TCP at {what} {socket} ({}); using UDP",
                    options.name,
                    if refused {
                        "port closed, not retried"
                    } else {
                        "no answer, retry in 30 min"
                    }
                );
                None
            }
        }
    }

    /// After login, check that a camera reached by the automatic TCP attempt is
    /// really the configured one: DHCP may have given its old IP to another
    /// Reolink camera. On a mismatch (or if the UID cannot be read) automatic
    /// TCP is suspended for this camera and an error is returned, so the
    /// caller reconnects and the camera is found by UID over UDP instead.
    /// No-op for every other connection.
    pub async fn verify_auto_tcp(&self) -> Result<()> {
        let Some(expected) = self.auto_tcp_uid.as_ref() else {
            return Ok(());
        };
        let got = tokio::time::timeout(Duration::from_secs(5), self.get_uid()).await;
        match got {
            Ok(Ok(uid)) if uid.uid.eq_ignore_ascii_case(expected) => Ok(()),
            other => {
                tcp_auto_result(expected, false, false);
                let why = match other {
                    Ok(Ok(uid)) => format!("address now belongs to camera {}", uid.uid),
                    Ok(Err(e)) => format!("cannot read its UID: {e}"),
                    Err(_) => "timed out reading its UID".to_string(),
                };
                warn!("Automatic TCP to {expected} rejected ({why}); falling back to UDP");
                Err(Error::CannotInitCamera)
            }
        }
    }

    /// Try to connect to the camera via appropaite methods and return
    /// the location that should be used
    async fn find_camera(options: &BcCameraOpt) -> Result<CameraLocation> {
        let discovery = Discovery::new_with_region(options.relay_server_region.clone()).await?;
        if let ConnectionProtocol::Tcp | ConnectionProtocol::TcpUdp = options.protocol {
            let mut sockets = vec![];
            match options.port {
                Some(BC_DEFAULT_PORT) | None => {
                    for addr in options.addrs.iter() {
                        sockets.push(SocketAddr::new(*addr, BC_DEFAULT_PORT));
                    }
                }
                Some(n) => {
                    for addr in options.addrs.iter() {
                        sockets.push(SocketAddr::new(*addr, n));
                        sockets.push(SocketAddr::new(*addr, BC_DEFAULT_PORT));
                    }
                }
            }
            if !sockets.is_empty() {
                info!("{}: Trying TCP discovery", options.name);
                for socket in sockets.drain(..) {
                    let channel_id: u8 = options.channel_id;
                    if let Ok(addr) = discovery.check_tcp(socket, channel_id).await.map(|_| {
                        info!("{}: TCP Discovery success at {:?}", options.name, &socket);
                        socket
                    }) {
                        return Ok(CameraLocation::Tcp(addr));
                    }
                }
            }
        }

        // UID-only camera on the LAN: before falling back to UDP P2P, try TCP
        // (port 9000) at the address it last answered local discovery from.
        // Mains-powered cameras serve TCP there, and TCP retransmits lost
        // packets itself, so a lossy Wi-Fi link no longer turns into skipped
        // UDP packets and corrupted frames. Battery cameras have no TCP service:
        // a refusal disables this for the camera, a timeout defers it, so they
        // pay at most `TCP_AUTO_CONNECT_WAIT` once.
        let auto_tcp_uid = Self::auto_tcp_uid(options);
        if let Some(uid) = auto_tcp_uid.filter(|uid| tcp_auto_allowed(uid)) {
            if let Some(ip) = last_local_address(uid) {
                if let Some(socket) = Self::auto_tcp_attempt(
                    &discovery,
                    options,
                    uid,
                    ip,
                    TCP_AUTO_CONNECT_WAIT,
                    "last known address",
                )
                .await
                {
                    return Ok(CameraLocation::Tcp(socket));
                }
            }
        }

        if let (Some(uid), ConnectionProtocol::Udp | ConnectionProtocol::TcpUdp) =
            (options.uid.as_ref(), options.protocol)
        {
            let mut sockets = vec![];
            match options.port {
                None | Some(2015) | Some(2018) => {
                    for addr in options.addrs.iter() {
                        sockets.push(SocketAddr::new(*addr, 2018));
                        sockets.push(SocketAddr::new(*addr, 2015));
                    }
                }
                Some(n) => {
                    for addr in options.addrs.iter() {
                        sockets.push(SocketAddr::new(*addr, n));
                        sockets.push(SocketAddr::new(*addr, 2015));
                        sockets.push(SocketAddr::new(*addr, 2018));
                    }
                }
            }
            let (allow_local, allow_remote, allow_map, allow_relay) = match options.discovery {
                DiscoveryMethods::None => (false, false, false, false),
                DiscoveryMethods::Local => (true, false, false, false),
                DiscoveryMethods::Remote => (false, true, false, false),
                DiscoveryMethods::Map => (false, false, true, false),
                DiscoveryMethods::Relay => (false, false, false, true),
                DiscoveryMethods::Cellular => (false, false, true, true),
                DiscoveryMethods::Debug => (true, true, true, true),
            };

            let res = tokio::select! {
                Ok(v) = async {
                    let uid_local = uid.clone();
                    info!("{}: Trying local discovery", options.name);
                    let result = discovery.local(&uid_local, Some(sockets)).await;
                    match result {
                        Ok(disc) => {
                            info!(
                                "{}: Local discovery success {} at {}",
                                options.name,
                                uid_local,
                                disc.get_addr()
                            );
                            // No (usable) saved address was tried above — first
                            // run, or the camera changed IP. It has just
                            // answered from this IP, so try TCP there too.
                            if let Some(uid) = auto_tcp_uid.filter(|uid| tcp_auto_allowed(uid)) {
                                if let Some(socket) = Self::auto_tcp_attempt(
                                    &discovery,
                                    options,
                                    uid,
                                    disc.get_addr().ip(),
                                    TCP_AUTO_CONNECT_WAIT_AFTER_DISCOVERY,
                                    "discovered address",
                                )
                                .await
                                {
                                    return Ok(CameraLocation::Tcp(socket));
                                }
                            }
                            Ok(CameraLocation::Udp(disc))
                        },
                        Err(e) => Err(e)
                    }
                }, if allow_local => Ok(v),
                Ok(v) = async {
                    let mut discovery =
                        Discovery::new_with_region(options.relay_server_region.clone()).await?;
                    let reg_result;
                    // Registration is looped as it seems that reolink
                    // only updates the registration lazily when someone attempts
                    // to connect. The first few connects fails until the server data
                    // is updated
                    //
                    // We loop infinitly and allow the caller to timeout at the
                    // interval they desire
                    let mut retry = 0;
                    let max_retry: usize = options.max_discovery_retries;
                    loop {
                        tokio::task::yield_now().await;
                        if let Ok(result) = discovery.get_registration(uid).await {
                            reg_result = result;
                            break;
                        }
                        if retry >= max_retry && max_retry > 0 {
                            return Err(Error::DiscoveryTimeout);
                        }
                        // Exponential backoff: 1s, 2s, 4s, 8s, 16s, 32s, then cap at MAX_BACKOFF_SECS
                        let backoff_secs = std::cmp::min(1u64 << retry, MAX_BACKOFF_SECS);
                        log::info!("{}: Registration with reolink servers failed. Retrying in {}s: {}/{}",
                            options.name,
                            backoff_secs,
                            retry + 1,
                            if max_retry > 0 {format!("{}", max_retry)} else {"infinite".to_string()}
                        );
                        retry += 1;
                        tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
                        // New discovery to get new client IDs
                        discovery =
                            Discovery::new_with_region(options.relay_server_region.clone()).await?;
                    };
                    tokio::select! {
                        Ok(v) = async {
                            let uid_remote = uid.clone();
                            info!("{}: Trying remote discovery", options.name);
                            let result = discovery
                                .remote(&reg_result)
                                .await;
                            match result {
                                Ok(disc) => {
                                    info!(
                                        "{}: Remote discovery success {} at {}",
                                        options.name,
                                        uid_remote,
                                        disc.get_addr()
                                    );
                                    Ok(CameraLocation::Udp(disc))
                                },
                                Err(e) => Err(e)
                            }
                        }, if allow_remote => Ok(v),
                        Ok(v) = async {
                            let uid_map = uid.clone();
                            tokio::time::sleep(tokio::time::Duration::from_millis(250)).await;
                            info!("{}: Trying map discovery", options.name);
                            let result = discovery.map(&reg_result).await;
                            match result {
                                Ok(disc) => {
                                    info!(
                                        "{}: Map success {} at {}",
                                        options.name,
                                        uid_map,
                                        disc.get_addr()
                                    );
                                    Ok(CameraLocation::Udp(disc))
                                },
                                Err(e) => Err(e),
                            }
                        }, if allow_map => Ok(v),
                        Ok(v) = async {
                            let uid_relay = uid.clone();
                            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                            info!("{}: Trying relay discovery", options.name);
                            let result = discovery.relay_assisted_p2p(&reg_result).await;
                            match result {
                                Ok(disc) => {
                                    info!(
                                        "{}: Relay-assisted discovery success {} at {}",
                                        options.name,
                                        uid_relay,
                                        disc.get_addr()
                                    );
                                    Ok(CameraLocation::Udp(disc))
                                },
                                Err(e) => Err(e),
                            }
                        }, if allow_relay => Ok(v),
                        else => Err(Error::DiscoveryTimeout),
                    }
                }, if allow_remote || allow_map || allow_relay => Ok(v),
                else => Err(Error::DiscoveryTimeout),
            }?;

            return Ok(res);
        }

        info!("{}: Discovery failed", options.name);
        // Nothing works
        Err(Error::CannotInitCamera)
    }

    ///
    /// Create a new camera interface
    ///
    /// # Parameters
    ///
    /// * `options` - Camera information see [`BcCameraOpt]
    ///
    /// # Returns
    ///
    /// returns either an error or the camera
    ///
    pub async fn new(options: &BcCameraOpt) -> Result<Self> {
        let username: String = options.credentials.username.clone();
        let passwd: Option<String> = options.credentials.password.clone();
        let gap_skip_wait = Duration::from_millis(
            options
                .udp_gap_skip_ms
                .unwrap_or(connection::DEFAULT_GAP_SKIP_WAIT_MS),
        );

        let mut auto_tcp_uid = None;
        let (sink, source): (BcConnSink, BcConnSource) = {
            match BcCamera::find_camera(options).await? {
                CameraLocation::Tcp(addr) => {
                    // With no configured address, TCP can only come from the
                    // automatic attempt at the last known address.
                    if options.addrs.is_empty() {
                        auto_tcp_uid = options.uid.clone();
                    }
                    let (x, r) = TcpSource::new(addr, &username, passwd.as_ref(), options.debug)
                        .await?
                        .split();
                    (Box::new(x), Box::new(r))
                }
                CameraLocation::Udp(discovery) => {
                    let (x, r) = UdpSource::new_from_discovery(
                        discovery,
                        &username,
                        passwd.as_ref(),
                        options.debug,
                        gap_skip_wait,
                    )
                    .await?
                    .split();
                    (Box::new(x), Box::new(r))
                }
            }
        };

        let conn = BcConnection::new(sink, source).await?;

        trace!("Success");
        let me = Self {
            connection: Arc::new(conn),
            message_num: AtomicU16::new(0),
            channel_id: options.channel_id,
            logged_in: AtomicBool::new(false),
            credentials: Credentials::new(username, passwd),
            abilities: Default::default(),
            unsupported: Default::default(),
            auto_tcp_uid,
        };
        me.keepalive().await?;
        Ok(me)
    }

    /// This method will get a new message number and increment the message count atomically
    pub fn new_message_num(&self) -> u16 {
        self.message_num.fetch_add(1, Ordering::Relaxed)
    }

    fn get_connection(&self) -> Arc<BcConnection> {
        self.connection.clone()
    }

    // Certains commands like logout need the username and password
    // this command will return
    // This will only work after login
    fn get_credentials(&self) -> &Credentials {
        &self.credentials
    }

    async fn has_ability<T: Into<String>>(&self, name: T) -> ReadKind {
        let abilities = self.abilities.read().await;
        if let Some(kind) = abilities.get(&name.into()).copied() {
            kind
        } else {
            ReadKind::None
        }
    }

    /// True if this camera previously rejected `feature` as unsupported on this
    /// connection (see [`Self::mark_feature_unsupported`]).
    async fn feature_unsupported(&self, feature: &str) -> bool {
        self.unsupported.read().await.contains(feature)
    }

    /// Record that the camera rejected `feature` as unsupported, so later calls
    /// can skip the doomed request. The set lives on the `BcCamera`, so a fresh
    /// connection re-probes once and a transient rejection self-heals.
    async fn mark_feature_unsupported(&self, feature: &str) {
        self.unsupported.write().await.insert(feature.to_string());
    }
    async fn has_ability_ro<T: Into<String>>(&self, name: T) -> Result<()> {
        let s: String = name.into();
        match self.has_ability(&s).await {
            ReadKind::ReadWrite | ReadKind::ReadOnly => Ok(()),
            ReadKind::None => Err(Error::MissingAbility {
                name: s.clone(),
                requested: "read".to_string(),
                actual: "none".to_string(),
            }),
        }
    }
    async fn has_ability_rw<T: Into<String>>(&self, name: T) -> Result<()> {
        let s: String = name.into();
        match self.has_ability(&s).await {
            ReadKind::ReadWrite => Ok(()),
            ReadKind::ReadOnly => Err(Error::MissingAbility {
                name: s.clone(),
                requested: "write".to_string(),
                actual: "read".to_string(),
            }),
            ReadKind::None => Err(Error::MissingAbility {
                name: s.clone(),
                requested: "write".to_string(),
                actual: "none".to_string(),
            }),
        }
    }

    /// Wait for all thread to finish
    ///
    /// If an error is returned in any thread it will return the first error
    pub async fn join(&self) -> Result<()> {
        self.connection.join().await
    }

    /// Disconnect from the camera. This is done by sending cancel to
    /// all threads then waiting for the join
    pub async fn shutdown(&self) -> Result<()> {
        self.connection.shutdown().await?;
        Ok(())
    }
}

/// The Baichuan library has a very peculiar behavior where it always zeros the last byte.  I
/// believe this is because the MD5'ing of the user/password is a recent retrofit to the code and
/// the original code wanted to prevent a buffer overflow with strcpy.  The modern and legacy login
/// messages have a slightly different behavior; the legacy message has a 32-byte buffer and the
/// modern message uses XML.  The legacy code copies all 32 bytes with memcpy, and the XML value is
/// copied from a C-style string, so the appended null byte is dropped by the XML library - see the
/// test below.
/// Emulate this behavior by providing a configurable mangling of the last character.
#[derive(PartialEq, Eq)]
enum Md5Trunc {
    ZeroLast,
    Truncate,
}

fn md5_string(input: &str, trunc: Md5Trunc) -> String {
    let mut md5 = format!("{:X}\0", md5::compute(input));
    md5.replace_range(31.., if trunc == Truncate { "" } else { "\0" });
    md5
}

#[test]
fn test_md5_string() {
    // Note that these literals are only 31 characters long - see explanation above.
    assert_eq!(
        md5_string("admin", Truncate),
        "21232F297A57A5A743894A0E4A801FC"
    );
    assert_eq!(
        md5_string("admin", ZeroLast),
        "21232F297A57A5A743894A0E4A801FC\0"
    );
}

#[cfg(test)]
mod auto_tcp_tests {
    use super::*;

    #[tokio::test]
    async fn auto_tcp_closed_port_is_refused() {
        // Grab a free port, then close it so connecting is refused. Uses
        // 127.0.0.2 so a concurrently running test (on 127.0.0.1) cannot be
        // handed the freed port.
        let listener = std::net::TcpListener::bind("127.0.0.2:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let discovery = Discovery::new_with_region(None).await.unwrap();
        assert_eq!(
            BcCamera::try_auto_tcp(&discovery, addr, 0, TCP_AUTO_CONNECT_WAIT).await,
            Err(true)
        );
    }

    #[tokio::test]
    async fn auto_tcp_non_camera_is_not_refused() {
        // Something listens but never speaks BC: a plain failure, not "refused".
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (_sock, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
        let discovery = Discovery::new_with_region(None).await.unwrap();
        assert_eq!(
            BcCamera::try_auto_tcp(&discovery, addr, 0, TCP_AUTO_CONNECT_WAIT).await,
            Err(false)
        );
    }
}
