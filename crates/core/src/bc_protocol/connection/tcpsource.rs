use crate::bc::model::*;
use crate::Result;
use crate::{bc::codex::BcCodex, Credentials};
use delegate::delegate;
use futures::{sink::Sink, stream::Stream};
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::net::{TcpSocket, TcpStream};
use tokio_util::codec::{Decoder, Encoder, Framed};

pub(crate) struct TcpSource {
    inner: Framed<TcpStream, BcCodex>,
}

impl TcpSource {
    pub(crate) async fn new<T: Into<String>, U: Into<String>>(
        addr: SocketAddr,
        username: T,
        password: Option<U>,
        debug: bool,
    ) -> Result<TcpSource> {
        let stream = connect_to(addr).await?;

        let codex = if debug {
            BcCodex::new_with_debug(Credentials::new(username, password))
        } else {
            BcCodex::new(Credentials::new(username, password))
        };
        Ok(Self {
            inner: Framed::new(stream, codex),
        })
    }
}

impl Stream for TcpSource {
    type Item = std::result::Result<<BcCodex as Decoder>::Item, <BcCodex as Decoder>::Error>;

    delegate! {
        to Pin::new(&mut self.inner) {
            fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>>;
        }
    }

    delegate! {
        to self.inner {
            fn size_hint(&self) -> (usize, Option<usize>);
        }
    }
}

impl Sink<Bc> for TcpSource {
    type Error = <BcCodex as Encoder<Bc>>::Error;

    delegate! {
        to Pin::new(&mut self.inner) {
            fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
            fn start_send(mut self: Pin<&mut Self>, item: Bc) -> std::result::Result<(), Self::Error>;
            fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
            fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>>;
        }
    }
}

/// How long to wait for the TCP handshake. Without a limit an unreachable
/// camera holds the connect for the OS default (about two minutes on Linux).
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Unacknowledged data older than this closes the connection. Without it a
/// camera that loses power mid-stream leaves the socket retransmitting for
/// ~15 minutes before the kernel gives up.
#[cfg(any(target_os = "linux", target_os = "android"))]
const USER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Helper to create a TcpStream with a connect timeout
async fn connect_to(addr: SocketAddr) -> Result<TcpStream> {
    let socket = match addr {
        SocketAddr::V4(_) => TcpSocket::new_v4()?,
        SocketAddr::V6(_) => TcpSocket::new_v6()?,
    };

    let stream = tokio::time::timeout(CONNECT_TIMEOUT, socket.connect(addr))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("TCP connect to {addr} timed out"),
            )
        })??;
    tune_socket(&stream);
    Ok(stream)
}

/// Detect a vanished camera in seconds rather than minutes. Best effort: a
/// failure only keeps the OS defaults.
fn tune_socket(stream: &TcpStream) {
    let sock = socket2::SockRef::from(stream);
    // Control messages are small and latency-sensitive.
    let _ = sock.set_tcp_nodelay(true);
    let keepalive = socket2::TcpKeepalive::new().with_time(std::time::Duration::from_secs(10));
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let keepalive = keepalive
        .with_interval(std::time::Duration::from_secs(5))
        .with_retries(3);
    if let Err(e) = sock.set_tcp_keepalive(&keepalive) {
        log::debug!("Could not enable TCP keepalive: {e}");
    }
    #[cfg(any(target_os = "linux", target_os = "android"))]
    if let Err(e) = sock.set_tcp_user_timeout(Some(USER_TIMEOUT)) {
        log::debug!("Could not set TCP user timeout: {e}");
    }
}
