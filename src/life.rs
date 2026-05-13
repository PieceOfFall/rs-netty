use std::{future::Future, net::SocketAddr};

use crate::{context::ConnInfo, Result};

/// Reason a TCP connection stopped.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseReason {
    /// The peer closed the TCP stream.
    PeerClosed,
    /// The local channel requested close.
    LocalClosed,
    /// All command senders were dropped.
    ChannelClosed,
    /// The handler requested close through its context.
    HandlerClosed,
    /// The owning server requested shutdown.
    ServerShutdown,
    /// The configured idle timeout elapsed.
    IdleTimeout,
    /// An I/O error occurred.
    IoError,
    /// Decoding failed.
    DecodeError,
    /// Encoding failed.
    EncodeError,
    /// Buffered frame size exceeded the configured maximum.
    FrameTooLarge,
    /// A handler or pipeline stage returned an error.
    HandlerError,
}

/// Default lifecycle hook implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoLife;

/// Lifecycle hooks for servers, connections, and UDP sockets.
///
/// All methods have no-op defaults. Hook failures during startup/opening are
/// returned to the caller; hook failures during shutdown are logged and the
/// original close result is preserved where possible.
pub trait Life: Clone + Send + Sync + 'static {
    /// Called after a TCP server successfully binds.
    fn tcp_server_started(
        &self,
        _local_addr: SocketAddr,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Called after a TCP server has stopped accepting and joined connections.
    fn tcp_server_stopped(
        &self,
        _local_addr: SocketAddr,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Called after a TCP connection is accepted or established.
    fn tcp_connection_opened(&self, _info: ConnInfo) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Called when a TCP connection is closing.
    fn tcp_connection_closed(
        &self,
        _info: ConnInfo,
        _reason: CloseReason,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Called after a UDP socket task starts.
    fn udp_socket_started(
        &self,
        _local_addr: SocketAddr,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Called after a UDP socket task stops.
    fn udp_socket_stopped(
        &self,
        _local_addr: SocketAddr,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

impl Life for NoLife {}
