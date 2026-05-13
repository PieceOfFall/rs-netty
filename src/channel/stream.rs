use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::{channel::command::StreamCommand, context::ConnectionStats, Error, Result};

/// Handle for writing to or closing one TCP connection.
///
/// Cloning a channel is cheap. Sends are routed through a bounded Tokio mpsc
/// queue owned by the connection task.
pub struct Channel<W> {
    id: u64,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
    tx: mpsc::Sender<StreamCommand<W>>,
    stats: Option<ConnectionStats>,
}

impl<W> Clone for Channel<W> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            peer_addr: self.peer_addr,
            local_addr: self.local_addr,
            tx: self.tx.clone(),
            stats: self.stats.clone(),
        }
    }
}

impl<W: Send + 'static> Channel<W> {
    pub(crate) fn new(
        id: u64,
        peer_addr: SocketAddr,
        local_addr: SocketAddr,
        tx: mpsc::Sender<StreamCommand<W>>,
        stats: Option<ConnectionStats>,
    ) -> Self {
        Self {
            id,
            peer_addr,
            local_addr,
            tx,
            stats,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Remote peer address for this connection.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Local socket address for this connection.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns whether the connection task has closed its command queue.
    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    /// Remaining outbound queue capacity.
    pub fn capacity(&self) -> usize {
        self.tx.capacity()
    }

    /// Configured outbound queue capacity.
    pub fn max_capacity(&self) -> usize {
        self.tx.max_capacity()
    }

    /// Connection stats when tracking was enabled on the server/client.
    pub fn stats(&self) -> Option<ConnectionStats> {
        self.stats.clone()
    }

    /// Queues a message for the connection task to encode and write.
    ///
    /// This method waits for queue capacity, but does not wait for the socket
    /// write to complete. Use [`Self::write_and_flush`] when the caller needs an
    /// acknowledgement from the connection task.
    pub async fn write(&self, msg: W) -> Result<()> {
        self.tx
            .send(StreamCommand::Write(msg))
            .await
            .map_err(|_| Error::ChannelClosed)
    }

    /// Queues a message and waits until the connection task has flushed it.
    pub async fn write_and_flush(&self, msg: W) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(StreamCommand::WriteAndFlush(msg, tx))
            .await
            .map_err(|_| Error::ChannelClosed)?;
        rx.await.map_err(|_| Error::ChannelClosed)?
    }

    /// Requests local connection shutdown.
    pub async fn close(&self) -> Result<()> {
        self.tx
            .send(StreamCommand::Close)
            .await
            .map_err(|_| Error::ChannelClosed)
    }
}
