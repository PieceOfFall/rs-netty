use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::{channel::command::DatagramCommand, Error, Result};

/// Handle for writing UDP datagrams through one socket pipeline.
///
/// Cloning a channel is cheap. Sends are routed through a bounded Tokio mpsc
/// queue owned by the socket task.
pub struct DatagramChannel<W> {
    id: u64,
    local_addr: SocketAddr,
    tx: mpsc::Sender<DatagramCommand<W>>,
}

impl<W> Clone for DatagramChannel<W> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            local_addr: self.local_addr,
            tx: self.tx.clone(),
        }
    }
}

impl<W: Send + 'static> DatagramChannel<W> {
    pub(crate) fn new(
        id: u64,
        local_addr: SocketAddr,
        tx: mpsc::Sender<DatagramCommand<W>>,
    ) -> Self {
        Self { id, local_addr, tx }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    /// Local socket address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns whether the socket task has closed its command queue.
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

    /// Queues a datagram for an explicit peer.
    pub async fn write_to(&self, peer_addr: SocketAddr, msg: W) -> Result<()> {
        self.tx
            .send(DatagramCommand::WriteTo(peer_addr, msg))
            .await
            .map_err(|_| Error::ChannelClosed)
    }

    /// Queues a datagram and waits until the socket task has sent it.
    pub async fn write_to_and_flush(&self, peer_addr: SocketAddr, msg: W) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(DatagramCommand::WriteToAndFlush(peer_addr, msg, tx))
            .await
            .map_err(|_| Error::ChannelClosed)?;
        rx.await.map_err(|_| Error::ChannelClosed)?
    }

    /// Requests local socket shutdown.
    pub async fn close(&self) -> Result<()> {
        self.tx
            .send(DatagramCommand::Close)
            .await
            .map_err(|_| Error::ChannelClosed)
    }
}
