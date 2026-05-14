use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::sync::oneshot;

use crate::{channel::DatagramChannel, context::DatagramInfo, Result};

/// Context passed to a UDP [`crate::DatagramHandler`].
///
/// Writes through this context are staged in a handler-local outbox. They are
/// sent when the handler returns, or earlier when [`Self::flush`] or a
/// `*_and_flush` method is awaited.
pub struct DatagramContext<W> {
    info: DatagramInfo,
    channel: DatagramChannel<W>,
    outbox: DatagramOutboxHandle<W>,
    close_requested: bool,
}

impl<W: Send + 'static> DatagramContext<W> {
    pub(crate) fn new(info: DatagramInfo, channel: DatagramChannel<W>) -> Self {
        Self {
            info,
            channel,
            outbox: DatagramOutboxHandle::new(),
            close_requested: false,
        }
    }

    /// Socket id assigned by the UDP runtime.
    pub fn id(&self) -> u64 {
        self.info.id()
    }

    /// Peer address for the current datagram.
    pub fn peer_addr(&self) -> SocketAddr {
        self.info.peer_addr()
    }

    /// Local socket address.
    pub fn local_addr(&self) -> SocketAddr {
        self.info.local_addr()
    }

    /// Returns a cloneable channel for writing from outside the current handler.
    pub fn channel(&self) -> DatagramChannel<W> {
        self.channel.clone()
    }

    /// Stages a response to the current datagram peer.
    ///
    /// The message is stored in the handler-local outbox and is sent when the
    /// handler returns or when the outbox is explicitly flushed.
    pub async fn write(&mut self, msg: W) -> Result<()> {
        self.outbox.push_write(self.info.peer_addr(), msg);
        Ok(())
    }

    /// Stages a datagram for an explicit peer.
    ///
    /// Use this when a handler needs to reply somewhere other than the sender
    /// of the current datagram.
    pub async fn write_to(&mut self, peer_addr: SocketAddr, msg: W) -> Result<()> {
        self.outbox.push_write(peer_addr, msg);
        Ok(())
    }

    /// Sends messages staged by this handler so far.
    ///
    /// The returned result is acknowledged by the socket task after `send_to`
    /// completes for all staged messages before this flush command.
    pub async fn flush(&mut self) -> Result<()> {
        let rx = self.outbox.push_flush();
        rx.await.unwrap_or(Err(crate::Error::ChannelClosed))
    }

    /// Stages a response to the current peer and waits until staged messages are sent.
    pub async fn write_and_flush(&mut self, msg: W) -> Result<()> {
        self.outbox.push_write(self.info.peer_addr(), msg);
        self.flush().await
    }

    /// Stages a datagram for an explicit peer and waits until staged messages are sent.
    pub async fn write_to_and_flush(&mut self, peer_addr: SocketAddr, msg: W) -> Result<()> {
        self.outbox.push_write(peer_addr, msg);
        self.flush().await
    }

    /// Requests that the socket task close after the current handler returns.
    pub async fn close(&mut self) -> Result<()> {
        self.close_requested = true;
        Ok(())
    }

    pub(crate) fn outbox(&self) -> DatagramOutboxHandle<W> {
        self.outbox.clone()
    }

    pub(crate) fn close_requested(&self) -> bool {
        self.close_requested
    }
}

pub(crate) enum DatagramOutboxCommand<W> {
    WriteTo(SocketAddr, W),
    Flush(oneshot::Sender<Result<()>>),
}

pub(crate) struct DatagramOutboxHandle<W> {
    inner: Arc<Mutex<VecDeque<DatagramOutboxCommand<W>>>>,
}

impl<W> Clone for DatagramOutboxHandle<W> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<W> DatagramOutboxHandle<W> {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn push_write(&self, peer_addr: SocketAddr, msg: W) {
        self.inner
            .lock()
            .expect("datagram outbox lock poisoned")
            .push_back(DatagramOutboxCommand::WriteTo(peer_addr, msg));
    }

    fn push_flush(&self) -> oneshot::Receiver<Result<()>> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .lock()
            .expect("datagram outbox lock poisoned")
            .push_back(DatagramOutboxCommand::Flush(tx));
        rx
    }

    pub(crate) fn has_flush_command(&self) -> bool {
        self.inner
            .lock()
            .expect("datagram outbox lock poisoned")
            .iter()
            .any(|command| matches!(command, DatagramOutboxCommand::Flush(_)))
    }

    pub(crate) fn take_commands(&self) -> VecDeque<DatagramOutboxCommand<W>> {
        std::mem::take(&mut *self.inner.lock().expect("datagram outbox lock poisoned"))
    }
}
