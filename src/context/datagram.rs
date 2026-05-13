use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::sync::oneshot;

use crate::{channel::DatagramChannel, context::DatagramInfo, Result};

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

    pub fn id(&self) -> u64 {
        self.info.id()
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.info.peer_addr()
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.info.local_addr()
    }

    pub fn channel(&self) -> DatagramChannel<W> {
        self.channel.clone()
    }

    pub async fn write(&mut self, msg: W) -> Result<()> {
        self.outbox.push_write(self.info.peer_addr(), msg);
        Ok(())
    }

    pub async fn write_to(&mut self, peer_addr: SocketAddr, msg: W) -> Result<()> {
        self.outbox.push_write(peer_addr, msg);
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        let rx = self.outbox.push_flush();
        rx.await.unwrap_or(Err(crate::Error::ChannelClosed))
    }

    pub async fn write_and_flush(&mut self, msg: W) -> Result<()> {
        self.outbox.push_write(self.info.peer_addr(), msg);
        self.flush().await
    }

    pub async fn write_to_and_flush(&mut self, peer_addr: SocketAddr, msg: W) -> Result<()> {
        self.outbox.push_write(peer_addr, msg);
        self.flush().await
    }

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
