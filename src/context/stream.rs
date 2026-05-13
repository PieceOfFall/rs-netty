use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use tokio::sync::oneshot;

use crate::{
    channel::Channel,
    context::{
        info::{ConnInfo, DatagramInfo},
        ConnectionStats,
    },
    Result,
};

pub struct InboundContext {
    info: DatagramInfo,
}

impl InboundContext {
    pub(crate) fn new(info: ConnInfo) -> Self {
        Self {
            info: DatagramInfo::new(info.id(), info.peer_addr(), info.local_addr()),
        }
    }

    pub(crate) fn new_datagram(info: DatagramInfo) -> Self {
        Self { info }
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
}

pub struct BusinessContext {
    info: DatagramInfo,
}

impl BusinessContext {
    pub(crate) fn new(info: ConnInfo) -> Self {
        Self {
            info: DatagramInfo::new(info.id(), info.peer_addr(), info.local_addr()),
        }
    }

    pub(crate) fn new_datagram(info: DatagramInfo) -> Self {
        Self { info }
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
}

pub struct OutboundContext {
    info: DatagramInfo,
}

impl OutboundContext {
    pub(crate) fn new(info: ConnInfo) -> Self {
        Self {
            info: DatagramInfo::new(info.id(), info.peer_addr(), info.local_addr()),
        }
    }

    pub(crate) fn new_datagram(info: DatagramInfo) -> Self {
        Self { info }
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
}

pub struct Context<W> {
    info: ConnInfo,
    channel: Channel<W>,
    outbox: StreamOutboxHandle<W>,
    close_requested: bool,
}

impl<W: Send + 'static> Context<W> {
    pub(crate) fn new(info: ConnInfo, channel: Channel<W>) -> Self {
        Self {
            info,
            channel,
            outbox: StreamOutboxHandle::new(),
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

    pub fn channel(&self) -> Channel<W> {
        self.channel.clone()
    }

    pub fn stats(&self) -> Option<ConnectionStats> {
        self.channel.stats()
    }

    pub async fn write(&mut self, msg: W) -> Result<()> {
        self.outbox.push_write(msg);
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<()> {
        let rx = self.outbox.push_flush();
        rx.await.unwrap_or(Err(crate::Error::ChannelClosed))
    }

    pub async fn write_and_flush(&mut self, msg: W) -> Result<()> {
        self.outbox.push_write(msg);
        self.flush().await
    }

    pub async fn close(&mut self) -> Result<()> {
        self.close_requested = true;
        Ok(())
    }

    pub(crate) fn outbox(&self) -> StreamOutboxHandle<W> {
        self.outbox.clone()
    }

    pub(crate) fn close_requested(&self) -> bool {
        self.close_requested
    }
}

pub(crate) enum StreamOutboxCommand<W> {
    Write(W),
    Flush(oneshot::Sender<Result<()>>),
}

pub(crate) struct StreamOutboxHandle<W> {
    inner: Arc<Mutex<VecDeque<StreamOutboxCommand<W>>>>,
}

impl<W> Clone for StreamOutboxHandle<W> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<W> StreamOutboxHandle<W> {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    fn push_write(&self, msg: W) {
        self.inner
            .lock()
            .expect("stream outbox lock poisoned")
            .push_back(StreamOutboxCommand::Write(msg));
    }

    fn push_flush(&self) -> oneshot::Receiver<Result<()>> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .lock()
            .expect("stream outbox lock poisoned")
            .push_back(StreamOutboxCommand::Flush(tx));
        rx
    }

    pub(crate) fn has_flush_command(&self) -> bool {
        self.inner
            .lock()
            .expect("stream outbox lock poisoned")
            .iter()
            .any(|command| matches!(command, StreamOutboxCommand::Flush(_)))
    }

    pub(crate) fn take_commands(&self) -> VecDeque<StreamOutboxCommand<W>> {
        std::mem::take(&mut *self.inner.lock().expect("stream outbox lock poisoned"))
    }
}
