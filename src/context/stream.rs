use std::net::SocketAddr;

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
    pending_writes: Vec<W>,
    close_requested: bool,
}

impl<W: Send + 'static> Context<W> {
    pub(crate) fn new(info: ConnInfo, channel: Channel<W>) -> Self {
        Self {
            info,
            channel,
            pending_writes: Vec::new(),
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
        self.pending_writes.push(msg);
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        self.close_requested = true;
        Ok(())
    }

    pub(crate) fn take_pending_writes(&mut self) -> Vec<W> {
        std::mem::take(&mut self.pending_writes)
    }

    pub(crate) fn close_requested(&self) -> bool {
        self.close_requested
    }
}
