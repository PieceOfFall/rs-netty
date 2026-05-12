use std::net::SocketAddr;

use crate::{channel::Channel, Result};

#[derive(Clone, Copy)]
pub struct ConnInfo {
    id: u64,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
}

impl ConnInfo {
    pub(crate) fn new(id: u64, peer_addr: SocketAddr, local_addr: SocketAddr) -> Self {
        Self {
            id,
            peer_addr,
            local_addr,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

pub struct InboundContext {
    info: ConnInfo,
}

impl InboundContext {
    pub(crate) fn new(info: ConnInfo) -> Self {
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
    info: ConnInfo,
}

impl BusinessContext {
    pub(crate) fn new(info: ConnInfo) -> Self {
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
    info: ConnInfo,
}

impl OutboundContext {
    pub(crate) fn new(info: ConnInfo) -> Self {
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
