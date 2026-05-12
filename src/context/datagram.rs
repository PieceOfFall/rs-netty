use std::net::SocketAddr;

use crate::{channel::DatagramChannel, context::DatagramInfo, Result};

pub struct DatagramContext<W> {
    info: DatagramInfo,
    channel: DatagramChannel<W>,
    pending_writes: Vec<(SocketAddr, W)>,
    close_requested: bool,
}

impl<W: Send + 'static> DatagramContext<W> {
    pub(crate) fn new(info: DatagramInfo, channel: DatagramChannel<W>) -> Self {
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

    pub fn channel(&self) -> DatagramChannel<W> {
        self.channel.clone()
    }

    pub async fn write(&mut self, msg: W) -> Result<()> {
        self.pending_writes.push((self.info.peer_addr(), msg));
        Ok(())
    }

    pub async fn write_to(&mut self, peer_addr: SocketAddr, msg: W) -> Result<()> {
        self.pending_writes.push((peer_addr, msg));
        Ok(())
    }

    pub async fn close(&mut self) -> Result<()> {
        self.close_requested = true;
        Ok(())
    }

    pub(crate) fn take_pending_writes(&mut self) -> Vec<(SocketAddr, W)> {
        std::mem::take(&mut self.pending_writes)
    }

    pub(crate) fn close_requested(&self) -> bool {
        self.close_requested
    }
}
