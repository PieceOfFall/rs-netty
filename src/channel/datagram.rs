use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::{channel::command::DatagramCommand, Error, Result};

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

    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub async fn write_to(&self, peer_addr: SocketAddr, msg: W) -> Result<()> {
        self.tx
            .send(DatagramCommand::WriteTo(peer_addr, msg))
            .await
            .map_err(|_| Error::ChannelClosed)
    }

    pub async fn close(&self) -> Result<()> {
        self.tx
            .send(DatagramCommand::Close)
            .await
            .map_err(|_| Error::ChannelClosed)
    }
}
