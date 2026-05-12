use std::net::SocketAddr;

use tokio::sync::mpsc;

use crate::{Error, Result};

pub(crate) enum Command<W> {
    Write(W),
    Close,
}

pub struct Channel<W> {
    id: u64,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
    tx: mpsc::Sender<Command<W>>,
}

impl<W> Clone for Channel<W> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            peer_addr: self.peer_addr,
            local_addr: self.local_addr,
            tx: self.tx.clone(),
        }
    }
}

impl<W: Send + 'static> Channel<W> {
    pub(crate) fn new(
        id: u64,
        peer_addr: SocketAddr,
        local_addr: SocketAddr,
        tx: mpsc::Sender<Command<W>>,
    ) -> Self {
        Self {
            id,
            peer_addr,
            local_addr,
            tx,
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

    pub async fn write(&self, msg: W) -> Result<()> {
        self.tx
            .send(Command::Write(msg))
            .await
            .map_err(|_| Error::Closed)
    }

    pub async fn close(&self) -> Result<()> {
        self.tx
            .send(Command::Close)
            .await
            .map_err(|_| Error::Closed)
    }
}
