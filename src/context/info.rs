use std::net::SocketAddr;

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

#[derive(Clone, Copy)]
pub struct DatagramInfo {
    id: u64,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
}

impl DatagramInfo {
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
