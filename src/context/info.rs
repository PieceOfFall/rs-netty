use std::net::SocketAddr;

/// Identity information for one TCP connection.
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

    /// Framework-assigned connection id.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Remote peer address.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Local socket address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

/// Identity information for one UDP datagram.
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

    /// Framework-assigned socket id.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Peer address for the current datagram.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Local socket address.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}
