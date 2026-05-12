use std::net::SocketAddr;

pub(crate) enum StreamCommand<W> {
    Write(W),
    Close,
}

pub(crate) enum DatagramCommand<W> {
    WriteTo(SocketAddr, W),
    Close,
}
