use std::net::SocketAddr;

use tokio::sync::oneshot;

use crate::Result;

pub(crate) enum StreamCommand<W> {
    Write(W),
    WriteAndFlush(W, oneshot::Sender<Result<()>>),
    Close,
}

pub(crate) enum DatagramCommand<W> {
    WriteTo(SocketAddr, W),
    WriteToAndFlush(SocketAddr, W, oneshot::Sender<Result<()>>),
    Close,
}
