use std::net::AddrParseError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("address parse error: {0}")]
    AddrParse(#[from] AddrParseError),

    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    #[error("connection closed")]
    Closed,

    #[error("channel closed")]
    ChannelClosed,

    #[error("frame too large: current={current}, max={max}")]
    FrameTooLarge { current: usize, max: usize },

    #[error("datagram too large: current={current}, max={max}")]
    DatagramTooLarge { current: usize, max: usize },

    #[error("decode error: {0}")]
    Decode(String),

    #[error("encode error: {0}")]
    Encode(String),

    #[error("pipeline error: {0}")]
    Pipeline(String),

    #[error("missing default peer for datagram write")]
    MissingDatagramPeer,
}

pub type Result<T> = std::result::Result<T, Error>;
