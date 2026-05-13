use std::net::AddrParseError;

/// Error type returned by rs-netty operations.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Underlying I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Socket address parsing failed.
    #[error("address parse error: {0}")]
    AddrParse(#[from] AddrParseError),

    /// Tokio task join failed.
    #[error("task join error: {0}")]
    Join(#[from] tokio::task::JoinError),

    /// Connection or socket is closed.
    #[error("connection closed")]
    Closed,

    /// Command channel is closed.
    #[error("channel closed")]
    ChannelClosed,

    /// Stream frame exceeded the configured limit.
    #[error("frame too large: current={current}, max={max}")]
    FrameTooLarge { current: usize, max: usize },

    /// Datagram payload exceeded the configured limit.
    #[error("datagram too large: current={current}, max={max}")]
    DatagramTooLarge { current: usize, max: usize },

    /// Decoder returned an error.
    #[error("decode error: {0}")]
    Decode(String),

    /// Encoder returned an error.
    #[error("encode error: {0}")]
    Encode(String),

    /// Pipeline stage returned a framework-level error.
    #[error("pipeline error: {0}")]
    Pipeline(String),

    /// Datagram write required a default peer but none was available.
    #[error("missing default peer for datagram write")]
    MissingDatagramPeer,
}

/// Convenience result alias for rs-netty operations.
pub type Result<T> = std::result::Result<T, Error>;
