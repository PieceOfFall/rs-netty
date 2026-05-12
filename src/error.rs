use std::net::AddrParseError;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("address parse error: {0}")]
    AddrParse(#[from] AddrParseError),

    #[error("connection closed")]
    Closed,

    #[error("frame too large: current={current}, max={max}")]
    FrameTooLarge { current: usize, max: usize },

    #[error("decode error: {0}")]
    Decode(String),

    #[error("encode error: {0}")]
    Encode(String),

    #[error("pipeline error: {0}")]
    Pipeline(String),
}

pub type Result<T> = std::result::Result<T, Error>;
