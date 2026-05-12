use bytes::BytesMut;

use crate::Result;

pub mod line;

pub use line::LineCodec;

pub trait Decoder: Send + 'static {
    type Item: Send + 'static;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>>;
}

pub trait Encoder<I>: Send + 'static {
    fn encode(&mut self, item: I, dst: &mut BytesMut) -> Result<()>;
}
