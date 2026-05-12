use bytes::BytesMut;

use crate::Result;

pub mod bytes_datagram;
pub mod line;
pub mod utf8_datagram;

pub use bytes_datagram::BytesDatagramCodec;
pub use line::LineCodec;
pub use utf8_datagram::Utf8DatagramCodec;

pub trait Decoder: Send + 'static {
    type Item: Send + 'static;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>>;
}

pub trait Encoder<I>: Send + 'static {
    fn encode(&mut self, item: I, dst: &mut BytesMut) -> Result<()>;
}

pub trait DatagramDecoder: Send + 'static {
    type Item: Send + 'static;

    fn decode_datagram(&mut self, src: &[u8]) -> Result<Self::Item>;
}

pub trait DatagramEncoder<I>: Send + 'static {
    fn encode_datagram(&mut self, item: I, dst: &mut BytesMut) -> Result<()>;
}
