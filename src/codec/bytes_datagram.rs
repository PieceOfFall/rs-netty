use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    codec::{DatagramDecoder, DatagramEncoder},
    Result,
};

/// UDP datagram codec that preserves each datagram as raw bytes.
pub struct BytesDatagramCodec;

impl DatagramDecoder for BytesDatagramCodec {
    type Item = Bytes;

    fn decode_datagram(&mut self, src: &[u8]) -> Result<Self::Item> {
        Ok(Bytes::copy_from_slice(src))
    }
}

impl DatagramEncoder<Bytes> for BytesDatagramCodec {
    fn encode_datagram(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<()> {
        dst.reserve(item.len());
        dst.put(item);
        Ok(())
    }
}
