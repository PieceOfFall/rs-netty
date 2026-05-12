use bytes::{BufMut, BytesMut};

use crate::{
    codec::{DatagramDecoder, DatagramEncoder},
    Error, Result,
};

pub struct Utf8DatagramCodec;

impl DatagramDecoder for Utf8DatagramCodec {
    type Item = String;

    fn decode_datagram(&mut self, src: &[u8]) -> Result<Self::Item> {
        String::from_utf8(src.to_vec()).map_err(|err| Error::Decode(err.to_string()))
    }
}

impl DatagramEncoder<String> for Utf8DatagramCodec {
    fn encode_datagram(&mut self, item: String, dst: &mut BytesMut) -> Result<()> {
        dst.reserve(item.len());
        dst.put_slice(item.as_bytes());
        Ok(())
    }
}
