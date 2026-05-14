use bytes::{Bytes, BytesMut};

use crate::{
    codec::{Decoder, Encoder},
    Result,
};

/// Byte stream drain codec.
///
/// Decoding returns all bytes currently buffered as one [`Bytes`] value. This
/// is useful for protocols that already have an external message boundary or
/// for tests; it is not a framing codec for arbitrary TCP streams.
pub struct ByteArrayDecoder;

impl Decoder for ByteArrayDecoder {
    type Item = Bytes;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.is_empty() {
            return Ok(None);
        }

        Ok(Some(src.split().freeze()))
    }
}

impl Encoder<Bytes> for ByteArrayDecoder {
    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<()> {
        dst.extend_from_slice(&item);
        Ok(())
    }
}

/// Pass-through byte encoder.
pub struct ByteArrayEncoder;

impl Encoder<Bytes> for ByteArrayEncoder {
    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<()> {
        dst.extend_from_slice(&item);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drains_available_bytes() {
        let mut codec = ByteArrayDecoder;
        let mut buf = BytesMut::from(&b"abc"[..]);

        assert_eq!(
            codec.decode(&mut buf).expect("decode"),
            Some(Bytes::from_static(b"abc"))
        );
        assert!(buf.is_empty());
    }
}
