use bytes::{Bytes, BytesMut};

use crate::{
    codec::{Decoder, Encoder},
    Error, Result,
};

pub struct FixedLengthFrameDecoder {
    frame_length: usize,
}

impl FixedLengthFrameDecoder {
    pub fn new(frame_length: usize) -> Self {
        assert!(frame_length > 0, "frame_length must be greater than zero");
        Self { frame_length }
    }

    pub fn frame_length(&self) -> usize {
        self.frame_length
    }
}

impl Decoder for FixedLengthFrameDecoder {
    type Item = Bytes;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < self.frame_length {
            return Ok(None);
        }

        Ok(Some(src.split_to(self.frame_length).freeze()))
    }
}

impl Encoder<Bytes> for FixedLengthFrameDecoder {
    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<()> {
        if item.len() != self.frame_length {
            return Err(Error::Encode(format!(
                "fixed frame length mismatch: current={}, expected={}",
                item.len(),
                self.frame_length
            )));
        }

        dst.extend_from_slice(&item);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_fixed_length_frames() {
        let mut codec = FixedLengthFrameDecoder::new(3);
        let mut buf = BytesMut::from(&b"abcdef"[..]);

        assert_eq!(
            codec.decode(&mut buf).expect("decode"),
            Some(Bytes::from_static(b"abc"))
        );
        assert_eq!(
            codec.decode(&mut buf).expect("decode"),
            Some(Bytes::from_static(b"def"))
        );
        assert!(buf.is_empty());
    }
}
