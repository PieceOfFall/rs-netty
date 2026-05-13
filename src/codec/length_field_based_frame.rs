use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::{
    codec::{Decoder, Encoder},
    Error, Result,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ByteOrder {
    #[default]
    BigEndian,
    LittleEndian,
}

pub struct LengthFieldBasedFrameDecoder {
    max_frame_length: usize,
    length_field_offset: usize,
    length_field_length: usize,
    length_adjustment: isize,
    initial_bytes_to_strip: usize,
    byte_order: ByteOrder,
}

impl LengthFieldBasedFrameDecoder {
    pub fn new(
        max_frame_length: usize,
        length_field_offset: usize,
        length_field_length: usize,
    ) -> Self {
        Self {
            max_frame_length,
            length_field_offset,
            length_field_length,
            length_adjustment: 0,
            initial_bytes_to_strip: length_field_offset + length_field_length,
            byte_order: ByteOrder::BigEndian,
        }
    }

    pub fn with_adjustment(
        max_frame_length: usize,
        length_field_offset: usize,
        length_field_length: usize,
        length_adjustment: isize,
        initial_bytes_to_strip: usize,
    ) -> Self {
        Self {
            max_frame_length,
            length_field_offset,
            length_field_length,
            length_adjustment,
            initial_bytes_to_strip,
            byte_order: ByteOrder::BigEndian,
        }
    }

    pub fn byte_order(mut self, byte_order: ByteOrder) -> Self {
        self.byte_order = byte_order;
        self
    }

    fn read_frame_length(&self, src: &BytesMut) -> Result<usize> {
        let start = self.length_field_offset;
        let end = start + self.length_field_length;
        let bytes = &src[start..end];

        let len = match (self.byte_order, self.length_field_length) {
            (ByteOrder::BigEndian, 1) | (ByteOrder::LittleEndian, 1) => bytes[0] as u64,
            (ByteOrder::BigEndian, 2) => u16::from_be_bytes([bytes[0], bytes[1]]) as u64,
            (ByteOrder::LittleEndian, 2) => u16::from_le_bytes([bytes[0], bytes[1]]) as u64,
            (ByteOrder::BigEndian, 3) => {
                ((bytes[0] as u64) << 16) | ((bytes[1] as u64) << 8) | bytes[2] as u64
            }
            (ByteOrder::LittleEndian, 3) => {
                ((bytes[2] as u64) << 16) | ((bytes[1] as u64) << 8) | bytes[0] as u64
            }
            (ByteOrder::BigEndian, 4) => {
                u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64
            }
            (ByteOrder::LittleEndian, 4) => {
                u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as u64
            }
            (ByteOrder::BigEndian, 8) => u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            (ByteOrder::LittleEndian, 8) => u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            _ => {
                return Err(Error::Decode(format!(
                    "unsupported length field length: {}",
                    self.length_field_length
                )))
            }
        };

        usize::try_from(len).map_err(|err| Error::Decode(err.to_string()))
    }

    fn write_frame_length(&self, item_len: usize, dst: &mut BytesMut) -> Result<()> {
        if self.length_field_offset != 0 || self.length_adjustment != 0 {
            return Err(Error::Encode(
                "LengthFieldBasedFrameDecoder encoder supports zero offset and zero adjustment only".to_string(),
            ));
        }

        write_length(item_len, self.length_field_length, self.byte_order, dst)
    }
}

impl Default for LengthFieldBasedFrameDecoder {
    fn default() -> Self {
        Self::new(8 * 1024 * 1024, 0, 4)
    }
}

impl Decoder for LengthFieldBasedFrameDecoder {
    type Item = Bytes;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let field_end = self.length_field_offset + self.length_field_length;
        if src.len() < field_end {
            return Ok(None);
        }

        let frame_length = self.read_frame_length(src)?;
        let adjusted = frame_length
            .checked_add_signed(self.length_adjustment)
            .ok_or_else(|| Error::Decode("negative adjusted frame length".to_string()))?;
        let frame_end = field_end
            .checked_add(adjusted)
            .ok_or_else(|| Error::Decode("frame length overflow".to_string()))?;

        if frame_end > self.max_frame_length {
            return Err(Error::FrameTooLarge {
                current: frame_end,
                max: self.max_frame_length,
            });
        }

        if src.len() < frame_end {
            return Ok(None);
        }

        if self.initial_bytes_to_strip > frame_end {
            return Err(Error::Decode(format!(
                "initial_bytes_to_strip={} exceeds frame length={frame_end}",
                self.initial_bytes_to_strip
            )));
        }

        let mut frame = src.split_to(frame_end);
        frame.advance(self.initial_bytes_to_strip);
        Ok(Some(frame.freeze()))
    }
}

impl Encoder<Bytes> for LengthFieldBasedFrameDecoder {
    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<()> {
        self.write_frame_length(item.len(), dst)?;
        dst.extend_from_slice(&item);
        Ok(())
    }
}

pub struct LengthFieldPrepender {
    length_field_length: usize,
    byte_order: ByteOrder,
}

impl LengthFieldPrepender {
    pub fn new(length_field_length: usize) -> Self {
        Self {
            length_field_length,
            byte_order: ByteOrder::BigEndian,
        }
    }

    pub fn byte_order(mut self, byte_order: ByteOrder) -> Self {
        self.byte_order = byte_order;
        self
    }
}

impl Encoder<Bytes> for LengthFieldPrepender {
    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<()> {
        write_length(item.len(), self.length_field_length, self.byte_order, dst)?;
        dst.extend_from_slice(&item);
        Ok(())
    }
}

fn write_length(
    len: usize,
    length_field_length: usize,
    byte_order: ByteOrder,
    dst: &mut BytesMut,
) -> Result<()> {
    match (byte_order, length_field_length) {
        (ByteOrder::BigEndian, 1) | (ByteOrder::LittleEndian, 1) => {
            let len = u8::try_from(len).map_err(|err| Error::Encode(err.to_string()))?;
            dst.put_u8(len);
        }
        (ByteOrder::BigEndian, 2) => {
            let len = u16::try_from(len).map_err(|err| Error::Encode(err.to_string()))?;
            dst.put_u16(len);
        }
        (ByteOrder::LittleEndian, 2) => {
            let len = u16::try_from(len).map_err(|err| Error::Encode(err.to_string()))?;
            dst.put_u16_le(len);
        }
        (ByteOrder::BigEndian, 3) => {
            if len > 0x00ff_ffff {
                return Err(Error::Encode(format!("length {len} exceeds 24-bit field")));
            }
            dst.put_u8(((len >> 16) & 0xff) as u8);
            dst.put_u8(((len >> 8) & 0xff) as u8);
            dst.put_u8((len & 0xff) as u8);
        }
        (ByteOrder::LittleEndian, 3) => {
            if len > 0x00ff_ffff {
                return Err(Error::Encode(format!("length {len} exceeds 24-bit field")));
            }
            dst.put_u8((len & 0xff) as u8);
            dst.put_u8(((len >> 8) & 0xff) as u8);
            dst.put_u8(((len >> 16) & 0xff) as u8);
        }
        (ByteOrder::BigEndian, 4) => {
            let len = u32::try_from(len).map_err(|err| Error::Encode(err.to_string()))?;
            dst.put_u32(len);
        }
        (ByteOrder::LittleEndian, 4) => {
            let len = u32::try_from(len).map_err(|err| Error::Encode(err.to_string()))?;
            dst.put_u32_le(len);
        }
        (ByteOrder::BigEndian, 8) => {
            let len = u64::try_from(len).map_err(|err| Error::Encode(err.to_string()))?;
            dst.put_u64(len);
        }
        (ByteOrder::LittleEndian, 8) => {
            let len = u64::try_from(len).map_err(|err| Error::Encode(err.to_string()))?;
            dst.put_u64_le(len);
        }
        _ => {
            return Err(Error::Encode(format!(
                "unsupported length field length: {length_field_length}"
            )))
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_and_encodes_length_prefixed_frame() {
        let mut codec = LengthFieldBasedFrameDecoder::default();
        let mut buf = BytesMut::new();

        codec
            .encode(Bytes::from_static(b"ping"), &mut buf)
            .expect("encode");
        assert_eq!(&buf[..], b"\0\0\0\x04ping");

        let frame = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(frame, Bytes::from_static(b"ping"));
        assert!(buf.is_empty());
    }
}
