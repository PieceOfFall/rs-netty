use bytes::{BufMut, BytesMut};

use crate::{
    codec::{Decoder, Encoder},
    Error, Result,
};

/// UTF-8 line codec for newline-delimited TCP streams.
///
/// Decoding returns one line at a time, strips the trailing `\n`, and also
/// strips a preceding `\r` for CRLF input. Encoding appends `\n`.
pub struct LineCodec {
    max_line_len: usize,
}

impl LineCodec {
    /// Creates a line codec with an 8 KiB maximum line length.
    pub fn new() -> Self {
        Self {
            max_line_len: 8 * 1024,
        }
    }

    /// Creates a line codec with a custom maximum decoded line length in bytes.
    pub fn with_max_line_len(max_line_len: usize) -> Self {
        Self { max_line_len }
    }
}

impl Default for LineCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for LineCodec {
    type Item = String;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let Some(pos) = src.iter().position(|b| *b == b'\n') else {
            if src.len() > self.max_line_len {
                return Err(Error::FrameTooLarge {
                    current: src.len(),
                    max: self.max_line_len,
                });
            }

            return Ok(None);
        };

        if pos > self.max_line_len {
            return Err(Error::FrameTooLarge {
                current: pos,
                max: self.max_line_len,
            });
        }

        let mut line = src.split_to(pos + 1);
        line.truncate(pos);

        if line.last() == Some(&b'\r') {
            line.truncate(line.len() - 1);
        }

        let line =
            String::from_utf8(line.to_vec()).map_err(|err| Error::Decode(err.to_string()))?;
        Ok(Some(line))
    }
}

impl Encoder<String> for LineCodec {
    fn encode(&mut self, item: String, dst: &mut BytesMut) -> Result<()> {
        dst.reserve(item.len() + 1);
        dst.put_slice(item.as_bytes());
        dst.put_u8(b'\n');
        Ok(())
    }
}
