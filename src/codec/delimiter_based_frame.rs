use bytes::{Bytes, BytesMut};

use crate::{
    codec::{Decoder, Encoder},
    Error, Result,
};

pub struct DelimiterBasedFrameDecoder {
    max_frame_length: usize,
    delimiters: Vec<Bytes>,
    strip_delimiter: bool,
}

impl DelimiterBasedFrameDecoder {
    pub fn new(max_frame_length: usize, delimiter: impl Into<Bytes>) -> Self {
        Self::new_many(max_frame_length, [delimiter])
    }

    pub fn new_many<I, D>(max_frame_length: usize, delimiters: I) -> Self
    where
        I: IntoIterator<Item = D>,
        D: Into<Bytes>,
    {
        let delimiters = delimiters.into_iter().map(Into::into).collect::<Vec<_>>();
        assert!(!delimiters.is_empty(), "at least one delimiter is required");
        assert!(
            delimiters.iter().all(|delimiter| !delimiter.is_empty()),
            "delimiters must not be empty"
        );

        Self {
            max_frame_length,
            delimiters,
            strip_delimiter: true,
        }
    }

    pub fn strip_delimiter(mut self, strip_delimiter: bool) -> Self {
        self.strip_delimiter = strip_delimiter;
        self
    }

    pub fn line_delimiter(max_frame_length: usize) -> Self {
        Self::new_many(
            max_frame_length,
            [Bytes::from_static(b"\r\n"), Bytes::from_static(b"\n")],
        )
    }
}

impl Decoder for DelimiterBasedFrameDecoder {
    type Item = Bytes;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        let Some((frame_end, delimiter_len)) = find_delimiter(src, &self.delimiters) else {
            if src.len() > self.max_frame_length {
                return Err(Error::FrameTooLarge {
                    current: src.len(),
                    max: self.max_frame_length,
                });
            }

            return Ok(None);
        };

        if frame_end > self.max_frame_length {
            return Err(Error::FrameTooLarge {
                current: frame_end,
                max: self.max_frame_length,
            });
        }

        let split_len = frame_end + delimiter_len;
        let mut frame = src.split_to(split_len);
        if self.strip_delimiter {
            frame.truncate(frame_end);
        }

        Ok(Some(frame.freeze()))
    }
}

impl Encoder<Bytes> for DelimiterBasedFrameDecoder {
    fn encode(&mut self, item: Bytes, dst: &mut BytesMut) -> Result<()> {
        let delimiter = self
            .delimiters
            .first()
            .ok_or_else(|| Error::Encode("missing delimiter".to_string()))?;

        dst.reserve(item.len() + delimiter.len());
        dst.extend_from_slice(&item);
        dst.extend_from_slice(delimiter);
        Ok(())
    }
}

fn find_delimiter(src: &BytesMut, delimiters: &[Bytes]) -> Option<(usize, usize)> {
    delimiters
        .iter()
        .filter_map(|delimiter| {
            src.windows(delimiter.len())
                .position(|window| window == delimiter.as_ref())
                .map(|pos| (pos, delimiter.len()))
        })
        .min_by_key(|(pos, len)| (*pos, *len))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_delimited_frame() {
        let mut codec = DelimiterBasedFrameDecoder::new(1024, Bytes::from_static(b"|"));
        let mut buf = BytesMut::from(&b"one|two|"[..]);

        assert_eq!(
            codec.decode(&mut buf).expect("decode"),
            Some(Bytes::from_static(b"one"))
        );
        assert_eq!(
            codec.decode(&mut buf).expect("decode"),
            Some(Bytes::from_static(b"two"))
        );
    }
}
