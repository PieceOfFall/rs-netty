use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use sha1::{Digest, Sha1};

use crate::{
    codec::{Decoder, Encoder},
    Error, Result,
};

const ACCEPT_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
const DEFAULT_MAX_HTTP_HEADER_LEN: usize = 16 * 1024;
const DEFAULT_MAX_FRAME_LEN: usize = 16 * 1024 * 1024;
const CLIENT_KEY_LEN: usize = 16;

/// Stateful server-side WebSocket codec.
///
/// This codec is available with the `websocket` feature:
///
/// ```toml
/// rs-netty = { version = "0.2", features = ["websocket"] }
/// ```
///
/// The codec starts in an HTTP Upgrade handshake state. After a valid
/// WebSocket handshake request is decoded, subsequent reads are decoded as
/// WebSocket frames without replacing the pipeline. This keeps the rs-netty
/// pipeline statically typed while still supporting the protocol transition
/// that WebSocket requires.
///
/// ```no_run
/// # use rs_netty::{
/// #     codec::{LineCodec, WebSocketCodec, WebSocketInbound, WebSocketOutbound},
/// #     handler, pipeline, Result, TcpServer,
/// # };
/// struct WsHandler;
///
/// #[handler(WsHandler)]
/// async fn handle_ws(msg: WebSocketInbound) -> Result<WebSocketOutbound> {
///     match msg {
///         WebSocketInbound::Handshake(handshake) => Ok(handshake.accept_response().into()),
///         WebSocketInbound::Text(text) => Ok(WebSocketOutbound::Text(text)),
///         WebSocketInbound::Ping(payload) => Ok(WebSocketOutbound::Pong(payload)),
///         WebSocketInbound::Close(close) => Ok(WebSocketOutbound::Close(close)),
///         WebSocketInbound::Binary(bytes) => Ok(WebSocketOutbound::Binary(bytes)),
///         WebSocketInbound::Pong(payload) => Ok(WebSocketOutbound::Pong(payload)),
///     }
/// }
///
/// # async fn run() -> Result<()> {
/// TcpServer::bind("127.0.0.1:9004")
///     .pipeline(|| {
///         pipeline()
///             .codec(WebSocketCodec::server())
///             .handler(WsHandler)
///     })
///     .run()
///     .await
/// # }
/// ```
pub struct WebSocketCodec {
    state: WebSocketState,
    max_http_header_len: usize,
    max_frame_len: usize,
    require_masked_client_frames: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WebSocketState {
    Handshake,
    Frames,
}

impl WebSocketCodec {
    /// Creates a server-side WebSocket codec.
    pub fn server() -> Self {
        Self {
            state: WebSocketState::Handshake,
            max_http_header_len: DEFAULT_MAX_HTTP_HEADER_LEN,
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
            require_masked_client_frames: true,
        }
    }

    /// Sets the maximum HTTP Upgrade header size accepted during handshake.
    pub fn max_http_header_len(mut self, value: usize) -> Self {
        self.max_http_header_len = value;
        self
    }

    /// Sets the maximum WebSocket payload size accepted after handshake.
    pub fn max_frame_len(mut self, value: usize) -> Self {
        self.max_frame_len = value;
        self
    }

    /// Controls whether decoded client frames must be masked.
    ///
    /// WebSocket clients are required to mask frames sent to servers. This is
    /// enabled by default. Disabling it is mostly useful for tests or trusted
    /// internal peers.
    pub fn require_masked_client_frames(mut self, value: bool) -> Self {
        self.require_masked_client_frames = value;
        self
    }
}

impl Default for WebSocketCodec {
    fn default() -> Self {
        Self::server()
    }
}

impl Decoder for WebSocketCodec {
    type Item = WebSocketInbound;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        match self.state {
            WebSocketState::Handshake => self.decode_handshake(src),
            WebSocketState::Frames => self.decode_frame(src),
        }
    }
}

impl Encoder<WebSocketOutbound> for WebSocketCodec {
    fn encode(&mut self, item: WebSocketOutbound, dst: &mut BytesMut) -> Result<()> {
        match item {
            WebSocketOutbound::HandshakeResponse(response) => {
                encode_handshake_response(response, dst);
                self.state = WebSocketState::Frames;
                Ok(())
            }
            WebSocketOutbound::Text(text) => encode_frame(0x1, text.into_bytes().into(), dst),
            WebSocketOutbound::Binary(bytes) => encode_frame(0x2, bytes, dst),
            WebSocketOutbound::Close(close) => encode_close(close, dst),
            WebSocketOutbound::Ping(bytes) => encode_control_frame(0x9, bytes, dst),
            WebSocketOutbound::Pong(bytes) => encode_control_frame(0xA, bytes, dst),
        }
    }
}

impl Encoder<WebSocketMessage> for WebSocketCodec {
    fn encode(&mut self, item: WebSocketMessage, dst: &mut BytesMut) -> Result<()> {
        self.encode(WebSocketOutbound::from(item), dst)
    }
}

impl WebSocketCodec {
    fn decode_handshake(&mut self, src: &mut BytesMut) -> Result<Option<WebSocketInbound>> {
        let Some(end) = find_http_header_end(src) else {
            if src.len() > self.max_http_header_len {
                return Err(Error::FrameTooLarge {
                    current: src.len(),
                    max: self.max_http_header_len,
                });
            }

            return Ok(None);
        };

        if end > self.max_http_header_len {
            return Err(Error::FrameTooLarge {
                current: end,
                max: self.max_http_header_len,
            });
        }

        let request = src.split_to(end + 4);
        let request = std::str::from_utf8(&request)
            .map_err(|err| Error::Decode(format!("websocket handshake is not utf-8: {err}")))?;
        let handshake = parse_handshake(request)?;
        self.state = WebSocketState::Frames;
        Ok(Some(WebSocketInbound::Handshake(handshake)))
    }

    fn decode_frame(&mut self, src: &mut BytesMut) -> Result<Option<WebSocketInbound>> {
        if src.len() < 2 {
            return Ok(None);
        }

        let first = src[0];
        let second = src[1];
        let fin = first & 0x80 != 0;
        let rsv = first & 0x70;
        let opcode = first & 0x0f;
        let masked = second & 0x80 != 0;
        let mut payload_len = u64::from(second & 0x7f);
        let mut header_len = 2usize;
        let encoded_len_kind = payload_len;

        if payload_len == 126 {
            if src.len() < header_len + 2 {
                return Ok(None);
            }
            payload_len = u64::from(u16::from_be_bytes([src[2], src[3]]));
            header_len += 2;
        } else if payload_len == 127 {
            if src.len() < header_len + 8 {
                return Ok(None);
            }
            payload_len = u64::from_be_bytes([
                src[2], src[3], src[4], src[5], src[6], src[7], src[8], src[9],
            ]);
            header_len += 8;
        }

        validate_payload_len_encoding(encoded_len_kind, payload_len)?;

        let payload_len = usize::try_from(payload_len)
            .map_err(|err| Error::Decode(format!("websocket payload length overflow: {err}")))?;
        if payload_len > self.max_frame_len {
            return Err(Error::FrameTooLarge {
                current: payload_len,
                max: self.max_frame_len,
            });
        }

        let mask_len = if masked { 4 } else { 0 };
        let frame_len = header_len
            .checked_add(mask_len)
            .and_then(|len| len.checked_add(payload_len))
            .ok_or_else(|| Error::Decode("websocket frame length overflow".to_string()))?;
        if src.len() < frame_len {
            return Ok(None);
        }

        validate_frame_header(
            fin,
            rsv,
            opcode,
            masked,
            payload_len,
            self.require_masked_client_frames,
        )?;

        let mut frame = src.split_to(frame_len);
        frame.advance(header_len);
        let mask = if masked {
            let mask = [frame[0], frame[1], frame[2], frame[3]];
            frame.advance(4);
            Some(mask)
        } else {
            None
        };

        let mut payload = frame.split_to(payload_len);
        if let Some(mask) = mask {
            for (index, byte) in payload.iter_mut().enumerate() {
                *byte ^= mask[index % 4];
            }
        }

        decode_payload(opcode, payload.freeze())
    }
}

/// Inbound messages decoded by [`WebSocketCodec`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WebSocketInbound {
    /// HTTP Upgrade request accepted by the codec.
    Handshake(WebSocketHandshake),
    /// Text frame payload.
    Text(String),
    /// Binary frame payload.
    Binary(Bytes),
    /// Ping control frame payload.
    Ping(Bytes),
    /// Pong control frame payload.
    Pong(Bytes),
    /// Close control frame payload.
    Close(Option<WebSocketClose>),
}

/// Outbound messages encoded by [`WebSocketCodec`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WebSocketOutbound {
    /// HTTP 101 Switching Protocols response.
    HandshakeResponse(WebSocketHandshakeResponse),
    /// Text frame payload.
    Text(String),
    /// Binary frame payload.
    Binary(Bytes),
    /// Close control frame payload.
    Close(Option<WebSocketClose>),
    /// Ping control frame payload.
    Ping(Bytes),
    /// Pong control frame payload.
    Pong(Bytes),
}

/// WebSocket data/control messages after the opening handshake.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum WebSocketMessage {
    /// Text frame payload.
    Text(String),
    /// Binary frame payload.
    Binary(Bytes),
    /// Close control frame payload.
    Close(Option<WebSocketClose>),
    /// Ping control frame payload.
    Ping(Bytes),
    /// Pong control frame payload.
    Pong(Bytes),
}

impl From<WebSocketMessage> for WebSocketOutbound {
    fn from(value: WebSocketMessage) -> Self {
        match value {
            WebSocketMessage::Text(text) => Self::Text(text),
            WebSocketMessage::Binary(bytes) => Self::Binary(bytes),
            WebSocketMessage::Close(close) => Self::Close(close),
            WebSocketMessage::Ping(bytes) => Self::Ping(bytes),
            WebSocketMessage::Pong(bytes) => Self::Pong(bytes),
        }
    }
}

impl From<WebSocketHandshakeResponse> for WebSocketOutbound {
    fn from(value: WebSocketHandshakeResponse) -> Self {
        Self::HandshakeResponse(value)
    }
}

/// Parsed WebSocket HTTP Upgrade request.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WebSocketHandshake {
    path: String,
    key: String,
    headers: Vec<(String, String)>,
}

impl WebSocketHandshake {
    /// Request path from the HTTP request line.
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Value of the `Sec-WebSocket-Key` header.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Returns a case-insensitive header lookup from the handshake request.
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    /// Builds the standard HTTP 101 response for this handshake.
    pub fn accept_response(&self) -> WebSocketHandshakeResponse {
        WebSocketHandshakeResponse {
            accept_key: websocket_accept_key(&self.key),
            headers: Vec::new(),
        }
    }
}

/// HTTP 101 Switching Protocols response for a WebSocket handshake.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WebSocketHandshakeResponse {
    accept_key: String,
    headers: Vec<(String, String)>,
}

impl WebSocketHandshakeResponse {
    /// Adds an extra response header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

/// WebSocket close status and optional reason.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct WebSocketClose {
    /// Close status code.
    pub code: u16,
    /// UTF-8 close reason.
    pub reason: String,
}

fn parse_handshake(src: &str) -> Result<WebSocketHandshake> {
    let mut lines = src.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| Error::Decode("missing websocket request line".to_string()))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default();
    let path = request_parts.next().unwrap_or_default();
    let version = request_parts.next().unwrap_or_default();

    if method != "GET" || path.is_empty() || !version.starts_with("HTTP/1.1") {
        return Err(Error::Decode(
            "invalid websocket HTTP upgrade request line".to_string(),
        ));
    }

    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }

        let Some((name, value)) = line.split_once(':') else {
            return Err(Error::Decode(format!("invalid websocket header: {line}")));
        };
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }

    require_header_value(&headers, "Upgrade", "websocket")?;
    require_connection_upgrade(&headers)?;
    require_header_value(&headers, "Sec-WebSocket-Version", "13")?;
    let key = header(&headers, "Sec-WebSocket-Key")
        .ok_or_else(|| Error::Decode("missing Sec-WebSocket-Key".to_string()))?
        .to_string();
    validate_client_key(&key)?;

    Ok(WebSocketHandshake {
        path: path.to_string(),
        key,
        headers,
    })
}

fn require_header_value(headers: &[(String, String)], name: &str, expected: &str) -> Result<()> {
    let Some(value) = header(headers, name) else {
        return Err(Error::Decode(format!("missing {name} header")));
    };

    if !value.eq_ignore_ascii_case(expected) {
        return Err(Error::Decode(format!("invalid {name} header")));
    }

    Ok(())
}

fn require_connection_upgrade(headers: &[(String, String)]) -> Result<()> {
    let Some(value) = header(headers, "Connection") else {
        return Err(Error::Decode("missing Connection header".to_string()));
    };

    if value
        .split(',')
        .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
    {
        return Ok(());
    }

    Err(Error::Decode("invalid Connection header".to_string()))
}

fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn find_http_header_end(src: &BytesMut) -> Option<usize> {
    src.windows(4).position(|window| window == b"\r\n\r\n")
}

fn websocket_accept_key(key: &str) -> String {
    let mut sha1 = Sha1::new();
    sha1.update(key.as_bytes());
    sha1.update(ACCEPT_GUID.as_bytes());
    STANDARD.encode(sha1.finalize())
}

fn validate_client_key(key: &str) -> Result<()> {
    let decoded = STANDARD
        .decode(key)
        .map_err(|err| Error::Decode(format!("invalid Sec-WebSocket-Key: {err}")))?;
    if decoded.len() != CLIENT_KEY_LEN {
        return Err(Error::Decode(format!(
            "invalid Sec-WebSocket-Key length: {}",
            decoded.len()
        )));
    }

    Ok(())
}

fn encode_handshake_response(response: WebSocketHandshakeResponse, dst: &mut BytesMut) {
    dst.extend_from_slice(b"HTTP/1.1 101 Switching Protocols\r\n");
    dst.extend_from_slice(b"Upgrade: websocket\r\n");
    dst.extend_from_slice(b"Connection: Upgrade\r\n");
    dst.extend_from_slice(b"Sec-WebSocket-Accept: ");
    dst.extend_from_slice(response.accept_key.as_bytes());
    dst.extend_from_slice(b"\r\n");
    for (name, value) in response.headers {
        dst.extend_from_slice(name.as_bytes());
        dst.extend_from_slice(b": ");
        dst.extend_from_slice(value.as_bytes());
        dst.extend_from_slice(b"\r\n");
    }
    dst.extend_from_slice(b"\r\n");
}

fn validate_frame_header(
    fin: bool,
    rsv: u8,
    opcode: u8,
    masked: bool,
    payload_len: usize,
    require_mask: bool,
) -> Result<()> {
    if require_mask && !masked {
        return Err(Error::Decode(
            "websocket client frame is not masked".to_string(),
        ));
    }

    if rsv != 0 {
        return Err(Error::Decode(
            "websocket reserved bits are set without an extension".to_string(),
        ));
    }

    if matches!(opcode, 0x8..=0xA) {
        if !fin {
            return Err(Error::Decode(
                "fragmented websocket control frame".to_string(),
            ));
        }
        if payload_len > 125 {
            return Err(Error::Decode(
                "websocket control frame payload exceeds 125 bytes".to_string(),
            ));
        }
    }

    if !fin {
        return Err(Error::Decode(
            "fragmented websocket data frames are not supported yet".to_string(),
        ));
    }

    Ok(())
}

fn validate_payload_len_encoding(encoded_len_kind: u64, payload_len: u64) -> Result<()> {
    match encoded_len_kind {
        126 if payload_len < 126 => Err(Error::Decode(
            "websocket payload length is not minimally encoded".to_string(),
        )),
        127 if payload_len <= 65535 => Err(Error::Decode(
            "websocket payload length is not minimally encoded".to_string(),
        )),
        127 if payload_len > (i64::MAX as u64) => Err(Error::Decode(
            "websocket 64-bit payload length uses the reserved high bit".to_string(),
        )),
        _ => Ok(()),
    }
}

fn decode_payload(opcode: u8, payload: Bytes) -> Result<Option<WebSocketInbound>> {
    match opcode {
        0x1 => {
            let text = String::from_utf8(payload.to_vec())
                .map_err(|err| Error::Decode(format!("invalid websocket text frame: {err}")))?;
            Ok(Some(WebSocketInbound::Text(text)))
        }
        0x2 => Ok(Some(WebSocketInbound::Binary(payload))),
        0x8 => Ok(Some(WebSocketInbound::Close(decode_close(payload)?))),
        0x9 => Ok(Some(WebSocketInbound::Ping(payload))),
        0xA => Ok(Some(WebSocketInbound::Pong(payload))),
        _ => Err(Error::Decode(format!(
            "unsupported websocket opcode: {opcode}"
        ))),
    }
}

fn decode_close(payload: Bytes) -> Result<Option<WebSocketClose>> {
    if payload.is_empty() {
        return Ok(None);
    }

    if payload.len() == 1 {
        return Err(Error::Decode(
            "websocket close payload cannot be one byte".to_string(),
        ));
    }

    let code = u16::from_be_bytes([payload[0], payload[1]]);
    validate_close_code(code).map_err(|message| {
        Error::Decode(format!("invalid websocket close status code: {message}"))
    })?;
    let reason = String::from_utf8(payload[2..].to_vec())
        .map_err(|err| Error::Decode(format!("invalid websocket close reason: {err}")))?;
    Ok(Some(WebSocketClose { code, reason }))
}

fn encode_close(close: Option<WebSocketClose>, dst: &mut BytesMut) -> Result<()> {
    let mut payload = BytesMut::new();
    if let Some(close) = close {
        validate_close_code(close.code).map_err(|message| {
            Error::Encode(format!("invalid websocket close status code: {message}"))
        })?;
        payload.put_u16(close.code);
        payload.extend_from_slice(close.reason.as_bytes());
    }
    encode_control_frame(0x8, payload.freeze(), dst)
}

fn validate_close_code(code: u16) -> std::result::Result<(), String> {
    let valid = match code {
        1000..=1003 | 1007..=1014 | 3000..=4999 => true,
        _ => false,
    };

    if valid {
        Ok(())
    } else {
        Err(code.to_string())
    }
}

fn encode_control_frame(opcode: u8, payload: Bytes, dst: &mut BytesMut) -> Result<()> {
    if payload.len() > 125 {
        return Err(Error::Encode(
            "websocket control frame payload exceeds 125 bytes".to_string(),
        ));
    }

    encode_frame(opcode, payload, dst)
}

fn encode_frame(opcode: u8, payload: Bytes, dst: &mut BytesMut) -> Result<()> {
    dst.put_u8(0x80 | opcode);
    match payload.len() {
        len @ 0..=125 => dst.put_u8(len as u8),
        len @ 126..=65535 => {
            dst.put_u8(126);
            dst.put_u16(len as u16);
        }
        len => {
            dst.put_u8(127);
            dst.put_u64(len as u64);
        }
    }
    dst.extend_from_slice(&payload);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const HANDSHAKE: &[u8] = b"GET /chat HTTP/1.1\r\n\
Host: server.example.com\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
Sec-WebSocket-Version: 13\r\n\
\r\n";

    #[test]
    fn decodes_handshake_and_encodes_accept_response() {
        let mut codec = WebSocketCodec::server();
        let mut buf = BytesMut::from(HANDSHAKE);

        let msg = codec.decode(&mut buf).expect("decode").expect("handshake");
        let WebSocketInbound::Handshake(handshake) = msg else {
            panic!("expected handshake");
        };
        assert_eq!(handshake.path(), "/chat");

        let mut out = BytesMut::new();
        codec
            .encode(
                WebSocketOutbound::from(handshake.accept_response()),
                &mut out,
            )
            .expect("encode");
        let response = std::str::from_utf8(&out).expect("utf-8 response");
        assert!(response.contains("HTTP/1.1 101 Switching Protocols\r\n"));
        assert!(response.contains("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n"));
    }

    #[test]
    fn decodes_masked_text_frame_after_handshake() {
        let mut codec = WebSocketCodec::server();
        let mut buf = BytesMut::from(HANDSHAKE);
        let _ = codec.decode(&mut buf).expect("decode").expect("handshake");

        buf.extend_from_slice(&[0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d]);
        buf.extend_from_slice(&[0x7f, 0x9f, 0x4d, 0x51, 0x58]);
        let msg = codec.decode(&mut buf).expect("decode").expect("frame");

        assert_eq!(msg, WebSocketInbound::Text("Hello".to_string()));
        assert!(buf.is_empty());
    }

    #[test]
    fn preserves_half_frame_and_decodes_when_complete() {
        let mut codec = WebSocketCodec::server().require_masked_client_frames(false);
        codec.state = WebSocketState::Frames;
        let mut buf = BytesMut::from(&[0x81, 0x05, b'H'][..]);

        assert!(codec.decode(&mut buf).expect("partial").is_none());
        assert_eq!(&buf[..], &[0x81, 0x05, b'H']);

        buf.extend_from_slice(b"ello");
        assert_eq!(
            codec.decode(&mut buf).expect("decode"),
            Some(WebSocketInbound::Text("Hello".to_string()))
        );
    }

    #[test]
    fn decodes_sticky_frames() {
        let mut codec = WebSocketCodec::server().require_masked_client_frames(false);
        codec.state = WebSocketState::Frames;
        let mut buf = BytesMut::from(&[0x81, 0x02, b'h', b'i', 0x81, 0x02, b'o', b'k'][..]);

        assert_eq!(
            codec.decode(&mut buf).expect("decode"),
            Some(WebSocketInbound::Text("hi".to_string()))
        );
        assert_eq!(
            codec.decode(&mut buf).expect("decode"),
            Some(WebSocketInbound::Text("ok".to_string()))
        );
        assert!(buf.is_empty());
    }

    #[test]
    fn rejects_invalid_handshake_key() {
        let mut codec = WebSocketCodec::server();
        let mut buf = BytesMut::from(
            &b"GET /chat HTTP/1.1\r\n\
Host: server.example.com\r\n\
Upgrade: websocket\r\n\
Connection: Upgrade\r\n\
Sec-WebSocket-Key: not-a-valid-key\r\n\
Sec-WebSocket-Version: 13\r\n\
\r\n"[..],
        );

        assert!(matches!(codec.decode(&mut buf), Err(Error::Decode(_))));
    }

    #[test]
    fn rejects_reserved_bits_without_extension() {
        let mut codec = WebSocketCodec::server().require_masked_client_frames(false);
        codec.state = WebSocketState::Frames;
        let mut buf = BytesMut::from(&[0xC1, 0x02, b'h', b'i'][..]);

        assert!(matches!(codec.decode(&mut buf), Err(Error::Decode(_))));
    }

    #[test]
    fn rejects_non_minimal_payload_length_encoding() {
        let mut codec = WebSocketCodec::server().require_masked_client_frames(false);
        codec.state = WebSocketState::Frames;
        let mut buf = BytesMut::from(&[0x81, 126, 0, 2, b'h', b'i'][..]);

        assert!(matches!(codec.decode(&mut buf), Err(Error::Decode(_))));
    }

    #[test]
    fn rejects_payload_length_with_reserved_high_bit() {
        let mut codec = WebSocketCodec::server().require_masked_client_frames(false);
        codec.state = WebSocketState::Frames;
        let mut buf = BytesMut::from(&[0x82, 127, 0x80, 0, 0, 0, 0, 0, 0, 0][..]);

        assert!(matches!(codec.decode(&mut buf), Err(Error::Decode(_))));
    }

    #[test]
    fn rejects_invalid_close_code_on_decode_and_encode() {
        let mut codec = WebSocketCodec::server().require_masked_client_frames(false);
        codec.state = WebSocketState::Frames;
        let mut buf = BytesMut::from(&[0x88, 0x02, 0x03, 0xEE][..]);

        assert!(matches!(codec.decode(&mut buf), Err(Error::Decode(_))));

        let mut out = BytesMut::new();
        assert!(matches!(
            codec.encode(
                WebSocketOutbound::Close(Some(WebSocketClose {
                    code: 1006,
                    reason: String::new(),
                })),
                &mut out,
            ),
            Err(Error::Encode(_))
        ));
    }
}
