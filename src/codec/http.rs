use bytes::{Bytes, BytesMut};

use crate::{
    codec::{Decoder, Encoder},
    Error, Result,
};

const DEFAULT_MAX_HTTP_HEADER_LEN: usize = 16 * 1024;

/// Minimal HTTP/1.1 server codec.
///
/// The codec decodes HTTP requests with headers and an optional
/// `Content-Length` body, and encodes HTTP responses. Chunked transfer coding
/// can be enabled with [`HttpCodec::allow_chunked`].
pub struct HttpCodec {
    max_header_len: usize,
    max_body_len: usize,
    allow_chunked: bool,
    preserve_trailers: bool,
}

impl HttpCodec {
    /// Creates a server-side HTTP/1.1 codec.
    pub fn server() -> Self {
        Self {
            max_header_len: DEFAULT_MAX_HTTP_HEADER_LEN,
            max_body_len: DEFAULT_MAX_HTTP_HEADER_LEN,
            allow_chunked: false,
            preserve_trailers: false,
        }
    }

    /// Sets the maximum HTTP request header size.
    pub fn max_header_len(mut self, value: usize) -> Self {
        self.max_header_len = value;
        self
    }

    /// Sets the maximum HTTP request body size accepted by this codec.
    pub fn max_body_len(mut self, value: usize) -> Self {
        self.max_body_len = value;
        self
    }

    /// Enables or disables decoding `Transfer-Encoding: chunked` request bodies.
    ///
    /// Chunked decoding is disabled by default. When enabled, chunks are
    /// aggregated into [`HttpRequest::body`] and are still bounded by
    /// [`HttpCodec::max_body_len`].
    pub fn allow_chunked(mut self, value: bool) -> Self {
        self.allow_chunked = value;
        self
    }

    /// Preserves chunked request trailer fields in [`HttpRequest::trailers`].
    ///
    /// Trailers are parsed and validated whenever chunked decoding is enabled.
    /// This option only controls whether they are retained on the request.
    pub fn preserve_trailers(mut self, value: bool) -> Self {
        self.preserve_trailers = value;
        self
    }
}

impl Default for HttpCodec {
    fn default() -> Self {
        Self::server()
    }
}

impl Decoder for HttpCodec {
    type Item = HttpRequest;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        decode_http_request(
            src,
            HttpDecodeOptions {
                max_header_len: self.max_header_len,
                max_body_len: self.max_body_len,
                allow_chunked: self.allow_chunked,
                preserve_trailers: self.preserve_trailers,
            },
        )
    }
}

impl Encoder<HttpResponse> for HttpCodec {
    fn encode(&mut self, item: HttpResponse, dst: &mut BytesMut) -> Result<()> {
        encode_http_response(item, dst)
    }
}

/// Minimal HTTP/1.1 request decoded by [`HttpCodec`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HttpRequest {
    pub(crate) method: String,
    pub(crate) target: String,
    pub(crate) version: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Bytes,
    pub(crate) trailers: Vec<(String, String)>,
}

impl HttpRequest {
    /// HTTP method from the request line.
    pub fn method(&self) -> &str {
        &self.method
    }

    /// Request target from the request line.
    pub fn target(&self) -> &str {
        &self.target
    }

    /// HTTP version from the request line.
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Request body bytes.
    pub fn body(&self) -> &Bytes {
        &self.body
    }

    /// Returns a case-insensitive header lookup.
    pub fn header(&self, name: &str) -> Option<&str> {
        header(&self.headers, name)
    }

    /// Returns all request headers in wire order.
    pub fn headers(&self) -> &[(String, String)] {
        &self.headers
    }

    /// Returns chunked request trailer fields in wire order.
    pub fn trailers(&self) -> &[(String, String)] {
        &self.trailers
    }

    /// True when the request asks to upgrade this connection to WebSocket.
    pub fn is_websocket_upgrade(&self) -> bool {
        self.method.eq_ignore_ascii_case("GET")
            && self
                .header("Upgrade")
                .is_some_and(|value| value.eq_ignore_ascii_case("websocket"))
            && self.header("Connection").is_some_and(|value| {
                value
                    .split(',')
                    .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
            })
    }
}

/// Minimal HTTP/1.1 response encoded by [`HttpCodec`].
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HttpResponse {
    pub(crate) status: u16,
    pub(crate) reason: String,
    pub(crate) headers: Vec<(String, String)>,
    pub(crate) body: Bytes,
}

impl HttpResponse {
    /// Creates a response with a default reason phrase for common statuses.
    pub fn new(status: u16) -> Self {
        Self {
            status,
            reason: default_reason(status).to_string(),
            headers: Vec::new(),
            body: Bytes::new(),
        }
    }

    /// Sets the reason phrase.
    pub fn reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = reason.into();
        self
    }

    /// Adds a response header.
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// Sets the response body.
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    /// HTTP response status code.
    pub fn status(&self) -> u16 {
        self.status
    }

    /// HTTP response body.
    pub fn body_bytes(&self) -> &Bytes {
        &self.body
    }
}

pub(crate) struct HttpDecodeOptions {
    pub max_header_len: usize,
    pub max_body_len: usize,
    pub allow_chunked: bool,
    pub preserve_trailers: bool,
}

pub(crate) fn decode_http_request(
    src: &mut BytesMut,
    options: HttpDecodeOptions,
) -> Result<Option<HttpRequest>> {
    let Some(header_end) = find_http_header_end(src) else {
        if src.len() > options.max_header_len {
            return Err(Error::FrameTooLarge {
                current: src.len(),
                max: options.max_header_len,
            });
        }

        return Ok(None);
    };

    if header_end > options.max_header_len {
        return Err(Error::FrameTooLarge {
            current: header_end,
            max: options.max_header_len,
        });
    }

    let header_bytes = &src[..header_end + 4];
    let header_text = std::str::from_utf8(header_bytes)
        .map_err(|err| Error::Decode(format!("http request header is not utf-8: {err}")))?;
    let (_, _, _, headers) = parse_http_head(header_text)?;
    let body_kind = request_body_kind(&headers, options.allow_chunked)?;

    let request = match body_kind {
        BodyKind::None => src.split_to(header_end + 4).freeze(),
        BodyKind::ContentLength(content_len) => {
            if content_len > options.max_body_len {
                return Err(Error::FrameTooLarge {
                    current: content_len,
                    max: options.max_body_len,
                });
            }

            let total_len = header_end
                .checked_add(4)
                .and_then(|len| len.checked_add(content_len))
                .ok_or_else(|| Error::Decode("http request length overflow".to_string()))?;
            if src.len() < total_len {
                return Ok(None);
            }

            src.split_to(total_len).freeze()
        }
        BodyKind::Chunked => {
            let body_start = header_end + 4;
            let Some((total_len, body, trailers)) = decode_chunked_body(
                src,
                body_start,
                options.max_body_len,
                options.preserve_trailers,
            )?
            else {
                return Ok(None);
            };

            let request = src.split_to(total_len).freeze();
            return parse_http_request_with_body(request, header_end, body, trailers).map(Some);
        }
    };

    parse_http_request(request, header_end).map(Some)
}

pub(crate) fn parse_http_request(src: Bytes, header_end: usize) -> Result<HttpRequest> {
    let body_start = header_end + 4;
    let body = src.slice(body_start..);
    parse_http_request_with_body(src, header_end, body, Vec::new())
}

fn parse_http_request_with_body(
    src: Bytes,
    header_end: usize,
    body: Bytes,
    trailers: Vec<(String, String)>,
) -> Result<HttpRequest> {
    let header = std::str::from_utf8(&src[..header_end])
        .map_err(|err| Error::Decode(format!("http request header is not utf-8: {err}")))?;
    let (method, target, version, headers) = parse_http_head(header)?;
    Ok(HttpRequest {
        method,
        target,
        version,
        headers,
        body,
        trailers,
    })
}

pub(crate) fn encode_http_response(response: HttpResponse, dst: &mut BytesMut) -> Result<()> {
    dst.extend_from_slice(b"HTTP/1.1 ");
    dst.extend_from_slice(response.status.to_string().as_bytes());
    dst.extend_from_slice(b" ");
    dst.extend_from_slice(response.reason.as_bytes());
    dst.extend_from_slice(b"\r\n");

    let mut has_content_len = false;
    for (name, value) in response.headers {
        if name.eq_ignore_ascii_case("Content-Length") {
            has_content_len = true;
        }
        dst.extend_from_slice(name.as_bytes());
        dst.extend_from_slice(b": ");
        dst.extend_from_slice(value.as_bytes());
        dst.extend_from_slice(b"\r\n");
    }

    if !has_content_len {
        dst.extend_from_slice(b"Content-Length: ");
        dst.extend_from_slice(response.body.len().to_string().as_bytes());
        dst.extend_from_slice(b"\r\n");
    }

    dst.extend_from_slice(b"\r\n");
    dst.extend_from_slice(&response.body);
    Ok(())
}

pub(crate) fn header<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(header, _)| header.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

pub(crate) fn find_http_header_end(src: &BytesMut) -> Option<usize> {
    src.windows(4).position(|window| window == b"\r\n\r\n")
}

fn default_reason(status: u16) -> &'static str {
    match status {
        100 => "Continue",
        101 => "Switching Protocols",
        200 => "OK",
        201 => "Created",
        202 => "Accepted",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        413 => "Payload Too Large",
        415 => "Unsupported Media Type",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "OK",
    }
}

enum BodyKind {
    None,
    ContentLength(usize),
    Chunked,
}

fn request_body_kind(headers: &[(String, String)], allow_chunked: bool) -> Result<BodyKind> {
    let content_len = content_length(headers)?;
    let transfer_encoding = header(headers, "Transfer-Encoding");

    if let Some(value) = transfer_encoding {
        let tokens: Vec<_> = value.split(',').map(|token| token.trim()).collect();
        let has_chunked = tokens
            .iter()
            .any(|token| token.eq_ignore_ascii_case("chunked"));
        if has_chunked {
            if content_len.is_some() {
                return Err(Error::Decode(
                    "request cannot contain both Transfer-Encoding: chunked and Content-Length"
                        .to_string(),
                ));
            }
            if !allow_chunked {
                return Err(Error::Decode(
                    "chunked transfer coding is disabled".to_string(),
                ));
            }
            if !tokens
                .last()
                .is_some_and(|token| token.eq_ignore_ascii_case("chunked"))
            {
                return Err(Error::Decode(
                    "chunked transfer coding must be the final transfer encoding".to_string(),
                ));
            }
            return Ok(BodyKind::Chunked);
        }

        if tokens
            .iter()
            .any(|token| !token.is_empty() && !token.eq_ignore_ascii_case("identity"))
        {
            return Err(Error::Decode(format!(
                "unsupported Transfer-Encoding header: {value}"
            )));
        }
    }

    Ok(content_len.map_or(BodyKind::None, BodyKind::ContentLength))
}

fn content_length(headers: &[(String, String)]) -> Result<Option<usize>> {
    let mut content_len = None;
    for (_, value) in headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("Content-Length"))
    {
        let parsed = value
            .trim()
            .parse::<usize>()
            .map_err(|err| Error::Decode(format!("invalid Content-Length header: {err}")))?;
        if content_len.replace(parsed).is_some() {
            return Err(Error::Decode(
                "multiple Content-Length headers are not supported".to_string(),
            ));
        }
    }

    Ok(content_len)
}

fn decode_chunked_body(
    src: &BytesMut,
    body_start: usize,
    max_body_len: usize,
    preserve_trailers: bool,
) -> Result<Option<(usize, Bytes, Vec<(String, String)>)>> {
    let mut pos = body_start;
    let mut body = BytesMut::new();

    loop {
        let Some(size_line_end) = find_crlf(src, pos) else {
            return Ok(None);
        };
        let size_line = std::str::from_utf8(&src[pos..size_line_end])
            .map_err(|err| Error::Decode(format!("chunk size line is not utf-8: {err}")))?;
        let size = parse_chunk_size(size_line)?;
        pos = size_line_end + 2;

        if size == 0 {
            if src.len() >= pos + 2 && &src[pos..pos + 2] == b"\r\n" {
                return Ok(Some((pos + 2, body.freeze(), Vec::new())));
            }

            let Some(trailer_end) = find_http_header_end_from(src, pos) else {
                return Ok(None);
            };
            let trailers = parse_trailers(&src[pos..trailer_end], preserve_trailers)?;
            return Ok(Some((trailer_end + 4, body.freeze(), trailers)));
        }

        if body.len().saturating_add(size) > max_body_len {
            return Err(Error::FrameTooLarge {
                current: body.len().saturating_add(size),
                max: max_body_len,
            });
        }

        let chunk_end = pos
            .checked_add(size)
            .ok_or_else(|| Error::Decode("chunk length overflow".to_string()))?;
        let crlf_end = chunk_end
            .checked_add(2)
            .ok_or_else(|| Error::Decode("chunk length overflow".to_string()))?;
        if src.len() < crlf_end {
            return Ok(None);
        }
        if &src[chunk_end..crlf_end] != b"\r\n" {
            return Err(Error::Decode(
                "chunk data is not followed by CRLF".to_string(),
            ));
        }

        body.extend_from_slice(&src[pos..chunk_end]);
        pos = crlf_end;
    }
}

fn parse_http_head(header: &str) -> Result<(String, String, String, Vec<(String, String)>)> {
    let mut lines = header.split("\r\n");
    let request_line = lines
        .next()
        .ok_or_else(|| Error::Decode("missing http request line".to_string()))?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next().unwrap_or_default();
    let target = request_parts.next().unwrap_or_default();
    let version = request_parts.next().unwrap_or_default();

    if method.is_empty() || target.is_empty() || !version.starts_with("HTTP/1.") {
        return Err(Error::Decode("invalid http request line".to_string()));
    }

    let headers = parse_header_fields(lines)?;
    Ok((
        method.to_string(),
        target.to_string(),
        version.to_string(),
        headers,
    ))
}

fn parse_header_fields<'a>(
    lines: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<(String, String)>> {
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }

        let Some((name, value)) = line.split_once(':') else {
            return Err(Error::Decode(format!("invalid http header: {line}")));
        };
        headers.push((name.trim().to_string(), value.trim().to_string()));
    }

    Ok(headers)
}

fn parse_trailers(src: &[u8], preserve: bool) -> Result<Vec<(String, String)>> {
    if src.is_empty() {
        return Ok(Vec::new());
    }

    let trailers = std::str::from_utf8(src)
        .map_err(|err| Error::Decode(format!("http trailers are not utf-8: {err}")))?;
    let fields = parse_header_fields(trailers.split("\r\n"))?;
    if preserve {
        Ok(fields)
    } else {
        Ok(Vec::new())
    }
}

fn parse_chunk_size(line: &str) -> Result<usize> {
    let size = line.split(';').next().unwrap_or_default().trim();
    if size.is_empty() {
        return Err(Error::Decode("missing chunk size".to_string()));
    }

    usize::from_str_radix(size, 16)
        .map_err(|err| Error::Decode(format!("invalid chunk size: {err}")))
}

fn find_crlf(src: &BytesMut, start: usize) -> Option<usize> {
    src[start..]
        .windows(2)
        .position(|window| window == b"\r\n")
        .map(|offset| start + offset)
}

fn find_http_header_end_from(src: &BytesMut, start: usize) -> Option<usize> {
    src[start..]
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|offset| start + offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_regular_http_request_and_encodes_response() {
        let mut codec = HttpCodec::server();
        let mut buf = BytesMut::from(
            &b"POST /hello HTTP/1.1\r\n\
Host: example.com\r\n\
Content-Length: 5\r\n\
\r\n\
world"[..],
        );

        let request = codec.decode(&mut buf).expect("decode").expect("request");
        assert_eq!(request.method(), "POST");
        assert_eq!(request.target(), "/hello");
        assert_eq!(request.header("host"), Some("example.com"));
        assert_eq!(request.body(), &Bytes::from_static(b"world"));

        let mut out = BytesMut::new();
        codec
            .encode(
                HttpResponse::new(200)
                    .header("Content-Type", "text/plain")
                    .body(Bytes::from_static(b"ok")),
                &mut out,
            )
            .expect("encode");
        assert_eq!(
            std::str::from_utf8(&out).expect("response"),
            "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nok"
        );
    }

    #[test]
    fn waits_for_declared_body() {
        let mut codec = HttpCodec::server();
        let mut buf = BytesMut::from(
            &b"POST / HTTP/1.1\r\n\
Content-Length: 5\r\n\
\r\n\
he"[..],
        );

        assert!(codec.decode(&mut buf).expect("partial").is_none());
        buf.extend_from_slice(b"llo");
        assert_eq!(
            codec
                .decode(&mut buf)
                .expect("decode")
                .expect("request")
                .body(),
            &Bytes::from_static(b"hello")
        );
    }

    #[test]
    fn rejects_chunked_by_default() {
        let mut codec = HttpCodec::server();
        let mut buf = BytesMut::from(
            &b"POST / HTTP/1.1\r\n\
Transfer-Encoding: chunked\r\n\
\r\n\
5\r\nhello\r\n0\r\n\r\n"[..],
        );

        assert!(matches!(codec.decode(&mut buf), Err(Error::Decode(_))));
    }

    #[test]
    fn decodes_chunked_body_when_enabled() {
        let mut codec = HttpCodec::server().allow_chunked(true);
        let mut buf = BytesMut::from(
            &b"POST / HTTP/1.1\r\n\
Transfer-Encoding: chunked\r\n\
\r\n\
5\r\nhello\r\n6;ext=value\r\n world\r\n0\r\n\r\n"[..],
        );

        let request = codec.decode(&mut buf).expect("decode").expect("request");
        assert_eq!(request.body(), &Bytes::from_static(b"hello world"));
        assert!(request.trailers().is_empty());
    }

    #[test]
    fn decodes_chunked_trailers_when_preserved() {
        let mut codec = HttpCodec::server()
            .allow_chunked(true)
            .preserve_trailers(true);
        let mut buf = BytesMut::from(
            &b"POST / HTTP/1.1\r\n\
Transfer-Encoding: chunked\r\n\
\r\n\
2\r\nhi\r\n0\r\nExpires: never\r\n\r\n"[..],
        );

        let request = codec.decode(&mut buf).expect("decode").expect("request");
        assert_eq!(request.body(), &Bytes::from_static(b"hi"));
        assert_eq!(
            request.trailers(),
            &[("Expires".to_string(), "never".to_string())]
        );
    }

    #[test]
    fn rejects_chunked_with_content_length() {
        let mut codec = HttpCodec::server().allow_chunked(true);
        let mut buf = BytesMut::from(
            &b"POST / HTTP/1.1\r\n\
Transfer-Encoding: chunked\r\n\
Content-Length: 5\r\n\
\r\n\
0\r\n\r\n"[..],
        );

        assert!(matches!(codec.decode(&mut buf), Err(Error::Decode(_))));
    }

    #[test]
    fn waits_for_complete_chunked_body() {
        let mut codec = HttpCodec::server().allow_chunked(true);
        let mut buf = BytesMut::from(
            &b"POST / HTTP/1.1\r\n\
Transfer-Encoding: chunked\r\n\
\r\n\
5\r\nhe"[..],
        );

        assert!(codec.decode(&mut buf).expect("partial").is_none());
        buf.extend_from_slice(b"llo\r\n0\r\n\r\n");
        assert_eq!(
            codec
                .decode(&mut buf)
                .expect("decode")
                .expect("request")
                .body(),
            &Bytes::from_static(b"hello")
        );
    }
}
