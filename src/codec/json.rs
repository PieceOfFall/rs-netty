use std::marker::PhantomData;

use bytes::Bytes;

use crate::{
    context::{InboundContext, OutboundContext},
    traits::{Flow, Inbound, Outbound},
    Error, Result,
};

/// Inbound JSON decoder stage backed by `sonic-rs`.
///
/// This stage keeps framing separate from JSON parsing. Use it after a codec
/// such as [`crate::codec::LineCodec`] or
/// [`crate::codec::LengthFieldBasedFrameDecoder`].
///
/// `JsonDecode<T>` is a pipeline stage, not a general-purpose JSON API. For
/// direct serialization or parsing outside a pipeline, use `sonic-rs`,
/// `serde_json`, or another JSON crate directly.
///
/// # Feature
///
/// This type is available with the `json` feature:
///
/// ```toml
/// rs-netty = { version = "0.2", features = ["json"] }
/// ```
///
/// # Example
///
/// ```no_run
/// # use rs_netty::{codec::{JsonDecode, JsonEncode, LineCodec}, handler, pipeline, Result};
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct Request { op: String }
/// # #[derive(serde::Serialize, serde::Deserialize)]
/// # struct Response { ok: bool }
/// # struct ApiHandler;
/// # #[handler(ApiHandler)]
/// # async fn handle(_req: Request) -> Result<Response> {
/// #     Ok(Response { ok: true })
/// # }
/// let _pipeline = pipeline()
///     .codec(LineCodec::new())
///     .inbound(JsonDecode::<Request>::new())
///     .handler(ApiHandler)
///     .outbound(JsonEncode::<Response>::new());
/// ```
pub struct JsonDecode<T> {
    _marker: PhantomData<fn() -> T>,
}

impl<T> JsonDecode<T> {
    /// Creates a JSON decoder stage.
    ///
    /// The input is provided by the preceding framing codec. This stage accepts
    /// both `String` and `bytes::Bytes` and produces `T` when deserialization
    /// succeeds.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Default for JsonDecode<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Inbound<String> for JsonDecode<T>
where
    T: for<'de> serde::Deserialize<'de> + Send + 'static,
{
    type Out = T;

    async fn read(&mut self, _ctx: &mut InboundContext, msg: String) -> Result<Flow<Self::Out>> {
        decode_json(msg.as_bytes())
    }
}

impl<T> Inbound<Bytes> for JsonDecode<T>
where
    T: for<'de> serde::Deserialize<'de> + Send + 'static,
{
    type Out = T;

    async fn read(&mut self, _ctx: &mut InboundContext, msg: Bytes) -> Result<Flow<Self::Out>> {
        decode_json(&msg)
    }
}

/// Outbound JSON encoder stage backed by `sonic-rs`.
///
/// The stage serializes typed messages into compact JSON strings. Pair it with
/// a framing codec that accepts `String`, such as [`crate::codec::LineCodec`].
///
/// `JsonEncode<T>` is intended for outbound pipeline rendering. It deliberately
/// does not expose a standalone `to_string` or `to_vec` wrapper; use your JSON
/// crate directly for serialization outside rs-netty pipelines.
pub struct JsonEncode<T> {
    _marker: PhantomData<fn() -> T>,
}

impl<T> JsonEncode<T> {
    /// Creates a JSON encoder stage.
    ///
    /// The output is a compact JSON `String` that is forwarded to the next
    /// outbound stage or final stream encoder.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Default for JsonEncode<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Outbound<T> for JsonEncode<T>
where
    T: serde::Serialize + Send + 'static,
{
    type Out = String;

    async fn write(&mut self, _ctx: &mut OutboundContext, msg: T) -> Result<Flow<Self::Out>> {
        sonic_rs::to_string(&msg)
            .map(Flow::Next)
            .map_err(|err| Error::Encode(format!("json encode failed: {err}")))
    }
}

fn decode_json<T>(src: &[u8]) -> Result<Flow<T>>
where
    T: for<'de> serde::Deserialize<'de>,
{
    sonic_rs::from_slice(src)
        .map(Flow::Next)
        .map_err(|err| Error::Decode(format!("json decode failed: {err}")))
}

#[cfg(test)]
mod tests {
    use crate::{
        codec::{JsonDecode, JsonEncode, LineCodec},
        pipeline,
        traits::{Flow, Inbound, Outbound},
        Context, Handler, InboundContext, OutboundContext, Result,
    };

    #[derive(Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
    struct Request {
        value: String,
    }

    #[derive(Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
    struct Response {
        value: String,
    }

    struct Echo;

    impl Handler<Request> for Echo {
        type Write = Response;

        async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: Request) -> Result<()> {
            ctx.write(Response { value: msg.value }).await
        }
    }

    #[tokio::test]
    async fn decodes_json_from_string() {
        let mut decoder = JsonDecode::<Request>::new();
        let mut ctx = InboundContext::new_datagram(crate::DatagramInfo::new(
            1,
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
        ));

        let decoded = decoder
            .read(&mut ctx, r#"{"value":"hello"}"#.to_string())
            .await
            .unwrap();

        assert!(matches!(
            decoded,
            Flow::Next(Request { value }) if value == "hello"
        ));
    }

    #[tokio::test]
    async fn encodes_json_to_string() {
        let mut encoder = JsonEncode::<Response>::new();
        let mut ctx = OutboundContext::new_datagram(crate::DatagramInfo::new(
            1,
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
        ));

        let encoded = encoder
            .write(
                &mut ctx,
                Response {
                    value: "hello".to_string(),
                },
            )
            .await
            .unwrap();

        assert!(matches!(encoded, Flow::Next(json) if json == r#"{"value":"hello"}"#));
    }

    #[test]
    fn composes_with_line_codec() {
        let _pipeline = pipeline()
            .codec(LineCodec::new())
            .inbound(JsonDecode::<Request>::new())
            .handler(Echo)
            .outbound(JsonEncode::<Response>::new());
    }
}
