use bytes::BytesMut;

use crate::Result;

pub mod byte_array;
pub mod bytes_datagram;
pub mod delimiter_based_frame;
pub mod fixed_length_frame;
#[cfg(feature = "json")]
pub mod json;
pub mod length_field_based_frame;
pub mod line;
pub mod mqtt;
pub mod utf8_datagram;
#[cfg(feature = "websocket")]
pub mod websocket;

pub use byte_array::{ByteArrayDecoder, ByteArrayEncoder};
pub use bytes_datagram::BytesDatagramCodec;
pub use delimiter_based_frame::DelimiterBasedFrameDecoder;
pub use fixed_length_frame::FixedLengthFrameDecoder;
#[cfg(feature = "json")]
pub use json::{JsonDecode, JsonEncode};
pub use length_field_based_frame::{ByteOrder, LengthFieldBasedFrameDecoder, LengthFieldPrepender};
pub use line::LineCodec;
pub use mqtt::{
    AuthPacket, ConnAckPacket, ConnectPacket, DisconnectPacket, MqttCodec, MqttPacket,
    MqttProperty, PublishPacket, QoS, SubAckPacket, SubscribePacket, Subscription,
    SubscriptionOptions, UnsubAckPacket, UnsubscribePacket, Will,
};
pub use utf8_datagram::Utf8DatagramCodec;
#[cfg(feature = "websocket")]
pub use websocket::{
    WebSocketClose, WebSocketCodec, WebSocketHandshake, WebSocketHandshakeResponse,
    WebSocketInbound, WebSocketMessage, WebSocketOutbound,
};

/// Decoder for byte-stream transports such as TCP.
///
/// Implementations consume bytes from `src` only when they can produce a full
/// item. Returning `Ok(None)` means more bytes are needed.
pub trait Decoder: Send + 'static {
    /// Message type produced by this decoder.
    type Item: Send + 'static;

    /// Attempts to decode one message from the source buffer.
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>>;
}

/// Encoder for byte-stream transports such as TCP.
pub trait Encoder<I>: Send + 'static {
    /// Encodes one item into the destination buffer.
    fn encode(&mut self, item: I, dst: &mut BytesMut) -> Result<()>;
}

/// Decoder for datagram transports such as UDP.
///
/// A datagram decoder receives exactly one datagram at a time.
pub trait DatagramDecoder: Send + 'static {
    /// Message type produced by this decoder.
    type Item: Send + 'static;

    /// Decodes one datagram payload.
    fn decode_datagram(&mut self, src: &[u8]) -> Result<Self::Item>;
}

/// Encoder for datagram transports such as UDP.
pub trait DatagramEncoder<I>: Send + 'static {
    /// Encodes one datagram payload into the destination buffer.
    fn encode_datagram(&mut self, item: I, dst: &mut BytesMut) -> Result<()>;
}
