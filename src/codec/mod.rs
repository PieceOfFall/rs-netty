use bytes::BytesMut;

use crate::Result;

pub mod byte_array;
pub mod bytes_datagram;
pub mod delimiter_based_frame;
pub mod fixed_length_frame;
pub mod length_field_based_frame;
pub mod line;
pub mod mqtt;
pub mod utf8_datagram;

pub use byte_array::{ByteArrayDecoder, ByteArrayEncoder};
pub use bytes_datagram::BytesDatagramCodec;
pub use delimiter_based_frame::DelimiterBasedFrameDecoder;
pub use fixed_length_frame::FixedLengthFrameDecoder;
pub use length_field_based_frame::{ByteOrder, LengthFieldBasedFrameDecoder, LengthFieldPrepender};
pub use line::LineCodec;
pub use mqtt::{
    AuthPacket, ConnAckPacket, ConnectPacket, DisconnectPacket, MqttCodec, MqttPacket,
    MqttProperty, PublishPacket, QoS, SubAckPacket, SubscribePacket, Subscription,
    SubscriptionOptions, UnsubAckPacket, UnsubscribePacket, Will,
};
pub use utf8_datagram::Utf8DatagramCodec;

pub trait Decoder: Send + 'static {
    type Item: Send + 'static;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>>;
}

pub trait Encoder<I>: Send + 'static {
    fn encode(&mut self, item: I, dst: &mut BytesMut) -> Result<()>;
}

pub trait DatagramDecoder: Send + 'static {
    type Item: Send + 'static;

    fn decode_datagram(&mut self, src: &[u8]) -> Result<Self::Item>;
}

pub trait DatagramEncoder<I>: Send + 'static {
    fn encode_datagram(&mut self, item: I, dst: &mut BytesMut) -> Result<()>;
}
