use bytes::{Bytes, BytesMut};

use crate::{
    codec::{Decoder, Encoder},
    Error, Result,
};

const MQTT_LEVEL_5: u8 = 5;
const MAX_REMAINING_LENGTH: usize = 268_435_455;

/// MQTT Quality of Service level.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum QoS {
    /// QoS 0: deliver at most once, with no acknowledgement flow.
    AtMostOnce,
    /// QoS 1: deliver at least once, acknowledged with PUBACK.
    AtLeastOnce,
    /// QoS 2: deliver exactly once, using the PUBREC/PUBREL/PUBCOMP flow.
    ExactlyOnce,
}

impl QoS {
    fn from_u8(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::AtMostOnce),
            1 => Ok(Self::AtLeastOnce),
            2 => Ok(Self::ExactlyOnce),
            _ => Err(Error::Decode(format!("invalid MQTT QoS: {value}"))),
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::AtMostOnce => 0,
            Self::AtLeastOnce => 1,
            Self::ExactlyOnce => 2,
        }
    }
}

/// MQTT 5 control packet decoded or encoded by [`MqttCodec`].
#[derive(Debug, Clone, PartialEq)]
pub enum MqttPacket {
    Connect(ConnectPacket),
    ConnAck(ConnAckPacket),
    Publish(PublishPacket),
    PubAck(AckPacket),
    PubRec(AckPacket),
    PubRel(AckPacket),
    PubComp(AckPacket),
    Subscribe(SubscribePacket),
    SubAck(SubAckPacket),
    Unsubscribe(UnsubscribePacket),
    UnsubAck(UnsubAckPacket),
    PingReq,
    PingResp,
    Disconnect(DisconnectPacket),
    Auth(AuthPacket),
}

/// MQTT 5 CONNECT packet.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnectPacket {
    /// Whether the server should start a fresh session.
    pub clean_start: bool,
    /// Keep-alive interval in seconds.
    pub keep_alive: u16,
    pub properties: Vec<MqttProperty>,
    pub client_id: String,
    /// Optional Will Message published by the server if the client disconnects unexpectedly.
    pub will: Option<Will>,
    pub username: Option<String>,
    pub password: Option<Bytes>,
}

/// MQTT Will Message carried in a CONNECT packet.
#[derive(Debug, Clone, PartialEq)]
pub struct Will {
    pub qos: QoS,
    pub retain: bool,
    pub properties: Vec<MqttProperty>,
    pub topic: String,
    pub payload: Bytes,
}

/// MQTT 5 CONNACK packet.
#[derive(Debug, Clone, PartialEq)]
pub struct ConnAckPacket {
    /// Whether the server resumed an existing session.
    pub session_present: bool,
    pub reason_code: u8,
    pub properties: Vec<MqttProperty>,
}

/// MQTT 5 PUBLISH packet.
#[derive(Debug, Clone, PartialEq)]
pub struct PublishPacket {
    /// Duplicate delivery flag.
    pub dup: bool,
    pub qos: QoS,
    /// Whether the message should be retained by the broker.
    pub retain: bool,
    pub topic_name: String,
    /// Packet identifier for QoS 1/2 publishes.
    pub packet_id: Option<u16>,
    pub properties: Vec<MqttProperty>,
    pub payload: Bytes,
}

/// Shared packet shape for PUBACK, PUBREC, PUBREL, and PUBCOMP.
#[derive(Debug, Clone, PartialEq)]
pub struct AckPacket {
    /// Non-zero packet identifier.
    pub packet_id: u16,
    pub reason_code: u8,
    pub properties: Vec<MqttProperty>,
}

impl AckPacket {
    /// Creates an acknowledgement packet without properties.
    pub fn new(packet_id: u16, reason_code: u8) -> Self {
        Self {
            packet_id,
            reason_code,
            properties: Vec::new(),
        }
    }
}

/// MQTT 5 SUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq)]
pub struct SubscribePacket {
    /// Non-zero packet identifier.
    pub packet_id: u16,
    pub properties: Vec<MqttProperty>,
    pub subscriptions: Vec<Subscription>,
}

/// One topic filter in a SUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq)]
pub struct Subscription {
    /// Topic filter, for example `sensors/+/temperature`.
    pub topic_filter: String,
    pub options: SubscriptionOptions,
}

/// Options attached to one MQTT subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionOptions {
    /// Maximum QoS accepted for matching publications.
    pub maximum_qos: QoS,
    /// Whether messages published by this client should be excluded.
    pub no_local: bool,
    /// Whether retained messages preserve the original retain flag.
    pub retain_as_published: bool,
    /// Retain handling mode. Valid values are 0, 1, and 2.
    pub retain_handling: u8,
}

impl Default for SubscriptionOptions {
    fn default() -> Self {
        Self {
            maximum_qos: QoS::AtMostOnce,
            no_local: false,
            retain_as_published: false,
            retain_handling: 0,
        }
    }
}

/// MQTT 5 SUBACK packet.
#[derive(Debug, Clone, PartialEq)]
pub struct SubAckPacket {
    /// Non-zero packet identifier matching the SUBSCRIBE packet.
    pub packet_id: u16,
    pub properties: Vec<MqttProperty>,
    /// Reason code for each requested subscription.
    pub reason_codes: Vec<u8>,
}

/// MQTT 5 UNSUBSCRIBE packet.
#[derive(Debug, Clone, PartialEq)]
pub struct UnsubscribePacket {
    /// Non-zero packet identifier.
    pub packet_id: u16,
    pub properties: Vec<MqttProperty>,
    pub topic_filters: Vec<String>,
}

/// MQTT 5 UNSUBACK packet.
#[derive(Debug, Clone, PartialEq)]
pub struct UnsubAckPacket {
    /// Non-zero packet identifier matching the UNSUBSCRIBE packet.
    pub packet_id: u16,
    pub properties: Vec<MqttProperty>,
    /// Reason code for each requested unsubscription.
    pub reason_codes: Vec<u8>,
}

/// MQTT 5 DISCONNECT packet.
#[derive(Debug, Clone, PartialEq)]
pub struct DisconnectPacket {
    /// Disconnect reason code. The default success code is 0.
    pub reason_code: u8,
    pub properties: Vec<MqttProperty>,
}

/// MQTT 5 AUTH packet.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthPacket {
    /// Authentication reason code. The default success code is 0.
    pub reason_code: u8,
    pub properties: Vec<MqttProperty>,
}

/// MQTT 5 property value.
#[derive(Debug, Clone, PartialEq)]
pub enum MqttProperty {
    PayloadFormatIndicator(u8),
    MessageExpiryInterval(u32),
    ContentType(String),
    ResponseTopic(String),
    CorrelationData(Bytes),
    SubscriptionIdentifier(u32),
    SessionExpiryInterval(u32),
    AssignedClientIdentifier(String),
    ServerKeepAlive(u16),
    AuthenticationMethod(String),
    AuthenticationData(Bytes),
    RequestProblemInformation(u8),
    WillDelayInterval(u32),
    RequestResponseInformation(u8),
    ResponseInformation(String),
    ServerReference(String),
    ReasonString(String),
    ReceiveMaximum(u16),
    TopicAliasMaximum(u16),
    TopicAlias(u16),
    MaximumQoS(u8),
    RetainAvailable(u8),
    /// User Property key/value pair. Unlike most MQTT properties, this can appear multiple times.
    UserProperty(String, String),
    MaximumPacketSize(u32),
    WildcardSubscriptionAvailable(u8),
    SubscriptionIdentifierAvailable(u8),
    SharedSubscriptionAvailable(u8),
}

/// MQTT 5 packet codec for TCP stream pipelines.
///
/// The codec handles MQTT fixed headers, Remaining Length framing, MQTT 5
/// variable byte integers, supported control packet bodies, and MQTT 5
/// properties. It does not maintain broker/client session state; semantic
/// validation that depends on connection state belongs in a handler.
pub struct MqttCodec {
    max_packet_size: usize,
}

impl MqttCodec {
    /// Creates a codec that accepts packets up to the MQTT Remaining Length maximum.
    pub fn new() -> Self {
        Self {
            max_packet_size: MAX_REMAINING_LENGTH,
        }
    }

    /// Creates a codec with a smaller maximum MQTT packet body size.
    pub fn with_max_packet_size(max_packet_size: usize) -> Self {
        Self { max_packet_size }
    }
}

impl Default for MqttCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for MqttCodec {
    type Item = MqttPacket;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>> {
        if src.len() < 2 {
            return Ok(None);
        }

        let fixed_header = src[0];
        let Some((remaining_len, remaining_len_bytes)) = decode_remaining_length_prefix(&src[1..])?
        else {
            return Ok(None);
        };

        if remaining_len > self.max_packet_size {
            return Err(Error::FrameTooLarge {
                current: remaining_len,
                max: self.max_packet_size,
            });
        }

        let header_len = 1 + remaining_len_bytes;
        let packet_len = header_len
            .checked_add(remaining_len)
            .ok_or_else(|| Error::Decode("MQTT packet length overflow".to_string()))?;

        if src.len() < packet_len {
            return Ok(None);
        }

        let packet = src.split_to(packet_len);
        let mut reader = Reader::new(&packet[header_len..]);
        let packet_type = fixed_header >> 4;
        let flags = fixed_header & 0x0f;
        let decoded = decode_packet(packet_type, flags, &mut reader)?;

        if reader.remaining() != 0 {
            return Err(Error::Decode(format!(
                "MQTT packet has {} trailing bytes",
                reader.remaining()
            )));
        }

        Ok(Some(decoded))
    }
}

impl Encoder<MqttPacket> for MqttCodec {
    fn encode(&mut self, item: MqttPacket, dst: &mut BytesMut) -> Result<()> {
        let (packet_type, flags, body) = encode_packet(item)?;
        if body.len() > self.max_packet_size {
            return Err(Error::FrameTooLarge {
                current: body.len(),
                max: self.max_packet_size,
            });
        }

        dst.reserve(1 + remaining_length_len(body.len()) + body.len());
        dst.extend_from_slice(&[(packet_type << 4) | flags]);
        write_variable_integer(body.len() as u32, dst)?;
        dst.extend_from_slice(&body);
        Ok(())
    }
}

fn decode_packet(packet_type: u8, flags: u8, reader: &mut Reader<'_>) -> Result<MqttPacket> {
    match packet_type {
        1 => {
            expect_flags(packet_type, flags, 0)?;
            decode_connect(reader).map(MqttPacket::Connect)
        }
        2 => {
            expect_flags(packet_type, flags, 0)?;
            decode_connack(reader).map(MqttPacket::ConnAck)
        }
        3 => decode_publish(flags, reader).map(MqttPacket::Publish),
        4 => {
            expect_flags(packet_type, flags, 0)?;
            decode_ack(reader).map(MqttPacket::PubAck)
        }
        5 => {
            expect_flags(packet_type, flags, 0)?;
            decode_ack(reader).map(MqttPacket::PubRec)
        }
        6 => {
            expect_flags(packet_type, flags, 2)?;
            decode_ack(reader).map(MqttPacket::PubRel)
        }
        7 => {
            expect_flags(packet_type, flags, 0)?;
            decode_ack(reader).map(MqttPacket::PubComp)
        }
        8 => {
            expect_flags(packet_type, flags, 2)?;
            decode_subscribe(reader).map(MqttPacket::Subscribe)
        }
        9 => {
            expect_flags(packet_type, flags, 0)?;
            decode_suback(reader).map(MqttPacket::SubAck)
        }
        10 => {
            expect_flags(packet_type, flags, 2)?;
            decode_unsubscribe(reader).map(MqttPacket::Unsubscribe)
        }
        11 => {
            expect_flags(packet_type, flags, 0)?;
            decode_unsuback(reader).map(MqttPacket::UnsubAck)
        }
        12 => {
            expect_flags(packet_type, flags, 0)?;
            expect_empty(reader)?;
            Ok(MqttPacket::PingReq)
        }
        13 => {
            expect_flags(packet_type, flags, 0)?;
            expect_empty(reader)?;
            Ok(MqttPacket::PingResp)
        }
        14 => {
            expect_flags(packet_type, flags, 0)?;
            decode_disconnect(reader).map(MqttPacket::Disconnect)
        }
        15 => {
            expect_flags(packet_type, flags, 0)?;
            decode_auth(reader).map(MqttPacket::Auth)
        }
        _ => Err(Error::Decode(format!(
            "invalid MQTT packet type: {packet_type}"
        ))),
    }
}

fn encode_packet(packet: MqttPacket) -> Result<(u8, u8, BytesMut)> {
    let mut body = BytesMut::new();
    let (packet_type, flags) = match packet {
        MqttPacket::Connect(packet) => {
            encode_connect(packet, &mut body)?;
            (1, 0)
        }
        MqttPacket::ConnAck(packet) => {
            encode_connack(packet, &mut body)?;
            (2, 0)
        }
        MqttPacket::Publish(packet) => {
            let flags = encode_publish(packet, &mut body)?;
            (3, flags)
        }
        MqttPacket::PubAck(packet) => {
            encode_ack(packet, &mut body)?;
            (4, 0)
        }
        MqttPacket::PubRec(packet) => {
            encode_ack(packet, &mut body)?;
            (5, 0)
        }
        MqttPacket::PubRel(packet) => {
            encode_ack(packet, &mut body)?;
            (6, 2)
        }
        MqttPacket::PubComp(packet) => {
            encode_ack(packet, &mut body)?;
            (7, 0)
        }
        MqttPacket::Subscribe(packet) => {
            encode_subscribe(packet, &mut body)?;
            (8, 2)
        }
        MqttPacket::SubAck(packet) => {
            encode_suback(packet, &mut body)?;
            (9, 0)
        }
        MqttPacket::Unsubscribe(packet) => {
            encode_unsubscribe(packet, &mut body)?;
            (10, 2)
        }
        MqttPacket::UnsubAck(packet) => {
            encode_unsuback(packet, &mut body)?;
            (11, 0)
        }
        MqttPacket::PingReq => (12, 0),
        MqttPacket::PingResp => (13, 0),
        MqttPacket::Disconnect(packet) => {
            encode_disconnect(packet, &mut body)?;
            (14, 0)
        }
        MqttPacket::Auth(packet) => {
            encode_auth(packet, &mut body)?;
            (15, 0)
        }
    };

    Ok((packet_type, flags, body))
}

fn decode_connect(reader: &mut Reader<'_>) -> Result<ConnectPacket> {
    let protocol_name = reader.read_utf8_string()?;
    if protocol_name != "MQTT" {
        return Err(Error::Decode(format!(
            "invalid MQTT protocol name: {protocol_name}"
        )));
    }

    let protocol_level = reader.read_u8()?;
    if protocol_level != MQTT_LEVEL_5 {
        return Err(Error::Decode(format!(
            "unsupported MQTT protocol level: {protocol_level}"
        )));
    }

    let flags = reader.read_u8()?;
    if flags & 0x01 != 0 {
        return Err(Error::Decode(
            "MQTT CONNECT reserved flag must be zero".to_string(),
        ));
    }

    let username_flag = flags & 0x80 != 0;
    let password_flag = flags & 0x40 != 0;
    let will_retain = flags & 0x20 != 0;
    let will_qos = QoS::from_u8((flags >> 3) & 0x03)?;
    let will_flag = flags & 0x04 != 0;
    let clean_start = flags & 0x02 != 0;

    if !will_flag && (will_retain || will_qos != QoS::AtMostOnce) {
        return Err(Error::Decode(
            "MQTT CONNECT will flags set without will flag".to_string(),
        ));
    }
    if password_flag && !username_flag {
        return Err(Error::Decode(
            "MQTT CONNECT password flag set without username flag".to_string(),
        ));
    }

    let keep_alive = reader.read_u16()?;
    let properties = reader.read_properties()?;
    let client_id = reader.read_utf8_string()?;
    let will = if will_flag {
        Some(Will {
            qos: will_qos,
            retain: will_retain,
            properties: reader.read_properties()?,
            topic: reader.read_utf8_string()?,
            payload: reader.read_binary_data()?,
        })
    } else {
        None
    };
    let username = if username_flag {
        Some(reader.read_utf8_string()?)
    } else {
        None
    };
    let password = if password_flag {
        Some(reader.read_binary_data()?)
    } else {
        None
    };

    Ok(ConnectPacket {
        clean_start,
        keep_alive,
        properties,
        client_id,
        will,
        username,
        password,
    })
}

fn encode_connect(packet: ConnectPacket, dst: &mut BytesMut) -> Result<()> {
    if packet.password.is_some() && packet.username.is_none() {
        return Err(Error::Encode(
            "MQTT CONNECT password requires username".to_string(),
        ));
    }

    write_utf8_string("MQTT", dst)?;
    write_u8(MQTT_LEVEL_5, dst);

    let mut flags = 0_u8;
    if packet.username.is_some() {
        flags |= 0x80;
    }
    if packet.password.is_some() {
        flags |= 0x40;
    }
    if let Some(will) = &packet.will {
        flags |= 0x04;
        flags |= will.qos.as_u8() << 3;
        if will.retain {
            flags |= 0x20;
        }
    }
    if packet.clean_start {
        flags |= 0x02;
    }

    write_u8(flags, dst);
    write_u16(packet.keep_alive, dst);
    write_properties(&packet.properties, dst)?;
    write_utf8_string(&packet.client_id, dst)?;

    if let Some(will) = packet.will {
        write_properties(&will.properties, dst)?;
        write_utf8_string(&will.topic, dst)?;
        write_binary_data(&will.payload, dst)?;
    }
    if let Some(username) = packet.username {
        write_utf8_string(&username, dst)?;
    }
    if let Some(password) = packet.password {
        write_binary_data(&password, dst)?;
    }

    Ok(())
}

fn decode_connack(reader: &mut Reader<'_>) -> Result<ConnAckPacket> {
    let flags = reader.read_u8()?;
    if flags & !0x01 != 0 {
        return Err(Error::Decode("invalid MQTT CONNACK flags".to_string()));
    }

    Ok(ConnAckPacket {
        session_present: flags & 0x01 != 0,
        reason_code: reader.read_u8()?,
        properties: reader.read_properties()?,
    })
}

fn encode_connack(packet: ConnAckPacket, dst: &mut BytesMut) -> Result<()> {
    write_u8(u8::from(packet.session_present), dst);
    write_u8(packet.reason_code, dst);
    write_properties(&packet.properties, dst)
}

fn decode_publish(flags: u8, reader: &mut Reader<'_>) -> Result<PublishPacket> {
    let dup = flags & 0x08 != 0;
    let qos = QoS::from_u8((flags >> 1) & 0x03)?;
    if ((flags >> 1) & 0x03) == 3 {
        return Err(Error::Decode("invalid MQTT PUBLISH QoS bits".to_string()));
    }
    let retain = flags & 0x01 != 0;
    let topic_name = reader.read_utf8_string()?;
    let packet_id = if qos == QoS::AtMostOnce {
        None
    } else {
        let packet_id = reader.read_u16()?;
        ensure_nonzero_packet_id_decode(packet_id)?;
        Some(packet_id)
    };
    let properties = reader.read_properties()?;
    let payload = reader.read_remaining_bytes();

    Ok(PublishPacket {
        dup,
        qos,
        retain,
        topic_name,
        packet_id,
        properties,
        payload,
    })
}

fn encode_publish(packet: PublishPacket, dst: &mut BytesMut) -> Result<u8> {
    if packet.qos == QoS::AtMostOnce && packet.packet_id.is_some() {
        return Err(Error::Encode(
            "QoS 0 MQTT PUBLISH must not include packet_id".to_string(),
        ));
    }
    if packet.qos != QoS::AtMostOnce && packet.packet_id.is_none() {
        return Err(Error::Encode(
            "QoS 1/2 MQTT PUBLISH must include packet_id".to_string(),
        ));
    }
    if let Some(packet_id) = packet.packet_id {
        ensure_nonzero_packet_id_encode(packet_id)?;
    }

    let mut flags = packet.qos.as_u8() << 1;
    if packet.dup {
        flags |= 0x08;
    }
    if packet.retain {
        flags |= 0x01;
    }

    write_utf8_string(&packet.topic_name, dst)?;
    if let Some(packet_id) = packet.packet_id {
        write_u16(packet_id, dst);
    }
    write_properties(&packet.properties, dst)?;
    dst.extend_from_slice(&packet.payload);
    Ok(flags)
}

fn decode_ack(reader: &mut Reader<'_>) -> Result<AckPacket> {
    let packet_id = reader.read_u16()?;
    ensure_nonzero_packet_id_decode(packet_id)?;
    if reader.remaining() == 0 {
        return Ok(AckPacket::new(packet_id, 0));
    }

    let reason_code = reader.read_u8()?;
    let properties = if reader.remaining() == 0 {
        Vec::new()
    } else {
        reader.read_properties()?
    };

    Ok(AckPacket {
        packet_id,
        reason_code,
        properties,
    })
}

fn encode_ack(packet: AckPacket, dst: &mut BytesMut) -> Result<()> {
    ensure_nonzero_packet_id_encode(packet.packet_id)?;
    write_u16(packet.packet_id, dst);
    if packet.reason_code != 0 || !packet.properties.is_empty() {
        write_u8(packet.reason_code, dst);
    }
    if !packet.properties.is_empty() {
        write_properties(&packet.properties, dst)?;
    }
    Ok(())
}

fn decode_subscribe(reader: &mut Reader<'_>) -> Result<SubscribePacket> {
    let packet_id = reader.read_u16()?;
    ensure_nonzero_packet_id_decode(packet_id)?;
    let properties = reader.read_properties()?;
    let mut subscriptions = Vec::new();

    while reader.remaining() > 0 {
        let topic_filter = reader.read_utf8_string()?;
        let options = reader.read_u8()?;
        let retain_handling = (options >> 4) & 0x03;
        if options & 0xc0 != 0 || retain_handling == 3 {
            return Err(Error::Decode("invalid MQTT SUBSCRIBE options".to_string()));
        }
        subscriptions.push(Subscription {
            topic_filter,
            options: SubscriptionOptions {
                maximum_qos: QoS::from_u8(options & 0x03)?,
                no_local: options & 0x04 != 0,
                retain_as_published: options & 0x08 != 0,
                retain_handling,
            },
        });
    }

    if subscriptions.is_empty() {
        return Err(Error::Decode(
            "MQTT SUBSCRIBE must include at least one subscription".to_string(),
        ));
    }

    Ok(SubscribePacket {
        packet_id,
        properties,
        subscriptions,
    })
}

fn encode_subscribe(packet: SubscribePacket, dst: &mut BytesMut) -> Result<()> {
    if packet.subscriptions.is_empty() {
        return Err(Error::Encode(
            "MQTT SUBSCRIBE must include at least one subscription".to_string(),
        ));
    }
    ensure_nonzero_packet_id_encode(packet.packet_id)?;

    write_u16(packet.packet_id, dst);
    write_properties(&packet.properties, dst)?;
    for subscription in packet.subscriptions {
        if subscription.options.retain_handling > 2 {
            return Err(Error::Encode(
                "invalid MQTT SUBSCRIBE retain handling option".to_string(),
            ));
        }
        write_utf8_string(&subscription.topic_filter, dst)?;
        let options = subscription.options.maximum_qos.as_u8()
            | (u8::from(subscription.options.no_local) << 2)
            | (u8::from(subscription.options.retain_as_published) << 3)
            | (subscription.options.retain_handling << 4);
        write_u8(options, dst);
    }

    Ok(())
}

fn decode_suback(reader: &mut Reader<'_>) -> Result<SubAckPacket> {
    let packet_id = reader.read_u16()?;
    ensure_nonzero_packet_id_decode(packet_id)?;
    let properties = reader.read_properties()?;
    let reason_codes = reader.read_remaining_bytes().to_vec();
    if reason_codes.is_empty() {
        return Err(Error::Decode(
            "MQTT SUBACK must include at least one reason code".to_string(),
        ));
    }

    Ok(SubAckPacket {
        packet_id,
        properties,
        reason_codes,
    })
}

fn encode_suback(packet: SubAckPacket, dst: &mut BytesMut) -> Result<()> {
    ensure_nonzero_packet_id_encode(packet.packet_id)?;
    if packet.reason_codes.is_empty() {
        return Err(Error::Encode(
            "MQTT SUBACK must include at least one reason code".to_string(),
        ));
    }

    write_u16(packet.packet_id, dst);
    write_properties(&packet.properties, dst)?;
    dst.extend_from_slice(&packet.reason_codes);
    Ok(())
}

fn decode_unsubscribe(reader: &mut Reader<'_>) -> Result<UnsubscribePacket> {
    let packet_id = reader.read_u16()?;
    ensure_nonzero_packet_id_decode(packet_id)?;
    let properties = reader.read_properties()?;
    let mut topic_filters = Vec::new();

    while reader.remaining() > 0 {
        topic_filters.push(reader.read_utf8_string()?);
    }

    if topic_filters.is_empty() {
        return Err(Error::Decode(
            "MQTT UNSUBSCRIBE must include at least one topic filter".to_string(),
        ));
    }

    Ok(UnsubscribePacket {
        packet_id,
        properties,
        topic_filters,
    })
}

fn encode_unsubscribe(packet: UnsubscribePacket, dst: &mut BytesMut) -> Result<()> {
    if packet.topic_filters.is_empty() {
        return Err(Error::Encode(
            "MQTT UNSUBSCRIBE must include at least one topic filter".to_string(),
        ));
    }
    ensure_nonzero_packet_id_encode(packet.packet_id)?;

    write_u16(packet.packet_id, dst);
    write_properties(&packet.properties, dst)?;
    for topic_filter in packet.topic_filters {
        write_utf8_string(&topic_filter, dst)?;
    }
    Ok(())
}

fn decode_unsuback(reader: &mut Reader<'_>) -> Result<UnsubAckPacket> {
    let packet_id = reader.read_u16()?;
    ensure_nonzero_packet_id_decode(packet_id)?;
    let properties = reader.read_properties()?;
    let reason_codes = reader.read_remaining_bytes().to_vec();
    if reason_codes.is_empty() {
        return Err(Error::Decode(
            "MQTT UNSUBACK must include at least one reason code".to_string(),
        ));
    }

    Ok(UnsubAckPacket {
        packet_id,
        properties,
        reason_codes,
    })
}

fn encode_unsuback(packet: UnsubAckPacket, dst: &mut BytesMut) -> Result<()> {
    ensure_nonzero_packet_id_encode(packet.packet_id)?;
    if packet.reason_codes.is_empty() {
        return Err(Error::Encode(
            "MQTT UNSUBACK must include at least one reason code".to_string(),
        ));
    }

    write_u16(packet.packet_id, dst);
    write_properties(&packet.properties, dst)?;
    dst.extend_from_slice(&packet.reason_codes);
    Ok(())
}

fn decode_disconnect(reader: &mut Reader<'_>) -> Result<DisconnectPacket> {
    if reader.remaining() == 0 {
        return Ok(DisconnectPacket {
            reason_code: 0,
            properties: Vec::new(),
        });
    }

    let reason_code = reader.read_u8()?;
    let properties = if reader.remaining() == 0 {
        Vec::new()
    } else {
        reader.read_properties()?
    };

    Ok(DisconnectPacket {
        reason_code,
        properties,
    })
}

fn encode_disconnect(packet: DisconnectPacket, dst: &mut BytesMut) -> Result<()> {
    if packet.reason_code != 0 || !packet.properties.is_empty() {
        write_u8(packet.reason_code, dst);
    }
    if !packet.properties.is_empty() {
        write_properties(&packet.properties, dst)?;
    }
    Ok(())
}

fn decode_auth(reader: &mut Reader<'_>) -> Result<AuthPacket> {
    if reader.remaining() == 0 {
        return Ok(AuthPacket {
            reason_code: 0,
            properties: Vec::new(),
        });
    }

    let reason_code = reader.read_u8()?;
    let properties = if reader.remaining() == 0 {
        Vec::new()
    } else {
        reader.read_properties()?
    };

    Ok(AuthPacket {
        reason_code,
        properties,
    })
}

fn encode_auth(packet: AuthPacket, dst: &mut BytesMut) -> Result<()> {
    if packet.reason_code != 0 || !packet.properties.is_empty() {
        write_u8(packet.reason_code, dst);
    }
    if !packet.properties.is_empty() {
        write_properties(&packet.properties, dst)?;
    }
    Ok(())
}

fn decode_property(reader: &mut Reader<'_>) -> Result<MqttProperty> {
    let id = reader.read_variable_integer()? as u8;
    match id {
        0x01 => Ok(MqttProperty::PayloadFormatIndicator(reader.read_u8()?)),
        0x02 => Ok(MqttProperty::MessageExpiryInterval(reader.read_u32()?)),
        0x03 => Ok(MqttProperty::ContentType(reader.read_utf8_string()?)),
        0x08 => Ok(MqttProperty::ResponseTopic(reader.read_utf8_string()?)),
        0x09 => Ok(MqttProperty::CorrelationData(reader.read_binary_data()?)),
        0x0b => Ok(MqttProperty::SubscriptionIdentifier(
            reader.read_variable_integer()?,
        )),
        0x11 => Ok(MqttProperty::SessionExpiryInterval(reader.read_u32()?)),
        0x12 => Ok(MqttProperty::AssignedClientIdentifier(
            reader.read_utf8_string()?,
        )),
        0x13 => Ok(MqttProperty::ServerKeepAlive(reader.read_u16()?)),
        0x15 => Ok(MqttProperty::AuthenticationMethod(
            reader.read_utf8_string()?,
        )),
        0x16 => Ok(MqttProperty::AuthenticationData(reader.read_binary_data()?)),
        0x17 => Ok(MqttProperty::RequestProblemInformation(reader.read_u8()?)),
        0x18 => Ok(MqttProperty::WillDelayInterval(reader.read_u32()?)),
        0x19 => Ok(MqttProperty::RequestResponseInformation(reader.read_u8()?)),
        0x1a => Ok(MqttProperty::ResponseInformation(
            reader.read_utf8_string()?,
        )),
        0x1c => Ok(MqttProperty::ServerReference(reader.read_utf8_string()?)),
        0x1f => Ok(MqttProperty::ReasonString(reader.read_utf8_string()?)),
        0x21 => Ok(MqttProperty::ReceiveMaximum(reader.read_u16()?)),
        0x22 => Ok(MqttProperty::TopicAliasMaximum(reader.read_u16()?)),
        0x23 => Ok(MqttProperty::TopicAlias(reader.read_u16()?)),
        0x24 => Ok(MqttProperty::MaximumQoS(reader.read_u8()?)),
        0x25 => Ok(MqttProperty::RetainAvailable(reader.read_u8()?)),
        0x26 => Ok(MqttProperty::UserProperty(
            reader.read_utf8_string()?,
            reader.read_utf8_string()?,
        )),
        0x27 => Ok(MqttProperty::MaximumPacketSize(reader.read_u32()?)),
        0x28 => Ok(MqttProperty::WildcardSubscriptionAvailable(
            reader.read_u8()?,
        )),
        0x29 => Ok(MqttProperty::SubscriptionIdentifierAvailable(
            reader.read_u8()?,
        )),
        0x2a => Ok(MqttProperty::SharedSubscriptionAvailable(reader.read_u8()?)),
        _ => Err(Error::Decode(format!("unknown MQTT v5 property id: {id}"))),
    }
}

fn encode_property(property: &MqttProperty, dst: &mut BytesMut) -> Result<()> {
    match property {
        MqttProperty::PayloadFormatIndicator(value) => {
            write_variable_integer(0x01, dst)?;
            write_u8(*value, dst);
        }
        MqttProperty::MessageExpiryInterval(value) => {
            write_variable_integer(0x02, dst)?;
            write_u32(*value, dst);
        }
        MqttProperty::ContentType(value) => {
            write_variable_integer(0x03, dst)?;
            write_utf8_string(value, dst)?;
        }
        MqttProperty::ResponseTopic(value) => {
            write_variable_integer(0x08, dst)?;
            write_utf8_string(value, dst)?;
        }
        MqttProperty::CorrelationData(value) => {
            write_variable_integer(0x09, dst)?;
            write_binary_data(value, dst)?;
        }
        MqttProperty::SubscriptionIdentifier(value) => {
            write_variable_integer(0x0b, dst)?;
            write_variable_integer(*value, dst)?;
        }
        MqttProperty::SessionExpiryInterval(value) => {
            write_variable_integer(0x11, dst)?;
            write_u32(*value, dst);
        }
        MqttProperty::AssignedClientIdentifier(value) => {
            write_variable_integer(0x12, dst)?;
            write_utf8_string(value, dst)?;
        }
        MqttProperty::ServerKeepAlive(value) => {
            write_variable_integer(0x13, dst)?;
            write_u16(*value, dst);
        }
        MqttProperty::AuthenticationMethod(value) => {
            write_variable_integer(0x15, dst)?;
            write_utf8_string(value, dst)?;
        }
        MqttProperty::AuthenticationData(value) => {
            write_variable_integer(0x16, dst)?;
            write_binary_data(value, dst)?;
        }
        MqttProperty::RequestProblemInformation(value) => {
            write_variable_integer(0x17, dst)?;
            write_u8(*value, dst);
        }
        MqttProperty::WillDelayInterval(value) => {
            write_variable_integer(0x18, dst)?;
            write_u32(*value, dst);
        }
        MqttProperty::RequestResponseInformation(value) => {
            write_variable_integer(0x19, dst)?;
            write_u8(*value, dst);
        }
        MqttProperty::ResponseInformation(value) => {
            write_variable_integer(0x1a, dst)?;
            write_utf8_string(value, dst)?;
        }
        MqttProperty::ServerReference(value) => {
            write_variable_integer(0x1c, dst)?;
            write_utf8_string(value, dst)?;
        }
        MqttProperty::ReasonString(value) => {
            write_variable_integer(0x1f, dst)?;
            write_utf8_string(value, dst)?;
        }
        MqttProperty::ReceiveMaximum(value) => {
            write_variable_integer(0x21, dst)?;
            write_u16(*value, dst);
        }
        MqttProperty::TopicAliasMaximum(value) => {
            write_variable_integer(0x22, dst)?;
            write_u16(*value, dst);
        }
        MqttProperty::TopicAlias(value) => {
            write_variable_integer(0x23, dst)?;
            write_u16(*value, dst);
        }
        MqttProperty::MaximumQoS(value) => {
            write_variable_integer(0x24, dst)?;
            write_u8(*value, dst);
        }
        MqttProperty::RetainAvailable(value) => {
            write_variable_integer(0x25, dst)?;
            write_u8(*value, dst);
        }
        MqttProperty::UserProperty(key, value) => {
            write_variable_integer(0x26, dst)?;
            write_utf8_string(key, dst)?;
            write_utf8_string(value, dst)?;
        }
        MqttProperty::MaximumPacketSize(value) => {
            write_variable_integer(0x27, dst)?;
            write_u32(*value, dst);
        }
        MqttProperty::WildcardSubscriptionAvailable(value) => {
            write_variable_integer(0x28, dst)?;
            write_u8(*value, dst);
        }
        MqttProperty::SubscriptionIdentifierAvailable(value) => {
            write_variable_integer(0x29, dst)?;
            write_u8(*value, dst);
        }
        MqttProperty::SharedSubscriptionAvailable(value) => {
            write_variable_integer(0x2a, dst)?;
            write_u8(*value, dst);
        }
    }

    Ok(())
}

fn expect_flags(packet_type: u8, actual: u8, expected: u8) -> Result<()> {
    if actual != expected {
        return Err(Error::Decode(format!(
            "invalid MQTT flags for packet type {packet_type}: actual={actual:#x}, expected={expected:#x}"
        )));
    }
    Ok(())
}

fn expect_empty(reader: &Reader<'_>) -> Result<()> {
    if reader.remaining() != 0 {
        return Err(Error::Decode(format!(
            "MQTT packet body must be empty, got {} bytes",
            reader.remaining()
        )));
    }
    Ok(())
}

fn ensure_nonzero_packet_id_decode(packet_id: u16) -> Result<()> {
    if packet_id == 0 {
        return Err(Error::Decode(
            "MQTT packet identifier must be non-zero".to_string(),
        ));
    }
    Ok(())
}

fn ensure_nonzero_packet_id_encode(packet_id: u16) -> Result<()> {
    if packet_id == 0 {
        return Err(Error::Encode(
            "MQTT packet identifier must be non-zero".to_string(),
        ));
    }
    Ok(())
}

fn decode_remaining_length_prefix(src: &[u8]) -> Result<Option<(usize, usize)>> {
    let mut multiplier = 1_usize;
    let mut value = 0_usize;

    for (index, encoded) in src.iter().copied().enumerate().take(4) {
        value += ((encoded & 0x7f) as usize) * multiplier;
        if encoded & 0x80 == 0 {
            validate_variable_integer_encoding(value, index + 1)?;
            return Ok(Some((value, index + 1)));
        }
        multiplier *= 128;
    }

    if src.len() >= 4 {
        return Err(Error::Decode("malformed MQTT Remaining Length".to_string()));
    }

    Ok(None)
}

fn validate_variable_integer_encoding(value: usize, encoded_len: usize) -> Result<()> {
    if remaining_length_len(value) != encoded_len {
        return Err(Error::Decode(
            "MQTT variable byte integer is not minimally encoded".to_string(),
        ));
    }
    Ok(())
}

fn remaining_length_len(mut value: usize) -> usize {
    let mut len = 1;
    while value >= 128 {
        value /= 128;
        len += 1;
    }
    len
}

fn write_properties(properties: &[MqttProperty], dst: &mut BytesMut) -> Result<()> {
    let mut properties_buf = BytesMut::new();
    for property in properties {
        encode_property(property, &mut properties_buf)?;
    }

    write_variable_integer(properties_buf.len() as u32, dst)?;
    dst.extend_from_slice(&properties_buf);
    Ok(())
}

fn write_variable_integer(mut value: u32, dst: &mut BytesMut) -> Result<()> {
    if value as usize > MAX_REMAINING_LENGTH {
        return Err(Error::Encode(format!(
            "MQTT variable integer too large: {value}"
        )));
    }

    loop {
        let mut encoded = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            encoded |= 0x80;
        }
        dst.extend_from_slice(&[encoded]);
        if value == 0 {
            break;
        }
    }

    Ok(())
}

fn write_u8(value: u8, dst: &mut BytesMut) {
    dst.extend_from_slice(&[value]);
}

fn write_u16(value: u16, dst: &mut BytesMut) {
    dst.extend_from_slice(&value.to_be_bytes());
}

fn write_u32(value: u32, dst: &mut BytesMut) {
    dst.extend_from_slice(&value.to_be_bytes());
}

fn write_utf8_string(value: &str, dst: &mut BytesMut) -> Result<()> {
    let len = u16::try_from(value.len()).map_err(|err| Error::Encode(err.to_string()))?;
    write_u16(len, dst);
    dst.extend_from_slice(value.as_bytes());
    Ok(())
}

fn write_binary_data(value: &Bytes, dst: &mut BytesMut) -> Result<()> {
    let len = u16::try_from(value.len()).map_err(|err| Error::Encode(err.to_string()))?;
    write_u16(len, dst);
    dst.extend_from_slice(value);
    Ok(())
}

struct Reader<'src> {
    src: &'src [u8],
    pos: usize,
}

impl<'src> Reader<'src> {
    fn new(src: &'src [u8]) -> Self {
        Self { src, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.src.len().saturating_sub(self.pos)
    }

    fn read_exact(&mut self, len: usize) -> Result<&'src [u8]> {
        if self.remaining() < len {
            return Err(Error::Decode(format!(
                "unexpected end of MQTT packet: need {len}, remaining {}",
                self.remaining()
            )));
        }

        let start = self.pos;
        self.pos += len;
        Ok(&self.src[start..self.pos])
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_utf8_string(&mut self) -> Result<String> {
        let len = self.read_u16()? as usize;
        let bytes = self.read_exact(len)?;
        String::from_utf8(bytes.to_vec()).map_err(|err| Error::Decode(err.to_string()))
    }

    fn read_binary_data(&mut self) -> Result<Bytes> {
        let len = self.read_u16()? as usize;
        Ok(Bytes::copy_from_slice(self.read_exact(len)?))
    }

    fn read_variable_integer(&mut self) -> Result<u32> {
        let mut multiplier = 1_u32;
        let mut value = 0_u32;

        for encoded_len in 1..=4 {
            let encoded = self.read_u8()?;
            value += ((encoded & 0x7f) as u32) * multiplier;
            if encoded & 0x80 == 0 {
                validate_variable_integer_encoding(value as usize, encoded_len)?;
                return Ok(value);
            }
            multiplier *= 128;
        }

        Err(Error::Decode("malformed MQTT variable integer".to_string()))
    }

    fn read_properties(&mut self) -> Result<Vec<MqttProperty>> {
        let len = self.read_variable_integer()? as usize;
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| Error::Decode("MQTT properties length overflow".to_string()))?;

        if end > self.src.len() {
            return Err(Error::Decode(format!(
                "MQTT properties length exceeds packet: end={end}, len={}",
                self.src.len()
            )));
        }

        let mut property_reader = Reader::new(&self.src[self.pos..end]);
        let mut properties = Vec::new();
        while property_reader.remaining() > 0 {
            properties.push(decode_property(&mut property_reader)?);
        }
        self.pos = end;
        Ok(properties)
    }

    fn read_remaining_bytes(&mut self) -> Bytes {
        let bytes = Bytes::copy_from_slice(&self.src[self.pos..]);
        self.pos = self.src.len();
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_connect_v5() {
        let mut codec = MqttCodec::new();
        let mut buf = BytesMut::from(
            &[
                0x10, 0x12, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3c, 0x00, 0x00,
                0x05, b'a', b'l', b'i', b'c', b'e',
            ][..],
        );

        let packet = codec.decode(&mut buf).expect("decode").expect("packet");
        assert_eq!(
            packet,
            MqttPacket::Connect(ConnectPacket {
                clean_start: true,
                keep_alive: 60,
                properties: Vec::new(),
                client_id: "alice".to_string(),
                will: None,
                username: None,
                password: None,
            })
        );
    }

    #[test]
    fn roundtrips_publish_with_properties() {
        let mut codec = MqttCodec::new();
        let packet = MqttPacket::Publish(PublishPacket {
            dup: false,
            qos: QoS::AtLeastOnce,
            retain: false,
            topic_name: "sensors/temp".to_string(),
            packet_id: Some(7),
            properties: vec![
                MqttProperty::PayloadFormatIndicator(1),
                MqttProperty::ContentType("text/plain".to_string()),
                MqttProperty::UserProperty("source".to_string(), "lab".to_string()),
            ],
            payload: Bytes::from_static(b"21.5"),
        });

        let mut buf = BytesMut::new();
        codec.encode(packet.clone(), &mut buf).expect("encode");
        assert_eq!(codec.decode(&mut buf).expect("decode"), Some(packet));
    }

    #[test]
    fn waits_for_complete_remaining_length() {
        let mut codec = MqttCodec::new();
        let mut buf = BytesMut::from(&[0x30, 0x80][..]);
        assert!(codec.decode(&mut buf).expect("decode").is_none());
    }

    #[test]
    fn round_trips_subscribe() {
        let mut codec = MqttCodec::new();
        let packet = MqttPacket::Subscribe(SubscribePacket {
            packet_id: 10,
            properties: vec![MqttProperty::SubscriptionIdentifier(42)],
            subscriptions: vec![Subscription {
                topic_filter: "devices/+/status".to_string(),
                options: SubscriptionOptions {
                    maximum_qos: QoS::AtLeastOnce,
                    no_local: true,
                    retain_as_published: false,
                    retain_handling: 1,
                },
            }],
        });

        let mut buf = BytesMut::new();
        codec.encode(packet.clone(), &mut buf).expect("encode");
        assert_eq!(codec.decode(&mut buf).expect("decode"), Some(packet));
    }

    #[test]
    fn rejects_non_minimal_remaining_length() {
        let mut codec = MqttCodec::new();
        let mut buf = BytesMut::from(&[0x30, 0x80, 0x00][..]);

        assert!(matches!(codec.decode(&mut buf), Err(Error::Decode(_))));
    }

    #[test]
    fn rejects_zero_packet_identifier() {
        let mut codec = MqttCodec::new();
        let mut buf = BytesMut::from(&[0x40, 0x02, 0x00, 0x00][..]);

        assert!(matches!(codec.decode(&mut buf), Err(Error::Decode(_))));
    }

    #[test]
    fn rejects_invalid_subscribe_options() {
        let mut codec = MqttCodec::new();
        let mut buf = BytesMut::from(&[0x82, 0x07, 0x00, 0x01, 0x00, 0x00, 0x01, b'a', 0x30][..]);

        assert!(matches!(codec.decode(&mut buf), Err(Error::Decode(_))));
    }

    #[test]
    fn rejects_password_without_username() {
        let mut codec = MqttCodec::new();
        let mut buf = BytesMut::new();

        assert!(matches!(
            codec.encode(
                MqttPacket::Connect(ConnectPacket {
                    clean_start: true,
                    keep_alive: 60,
                    properties: Vec::new(),
                    client_id: "client".to_string(),
                    will: None,
                    username: None,
                    password: Some(Bytes::from_static(b"secret")),
                }),
                &mut buf,
            ),
            Err(Error::Encode(_))
        ));
    }

    #[test]
    fn rejects_empty_suback_reason_codes() {
        let mut codec = MqttCodec::new();
        let mut buf = BytesMut::from(&[0x90, 0x03, 0x00, 0x01, 0x00][..]);

        assert!(matches!(codec.decode(&mut buf), Err(Error::Decode(_))));
    }
}
