use crate::{
    context::{BusinessContext, Context, DatagramContext, InboundContext, OutboundContext},
    Result,
};

/// Result of an inbound, business, or outbound stage.
///
/// `Next` forwards the transformed message to the next stage. `Stop` consumes
/// the message and stops processing for that direction.
pub enum Flow<T> {
    /// Continue the pipeline with this message.
    Next(T),
    /// Stop processing this message without treating it as an error.
    Stop,
}

impl<T> Flow<T> {
    /// Maps the contained `Next` value while preserving `Stop`.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Flow<U> {
        match self {
            Flow::Next(value) => Flow::Next(f(value)),
            Flow::Stop => Flow::Stop,
        }
    }
}

#[trait_variant::make(Inbound: Send)]
/// Inbound transformation stage for decoded messages.
///
/// Implement this trait when a stage should validate, filter, or transform a
/// message before it reaches the business stages or final handler.
pub trait LocalInbound<I>: 'static {
    /// Message type forwarded to the next stage.
    type Out: Send + 'static;

    /// Processes one inbound message.
    async fn read(&mut self, ctx: &mut InboundContext, msg: I) -> Result<Flow<Self::Out>>;
}

#[trait_variant::make(Business: Send)]
/// Middle pipeline stage for application-level transformations.
pub trait LocalBusiness<I>: 'static {
    /// Message type forwarded to the next stage.
    type Out: Send + 'static;

    /// Handles one business message.
    async fn handle(&mut self, ctx: &mut BusinessContext, msg: I) -> Result<Flow<Self::Out>>;
}

#[trait_variant::make(Handler: Send)]
/// Final TCP handler for inbound messages.
///
/// A handler receives the fully transformed inbound message and writes values
/// of type [`Self::Write`] back through the outbound side of the pipeline.
pub trait LocalHandler<I>: 'static {
    /// Application message type accepted by `Context::write`.
    type Write: Send + 'static;

    /// Handles one inbound TCP message.
    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: I) -> Result<()>;
}

#[trait_variant::make(DatagramHandler: Send)]
/// Final UDP handler for inbound datagrams.
pub trait LocalDatagramHandler<I>: 'static {
    /// Application message type accepted by `DatagramContext::write`.
    type Write: Send + 'static;

    /// Handles one inbound UDP datagram.
    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: I) -> Result<()>;
}

#[trait_variant::make(Outbound: Send)]
/// Outbound transformation stage for application writes.
pub trait LocalOutbound<I>: 'static {
    /// Message type forwarded to the next outbound stage or final encoder.
    type Out: Send + 'static;

    /// Processes one outbound message.
    async fn write(&mut self, ctx: &mut OutboundContext, msg: I) -> Result<Flow<Self::Out>>;
}
