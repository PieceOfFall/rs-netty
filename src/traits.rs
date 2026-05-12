use crate::{
    context::{BusinessContext, Context, DatagramContext, InboundContext, OutboundContext},
    Result,
};

pub enum Flow<T> {
    Next(T),
    Stop,
}

impl<T> Flow<T> {
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Flow<U> {
        match self {
            Flow::Next(value) => Flow::Next(f(value)),
            Flow::Stop => Flow::Stop,
        }
    }
}

#[trait_variant::make(Inbound: Send)]
pub trait LocalInbound<I>: 'static {
    type Out: Send + 'static;

    async fn read(&mut self, ctx: &mut InboundContext, msg: I) -> Result<Flow<Self::Out>>;
}

#[trait_variant::make(Business: Send)]
pub trait LocalBusiness<I>: 'static {
    type Out: Send + 'static;

    async fn handle(&mut self, ctx: &mut BusinessContext, msg: I) -> Result<Flow<Self::Out>>;
}

#[trait_variant::make(Handler: Send)]
pub trait LocalHandler<I>: 'static {
    type Write: Send + 'static;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: I) -> Result<()>;
}

#[trait_variant::make(DatagramHandler: Send)]
pub trait LocalDatagramHandler<I>: 'static {
    type Write: Send + 'static;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: I) -> Result<()>;
}

#[trait_variant::make(Outbound: Send)]
pub trait LocalOutbound<I>: 'static {
    type Out: Send + 'static;

    async fn write(&mut self, ctx: &mut OutboundContext, msg: I) -> Result<Flow<Self::Out>>;
}
