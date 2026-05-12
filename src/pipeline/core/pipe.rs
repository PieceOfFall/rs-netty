use std::future::Future;

use crate::{
    context::{BusinessContext, InboundContext, OutboundContext},
    traits::{Business, Flow, Inbound, Outbound},
    Result,
};

pub struct Identity;

pub struct Then<A, B> {
    pub a: A,
    pub b: B,
}

impl<A, B> Then<A, B> {
    pub fn new(a: A, b: B) -> Self {
        Self { a, b }
    }
}

pub trait InboundPipe<I>: Send + 'static {
    type Out: Send + 'static;

    fn process<'ctx>(
        &'ctx mut self,
        ctx: &'ctx mut InboundContext,
        msg: I,
    ) -> impl Future<Output = Result<Flow<Self::Out>>> + Send + 'ctx;
}

impl<I: Send + 'static> InboundPipe<I> for Identity {
    type Out = I;

    async fn process(&mut self, _ctx: &mut InboundContext, msg: I) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(msg))
    }
}

impl<I, A, B> InboundPipe<I> for Then<A, B>
where
    I: Send + 'static,
    A: InboundPipe<I>,
    B: Inbound<A::Out>,
{
    type Out = B::Out;

    async fn process(&mut self, ctx: &mut InboundContext, msg: I) -> Result<Flow<Self::Out>> {
        match self.a.process(ctx, msg).await? {
            Flow::Next(mid) => self.b.read(ctx, mid).await,
            Flow::Stop => Ok(Flow::Stop),
        }
    }
}

pub trait BusinessPipe<I>: Send + 'static {
    type Out: Send + 'static;

    fn process<'ctx>(
        &'ctx mut self,
        ctx: &'ctx mut BusinessContext,
        msg: I,
    ) -> impl Future<Output = Result<Flow<Self::Out>>> + Send + 'ctx;
}

impl<I: Send + 'static> BusinessPipe<I> for Identity {
    type Out = I;

    async fn process(&mut self, _ctx: &mut BusinessContext, msg: I) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(msg))
    }
}

impl<I, A, B> BusinessPipe<I> for Then<A, B>
where
    I: Send + 'static,
    A: BusinessPipe<I>,
    B: Business<A::Out>,
{
    type Out = B::Out;

    async fn process(&mut self, ctx: &mut BusinessContext, msg: I) -> Result<Flow<Self::Out>> {
        match self.a.process(ctx, msg).await? {
            Flow::Next(mid) => self.b.handle(ctx, mid).await,
            Flow::Stop => Ok(Flow::Stop),
        }
    }
}

pub trait OutboundPipe<I>: Send + 'static {
    type Out: Send + 'static;

    fn process<'ctx>(
        &'ctx mut self,
        ctx: &'ctx mut OutboundContext,
        msg: I,
    ) -> impl Future<Output = Result<Flow<Self::Out>>> + Send + 'ctx;
}

impl<I: Send + 'static> OutboundPipe<I> for Identity {
    type Out = I;

    async fn process(&mut self, _ctx: &mut OutboundContext, msg: I) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(msg))
    }
}

impl<I, A, B> OutboundPipe<I> for Then<A, B>
where
    I: Send + 'static,
    A: OutboundPipe<I>,
    B: Outbound<A::Out>,
{
    type Out = B::Out;

    async fn process(&mut self, ctx: &mut OutboundContext, msg: I) -> Result<Flow<Self::Out>> {
        match self.a.process(ctx, msg).await? {
            Flow::Next(mid) => self.b.write(ctx, mid).await,
            Flow::Stop => Ok(Flow::Stop),
        }
    }
}
