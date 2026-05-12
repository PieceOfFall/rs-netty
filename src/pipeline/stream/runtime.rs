use std::{future::Future, marker::PhantomData};

use bytes::BytesMut;

use crate::{
    codec::{Decoder, Encoder},
    context::{BusinessContext, Context, InboundContext, OutboundContext},
    pipeline::core::pipe::{BusinessPipe, InboundPipe, OutboundPipe},
    traits::{Flow, Handler},
    Result,
};

pub struct StreamPipeline<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut> {
    codec: C,
    inbound: InP,
    business: BizP,
    handler: H,
    outbound: OutP,
    _marker: PhantomData<(CurrentIn, Write, CurrentOut)>,
}

pub type Pipeline<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut> =
    StreamPipeline<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>;

impl<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>
    StreamPipeline<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>
{
    pub(crate) fn new(codec: C, inbound: InP, business: BizP, handler: H, outbound: OutP) -> Self {
        Self {
            codec,
            inbound,
            business,
            handler,
            outbound,
            _marker: PhantomData,
        }
    }
}

pub trait StreamRuntimePipeline: Send + 'static {
    type Write: Send + 'static;
    type Decoded: Send + 'static;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Decoded>>;

    fn process_inbound<'ctx>(
        &'ctx mut self,
        inbound_ctx: &'ctx mut InboundContext,
        business_ctx: &'ctx mut BusinessContext,
        ctx: &'ctx mut Context<Self::Write>,
        msg: Self::Decoded,
    ) -> impl Future<Output = Result<()>> + Send + 'ctx;

    fn process_outbound<'ctx>(
        &'ctx mut self,
        outbound_ctx: &'ctx mut OutboundContext,
        msg: Self::Write,
        dst: &'ctx mut BytesMut,
    ) -> impl Future<Output = Result<()>> + Send + 'ctx;
}

pub trait RuntimePipeline: StreamRuntimePipeline {}

impl<T> RuntimePipeline for T where T: StreamRuntimePipeline {}

impl<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut> StreamRuntimePipeline
    for StreamPipeline<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>
where
    C: Decoder + Encoder<CurrentOut>,
    InP: InboundPipe<C::Item>,
    BizP: BusinessPipe<InP::Out, Out = CurrentIn>,
    H: Handler<CurrentIn, Write = Write>,
    OutP: OutboundPipe<Write, Out = CurrentOut>,
    C::Item: Send + 'static,
    CurrentIn: Send + 'static,
    Write: Send + 'static,
    CurrentOut: Send + 'static,
{
    type Write = Write;
    type Decoded = C::Item;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Decoded>> {
        self.codec.decode(src)
    }

    async fn process_inbound(
        &mut self,
        inbound_ctx: &mut InboundContext,
        business_ctx: &mut BusinessContext,
        ctx: &mut Context<Self::Write>,
        msg: Self::Decoded,
    ) -> Result<()> {
        let msg = match self.inbound.process(inbound_ctx, msg).await? {
            Flow::Next(msg) => msg,
            Flow::Stop => return Ok(()),
        };

        let msg = match self.business.process(business_ctx, msg).await? {
            Flow::Next(msg) => msg,
            Flow::Stop => return Ok(()),
        };

        self.handler.read(ctx, msg).await
    }

    async fn process_outbound(
        &mut self,
        outbound_ctx: &mut OutboundContext,
        msg: Self::Write,
        dst: &mut BytesMut,
    ) -> Result<()> {
        let msg = match self.outbound.process(outbound_ctx, msg).await? {
            Flow::Next(msg) => msg,
            Flow::Stop => return Ok(()),
        };

        self.codec.encode(msg, dst)
    }
}
