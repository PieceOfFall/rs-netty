use std::{future::Future, marker::PhantomData};

use bytes::BytesMut;

use crate::{
    codec::{DatagramDecoder, DatagramEncoder},
    context::{BusinessContext, DatagramContext, InboundContext, OutboundContext},
    pipeline::core::pipe::{BusinessPipe, InboundPipe, OutboundPipe},
    traits::{DatagramHandler, Flow},
    Result,
};

pub struct DatagramPipeline<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut> {
    codec: C,
    inbound: InP,
    business: BizP,
    handler: H,
    outbound: OutP,
    _marker: PhantomData<(CurrentIn, Write, CurrentOut)>,
}

impl<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>
    DatagramPipeline<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>
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

pub trait DatagramRuntimePipeline: Send + 'static {
    type Write: Send + 'static;
    type Decoded: Send + 'static;

    fn decode_datagram(&mut self, src: &[u8]) -> Result<Self::Decoded>;

    fn process_inbound<'ctx>(
        &'ctx mut self,
        inbound_ctx: &'ctx mut InboundContext,
        business_ctx: &'ctx mut BusinessContext,
        ctx: &'ctx mut DatagramContext<Self::Write>,
        msg: Self::Decoded,
    ) -> impl Future<Output = Result<()>> + Send + 'ctx;

    fn process_outbound<'ctx>(
        &'ctx mut self,
        outbound_ctx: &'ctx mut OutboundContext,
        msg: Self::Write,
        dst: &'ctx mut BytesMut,
    ) -> impl Future<Output = Result<()>> + Send + 'ctx;
}

impl<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut> DatagramRuntimePipeline
    for DatagramPipeline<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>
where
    C: DatagramDecoder + DatagramEncoder<CurrentOut>,
    InP: InboundPipe<C::Item>,
    BizP: BusinessPipe<InP::Out, Out = CurrentIn>,
    H: DatagramHandler<CurrentIn, Write = Write>,
    OutP: OutboundPipe<Write, Out = CurrentOut>,
    C::Item: Send + 'static,
    CurrentIn: Send + 'static,
    Write: Send + 'static,
    CurrentOut: Send + 'static,
{
    type Write = Write;
    type Decoded = C::Item;

    fn decode_datagram(&mut self, src: &[u8]) -> Result<Self::Decoded> {
        self.codec.decode_datagram(src)
    }

    async fn process_inbound(
        &mut self,
        inbound_ctx: &mut InboundContext,
        business_ctx: &mut BusinessContext,
        ctx: &mut DatagramContext<Self::Write>,
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

        self.codec.encode_datagram(msg, dst)
    }
}
