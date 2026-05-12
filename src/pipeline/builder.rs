use std::marker::PhantomData;

use crate::{
    codec::{Decoder, Encoder},
    pipeline::{
        pipe::{BusinessPipe, Identity, InboundPipe, OutboundPipe, Then},
        runtime::Pipeline,
        state::{BusinessPhase, InboundPhase, Ready, Start},
    },
    traits::{Business, Handler, Inbound, Outbound},
};

pub struct Missing;

pub fn pipeline(
) -> PipelineBuilder<Start, Missing, Identity, Identity, Missing, Identity, (), (), ()> {
    PipelineBuilder {
        codec: Missing,
        inbound: Identity,
        business: Identity,
        handler: Missing,
        outbound: Identity,
        _marker: PhantomData,
    }
}

pub struct PipelineBuilder<State, C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut> {
    pub(crate) codec: C,
    pub(crate) inbound: InP,
    pub(crate) business: BizP,
    pub(crate) handler: H,
    pub(crate) outbound: OutP,
    pub(crate) _marker: PhantomData<(State, CurrentIn, Write, CurrentOut)>,
}

impl PipelineBuilder<Start, Missing, Identity, Identity, Missing, Identity, (), (), ()> {
    pub fn codec<C>(
        self,
        codec: C,
    ) -> PipelineBuilder<InboundPhase, C, Identity, Identity, Missing, Identity, C::Item, (), ()>
    where
        C: Decoder,
    {
        PipelineBuilder {
            codec,
            inbound: Identity,
            business: Identity,
            handler: Missing,
            outbound: Identity,
            _marker: PhantomData,
        }
    }
}

impl<C, InP, CurrentIn>
    PipelineBuilder<InboundPhase, C, InP, Identity, Missing, Identity, CurrentIn, (), ()>
where
    C: Decoder,
    InP: InboundPipe<C::Item, Out = CurrentIn>,
    CurrentIn: Send + 'static,
{
    pub fn inbound<H>(
        self,
        handler: H,
    ) -> PipelineBuilder<InboundPhase, C, Then<InP, H>, Identity, Missing, Identity, H::Out, (), ()>
    where
        H: Inbound<CurrentIn>,
    {
        PipelineBuilder {
            codec: self.codec,
            inbound: Then::new(self.inbound, handler),
            business: self.business,
            handler: self.handler,
            outbound: self.outbound,
            _marker: PhantomData,
        }
    }

    pub fn business<B>(
        self,
        business: B,
    ) -> PipelineBuilder<BusinessPhase, C, InP, Then<Identity, B>, Missing, Identity, B::Out, (), ()>
    where
        B: Business<CurrentIn>,
    {
        PipelineBuilder {
            codec: self.codec,
            inbound: self.inbound,
            business: Then::new(self.business, business),
            handler: self.handler,
            outbound: self.outbound,
            _marker: PhantomData,
        }
    }

    pub fn handler<H>(
        self,
        handler: H,
    ) -> PipelineBuilder<Ready, C, InP, Identity, H, Identity, CurrentIn, H::Write, H::Write>
    where
        H: Handler<CurrentIn>,
    {
        PipelineBuilder {
            codec: self.codec,
            inbound: self.inbound,
            business: self.business,
            handler,
            outbound: self.outbound,
            _marker: PhantomData,
        }
    }
}

impl<C, InP, BizP, CurrentIn>
    PipelineBuilder<BusinessPhase, C, InP, BizP, Missing, Identity, CurrentIn, (), ()>
where
    C: Decoder,
    InP: InboundPipe<C::Item>,
    BizP: BusinessPipe<InP::Out, Out = CurrentIn>,
    CurrentIn: Send + 'static,
{
    pub fn business<B>(
        self,
        business: B,
    ) -> PipelineBuilder<BusinessPhase, C, InP, Then<BizP, B>, Missing, Identity, B::Out, (), ()>
    where
        B: Business<CurrentIn>,
    {
        PipelineBuilder {
            codec: self.codec,
            inbound: self.inbound,
            business: Then::new(self.business, business),
            handler: self.handler,
            outbound: self.outbound,
            _marker: PhantomData,
        }
    }

    pub fn handler<H>(
        self,
        handler: H,
    ) -> PipelineBuilder<Ready, C, InP, BizP, H, Identity, CurrentIn, H::Write, H::Write>
    where
        H: Handler<CurrentIn>,
    {
        PipelineBuilder {
            codec: self.codec,
            inbound: self.inbound,
            business: self.business,
            handler,
            outbound: self.outbound,
            _marker: PhantomData,
        }
    }
}

impl<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>
    PipelineBuilder<Ready, C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>
where
    CurrentOut: Send + 'static,
{
    pub fn outbound<O>(
        self,
        outbound: O,
    ) -> PipelineBuilder<Ready, C, InP, BizP, H, Then<OutP, O>, CurrentIn, Write, O::Out>
    where
        O: Outbound<CurrentOut>,
    {
        PipelineBuilder {
            codec: self.codec,
            inbound: self.inbound,
            business: self.business,
            handler: self.handler,
            outbound: Then::new(self.outbound, outbound),
            _marker: PhantomData,
        }
    }
}

pub trait IntoPipeline {
    type Pipeline;

    fn into_pipeline(self) -> Self::Pipeline;
}

impl<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut> IntoPipeline
    for PipelineBuilder<Ready, C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>
where
    C: Decoder + Encoder<CurrentOut>,
    InP: InboundPipe<C::Item>,
    BizP: BusinessPipe<InP::Out, Out = CurrentIn>,
    H: Handler<CurrentIn, Write = Write>,
    OutP: OutboundPipe<Write, Out = CurrentOut>,
    CurrentIn: Send + 'static,
    Write: Send + 'static,
    CurrentOut: Send + 'static,
{
    type Pipeline = Pipeline<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>;

    fn into_pipeline(self) -> Self::Pipeline {
        Pipeline::new(
            self.codec,
            self.inbound,
            self.business,
            self.handler,
            self.outbound,
        )
    }
}
