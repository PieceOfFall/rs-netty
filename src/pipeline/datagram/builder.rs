use std::marker::PhantomData;

use crate::{
    codec::{DatagramDecoder, DatagramEncoder},
    pipeline::{
        core::{
            pipe::{BusinessPipe, Identity, InboundPipe, OutboundPipe, Then},
            state::{BusinessPhase, InboundPhase, Ready, Start},
        },
        datagram::runtime::DatagramPipeline,
    },
    traits::{Business, DatagramHandler, Inbound, Outbound},
};

pub struct Missing;

pub fn datagram_pipeline(
) -> DatagramPipelineBuilder<Start, Missing, Identity, Identity, Missing, Identity, (), (), ()> {
    DatagramPipelineBuilder {
        codec: Missing,
        inbound: Identity,
        business: Identity,
        handler: Missing,
        outbound: Identity,
        _marker: PhantomData,
    }
}

pub struct DatagramPipelineBuilder<State, C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut> {
    pub(crate) codec: C,
    pub(crate) inbound: InP,
    pub(crate) business: BizP,
    pub(crate) handler: H,
    pub(crate) outbound: OutP,
    pub(crate) _marker: PhantomData<(State, CurrentIn, Write, CurrentOut)>,
}

impl DatagramPipelineBuilder<Start, Missing, Identity, Identity, Missing, Identity, (), (), ()> {
    pub fn codec<C>(
        self,
        codec: C,
    ) -> DatagramPipelineBuilder<
        InboundPhase,
        C,
        Identity,
        Identity,
        Missing,
        Identity,
        C::Item,
        (),
        (),
    >
    where
        C: DatagramDecoder,
    {
        DatagramPipelineBuilder {
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
    DatagramPipelineBuilder<InboundPhase, C, InP, Identity, Missing, Identity, CurrentIn, (), ()>
where
    C: DatagramDecoder,
    InP: InboundPipe<C::Item, Out = CurrentIn>,
    CurrentIn: Send + 'static,
{
    pub fn inbound<H>(
        self,
        handler: H,
    ) -> DatagramPipelineBuilder<
        InboundPhase,
        C,
        Then<InP, H>,
        Identity,
        Missing,
        Identity,
        H::Out,
        (),
        (),
    >
    where
        H: Inbound<CurrentIn>,
    {
        DatagramPipelineBuilder {
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
    ) -> DatagramPipelineBuilder<
        BusinessPhase,
        C,
        InP,
        Then<Identity, B>,
        Missing,
        Identity,
        B::Out,
        (),
        (),
    >
    where
        B: Business<CurrentIn>,
    {
        DatagramPipelineBuilder {
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
    ) -> DatagramPipelineBuilder<Ready, C, InP, Identity, H, Identity, CurrentIn, H::Write, H::Write>
    where
        H: DatagramHandler<CurrentIn>,
    {
        DatagramPipelineBuilder {
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
    DatagramPipelineBuilder<BusinessPhase, C, InP, BizP, Missing, Identity, CurrentIn, (), ()>
where
    C: DatagramDecoder,
    InP: InboundPipe<C::Item>,
    BizP: BusinessPipe<InP::Out, Out = CurrentIn>,
    CurrentIn: Send + 'static,
{
    pub fn business<B>(
        self,
        business: B,
    ) -> DatagramPipelineBuilder<
        BusinessPhase,
        C,
        InP,
        Then<BizP, B>,
        Missing,
        Identity,
        B::Out,
        (),
        (),
    >
    where
        B: Business<CurrentIn>,
    {
        DatagramPipelineBuilder {
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
    ) -> DatagramPipelineBuilder<Ready, C, InP, BizP, H, Identity, CurrentIn, H::Write, H::Write>
    where
        H: DatagramHandler<CurrentIn>,
    {
        DatagramPipelineBuilder {
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
    DatagramPipelineBuilder<Ready, C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>
where
    CurrentOut: Send + 'static,
{
    pub fn outbound<O>(
        self,
        outbound: O,
    ) -> DatagramPipelineBuilder<Ready, C, InP, BizP, H, Then<OutP, O>, CurrentIn, Write, O::Out>
    where
        O: Outbound<CurrentOut>,
    {
        DatagramPipelineBuilder {
            codec: self.codec,
            inbound: self.inbound,
            business: self.business,
            handler: self.handler,
            outbound: Then::new(self.outbound, outbound),
            _marker: PhantomData,
        }
    }
}

pub trait IntoDatagramPipeline {
    type Pipeline;

    fn into_datagram_pipeline(self) -> Self::Pipeline;
}

impl<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut> IntoDatagramPipeline
    for DatagramPipelineBuilder<Ready, C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>
where
    C: DatagramDecoder + DatagramEncoder<CurrentOut>,
    InP: InboundPipe<C::Item>,
    BizP: BusinessPipe<InP::Out, Out = CurrentIn>,
    H: DatagramHandler<CurrentIn, Write = Write>,
    OutP: OutboundPipe<Write, Out = CurrentOut>,
    CurrentIn: Send + 'static,
    Write: Send + 'static,
    CurrentOut: Send + 'static,
{
    type Pipeline = DatagramPipeline<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>;

    fn into_datagram_pipeline(self) -> Self::Pipeline {
        DatagramPipeline::new(
            self.codec,
            self.inbound,
            self.business,
            self.handler,
            self.outbound,
        )
    }
}
