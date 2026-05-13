use std::marker::PhantomData;

use crate::{
    codec::{Decoder, Encoder},
    pipeline::{
        core::{
            pipe::{BusinessPipe, Identity, InboundPipe, OutboundPipe, Then},
            state::{BusinessPhase, InboundPhase, Ready, Start},
        },
        stream::runtime::StreamPipeline,
    },
    traits::{Business, Handler, Inbound, Outbound},
};

pub struct Missing;

/// Starts a typed TCP stream pipeline builder.
///
/// The builder encodes legal pipeline order in its type parameters. Methods
/// become available only when the previous required stage has been supplied.
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

/// Typed TCP pipeline builder.
///
/// Most type parameters are implementation details used to track the current
/// build phase and the message type at each pipeline boundary.
pub struct PipelineBuilder<State, C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut> {
    pub(crate) codec: C,
    pub(crate) inbound: InP,
    pub(crate) business: BizP,
    pub(crate) handler: H,
    pub(crate) outbound: OutP,
    pub(crate) _marker: PhantomData<(State, CurrentIn, Write, CurrentOut)>,
}

impl PipelineBuilder<Start, Missing, Identity, Identity, Missing, Identity, (), (), ()> {
    /// Adds the stream codec and enters the inbound phase.
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
    /// Adds an inbound transformation stage.
    #[allow(
        clippy::type_complexity,
        reason = "The builder's return type intentionally carries pipeline state and message types for compile-time API validation."
    )]
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

    /// Adds the first business transformation stage.
    #[allow(
        clippy::type_complexity,
        reason = "The builder's return type intentionally carries pipeline state and message types for compile-time API validation."
    )]
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

    /// Adds the final inbound handler and enters the ready/outbound phase.
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
    /// Adds another business transformation stage.
    #[allow(
        clippy::type_complexity,
        reason = "The builder's return type intentionally carries pipeline state and message types for compile-time API validation."
    )]
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

    /// Adds the final inbound handler and enters the ready/outbound phase.
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
    /// Adds an outbound transformation stage.
    #[allow(
        clippy::type_complexity,
        reason = "The builder's return type intentionally carries pipeline state and message types for compile-time API validation."
    )]
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

pub trait IntoStreamPipeline {
    /// Concrete runtime pipeline produced by this builder.
    type Pipeline;

    /// Converts the builder into its runtime pipeline.
    fn into_stream_pipeline(self) -> Self::Pipeline;
}

/// Compatibility conversion trait for stream pipelines.
pub trait IntoPipeline {
    /// Concrete runtime pipeline produced by this builder.
    type Pipeline;

    /// Converts the builder into its runtime pipeline.
    fn into_pipeline(self) -> Self::Pipeline;
}

impl<T> IntoPipeline for T
where
    T: IntoStreamPipeline,
{
    type Pipeline = T::Pipeline;

    fn into_pipeline(self) -> Self::Pipeline {
        self.into_stream_pipeline()
    }
}

impl<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut> IntoStreamPipeline
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
    type Pipeline = StreamPipeline<C, InP, BizP, H, OutP, CurrentIn, Write, CurrentOut>;

    fn into_stream_pipeline(self) -> Self::Pipeline {
        StreamPipeline::new(
            self.codec,
            self.inbound,
            self.business,
            self.handler,
            self.outbound,
        )
    }
}
