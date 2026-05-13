use std::{
    future::{poll_fn, Future},
    marker::PhantomData,
    task::Poll,
};

use bytes::BytesMut;
use tokio::net::UdpSocket;

use crate::{
    codec::{DatagramDecoder, DatagramEncoder},
    context::{
        datagram::{DatagramOutboxCommand, DatagramOutboxHandle},
        BusinessContext, DatagramContext, InboundContext, OutboundContext,
    },
    pipeline::core::pipe::{BusinessPipe, InboundPipe, OutboundPipe},
    traits::{DatagramHandler, Flow},
    Result,
};

/// Runtime representation of a typed UDP datagram pipeline.
///
/// Applications normally construct this through [`crate::datagram_pipeline()`]
/// instead of naming the type directly.
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

/// Internal runtime contract for UDP datagram pipelines.
///
/// This trait is public so typed builders can appear in public bounds, but most
/// users should implement [`crate::DatagramHandler`], [`crate::Inbound`],
/// [`crate::Business`], and [`crate::Outbound`] instead.
pub trait DatagramRuntimePipeline: Send + 'static {
    /// Application write type accepted by the datagram channel/context.
    type Write: Send + 'static;
    /// Type produced by the datagram decoder.
    type Decoded: Send + 'static;

    /// Decodes one received datagram.
    fn decode_datagram(&mut self, src: &[u8]) -> Result<Self::Decoded>;

    /// Processes one decoded datagram without eager flush support.
    fn process_inbound<'ctx>(
        &'ctx mut self,
        inbound_ctx: &'ctx mut InboundContext,
        business_ctx: &'ctx mut BusinessContext,
        ctx: &'ctx mut DatagramContext<Self::Write>,
        msg: Self::Decoded,
    ) -> impl Future<Output = Result<()>> + Send + 'ctx;

    /// Processes one decoded datagram and supports handler-local flushes.
    #[allow(
        clippy::too_many_arguments,
        reason = "The runtime deliberately passes split mutable state to avoid bundling socket, buffers, and contexts into a broad mutable facade."
    )]
    fn process_inbound_flushable<'ctx>(
        &'ctx mut self,
        inbound_ctx: &'ctx mut InboundContext,
        business_ctx: &'ctx mut BusinessContext,
        outbound_ctx: &'ctx mut OutboundContext,
        ctx: &'ctx mut DatagramContext<Self::Write>,
        socket: &'ctx UdpSocket,
        write_buf: &'ctx mut BytesMut,
        msg: Self::Decoded,
    ) -> impl Future<Output = Result<()>> + Send + 'ctx;

    /// Runs the outbound stages and encodes one application write.
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

    #[allow(
        clippy::too_many_arguments,
        reason = "The runtime deliberately passes split mutable state to avoid bundling socket, buffers, and contexts into a broad mutable facade."
    )]
    async fn process_inbound_flushable(
        &mut self,
        inbound_ctx: &mut InboundContext,
        business_ctx: &mut BusinessContext,
        outbound_ctx: &mut OutboundContext,
        ctx: &mut DatagramContext<Self::Write>,
        socket: &UdpSocket,
        write_buf: &mut BytesMut,
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

        let outbox = ctx.outbox();
        let handler = self.handler.read(ctx, msg);
        tokio::pin!(handler);

        let result = loop {
            let poll_result = poll_fn(|cx| match handler.as_mut().poll(cx) {
                Poll::Ready(result) => Poll::Ready(DatagramHandlerPoll::Ready(result)),
                Poll::Pending if outbox.has_flush_command() => {
                    Poll::Ready(DatagramHandlerPoll::Pending)
                }
                Poll::Pending => Poll::Pending,
            })
            .await;

            match poll_result {
                DatagramHandlerPoll::Ready(result) => break result,
                DatagramHandlerPoll::Pending => {
                    drain_datagram_outbox(
                        &outbox,
                        &mut self.codec,
                        &mut self.outbound,
                        outbound_ctx,
                        socket,
                        write_buf,
                    )
                    .await?;
                }
            }
        };

        #[allow(
            clippy::drop_non_drop,
            reason = "This explicit drop marks the end of the pinned handler borrow before draining the outbox."
        )]
        drop(handler);

        drain_datagram_outbox(
            &outbox,
            &mut self.codec,
            &mut self.outbound,
            outbound_ctx,
            socket,
            write_buf,
        )
        .await?;

        result
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

enum DatagramHandlerPoll {
    Ready(Result<()>),
    Pending,
}

pub(crate) async fn drain_datagram_outbox<C, OutP, Write, CurrentOut>(
    outbox: &DatagramOutboxHandle<Write>,
    codec: &mut C,
    outbound: &mut OutP,
    outbound_ctx: &mut OutboundContext,
    socket: &UdpSocket,
    write_buf: &mut BytesMut,
) -> Result<()>
where
    C: DatagramEncoder<CurrentOut>,
    OutP: OutboundPipe<Write, Out = CurrentOut>,
    Write: Send + 'static,
    CurrentOut: Send + 'static,
{
    let commands = outbox.take_commands();

    for command in commands {
        match command {
            DatagramOutboxCommand::WriteTo(peer_addr, msg) => {
                encode_datagram_outbound(codec, outbound, outbound_ctx, msg, write_buf).await?;
                flush_datagram(socket, peer_addr, write_buf).await?;
            }
            DatagramOutboxCommand::Flush(done) => {
                let result = Ok(());
                let _ = done.send(result);
            }
        }
    }

    Ok(())
}

pub(crate) async fn encode_datagram_outbound<C, OutP, Write, CurrentOut>(
    codec: &mut C,
    outbound: &mut OutP,
    outbound_ctx: &mut OutboundContext,
    msg: Write,
    write_buf: &mut BytesMut,
) -> Result<()>
where
    C: DatagramEncoder<CurrentOut>,
    OutP: OutboundPipe<Write, Out = CurrentOut>,
    Write: Send + 'static,
    CurrentOut: Send + 'static,
{
    let msg = match outbound.process(outbound_ctx, msg).await? {
        Flow::Next(msg) => msg,
        Flow::Stop => return Ok(()),
    };

    codec.encode_datagram(msg, write_buf)
}

pub(crate) async fn flush_datagram(
    socket: &UdpSocket,
    peer_addr: std::net::SocketAddr,
    write_buf: &mut BytesMut,
) -> Result<()> {
    if !write_buf.is_empty() {
        socket.send_to(write_buf, peer_addr).await?;
        write_buf.clear();
    }

    Ok(())
}
