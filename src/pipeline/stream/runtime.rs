use std::{
    future::{poll_fn, Future},
    marker::PhantomData,
    task::Poll,
};

use bytes::BytesMut;
use tokio::{io::AsyncWriteExt, net::TcpStream};

use crate::{
    codec::{Decoder, Encoder},
    context::{
        stream::{StreamOutboxCommand, StreamOutboxHandle},
        BusinessContext, ConnectionStats, Context, InboundContext, OutboundContext,
    },
    pipeline::core::pipe::{BusinessPipe, InboundPipe, OutboundPipe},
    traits::{Flow, Handler},
    Error, Result,
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

    fn process_inbound_flushable<'ctx>(
        &'ctx mut self,
        inbound_ctx: &'ctx mut InboundContext,
        business_ctx: &'ctx mut BusinessContext,
        outbound_ctx: &'ctx mut OutboundContext,
        ctx: &'ctx mut Context<Self::Write>,
        stream: &'ctx mut TcpStream,
        write_buf: &'ctx mut BytesMut,
        stats: &'ctx Option<ConnectionStats>,
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

    async fn process_inbound_flushable(
        &mut self,
        inbound_ctx: &mut InboundContext,
        business_ctx: &mut BusinessContext,
        outbound_ctx: &mut OutboundContext,
        ctx: &mut Context<Self::Write>,
        stream: &mut TcpStream,
        write_buf: &mut BytesMut,
        stats: &Option<ConnectionStats>,
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
                Poll::Ready(result) => Poll::Ready(HandlerPoll::Ready(result)),
                Poll::Pending if outbox.has_flush_command() => Poll::Ready(HandlerPoll::Pending),
                Poll::Pending => Poll::Pending,
            })
            .await;

            match poll_result {
                HandlerPoll::Ready(result) => break result,
                HandlerPoll::Pending => {
                    drain_stream_outbox(
                        &outbox,
                        &mut self.codec,
                        &mut self.outbound,
                        outbound_ctx,
                        stream,
                        write_buf,
                        stats,
                        false,
                    )
                    .await?;
                }
            }
        };

        drop(handler);

        drain_stream_outbox(
            &outbox,
            &mut self.codec,
            &mut self.outbound,
            outbound_ctx,
            stream,
            write_buf,
            stats,
            true,
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

        self.codec.encode(msg, dst)
    }
}

enum HandlerPoll {
    Ready(Result<()>),
    Pending,
}

async fn drain_stream_outbox<C, OutP, Write, CurrentOut>(
    outbox: &StreamOutboxHandle<Write>,
    codec: &mut C,
    outbound: &mut OutP,
    outbound_ctx: &mut OutboundContext,
    stream: &mut TcpStream,
    write_buf: &mut BytesMut,
    stats: &Option<ConnectionStats>,
    flush_at_end: bool,
) -> Result<()>
where
    C: Encoder<CurrentOut>,
    OutP: OutboundPipe<Write, Out = CurrentOut>,
    Write: Send + 'static,
    CurrentOut: Send + 'static,
{
    let commands = outbox.take_commands();

    for command in commands {
        match command {
            StreamOutboxCommand::Write(msg) => {
                encode_outbound(codec, outbound, outbound_ctx, msg, write_buf, stats).await?;
            }
            StreamOutboxCommand::Flush(done) => {
                let result = flush_write_buf(stream, write_buf, stats).await;
                let ack = match &result {
                    Ok(()) => Ok(()),
                    Err(err) => Err(Error::Pipeline(format!("flush failed: {err}"))),
                };
                let _ = done.send(ack);
                result?;
            }
        }
    }

    if flush_at_end {
        flush_write_buf(stream, write_buf, stats).await?;
    }

    Ok(())
}

async fn encode_outbound<C, OutP, Write, CurrentOut>(
    codec: &mut C,
    outbound: &mut OutP,
    outbound_ctx: &mut OutboundContext,
    msg: Write,
    write_buf: &mut BytesMut,
    stats: &Option<ConnectionStats>,
) -> Result<()>
where
    C: Encoder<CurrentOut>,
    OutP: OutboundPipe<Write, Out = CurrentOut>,
    Write: Send + 'static,
    CurrentOut: Send + 'static,
{
    let msg = match outbound.process(outbound_ctx, msg).await? {
        Flow::Next(msg) => msg,
        Flow::Stop => return Ok(()),
    };

    codec.encode(msg, write_buf)?;
    if let Some(stats) = stats {
        stats.add_frame_written();
    }
    Ok(())
}

async fn flush_write_buf(
    stream: &mut TcpStream,
    write_buf: &mut BytesMut,
    stats: &Option<ConnectionStats>,
) -> Result<()> {
    if !write_buf.is_empty() {
        let len = write_buf.len();
        stream.write_all(write_buf).await?;
        if let Some(stats) = stats {
            stats.add_bytes_written(len);
        }
        write_buf.clear();
    }

    Ok(())
}
