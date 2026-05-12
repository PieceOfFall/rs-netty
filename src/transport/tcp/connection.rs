use std::net::SocketAddr;

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::mpsc,
};

use crate::{
    channel::{command::StreamCommand, Channel},
    context::{BusinessContext, ConnInfo, Context, InboundContext, OutboundContext},
    pipeline::stream::runtime::StreamRuntimePipeline,
    transport::tcp::config::TcpConnectionConfig,
    Error, Result,
};

pub(crate) async fn run_stream_connection<P>(
    id: u64,
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
    mut pipeline: P,
    config: TcpConnectionConfig,
    channel: Channel<P::Write>,
    mut rx: mpsc::Receiver<StreamCommand<P::Write>>,
) -> Result<()>
where
    P: StreamRuntimePipeline,
{
    let info = ConnInfo::new(id, peer_addr, local_addr);

    let mut ctx = Context::new(info, channel);
    let mut inbound_ctx = InboundContext::new(info);
    let mut business_ctx = BusinessContext::new(info);
    let mut outbound_ctx = OutboundContext::new(info);

    let mut read_buf = BytesMut::with_capacity(config.read_buffer_capacity);
    let mut write_buf = BytesMut::with_capacity(config.write_buffer_capacity);

    loop {
        tokio::select! {
            read = stream.read_buf(&mut read_buf) => {
                let read_len = read?;

                if read_len == 0 {
                    break;
                }

                if read_buf.len() > config.max_frame_size {
                    return Err(Error::FrameTooLarge {
                        current: read_buf.len(),
                        max: config.max_frame_size,
                    });
                }

                while let Some(msg) = pipeline.decode(&mut read_buf)? {
                    pipeline
                        .process_inbound(&mut inbound_ctx, &mut business_ctx, &mut ctx, msg)
                        .await?;

                    drain_pending_writes(
                        &mut pipeline,
                        &mut outbound_ctx,
                        &mut ctx,
                        &mut write_buf,
                        &mut stream,
                    )
                    .await?;

                    if ctx.close_requested() {
                        return Ok(());
                    }
                }
            }

            cmd = rx.recv() => {
                match cmd {
                    Some(StreamCommand::Write(msg)) => {
                        pipeline
                            .process_outbound(&mut outbound_ctx, msg, &mut write_buf)
                            .await?;

                        flush_write_buf(&mut write_buf, &mut stream).await?;
                    }
                    Some(StreamCommand::Close) | None => {
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn drain_pending_writes<P>(
    pipeline: &mut P,
    outbound_ctx: &mut OutboundContext,
    ctx: &mut Context<P::Write>,
    write_buf: &mut BytesMut,
    stream: &mut TcpStream,
) -> Result<()>
where
    P: StreamRuntimePipeline,
{
    let writes = ctx.take_pending_writes();

    for msg in writes {
        pipeline
            .process_outbound(outbound_ctx, msg, write_buf)
            .await?;
    }

    flush_write_buf(write_buf, stream).await
}

async fn flush_write_buf(write_buf: &mut BytesMut, stream: &mut TcpStream) -> Result<()> {
    if !write_buf.is_empty() {
        stream.write_all(write_buf).await?;
        write_buf.clear();
    }

    Ok(())
}
