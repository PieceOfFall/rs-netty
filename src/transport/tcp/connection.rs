use std::net::SocketAddr;

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{mpsc, watch},
};

use crate::{
    channel::{command::StreamCommand, Channel},
    context::{BusinessContext, ConnInfo, Context, InboundContext, OutboundContext},
    life::{CloseReason, Life},
    pipeline::stream::runtime::StreamRuntimePipeline,
    transport::{shutdown, tcp::config::TcpConnectionConfig},
    Error, Result,
};

pub(crate) struct StreamConnection<P>
where
    P: StreamRuntimePipeline,
{
    pub id: u64,
    pub stream: TcpStream,
    pub peer_addr: SocketAddr,
    pub local_addr: SocketAddr,
    pub pipeline: P,
    pub config: TcpConnectionConfig,
    pub channel: Channel<P::Write>,
    pub rx: mpsc::Receiver<StreamCommand<P::Write>>,
    pub shutdown_rx: Option<watch::Receiver<bool>>,
}

pub(crate) async fn run_stream_connection_with_life<P, L>(
    connection: StreamConnection<P>,
    life: L,
) -> Result<()>
where
    P: StreamRuntimePipeline,
    L: Life,
{
    let id = connection.id;
    let peer_addr = connection.peer_addr;
    let local_addr = connection.local_addr;
    let info = ConnInfo::new(id, peer_addr, local_addr);

    life.tcp_connection_opened(info).await?;

    let result = run_stream_connection(connection).await;

    match result {
        Ok(()) => {
            life.tcp_connection_closed(info, CloseReason::Completed)
                .await
        }
        Err(err) => {
            if let Err(life_err) = life.tcp_connection_closed(info, CloseReason::Error).await {
                tracing::debug!(
                    connection_id = id,
                    error = ?life_err,
                    "tcp life hook failed while closing errored connection"
                );
            }

            Err(err)
        }
    }
}

pub(crate) async fn run_stream_connection<P>(connection: StreamConnection<P>) -> Result<()>
where
    P: StreamRuntimePipeline,
{
    let StreamConnection {
        id,
        mut stream,
        peer_addr,
        local_addr,
        mut pipeline,
        config,
        channel,
        mut rx,
        mut shutdown_rx,
    } = connection;

    let info = ConnInfo::new(id, peer_addr, local_addr);

    let mut ctx = Context::new(info, channel);
    let mut inbound_ctx = InboundContext::new(info);
    let mut business_ctx = BusinessContext::new(info);
    let mut outbound_ctx = OutboundContext::new(info);

    let mut read_buf = BytesMut::with_capacity(config.read_buffer_capacity);
    let mut write_buf = BytesMut::with_capacity(config.write_buffer_capacity);

    loop {
        if shutdown::requested(&shutdown_rx) {
            break;
        }

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

            _ = shutdown::wait(&mut shutdown_rx) => {
                break;
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
