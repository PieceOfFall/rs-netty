use std::{net::SocketAddr, time::Duration};

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

type ConnectionResult<T> = std::result::Result<T, ConnectionFailure>;

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

struct ConnectionFailure {
    reason: CloseReason,
    error: Error,
}

impl ConnectionFailure {
    fn new(reason: CloseReason, error: Error) -> Self {
        Self { reason, error }
    }
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

    match run_stream_connection(connection).await {
        Ok(reason) => life.tcp_connection_closed(info, reason).await,
        Err(failure) => {
            if let Err(life_err) = life.tcp_connection_closed(info, failure.reason).await {
                tracing::debug!(
                    connection_id = id,
                    error = ?life_err,
                    "tcp life hook failed while closing errored connection"
                );
            }

            Err(failure.error)
        }
    }
}

async fn run_stream_connection<P>(connection: StreamConnection<P>) -> ConnectionResult<CloseReason>
where
    P: StreamRuntimePipeline,
{
    let StreamConnection {
        id,
        stream,
        peer_addr,
        local_addr,
        pipeline,
        config,
        channel,
        rx,
        shutdown_rx,
    } = connection;

    let info = ConnInfo::new(id, peer_addr, local_addr);
    let idle_timeout = config.idle_timeout;
    let mut runtime = StreamConnectionRuntime {
        stream,
        pipeline,
        config,
        rx,
        shutdown_rx,
        ctx: Context::new(info, channel),
        inbound_ctx: InboundContext::new(info),
        business_ctx: BusinessContext::new(info),
        outbound_ctx: OutboundContext::new(info),
        read_buf: BytesMut::new(),
        write_buf: BytesMut::new(),
    };

    runtime.read_buf = BytesMut::with_capacity(runtime.config.read_buffer_capacity);
    runtime.write_buf = BytesMut::with_capacity(runtime.config.write_buffer_capacity);

    match idle_timeout {
        Some(idle_timeout) => runtime.run_with_idle_timeout(idle_timeout).await,
        None => runtime.run_without_idle_timeout().await,
    }
}

struct StreamConnectionRuntime<P>
where
    P: StreamRuntimePipeline,
{
    stream: TcpStream,
    pipeline: P,
    config: TcpConnectionConfig,
    rx: mpsc::Receiver<StreamCommand<P::Write>>,
    shutdown_rx: Option<watch::Receiver<bool>>,
    ctx: Context<P::Write>,
    inbound_ctx: InboundContext,
    business_ctx: BusinessContext,
    outbound_ctx: OutboundContext,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl<P> StreamConnectionRuntime<P>
where
    P: StreamRuntimePipeline,
{
    async fn run_without_idle_timeout(&mut self) -> ConnectionResult<CloseReason> {
        loop {
            if shutdown::requested(&self.shutdown_rx) {
                return Ok(CloseReason::ServerShutdown);
            }

            tokio::select! {
                read = self.stream.read_buf(&mut self.read_buf) => {
                    if let Some(reason) = self.handle_read(read).await? {
                        return Ok(reason);
                    }
                }

                cmd = self.rx.recv() => {
                    if let Some(reason) = self.handle_command(cmd).await? {
                        return Ok(reason);
                    }
                }

                _ = shutdown::wait(&mut self.shutdown_rx) => {
                    return Ok(CloseReason::ServerShutdown);
                }
            }
        }
    }

    async fn run_with_idle_timeout(
        &mut self,
        idle_timeout: Duration,
    ) -> ConnectionResult<CloseReason> {
        let idle = tokio::time::sleep(idle_timeout);
        tokio::pin!(idle);

        loop {
            if shutdown::requested(&self.shutdown_rx) {
                return Ok(CloseReason::ServerShutdown);
            }

            tokio::select! {
                read = self.stream.read_buf(&mut self.read_buf) => {
                    if let Some(reason) = self.handle_read(read).await? {
                        return Ok(reason);
                    }
                    idle.as_mut().reset(tokio::time::Instant::now() + idle_timeout);
                }

                cmd = self.rx.recv() => {
                    if let Some(reason) = self.handle_command(cmd).await? {
                        return Ok(reason);
                    }
                }

                _ = shutdown::wait(&mut self.shutdown_rx) => {
                    return Ok(CloseReason::ServerShutdown);
                }

                _ = &mut idle => {
                    return Ok(CloseReason::IdleTimeout);
                }
            }
        }
    }

    async fn handle_read(
        &mut self,
        read: std::io::Result<usize>,
    ) -> ConnectionResult<Option<CloseReason>> {
        let read_len = read.map_err(|err| failure(CloseReason::IoError, err.into()))?;

        if read_len == 0 {
            return Ok(Some(CloseReason::PeerClosed));
        }

        if self.read_buf.len() > self.config.max_frame_size {
            return Err(failure(
                CloseReason::FrameTooLarge,
                Error::FrameTooLarge {
                    current: self.read_buf.len(),
                    max: self.config.max_frame_size,
                },
            ));
        }

        while let Some(msg) = self
            .pipeline
            .decode(&mut self.read_buf)
            .map_err(decode_failure)?
        {
            self.pipeline
                .process_inbound(
                    &mut self.inbound_ctx,
                    &mut self.business_ctx,
                    &mut self.ctx,
                    msg,
                )
                .await
                .map_err(handler_failure)?;

            self.drain_pending_writes().await?;

            if self.ctx.close_requested() {
                return Ok(Some(CloseReason::HandlerClosed));
            }
        }

        Ok(None)
    }

    async fn handle_command(
        &mut self,
        cmd: Option<StreamCommand<P::Write>>,
    ) -> ConnectionResult<Option<CloseReason>> {
        match cmd {
            Some(StreamCommand::Write(msg)) => {
                self.pipeline
                    .process_outbound(&mut self.outbound_ctx, msg, &mut self.write_buf)
                    .await
                    .map_err(outbound_failure)?;

                self.flush_write_buf().await?;
                Ok(None)
            }
            Some(StreamCommand::Close) => Ok(Some(CloseReason::LocalClosed)),
            None => Ok(Some(CloseReason::ChannelClosed)),
        }
    }

    async fn drain_pending_writes(&mut self) -> ConnectionResult<()> {
        let writes = self.ctx.take_pending_writes();

        for msg in writes {
            self.pipeline
                .process_outbound(&mut self.outbound_ctx, msg, &mut self.write_buf)
                .await
                .map_err(outbound_failure)?;
        }

        self.flush_write_buf().await
    }

    async fn flush_write_buf(&mut self) -> ConnectionResult<()> {
        if !self.write_buf.is_empty() {
            self.stream
                .write_all(&self.write_buf)
                .await
                .map_err(|err| failure(CloseReason::IoError, err.into()))?;
            self.write_buf.clear();
        }

        Ok(())
    }
}

fn failure(reason: CloseReason, error: Error) -> ConnectionFailure {
    ConnectionFailure::new(reason, error)
}

fn decode_failure(error: Error) -> ConnectionFailure {
    let reason = match error {
        Error::Decode(_) => CloseReason::DecodeError,
        Error::FrameTooLarge { .. } => CloseReason::FrameTooLarge,
        Error::Io(_) => CloseReason::IoError,
        _ => CloseReason::HandlerError,
    };
    failure(reason, error)
}

fn outbound_failure(error: Error) -> ConnectionFailure {
    let reason = match error {
        Error::Encode(_) => CloseReason::EncodeError,
        Error::Io(_) => CloseReason::IoError,
        Error::FrameTooLarge { .. } => CloseReason::FrameTooLarge,
        _ => CloseReason::HandlerError,
    };
    failure(reason, error)
}

fn handler_failure(error: Error) -> ConnectionFailure {
    let reason = match error {
        Error::Decode(_) => CloseReason::DecodeError,
        Error::Encode(_) => CloseReason::EncodeError,
        Error::Io(_) => CloseReason::IoError,
        Error::FrameTooLarge { .. } => CloseReason::FrameTooLarge,
        _ => CloseReason::HandlerError,
    };
    failure(reason, error)
}
