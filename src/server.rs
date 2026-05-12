use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};

use crate::{
    channel::{Channel, Command},
    context::{BusinessContext, ConnInfo, Context, InboundContext, OutboundContext},
    pipeline::{builder::IntoPipeline, runtime::RuntimePipeline},
    Error, Result,
};

#[derive(Clone)]
pub struct ServerConfig {
    pub read_buffer_capacity: usize,
    pub write_buffer_capacity: usize,
    pub max_frame_size: usize,
    pub outbound_queue_size: usize,
    pub tcp_nodelay: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            read_buffer_capacity: 8 * 1024,
            write_buffer_capacity: 8 * 1024,
            max_frame_size: 1024 * 1024,
            outbound_queue_size: 1024,
            tcp_nodelay: true,
        }
    }
}

pub struct NoPipeline;

pub struct TcpServer<F = NoPipeline> {
    addr: String,
    pipeline_factory: F,
    config: ServerConfig,
}

impl TcpServer<NoPipeline> {
    pub fn bind(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            pipeline_factory: NoPipeline,
            config: ServerConfig::default(),
        }
    }

    pub fn pipeline<F, B, P>(self, factory: F) -> TcpServer<F>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoPipeline<Pipeline = P>,
        P: RuntimePipeline,
    {
        TcpServer {
            addr: self.addr,
            pipeline_factory: factory,
            config: self.config,
        }
    }
}

impl<F> TcpServer<F> {
    pub fn read_buffer_capacity(mut self, value: usize) -> Self {
        self.config.read_buffer_capacity = value;
        self
    }

    pub fn write_buffer_capacity(mut self, value: usize) -> Self {
        self.config.write_buffer_capacity = value;
        self
    }

    pub fn max_frame_size(mut self, value: usize) -> Self {
        self.config.max_frame_size = value;
        self
    }

    pub fn outbound_queue_size(mut self, value: usize) -> Self {
        self.config.outbound_queue_size = value.max(1);
        self
    }

    pub fn tcp_nodelay(mut self, value: bool) -> Self {
        self.config.tcp_nodelay = value;
        self
    }

    pub async fn run<B, P>(self) -> Result<()>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoPipeline<Pipeline = P>,
        P: RuntimePipeline,
    {
        let listener = TcpListener::bind(&self.addr).await?;
        let ids = Arc::new(AtomicU64::new(1));

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            stream.set_nodelay(self.config.tcp_nodelay)?;

            let local_addr = stream.local_addr()?;
            let id = ids.fetch_add(1, Ordering::Relaxed);
            let pipeline = (self.pipeline_factory)().into_pipeline();
            let config = self.config.clone();

            tokio::spawn(async move {
                if let Err(err) =
                    run_connection(id, stream, peer_addr, local_addr, pipeline, config).await
                {
                    tracing::debug!(
                        connection_id = id,
                        error = ?err,
                        "connection closed with error"
                    );
                }
            });
        }
    }
}

async fn run_connection<P>(
    id: u64,
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
    mut pipeline: P,
    config: ServerConfig,
) -> Result<()>
where
    P: RuntimePipeline,
{
    let (tx, mut rx) = mpsc::channel::<Command<P::Write>>(config.outbound_queue_size);

    let info = ConnInfo::new(id, peer_addr, local_addr);
    let channel = Channel::new(id, peer_addr, local_addr, tx);

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
                    Some(Command::Write(msg)) => {
                        pipeline
                            .process_outbound(&mut outbound_ctx, msg, &mut write_buf)
                            .await?;

                        flush_write_buf(&mut write_buf, &mut stream).await?;
                    }
                    Some(Command::Close) | None => {
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
    P: RuntimePipeline,
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
