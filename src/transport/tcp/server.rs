use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use tokio::{net::TcpListener, sync::mpsc};

use crate::{
    channel::{command::StreamCommand, Channel},
    pipeline::{stream::builder::IntoStreamPipeline, stream::runtime::StreamRuntimePipeline},
    transport::tcp::{config::TcpConnectionConfig, connection::run_stream_connection},
    Result,
};

pub type TcpServerConfig = TcpConnectionConfig;
pub type ServerConfig = TcpConnectionConfig;

pub struct NoPipeline;

pub struct TcpServer<F = NoPipeline> {
    addr: String,
    pipeline_factory: F,
    config: TcpConnectionConfig,
}

impl TcpServer<NoPipeline> {
    pub fn bind(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            pipeline_factory: NoPipeline,
            config: TcpConnectionConfig::default(),
        }
    }

    pub fn pipeline<F, B, P>(self, factory: F) -> TcpServer<F>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoStreamPipeline<Pipeline = P>,
        P: StreamRuntimePipeline,
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
        B: IntoStreamPipeline<Pipeline = P>,
        P: StreamRuntimePipeline,
    {
        let listener = TcpListener::bind(&self.addr).await?;
        let ids = Arc::new(AtomicU64::new(1));

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            stream.set_nodelay(self.config.tcp_nodelay)?;

            let local_addr = stream.local_addr()?;
            let id = ids.fetch_add(1, Ordering::Relaxed);
            let pipeline = (self.pipeline_factory)().into_stream_pipeline();
            let config = self.config.clone();
            let (tx, rx) = mpsc::channel::<StreamCommand<P::Write>>(config.outbound_queue_size);
            let channel = Channel::new(id, peer_addr, local_addr, tx);

            tokio::spawn(async move {
                if let Err(err) = run_stream_connection(
                    id, stream, peer_addr, local_addr, pipeline, config, channel, rx,
                )
                .await
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
