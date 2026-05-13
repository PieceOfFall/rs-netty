use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use tokio::{net::TcpListener, sync::mpsc};

use crate::{
    channel::{command::StreamCommand, Channel},
    life::{Life, NoLife},
    pipeline::{stream::builder::IntoStreamPipeline, stream::runtime::StreamRuntimePipeline},
    transport::tcp::{
        config::TcpConnectionConfig,
        connection::{run_stream_connection_with_life, StreamConnection},
    },
    Result,
};

pub type TcpServerConfig = TcpConnectionConfig;
pub type ServerConfig = TcpConnectionConfig;

pub struct NoPipeline;

pub struct TcpServer<F = NoPipeline, L = NoLife> {
    addr: String,
    pipeline_factory: F,
    config: TcpConnectionConfig,
    life: L,
}

impl TcpServer<NoPipeline, NoLife> {
    pub fn bind(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            pipeline_factory: NoPipeline,
            config: TcpConnectionConfig::default(),
            life: NoLife,
        }
    }
}

impl<L> TcpServer<NoPipeline, L> {
    pub fn pipeline<F, B, P>(self, factory: F) -> TcpServer<F, L>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoStreamPipeline<Pipeline = P>,
        P: StreamRuntimePipeline,
    {
        TcpServer {
            addr: self.addr,
            pipeline_factory: factory,
            config: self.config,
            life: self.life,
        }
    }
}

impl<F, L> TcpServer<F, L> {
    pub fn life<NextLife>(self, life: NextLife) -> TcpServer<F, NextLife> {
        TcpServer {
            addr: self.addr,
            pipeline_factory: self.pipeline_factory,
            config: self.config,
            life,
        }
    }

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
        L: Life,
    {
        let TcpServer {
            addr,
            pipeline_factory,
            config,
            life,
        } = self;

        let listener = TcpListener::bind(&addr).await?;
        let server_addr = listener.local_addr()?;
        let ids = Arc::new(AtomicU64::new(1));

        life.tcp_server_started(server_addr).await?;

        let result: Result<()> = async {
            loop {
                let (stream, peer_addr) = listener.accept().await?;
                stream.set_nodelay(config.tcp_nodelay)?;

                let local_addr = stream.local_addr()?;
                let id = ids.fetch_add(1, Ordering::Relaxed);
                let pipeline = (pipeline_factory)().into_stream_pipeline();
                let config = config.clone();
                let (tx, rx) = mpsc::channel::<StreamCommand<P::Write>>(config.outbound_queue_size);
                let channel = Channel::new(id, peer_addr, local_addr, tx);
                let life = life.clone();

                tokio::spawn(async move {
                    let connection = StreamConnection {
                        id,
                        stream,
                        peer_addr,
                        local_addr,
                        pipeline,
                        config,
                        channel,
                        rx,
                    };

                    if let Err(err) = run_stream_connection_with_life(connection, life).await {
                        tracing::debug!(
                            connection_id = id,
                            error = ?err,
                            "connection closed with error"
                        );
                    }
                });
            }
        }
        .await;

        if let Err(err) = life.tcp_server_stopped(server_addr).await {
            tracing::debug!(
                local_addr = %server_addr,
                error = ?err,
                "tcp life hook failed while stopping server"
            );
        }

        result
    }
}
