use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    task::{JoinError, JoinHandle, JoinSet},
};

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

    pub fn idle_timeout(mut self, value: Duration) -> Self {
        self.config.idle_timeout = Some(value);
        self
    }

    pub async fn start<B, P>(self) -> Result<TcpServerHandle>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoStreamPipeline<Pipeline = P> + 'static,
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
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        life.tcp_server_started(local_addr).await?;

        let join = tokio::spawn(run_tcp_server(
            listener,
            pipeline_factory,
            config,
            life,
            shutdown_tx.clone(),
            shutdown_rx,
        ));

        Ok(TcpServerHandle {
            local_addr,
            shutdown_tx,
            join,
        })
    }

    pub async fn run<B, P>(self) -> Result<()>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoStreamPipeline<Pipeline = P> + 'static,
        P: StreamRuntimePipeline,
        L: Life,
    {
        self.start().await?.wait().await
    }
}

pub struct TcpServerHandle {
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    join: JoinHandle<Result<()>>,
}

impl TcpServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn shutdown(&self) {
        if !*self.shutdown_tx.borrow() {
            let _ = self.shutdown_tx.send(true);
        }
    }

    pub async fn wait(self) -> Result<()> {
        self.join.await?
    }
}

async fn run_tcp_server<F, B, P, L>(
    listener: TcpListener,
    pipeline_factory: F,
    config: TcpConnectionConfig,
    life: L,
    shutdown_tx: watch::Sender<bool>,
    mut shutdown_rx: watch::Receiver<bool>,
) -> Result<()>
where
    F: Fn() -> B + Clone + Send + Sync + 'static,
    B: IntoStreamPipeline<Pipeline = P> + 'static,
    P: StreamRuntimePipeline,
    L: Life,
{
    let local_addr = listener.local_addr()?;
    let ids = Arc::new(AtomicU64::new(1));
    let mut connections = JoinSet::new();

    let result: Result<()> = async {
        loop {
            if *shutdown_rx.borrow() {
                break;
            }

            tokio::select! {
                accepted = listener.accept() => {
                    let (stream, peer_addr) = accepted?;
                    stream.set_nodelay(config.tcp_nodelay)?;

                    let local_addr = stream.local_addr()?;
                    let id = ids.fetch_add(1, Ordering::Relaxed);
                    let pipeline = (pipeline_factory)().into_stream_pipeline();
                    let config = config.clone();
                    let (tx, rx) =
                        mpsc::channel::<StreamCommand<P::Write>>(config.outbound_queue_size);
                    let channel = Channel::new(id, peer_addr, local_addr, tx);
                    let life = life.clone();
                    let shutdown_rx = Some(shutdown_tx.subscribe());

                    connections.spawn(async move {
                        let connection = StreamConnection {
                            id,
                            stream,
                            peer_addr,
                            local_addr,
                            pipeline,
                            config,
                            channel,
                            rx,
                            shutdown_rx,
                        };

                        run_stream_connection_with_life(connection, life).await
                    });
                }

                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        break;
                    }
                }

                joined = connections.join_next(), if !connections.is_empty() => {
                    handle_connection_result(joined);
                }
            }
        }

        Ok(())
    }
    .await;

    if !*shutdown_tx.borrow() {
        let _ = shutdown_tx.send(true);
    }

    while let Some(joined) = connections.join_next().await {
        handle_connection_result(Some(joined));
    }

    if let Err(err) = life.tcp_server_stopped(local_addr).await {
        tracing::debug!(
            local_addr = %local_addr,
            error = ?err,
            "tcp life hook failed while stopping server"
        );
    }

    result
}

fn handle_connection_result(result: Option<std::result::Result<Result<()>, JoinError>>) {
    let Some(result) = result else {
        return;
    };

    match result {
        Ok(Ok(())) => {}
        Ok(Err(err)) => {
            tracing::debug!(error = ?err, "connection closed with error");
        }
        Err(err) => {
            tracing::debug!(error = ?err, "connection task failed");
        }
    }
}
