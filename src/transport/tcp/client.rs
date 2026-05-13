use std::{net::SocketAddr, time::Duration};

use tokio::{
    net::{TcpSocket, TcpStream},
    sync::mpsc,
    task::JoinHandle,
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

pub type TcpClientConfig = TcpConnectionConfig;

pub struct NoPipeline;

pub struct TcpClient<F = NoPipeline, L = NoLife> {
    remote_addr: String,
    local_addr: Option<String>,
    pipeline_factory: F,
    config: TcpConnectionConfig,
    life: L,
}

impl TcpClient<NoPipeline, NoLife> {
    pub fn connect(remote_addr: impl Into<String>) -> Self {
        Self {
            remote_addr: remote_addr.into(),
            local_addr: None,
            pipeline_factory: NoPipeline,
            config: TcpConnectionConfig::default(),
            life: NoLife,
        }
    }
}

impl<L> TcpClient<NoPipeline, L> {
    pub fn pipeline<F, B, P>(self, factory: F) -> TcpClient<F, L>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoStreamPipeline<Pipeline = P>,
        P: StreamRuntimePipeline,
    {
        TcpClient {
            remote_addr: self.remote_addr,
            local_addr: self.local_addr,
            pipeline_factory: factory,
            config: self.config,
            life: self.life,
        }
    }
}

impl<F, L> TcpClient<F, L> {
    pub fn life<NextLife>(self, life: NextLife) -> TcpClient<F, NextLife> {
        TcpClient {
            remote_addr: self.remote_addr,
            local_addr: self.local_addr,
            pipeline_factory: self.pipeline_factory,
            config: self.config,
            life,
        }
    }

    pub fn bind(mut self, local_addr: impl Into<String>) -> Self {
        self.local_addr = Some(local_addr.into());
        self
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

    pub fn track_connection_stats(mut self) -> Self {
        self.config.track_connection_stats = true;
        self
    }

    pub async fn run<B, P>(self) -> Result<TcpClientHandle<P::Write>>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoStreamPipeline<Pipeline = P>,
        P: StreamRuntimePipeline,
        L: Life,
    {
        let remote_addr = self.remote_addr.parse::<SocketAddr>()?;
        let stream = connect_stream(remote_addr, self.local_addr.as_deref()).await?;
        stream.set_nodelay(self.config.tcp_nodelay)?;

        let local_addr = stream.local_addr()?;
        let peer_addr = stream.peer_addr()?;
        let pipeline = (self.pipeline_factory)().into_stream_pipeline();
        let config = self.config;
        let stats = config
            .track_connection_stats
            .then(crate::context::ConnectionStats::new);
        let (tx, rx) = mpsc::channel::<StreamCommand<P::Write>>(config.outbound_queue_size);
        let channel = Channel::new(1, peer_addr, local_addr, tx, stats.clone());
        let connection_channel = channel.clone();
        let life = self.life;

        let join = tokio::spawn(async move {
            run_stream_connection_with_life(
                StreamConnection {
                    id: 1,
                    stream,
                    peer_addr,
                    local_addr,
                    pipeline,
                    config,
                    channel: connection_channel,
                    rx,
                    shutdown_rx: None,
                    stats,
                },
                life,
            )
            .await
        });

        Ok(TcpClientHandle { channel, join })
    }
}

pub struct TcpClientHandle<W> {
    channel: Channel<W>,
    join: JoinHandle<Result<()>>,
}

impl<W: Send + 'static> TcpClientHandle<W> {
    pub fn channel(&self) -> Channel<W> {
        self.channel.clone()
    }

    pub async fn write(&self, msg: W) -> Result<()> {
        self.channel.write(msg).await
    }

    pub async fn close(&self) -> Result<()> {
        self.channel.close().await
    }

    pub async fn wait(self) -> Result<()> {
        self.join.await?
    }
}

async fn connect_stream(remote_addr: SocketAddr, local_addr: Option<&str>) -> Result<TcpStream> {
    let Some(local_addr) = local_addr else {
        return Ok(TcpStream::connect(remote_addr).await?);
    };

    let local_addr = local_addr.parse::<SocketAddr>()?;
    let socket = if remote_addr.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };

    socket.bind(local_addr)?;
    Ok(socket.connect(remote_addr).await?)
}
