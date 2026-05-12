use std::net::SocketAddr;

use tokio::{
    net::{TcpSocket, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};

use crate::{
    channel::{command::StreamCommand, Channel},
    pipeline::{stream::builder::IntoStreamPipeline, stream::runtime::StreamRuntimePipeline},
    transport::tcp::{config::TcpConnectionConfig, connection::run_stream_connection},
    Result,
};

pub type TcpClientConfig = TcpConnectionConfig;

pub struct NoPipeline;

pub struct TcpClient<F = NoPipeline> {
    remote_addr: String,
    local_addr: Option<String>,
    pipeline_factory: F,
    config: TcpConnectionConfig,
}

impl TcpClient<NoPipeline> {
    pub fn connect(remote_addr: impl Into<String>) -> Self {
        Self {
            remote_addr: remote_addr.into(),
            local_addr: None,
            pipeline_factory: NoPipeline,
            config: TcpConnectionConfig::default(),
        }
    }

    pub fn pipeline<F, B, P>(self, factory: F) -> TcpClient<F>
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
        }
    }
}

impl<F> TcpClient<F> {
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

    pub async fn run<B, P>(self) -> Result<TcpClientHandle<P::Write>>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoStreamPipeline<Pipeline = P>,
        P: StreamRuntimePipeline,
    {
        let remote_addr = self.remote_addr.parse::<SocketAddr>()?;
        let stream = connect_stream(remote_addr, self.local_addr.as_deref()).await?;
        stream.set_nodelay(self.config.tcp_nodelay)?;

        let local_addr = stream.local_addr()?;
        let peer_addr = stream.peer_addr()?;
        let pipeline = (self.pipeline_factory)().into_stream_pipeline();
        let config = self.config;
        let (tx, rx) = mpsc::channel::<StreamCommand<P::Write>>(config.outbound_queue_size);
        let channel = Channel::new(1, peer_addr, local_addr, tx);
        let connection_channel = channel.clone();

        let join = tokio::spawn(async move {
            run_stream_connection(
                1,
                stream,
                peer_addr,
                local_addr,
                pipeline,
                config,
                connection_channel,
                rx,
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
