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

/// Configuration type shared by TCP clients and TCP server connections.
pub type TcpClientConfig = TcpConnectionConfig;

/// Marker used before a TCP client pipeline has been configured.
pub struct NoPipeline;

/// Stores a reusable TCP client pipeline factory.
pub struct PipelineFactory<F> {
    factory: F,
}

/// Stores a TCP client pipeline that will be consumed exactly once by `run`.
pub struct PipelineInstance<B> {
    pipeline: B,
}

/// Builder for a TCP client connection.
pub struct TcpClient<F = NoPipeline, L = NoLife> {
    remote_addr: String,
    local_addr: Option<String>,
    pipeline_factory: F,
    config: TcpConnectionConfig,
    life: L,
}

impl TcpClient<NoPipeline, NoLife> {
    /// Creates a TCP client builder for a remote socket address.
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
    /// Sets the connection pipeline factory.
    pub fn pipeline<F, B, P>(self, factory: F) -> TcpClient<PipelineFactory<F>, L>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoStreamPipeline<Pipeline = P>,
        P: StreamRuntimePipeline,
    {
        TcpClient {
            remote_addr: self.remote_addr,
            local_addr: self.local_addr,
            pipeline_factory: PipelineFactory { factory },
            config: self.config,
            life: self.life,
        }
    }

    /// Sets a single pipeline instance for this client connection.
    ///
    /// This is useful for client handlers that own one-shot state such as a
    /// `oneshot::Sender`, where a reusable pipeline factory would require
    /// extra shared-state wrapping.
    pub fn pipeline_instance<B, P>(self, pipeline: B) -> TcpClient<PipelineInstance<B>, L>
    where
        B: IntoStreamPipeline<Pipeline = P>,
        P: StreamRuntimePipeline,
    {
        TcpClient {
            remote_addr: self.remote_addr,
            local_addr: self.local_addr,
            pipeline_factory: PipelineInstance { pipeline },
            config: self.config,
            life: self.life,
        }
    }
}

impl<F, L> TcpClient<F, L> {
    /// Attaches lifecycle hooks.
    pub fn life<NextLife>(self, life: NextLife) -> TcpClient<F, NextLife> {
        TcpClient {
            remote_addr: self.remote_addr,
            local_addr: self.local_addr,
            pipeline_factory: self.pipeline_factory,
            config: self.config,
            life,
        }
    }

    /// Binds the outgoing socket to a local address before connecting.
    pub fn bind(mut self, local_addr: impl Into<String>) -> Self {
        self.local_addr = Some(local_addr.into());
        self
    }

    /// Sets the initial TCP read buffer capacity.
    pub fn read_buffer_capacity(mut self, value: usize) -> Self {
        self.config.read_buffer_capacity = value;
        self
    }

    /// Sets the initial TCP write buffer capacity.
    pub fn write_buffer_capacity(mut self, value: usize) -> Self {
        self.config.write_buffer_capacity = value;
        self
    }

    /// Sets the maximum buffered frame size before the connection is closed.
    pub fn max_frame_size(mut self, value: usize) -> Self {
        self.config.max_frame_size = value;
        self
    }

    /// Sets the bounded outbound command queue size.
    pub fn outbound_queue_size(mut self, value: usize) -> Self {
        self.config.outbound_queue_size = value.max(1);
        self
    }

    /// Enables or disables `TCP_NODELAY`.
    pub fn tcp_nodelay(mut self, value: bool) -> Self {
        self.config.tcp_nodelay = value;
        self
    }

    /// Closes the connection after the provided idle duration.
    pub fn idle_timeout(mut self, value: Duration) -> Self {
        self.config.idle_timeout = Some(value);
        self
    }

    /// Enables byte/frame counters for this connection.
    pub fn track_connection_stats(mut self) -> Self {
        self.config.track_connection_stats = true;
        self
    }
}

impl<F, L> TcpClient<PipelineFactory<F>, L> {
    /// Connects with a reusable pipeline factory, starts the connection task,
    /// and returns a client handle.
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
        let pipeline = (self.pipeline_factory.factory)().into_stream_pipeline();
        run_connected_client(
            stream,
            peer_addr,
            local_addr,
            pipeline,
            self.config,
            self.life,
        )
        .await
    }
}

impl<B, L> TcpClient<PipelineInstance<B>, L> {
    /// Connects with a single-use pipeline, starts the connection task, and
    /// returns a client handle.
    pub async fn run<P>(self) -> Result<TcpClientHandle<P::Write>>
    where
        B: IntoStreamPipeline<Pipeline = P>,
        P: StreamRuntimePipeline,
        L: Life,
    {
        let remote_addr = self.remote_addr.parse::<SocketAddr>()?;
        let stream = connect_stream(remote_addr, self.local_addr.as_deref()).await?;
        stream.set_nodelay(self.config.tcp_nodelay)?;

        let local_addr = stream.local_addr()?;
        let peer_addr = stream.peer_addr()?;
        let pipeline = self.pipeline_factory.pipeline.into_stream_pipeline();
        run_connected_client(
            stream,
            peer_addr,
            local_addr,
            pipeline,
            self.config,
            self.life,
        )
        .await
    }
}

async fn run_connected_client<P, L>(
    stream: TcpStream,
    peer_addr: SocketAddr,
    local_addr: SocketAddr,
    pipeline: P,
    config: TcpConnectionConfig,
    life: L,
) -> Result<TcpClientHandle<P::Write>>
where
    P: StreamRuntimePipeline,
    L: Life,
{
    let stats = config
        .track_connection_stats
        .then(crate::context::ConnectionStats::new);
    let (tx, rx) = mpsc::channel::<StreamCommand<P::Write>>(config.outbound_queue_size);
    let channel = Channel::new(1, peer_addr, local_addr, tx, stats.clone());
    let connection_channel = channel.clone();

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

/// Handle for an active TCP client connection.
pub struct TcpClientHandle<W> {
    channel: Channel<W>,
    join: JoinHandle<Result<()>>,
}

impl<W: Send + 'static> TcpClientHandle<W> {
    /// Returns the underlying cloneable channel.
    pub fn channel(&self) -> Channel<W> {
        self.channel.clone()
    }

    /// Queues a message for the connection task.
    pub async fn write(&self, msg: W) -> Result<()> {
        self.channel.write(msg).await
    }

    /// Queues a message and waits until it has been flushed.
    pub async fn write_and_flush(&self, msg: W) -> Result<()> {
        self.channel.write_and_flush(msg).await
    }

    /// Requests local connection shutdown.
    pub async fn close(&self) -> Result<()> {
        self.channel.close().await
    }

    /// Waits for the connection task to finish.
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
