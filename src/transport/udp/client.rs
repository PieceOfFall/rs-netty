use std::net::SocketAddr;

use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle};

use crate::{
    channel::{command::DatagramCommand, DatagramChannel},
    life::{Life, NoLife},
    pipeline::{
        datagram::builder::IntoDatagramPipeline, datagram::runtime::DatagramRuntimePipeline,
    },
    transport::udp::{
        config::UdpSocketConfig,
        socket::{run_datagram_socket_with_life, DatagramSocketRuntime},
    },
    Result,
};

/// Configuration type for UDP client sockets.
pub type UdpClientConfig = UdpSocketConfig;

/// Marker used before a UDP client pipeline has been configured.
pub struct NoPipeline;

/// Builder for a UDP client socket.
pub struct UdpClient<F = NoPipeline, L = NoLife> {
    remote_addr: String,
    local_addr: String,
    pipeline_factory: F,
    config: UdpSocketConfig,
    life: L,
}

impl UdpClient<NoPipeline, NoLife> {
    /// Creates a UDP client builder with a default ephemeral local bind address.
    pub fn connect(remote_addr: impl Into<String>) -> Self {
        Self {
            remote_addr: remote_addr.into(),
            local_addr: "0.0.0.0:0".to_string(),
            pipeline_factory: NoPipeline,
            config: UdpSocketConfig::default(),
            life: NoLife,
        }
    }
}

impl<L> UdpClient<NoPipeline, L> {
    /// Sets the socket pipeline factory.
    pub fn pipeline<F, B, P>(self, factory: F) -> UdpClient<F, L>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoDatagramPipeline<Pipeline = P>,
        P: DatagramRuntimePipeline,
    {
        UdpClient {
            remote_addr: self.remote_addr,
            local_addr: self.local_addr,
            pipeline_factory: factory,
            config: self.config,
            life: self.life,
        }
    }
}

impl<F, L> UdpClient<F, L> {
    /// Attaches lifecycle hooks.
    pub fn life<NextLife>(self, life: NextLife) -> UdpClient<F, NextLife> {
        UdpClient {
            remote_addr: self.remote_addr,
            local_addr: self.local_addr,
            pipeline_factory: self.pipeline_factory,
            config: self.config,
            life,
        }
    }

    /// Binds the UDP socket to a local address.
    pub fn bind(mut self, local_addr: impl Into<String>) -> Self {
        self.local_addr = local_addr.into();
        self
    }

    /// Sets the receive buffer size used by the socket task.
    pub fn read_buffer_capacity(mut self, value: usize) -> Self {
        self.config.read_buffer_capacity = value;
        self.config.normalize();
        self
    }

    /// Sets the initial write buffer capacity.
    pub fn write_buffer_capacity(mut self, value: usize) -> Self {
        self.config.write_buffer_capacity = value;
        self
    }

    /// Sets the maximum accepted datagram payload size.
    pub fn max_datagram_size(mut self, value: usize) -> Self {
        self.config.max_datagram_size = value;
        self.config.normalize();
        self
    }

    /// Sets the bounded outbound command queue size.
    pub fn outbound_queue_size(mut self, value: usize) -> Self {
        self.config.outbound_queue_size = value.max(1);
        self
    }

    /// Starts the socket task and returns a client handle.
    pub async fn run<B, P>(self) -> Result<UdpClientHandle<P::Write>>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoDatagramPipeline<Pipeline = P>,
        P: DatagramRuntimePipeline,
        L: Life,
    {
        let remote_addr = self.remote_addr.parse::<SocketAddr>()?;
        let socket = UdpSocket::bind(&self.local_addr).await?;
        let local_addr = socket.local_addr()?;
        let pipeline = (self.pipeline_factory)().into_datagram_pipeline();
        let mut config = self.config;
        config.normalize();
        let (tx, rx) = mpsc::channel::<DatagramCommand<P::Write>>(config.outbound_queue_size);
        let channel = DatagramChannel::new(1, local_addr, tx);
        let socket_channel = channel.clone();
        let life = self.life;

        let join = tokio::spawn(async move {
            run_datagram_socket_with_life(
                DatagramSocketRuntime {
                    id: 1,
                    socket,
                    pipeline,
                    config,
                    channel: socket_channel,
                    rx,
                    shutdown_rx: None,
                },
                life,
            )
            .await
        });

        Ok(UdpClientHandle {
            remote_addr,
            channel,
            join,
        })
    }
}

/// Handle for an active UDP client socket.
pub struct UdpClientHandle<W> {
    remote_addr: SocketAddr,
    channel: DatagramChannel<W>,
    join: JoinHandle<Result<()>>,
}

impl<W: Send + 'static> UdpClientHandle<W> {
    /// Returns the underlying cloneable channel.
    pub fn channel(&self) -> DatagramChannel<W> {
        self.channel.clone()
    }

    /// Queues a datagram for the default remote peer.
    pub async fn write(&self, msg: W) -> Result<()> {
        self.channel.write_to(self.remote_addr, msg).await
    }

    /// Queues a datagram for the default remote peer and waits until it is sent.
    pub async fn write_and_flush(&self, msg: W) -> Result<()> {
        self.channel.write_to_and_flush(self.remote_addr, msg).await
    }

    /// Queues a datagram for an explicit peer.
    pub async fn write_to(&self, peer_addr: SocketAddr, msg: W) -> Result<()> {
        self.channel.write_to(peer_addr, msg).await
    }

    /// Queues a datagram for an explicit peer and waits until it is sent.
    pub async fn write_to_and_flush(&self, peer_addr: SocketAddr, msg: W) -> Result<()> {
        self.channel.write_to_and_flush(peer_addr, msg).await
    }

    /// Requests local socket shutdown.
    pub async fn close(&self) -> Result<()> {
        self.channel.close().await
    }

    /// Waits for the socket task to finish.
    pub async fn wait(self) -> Result<()> {
        self.join.await?
    }
}
