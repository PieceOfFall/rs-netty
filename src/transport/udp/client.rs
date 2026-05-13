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

pub type UdpClientConfig = UdpSocketConfig;

pub struct NoPipeline;

pub struct UdpClient<F = NoPipeline, L = NoLife> {
    remote_addr: String,
    local_addr: String,
    pipeline_factory: F,
    config: UdpSocketConfig,
    life: L,
}

impl UdpClient<NoPipeline, NoLife> {
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
    pub fn life<NextLife>(self, life: NextLife) -> UdpClient<F, NextLife> {
        UdpClient {
            remote_addr: self.remote_addr,
            local_addr: self.local_addr,
            pipeline_factory: self.pipeline_factory,
            config: self.config,
            life,
        }
    }

    pub fn bind(mut self, local_addr: impl Into<String>) -> Self {
        self.local_addr = local_addr.into();
        self
    }

    pub fn read_buffer_capacity(mut self, value: usize) -> Self {
        self.config.read_buffer_capacity = value;
        self.config.normalize();
        self
    }

    pub fn write_buffer_capacity(mut self, value: usize) -> Self {
        self.config.write_buffer_capacity = value;
        self
    }

    pub fn max_datagram_size(mut self, value: usize) -> Self {
        self.config.max_datagram_size = value;
        self.config.normalize();
        self
    }

    pub fn outbound_queue_size(mut self, value: usize) -> Self {
        self.config.outbound_queue_size = value.max(1);
        self
    }

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

pub struct UdpClientHandle<W> {
    remote_addr: SocketAddr,
    channel: DatagramChannel<W>,
    join: JoinHandle<Result<()>>,
}

impl<W: Send + 'static> UdpClientHandle<W> {
    pub fn channel(&self) -> DatagramChannel<W> {
        self.channel.clone()
    }

    pub async fn write(&self, msg: W) -> Result<()> {
        self.channel.write_to(self.remote_addr, msg).await
    }

    pub async fn write_to(&self, peer_addr: SocketAddr, msg: W) -> Result<()> {
        self.channel.write_to(peer_addr, msg).await
    }

    pub async fn close(&self) -> Result<()> {
        self.channel.close().await
    }

    pub async fn wait(self) -> Result<()> {
        self.join.await?
    }
}
