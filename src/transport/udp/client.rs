use std::net::SocketAddr;

use tokio::{net::UdpSocket, sync::mpsc, task::JoinHandle};

use crate::{
    channel::{command::DatagramCommand, DatagramChannel},
    pipeline::{
        datagram::builder::IntoDatagramPipeline, datagram::runtime::DatagramRuntimePipeline,
    },
    transport::udp::{config::UdpSocketConfig, socket::run_datagram_socket},
    Result,
};

pub type UdpClientConfig = UdpSocketConfig;

pub struct NoPipeline;

pub struct UdpClient<F = NoPipeline> {
    remote_addr: String,
    local_addr: String,
    pipeline_factory: F,
    config: UdpSocketConfig,
}

impl UdpClient<NoPipeline> {
    pub fn connect(remote_addr: impl Into<String>) -> Self {
        Self {
            remote_addr: remote_addr.into(),
            local_addr: "0.0.0.0:0".to_string(),
            pipeline_factory: NoPipeline,
            config: UdpSocketConfig::default(),
        }
    }

    pub fn pipeline<F, B, P>(self, factory: F) -> UdpClient<F>
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
        }
    }
}

impl<F> UdpClient<F> {
    pub fn bind(mut self, local_addr: impl Into<String>) -> Self {
        self.local_addr = local_addr.into();
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

    pub fn max_datagram_size(mut self, value: usize) -> Self {
        self.config.max_datagram_size = value;
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
    {
        let remote_addr = self.remote_addr.parse::<SocketAddr>()?;
        let socket = UdpSocket::bind(&self.local_addr).await?;
        let local_addr = socket.local_addr()?;
        let pipeline = (self.pipeline_factory)().into_datagram_pipeline();
        let config = self.config;
        let (tx, rx) = mpsc::channel::<DatagramCommand<P::Write>>(config.outbound_queue_size);
        let channel = DatagramChannel::new(1, local_addr, tx);
        let socket_channel = channel.clone();

        let join = tokio::spawn(async move {
            run_datagram_socket(1, socket, pipeline, config, socket_channel, rx).await
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
