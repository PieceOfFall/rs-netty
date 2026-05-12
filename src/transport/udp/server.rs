use tokio::{net::UdpSocket, sync::mpsc};

use crate::{
    channel::{command::DatagramCommand, DatagramChannel},
    pipeline::{
        datagram::builder::IntoDatagramPipeline, datagram::runtime::DatagramRuntimePipeline,
    },
    transport::udp::{config::UdpSocketConfig, socket::run_datagram_socket},
    Result,
};

pub type UdpServerConfig = UdpSocketConfig;

pub struct NoPipeline;

pub struct UdpServer<F = NoPipeline> {
    addr: String,
    pipeline_factory: F,
    config: UdpSocketConfig,
}

impl UdpServer<NoPipeline> {
    pub fn bind(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            pipeline_factory: NoPipeline,
            config: UdpSocketConfig::default(),
        }
    }

    pub fn pipeline<F, B, P>(self, factory: F) -> UdpServer<F>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoDatagramPipeline<Pipeline = P>,
        P: DatagramRuntimePipeline,
    {
        UdpServer {
            addr: self.addr,
            pipeline_factory: factory,
            config: self.config,
        }
    }
}

impl<F> UdpServer<F> {
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

    pub async fn run<B, P>(self) -> Result<()>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoDatagramPipeline<Pipeline = P>,
        P: DatagramRuntimePipeline,
    {
        let socket = UdpSocket::bind(&self.addr).await?;
        let local_addr = socket.local_addr()?;
        let pipeline = (self.pipeline_factory)().into_datagram_pipeline();
        let config = self.config;
        let (tx, rx) = mpsc::channel::<DatagramCommand<P::Write>>(config.outbound_queue_size);
        let channel = DatagramChannel::new(1, local_addr, tx);

        run_datagram_socket(1, socket, pipeline, config, channel, rx).await
    }
}
