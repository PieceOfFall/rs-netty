use std::net::SocketAddr;

use tokio::{
    net::UdpSocket,
    sync::{mpsc, watch},
    task::JoinHandle,
};

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

pub type UdpServerConfig = UdpSocketConfig;

pub struct NoPipeline;

pub struct UdpServer<F = NoPipeline, L = NoLife> {
    addr: String,
    pipeline_factory: F,
    config: UdpSocketConfig,
    life: L,
}

impl UdpServer<NoPipeline, NoLife> {
    pub fn bind(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            pipeline_factory: NoPipeline,
            config: UdpSocketConfig::default(),
            life: NoLife,
        }
    }
}

impl<L> UdpServer<NoPipeline, L> {
    pub fn pipeline<F, B, P>(self, factory: F) -> UdpServer<F, L>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoDatagramPipeline<Pipeline = P>,
        P: DatagramRuntimePipeline,
    {
        UdpServer {
            addr: self.addr,
            pipeline_factory: factory,
            config: self.config,
            life: self.life,
        }
    }
}

impl<F, L> UdpServer<F, L> {
    pub fn life<NextLife>(self, life: NextLife) -> UdpServer<F, NextLife> {
        UdpServer {
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

    pub fn max_datagram_size(mut self, value: usize) -> Self {
        self.config.max_datagram_size = value;
        self
    }

    pub fn outbound_queue_size(mut self, value: usize) -> Self {
        self.config.outbound_queue_size = value.max(1);
        self
    }

    pub async fn start<B, P>(self) -> Result<UdpServerHandle>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoDatagramPipeline<Pipeline = P> + 'static,
        P: DatagramRuntimePipeline,
        L: Life,
    {
        let socket = UdpSocket::bind(&self.addr).await?;
        let local_addr = socket.local_addr()?;
        let pipeline = (self.pipeline_factory)().into_datagram_pipeline();
        let config = self.config;
        let (tx, rx) = mpsc::channel::<DatagramCommand<P::Write>>(config.outbound_queue_size);
        let channel = DatagramChannel::new(1, local_addr, tx);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let life = self.life;

        let join = tokio::spawn(run_datagram_socket_with_life(
            DatagramSocketRuntime {
                id: 1,
                socket,
                pipeline,
                config,
                channel,
                rx,
                shutdown_rx: Some(shutdown_rx),
            },
            life,
        ));

        Ok(UdpServerHandle {
            local_addr,
            shutdown_tx,
            join,
        })
    }

    pub async fn run<B, P>(self) -> Result<()>
    where
        F: Fn() -> B + Clone + Send + Sync + 'static,
        B: IntoDatagramPipeline<Pipeline = P> + 'static,
        P: DatagramRuntimePipeline,
        L: Life,
    {
        self.start().await?.wait().await
    }
}

pub struct UdpServerHandle {
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    join: JoinHandle<Result<()>>,
}

impl UdpServerHandle {
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
