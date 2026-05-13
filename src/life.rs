use std::{future::Future, net::SocketAddr};

use crate::{context::ConnInfo, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CloseReason {
    Completed,
    Error,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoLife;

pub trait Life: Clone + Send + Sync + 'static {
    fn tcp_server_started(
        &self,
        _local_addr: SocketAddr,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn tcp_server_stopped(
        &self,
        _local_addr: SocketAddr,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn tcp_connection_opened(&self, _info: ConnInfo) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn tcp_connection_closed(
        &self,
        _info: ConnInfo,
        _reason: CloseReason,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn udp_socket_started(
        &self,
        _local_addr: SocketAddr,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    fn udp_socket_stopped(
        &self,
        _local_addr: SocketAddr,
    ) -> impl Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}

impl Life for NoLife {}
