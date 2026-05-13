use std::net::SocketAddr;

use rs_netty::{
    codec::{LineCodec, Utf8DatagramCodec},
    datagram_pipeline, pipeline, CloseReason, ConnInfo, Context, DatagramContext, DatagramHandler,
    Life, Result, TcpServer, UdpServer,
};

fn main() {
    let life = TestLife;

    let _tcp = TcpServer::bind("127.0.0.1:0")
        .pipeline(|| pipeline().codec(LineCodec::new()).handler(Echo))
        .life(life);

    let _udp = UdpServer::bind("127.0.0.1:0")
        .pipeline(|| datagram_pipeline().codec(Utf8DatagramCodec).handler(UdpEcho))
        .life(life);
}

#[derive(Clone, Copy)]
struct TestLife;

impl Life for TestLife {
    async fn tcp_server_started(&self, _local_addr: SocketAddr) -> Result<()> {
        Ok(())
    }

    async fn tcp_connection_closed(&self, _info: ConnInfo, _reason: CloseReason) -> Result<()> {
        Ok(())
    }

    async fn udp_socket_started(&self, _local_addr: SocketAddr) -> Result<()> {
        Ok(())
    }
}

struct Echo;

impl rs_netty::Handler<String> for Echo {
    type Write = String;

    async fn read(&mut self, _ctx: &mut Context<Self::Write>, _msg: String) -> Result<()> {
        Ok(())
    }
}

struct UdpEcho;

impl DatagramHandler<String> for UdpEcho {
    type Write = String;

    async fn read(&mut self, _ctx: &mut DatagramContext<Self::Write>, _msg: String) -> Result<()> {
        Ok(())
    }
}
