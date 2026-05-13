use std::net::SocketAddr;

use rs_netty::{
    codec::LineCodec, pipeline, CloseReason, ConnInfo, Context, Handler, Life, Result, TcpServer,
};

#[tokio::main]
async fn main() -> Result<()> {
    TcpServer::bind("127.0.0.1:9003")
        .pipeline(|| pipeline().codec(LineCodec::new()).handler(Echo))
        .life(PrintLife)
        .run()
        .await
}

#[derive(Clone, Copy)]
struct PrintLife;

impl Life for PrintLife {
    async fn tcp_server_started(&self, local_addr: SocketAddr) -> Result<()> {
        println!("server started: {local_addr}");
        Ok(())
    }

    async fn tcp_server_stopped(&self, local_addr: SocketAddr) -> Result<()> {
        println!("server stopped: {local_addr}");
        Ok(())
    }

    async fn tcp_connection_opened(&self, info: ConnInfo) -> Result<()> {
        println!(
            "connection opened: id={} peer={} local={}",
            info.id(),
            info.peer_addr(),
            info.local_addr()
        );
        Ok(())
    }

    async fn tcp_connection_closed(&self, info: ConnInfo, reason: CloseReason) -> Result<()> {
        println!(
            "connection closed: id={} peer={} reason={reason:?}",
            info.id(),
            info.peer_addr()
        );
        Ok(())
    }
}

struct Echo;

impl Handler<String> for Echo {
    type Write = String;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
        ctx.write(format!("echo: {msg}")).await
    }
}
