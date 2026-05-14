use std::net::SocketAddr;

use rs_netty::{
    codec::LineCodec, handler, pipeline, CloseReason, ConnInfo, Life, Result, TcpServer,
};

#[tokio::main]
async fn main() -> Result<()> {
    let server = TcpServer::bind("127.0.0.1:9003")
        .pipeline(|| {
            let pipeline = pipeline().codec(LineCodec::new()).handler(Echo);
            pipeline
        })
        .life(PrintLife)
        .start()
        .await?;

    println!("listening on {}", server.local_addr());
    println!("press Ctrl+C to shutdown gracefully");

    tokio::signal::ctrl_c().await?;
    server.shutdown();
    server.wait().await
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

#[handler(Echo)]
async fn echo(msg: String) -> Result<String> {
    Ok(format!("echo: {msg}"))
}
