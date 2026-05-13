use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use rs_netty::{
    codec::{LineCodec, Utf8DatagramCodec},
    datagram_pipeline, pipeline, CloseReason, ConnInfo, Context, DatagramContext, DatagramHandler,
    Life, Result, TcpServer, UdpServer,
};
use tokio::net::TcpStream;

#[tokio::test]
async fn tcp_server_shutdown_stops_server() -> Result<()> {
    let life = CountLife::default();
    let server = TcpServer::bind("127.0.0.1:0")
        .pipeline(|| pipeline().codec(LineCodec::new()).handler(Echo))
        .life(life.clone())
        .start()
        .await?;

    assert_ne!(server.local_addr().port(), 0);
    assert_eq!(life.started.load(Ordering::SeqCst), 1);

    server.shutdown();
    server.wait().await?;

    assert_eq!(life.stopped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn udp_server_shutdown_stops_socket() -> Result<()> {
    let life = CountLife::default();
    let server = UdpServer::bind("127.0.0.1:0")
        .pipeline(|| {
            datagram_pipeline()
                .codec(Utf8DatagramCodec)
                .handler(UdpEcho)
        })
        .life(life.clone())
        .start()
        .await?;

    assert_ne!(server.local_addr().port(), 0);
    tokio::task::yield_now().await;
    assert_eq!(life.started.load(Ordering::SeqCst), 1);

    server.shutdown();
    server.wait().await?;

    assert_eq!(life.stopped.load(Ordering::SeqCst), 1);
    Ok(())
}

#[tokio::test]
async fn tcp_idle_timeout_closes_idle_connection() -> Result<()> {
    let life = ReasonLife::default();
    let server = TcpServer::bind("127.0.0.1:0")
        .pipeline(|| pipeline().codec(LineCodec::new()).handler(Echo))
        .idle_timeout(Duration::from_millis(20))
        .life(life.clone())
        .start()
        .await?;

    let _stream = TcpStream::connect(server.local_addr()).await?;
    tokio::time::sleep(Duration::from_millis(80)).await;

    server.shutdown();
    server.wait().await?;

    assert!(life.contains(CloseReason::IdleTimeout));
    Ok(())
}

#[derive(Clone, Default)]
struct CountLife {
    started: Arc<AtomicUsize>,
    stopped: Arc<AtomicUsize>,
}

impl Life for CountLife {
    async fn tcp_server_started(&self, _local_addr: std::net::SocketAddr) -> Result<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn tcp_server_stopped(&self, _local_addr: std::net::SocketAddr) -> Result<()> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn udp_socket_started(&self, _local_addr: std::net::SocketAddr) -> Result<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn udp_socket_stopped(&self, _local_addr: std::net::SocketAddr) -> Result<()> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Clone, Default)]
struct ReasonLife {
    reasons: Arc<Mutex<Vec<CloseReason>>>,
}

impl ReasonLife {
    fn contains(&self, reason: CloseReason) -> bool {
        self.reasons.lock().expect("reasons").contains(&reason)
    }
}

impl Life for ReasonLife {
    async fn tcp_connection_closed(&self, _info: ConnInfo, reason: CloseReason) -> Result<()> {
        self.reasons.lock().expect("reasons").push(reason);
        Ok(())
    }
}

struct Echo;

impl rs_netty::Handler<String> for Echo {
    type Write = String;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
        ctx.write(msg).await
    }
}

struct UdpEcho;

impl DatagramHandler<String> for UdpEcho {
    type Write = String;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: String) -> Result<()> {
        ctx.write(msg).await
    }
}
