use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use rs_netty::{
    codec::{LineCodec, Utf8DatagramCodec},
    datagram_pipeline, pipeline, CloseReason, ConnInfo, ConnectionStats, Context, DatagramContext,
    DatagramHandler, Error, Life, Result, TcpServer, UdpServer,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpStream, UdpSocket},
};

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

#[tokio::test]
async fn tcp_connection_stats_are_opt_in() -> Result<()> {
    let seen = Arc::new(Mutex::new(None));
    let seen_stats = seen.clone();
    let server = TcpServer::bind("127.0.0.1:0")
        .pipeline(move || {
            pipeline().codec(LineCodec::new()).handler(StatsEcho {
                seen: seen_stats.clone(),
            })
        })
        .track_connection_stats()
        .start()
        .await?;

    let mut stream = TcpStream::connect(server.local_addr()).await?;
    stream.write_all(b"hello\n").await?;

    let mut response = vec![0; 6];
    stream.read_exact(&mut response).await?;
    drop(stream);

    server.shutdown();
    server.wait().await?;

    let stats = seen.lock().expect("stats").clone().expect("stats");
    assert!(stats.bytes_read() >= 6);
    assert!(stats.bytes_written() >= 6);
    assert_eq!(stats.frames_read(), 1);
    assert_eq!(stats.frames_written(), 1);
    Ok(())
}

#[tokio::test]
async fn tcp_context_write_and_flush_flushes_before_handler_returns() -> Result<()> {
    let server = TcpServer::bind("127.0.0.1:0")
        .pipeline(|| pipeline().codec(LineCodec::new()).handler(FlushTwice))
        .start()
        .await?;

    let mut stream = TcpStream::connect(server.local_addr()).await?;
    stream.write_all(b"go\n").await?;

    let mut first = vec![0; b"first\n".len()];
    tokio::time::timeout(Duration::from_millis(50), stream.read_exact(&mut first))
        .await
        .map_err(|err| Error::Pipeline(err.to_string()))??;
    assert_eq!(first, b"first\n");

    let mut second = vec![0; b"second\n".len()];
    tokio::time::timeout(Duration::from_millis(200), stream.read_exact(&mut second))
        .await
        .map_err(|err| Error::Pipeline(err.to_string()))??;
    assert_eq!(second, b"second\n");

    drop(stream);
    server.shutdown();
    server.wait().await
}

#[tokio::test]
async fn udp_context_write_and_flush_sends_before_handler_returns() -> Result<()> {
    let server = UdpServer::bind("127.0.0.1:0")
        .pipeline(|| {
            datagram_pipeline()
                .codec(Utf8DatagramCodec)
                .handler(UdpFlushTwice)
        })
        .start()
        .await?;

    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    socket.send_to(b"go", server.local_addr()).await?;

    let mut first = vec![0; b"first".len()];
    let (first_len, _) =
        tokio::time::timeout(Duration::from_millis(50), socket.recv_from(&mut first))
            .await
            .map_err(|err| Error::Pipeline(err.to_string()))??;
    assert_eq!(&first[..first_len], b"first");

    let mut second = vec![0; b"second".len()];
    let (second_len, _) =
        tokio::time::timeout(Duration::from_millis(200), socket.recv_from(&mut second))
            .await
            .map_err(|err| Error::Pipeline(err.to_string()))??;
    assert_eq!(&second[..second_len], b"second");

    server.shutdown();
    server.wait().await
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

struct StatsEcho {
    seen: Arc<Mutex<Option<ConnectionStats>>>,
}

impl rs_netty::Handler<String> for StatsEcho {
    type Write = String;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
        *self.seen.lock().expect("stats") = ctx.stats();
        ctx.write(msg).await
    }
}

struct FlushTwice;

impl rs_netty::Handler<String> for FlushTwice {
    type Write = String;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, _msg: String) -> Result<()> {
        ctx.write_and_flush("first".to_string()).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        ctx.write_and_flush("second".to_string()).await
    }
}

struct UdpEcho;

impl DatagramHandler<String> for UdpEcho {
    type Write = String;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: String) -> Result<()> {
        ctx.write(msg).await
    }
}

struct UdpFlushTwice;

impl DatagramHandler<String> for UdpFlushTwice {
    type Write = String;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, _msg: String) -> Result<()> {
        ctx.write_and_flush("first".to_string()).await?;
        tokio::time::sleep(Duration::from_millis(100)).await;
        ctx.write_and_flush("second".to_string()).await
    }
}
