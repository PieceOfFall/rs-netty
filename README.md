# rs-netty

rs-netty is a Tokio-native typed TCP/UDP pipeline framework inspired by Netty. It keeps the Channel / Pipeline / Handler mental model, but uses Rust ownership, async/await, Tokio tasks, bounded mpsc, and typed messages instead of EventLoop, ChannelFuture, Promise, Object messages, and reference-counted ByteBuf.

## TCP Echo Server

```rust
use rs_netty::{codec::LineCodec, pipeline, Context, Handler, Result, TcpServer};

#[tokio::main]
async fn main() -> Result<()> {
    TcpServer::bind("127.0.0.1:9000")
        .pipeline(|| pipeline().codec(LineCodec::new()).handler(Echo))
        .run()
        .await
}

struct Echo;

impl Handler<String> for Echo {
    type Write = String;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
        ctx.write(msg).await
    }
}
```

## TCP Client

```rust
use rs_netty::{codec::LineCodec, pipeline, Context, Handler, Result, TcpClient};

#[tokio::main]
async fn main() -> Result<()> {
    let client = TcpClient::connect("127.0.0.1:9000")
        .pipeline(|| pipeline().codec(LineCodec::new()).handler(PrintResponse))
        .run()
        .await?;

    client.write("hello".to_string()).await?;
    client.close().await?;
    client.wait().await
}

struct PrintResponse;

impl Handler<String> for PrintResponse {
    type Write = String;

    async fn read(&mut self, _ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
        println!("server -> {msg}");
        Ok(())
    }
}
```

## UDP Echo Server

```rust
use rs_netty::{
    codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Result,
    UdpServer,
};

#[tokio::main]
async fn main() -> Result<()> {
    UdpServer::bind("127.0.0.1:9002")
        .pipeline(|| datagram_pipeline().codec(Utf8DatagramCodec).handler(UdpEcho))
        .run()
        .await
}

struct UdpEcho;

impl DatagramHandler<String> for UdpEcho {
    type Write = String;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: String) -> Result<()> {
        ctx.write(format!("echo: {msg}")).await
    }
}
```

## Compile-Time Constraints

TCP uses the stream pipeline:

```text
pipeline()
  .codec(...)
  .inbound(...)*
  .business(...)*
  .handler(...)
  .outbound(...)*
```

UDP uses the datagram pipeline:

```text
datagram_pipeline()
  .codec(...)
  .inbound(...)*
  .business(...)*
  .handler(...)
  .outbound(...)*
```

Methods only exist in valid states. Message transitions are checked with trait bounds, so handler inputs must match previous stage outputs, outbound inputs must match `Handler::Write` or `DatagramHandler::Write`, and final outbound types must be encodable by the selected codec.

`TcpServer` and `TcpClient` only accept stream pipelines. `UdpServer` and `UdpClient` only accept datagram pipelines.

## UDP Semantics

UDP support is datagram-oriented. `UdpServer` uses one socket-level pipeline and does not create per-peer child pipelines. If you need per-peer state, store it explicitly inside your handler, for example with `HashMap<SocketAddr, PeerState>`.

`DatagramContext::write(msg)` replies to the current datagram peer. `DatagramContext::write_to(peer, msg)` and `DatagramChannel::write_to(peer, msg)` send to an explicit peer.

## Lifecycle Hooks

Servers and clients can attach optional lifecycle hooks with `.life(...)`. The default is `NoLife`, so applications that do not need hooks pay no dynamic dispatch cost.

```rust
use std::net::SocketAddr;

use rs_netty::{codec::LineCodec, pipeline, Life, Result, TcpServer};

#[derive(Clone, Copy)]
struct TraceLife;

impl Life for TraceLife {
    async fn tcp_server_started(&self, local_addr: SocketAddr) -> Result<()> {
        tracing::info!(%local_addr, "tcp server started");
        Ok(())
    }
}

TcpServer::bind("127.0.0.1:9000")
    .pipeline(|| pipeline().codec(LineCodec::new()).handler(MyHandler))
    .life(TraceLife)
    .run()
    .await
```

Servers also support an external shutdown handle:

```rust
let server = TcpServer::bind("127.0.0.1:9000")
    .pipeline(|| pipeline().codec(LineCodec::new()).handler(MyHandler))
    .start()
    .await?;

server.shutdown();
server.wait().await?;
```

TCP servers and clients can also enable an optional idle timeout:

```rust
TcpServer::bind("127.0.0.1:9000")
    .idle_timeout(std::time::Duration::from_secs(90))
    .pipeline(|| pipeline().codec(LineCodec::new()).handler(MyHandler))
    .run()
    .await
```

When no idle timeout is configured, the TCP connection loop uses the no-timeout path and does not create a timer.

TCP connection stats are also opt-in:

```rust
TcpServer::bind("127.0.0.1:9000")
    .track_connection_stats()
    .pipeline(|| pipeline().codec(LineCodec::new()).handler(MyHandler))
    .run()
    .await
```

When enabled, `Context::stats()` and `Channel::stats()` expose connection time, bytes read/written, and frames read/written. Channels also expose `is_closed()`, `capacity()`, and `max_capacity()` from the underlying Tokio queue.

## Built-In Codecs

Stream codecs use Netty-style names:

- `LineCodec`
- `LengthFieldBasedFrameDecoder`
- `LengthFieldPrepender`
- `FixedLengthFrameDecoder`
- `DelimiterBasedFrameDecoder`
- `ByteArrayDecoder`
- `ByteArrayEncoder`
- `MqttCodec`

Datagram codecs:

- `Utf8DatagramCodec`
- `BytesDatagramCodec`

## Examples

```bash
cargo run --example tcp_echo_server
cargo run --example tcp_echo_client
cargo run --example tcp_typed_chain
cargo run --example tcp_lifecycle
cargo run --example udp_echo_server
cargo run --example udp_echo_client
cargo run --example udp_typed_chain
```

## Non-Goals

Non-goals for v0.2:

- No EventLoop API.
- No ByteBuf refCnt API.
- No ChannelFuture / Promise API.
- No dynamic Box<dyn Handler> main path.
- No TLS yet.
- No codec registry yet.
- No automatic UDP reliability / ordering / retransmission.
- No per-peer UDP child pipeline yet.
