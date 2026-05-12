# rs_netty

rs_netty is a Tokio-native typed TCP pipeline framework inspired by Netty. It keeps the Channel / Pipeline / Handler mental model, but uses Rust ownership, async/await, Tokio tasks, bounded mpsc, and typed messages instead of EventLoop, ChannelFuture, Object messages, and reference-counted ByteBuf.

## Echo

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

Run it with:

```bash
cargo run --example echo
```

## Typed Chain

```rust
use rs_netty::{
    codec::LineCodec, pipeline, Context, Flow, Handler, Inbound, Outbound, Result, TcpServer,
};

struct Request(String);
struct Response(String);

struct Parse;

impl Inbound<String> for Parse {
    type Out = Request;

    async fn read(
        &mut self,
        _ctx: &mut rs_netty::InboundContext,
        msg: String,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(Request(msg)))
    }
}

struct Router;

impl Handler<Request> for Router {
    type Write = Response;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, req: Request) -> Result<()> {
        ctx.write(Response(format!("echo: {}", req.0))).await
    }
}

struct Render;

impl Outbound<Response> for Render {
    type Out = String;

    async fn write(
        &mut self,
        _ctx: &mut rs_netty::OutboundContext,
        msg: Response,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(msg.0))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    TcpServer::bind("127.0.0.1:9001")
        .pipeline(|| {
            pipeline()
                .codec(LineCodec::new())
                .inbound(Parse)
                .handler(Router)
                .outbound(Render)
        })
        .run()
        .await
}
```

## Compile-Time Constraints

The builder is a type-state pipeline:

```text
pipeline()
  .codec(...)
  .inbound(...)*
  .business(...)*
  .handler(...)
  .outbound(...)*
```

Methods only exist in valid states. Message transitions are checked with trait bounds, so a handler input must match the previous inbound output, outbound input must match `Handler::Write`, and the final outbound type must be encodable by the codec.

`Context<W>::write` and `Channel<W>::write` only accept `W`, so response types are checked at compile time too.

## Non-Goals

Non-goals for v0.1:

- No EventLoop API.
- No ByteBuf refCnt API.
- No dynamic Box<dyn Handler> main path.
- No TLS yet.
- No UDP yet.
- No codec registry yet.
