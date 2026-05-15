use rs_netty::{
    codec::{WebSocketCodec, WebSocketInbound, WebSocketOutbound},
    pipeline, Context, Handler, Result, TcpServer,
};

#[tokio::main]
async fn main() -> Result<()> {
    TcpServer::bind("127.0.0.1:9006")
        .pipeline(|| {
            pipeline()
                .codec(WebSocketCodec::server())
                .handler(WebSocketEcho)
        })
        .run()
        .await
}

struct WebSocketEcho;

impl Handler<WebSocketInbound> for WebSocketEcho {
    type Write = WebSocketOutbound;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: WebSocketInbound) -> Result<()> {
        let response = match msg {
            WebSocketInbound::Handshake(handshake) => {
                WebSocketOutbound::from(handshake.accept_response())
            }
            WebSocketInbound::Text(text) => WebSocketOutbound::Text(format!("echo: {text}")),
            WebSocketInbound::Binary(bytes) => WebSocketOutbound::Binary(bytes),
            WebSocketInbound::Ping(bytes) => WebSocketOutbound::Pong(bytes),
            WebSocketInbound::Pong(bytes) => WebSocketOutbound::Pong(bytes),
            WebSocketInbound::Close(close) => WebSocketOutbound::Close(close),
        };

        ctx.write(response).await
    }
}
