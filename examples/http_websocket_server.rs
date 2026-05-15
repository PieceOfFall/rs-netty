use rs_netty::{
    codec::{
        HttpRequest, HttpResponse, HttpService, HttpWsCodec, HttpWsRouter, WebSocketHandshake,
        WebSocketHandshakeResponse, WebSocketMessage, WebSocketService,
    },
    pipeline, Result, TcpServer,
};

#[tokio::main]
async fn main() -> Result<()> {
    TcpServer::bind("127.0.0.1:9007")
        .pipeline(|| {
            pipeline()
                .codec(HttpWsCodec::server().allow_http_chunked(true))
                .handler(HttpWsRouter::new(HttpApp, WsApp))
        })
        .run()
        .await
}

struct HttpApp;

impl HttpService for HttpApp {
    async fn call(&mut self, request: HttpRequest) -> Result<HttpResponse> {
        let body = format!(
            "hello from shared http/ws port\nmethod={}\ntarget={}\n",
            request.method(),
            request.target()
        );

        Ok(HttpResponse::new(200)
            .header("Content-Type", "text/plain; charset=utf-8")
            .body(body))
    }
}

struct WsApp;

impl WebSocketService for WsApp {
    async fn open(&mut self, handshake: WebSocketHandshake) -> Result<WebSocketHandshakeResponse> {
        Ok(handshake.accept_response())
    }

    async fn message(&mut self, message: WebSocketMessage) -> Result<Option<WebSocketMessage>> {
        let response = match message {
            WebSocketMessage::Text(text) => WebSocketMessage::Text(format!("echo: {text}")),
            WebSocketMessage::Binary(bytes) => WebSocketMessage::Binary(bytes),
            WebSocketMessage::Ping(bytes) => WebSocketMessage::Pong(bytes),
            WebSocketMessage::Pong(bytes) => WebSocketMessage::Pong(bytes),
            WebSocketMessage::Close(close) => WebSocketMessage::Close(close),
        };

        Ok(Some(response))
    }
}
