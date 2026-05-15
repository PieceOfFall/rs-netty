use rs_netty::{
    codec::{
        HttpRequest, HttpResponse, HttpService, HttpWsCodec, HttpWsRouter, WebSocketHandshake,
        WebSocketHandshakeResponse, WebSocketMessage, WebSocketService,
    },
    pipeline, Result, TcpServer,
};

fn main() {
    let _server = TcpServer::bind("127.0.0.1:0").pipeline(|| {
        pipeline()
            .codec(HttpWsCodec::server())
            .handler(HttpWsRouter::new(HttpApp, WsApp))
    });
}

struct HttpApp;

impl HttpService for HttpApp {
    async fn call(&mut self, request: HttpRequest) -> Result<HttpResponse> {
        Ok(HttpResponse::new(200).body(format!("path={}", request.target())))
    }
}

struct WsApp;

impl WebSocketService for WsApp {
    async fn open(
        &mut self,
        handshake: WebSocketHandshake,
    ) -> Result<WebSocketHandshakeResponse> {
        Ok(handshake.accept_response())
    }

    async fn message(&mut self, message: WebSocketMessage) -> Result<Option<WebSocketMessage>> {
        Ok(Some(message))
    }
}
