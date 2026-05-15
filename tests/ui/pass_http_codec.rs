use rs_netty::{
    codec::{HttpCodec, HttpRequest, HttpResponse},
    pipeline, Context, Handler, Result, TcpServer,
};

fn main() {
    let _server = TcpServer::bind("127.0.0.1:0").pipeline(|| {
        pipeline()
            .codec(HttpCodec::server())
            .handler(HttpHandler)
    });
}

struct HttpHandler;

impl Handler<HttpRequest> for HttpHandler {
    type Write = HttpResponse;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, request: HttpRequest) -> Result<()> {
        ctx.write(HttpResponse::new(200).body(format!("path={}", request.target())))
            .await
    }
}
