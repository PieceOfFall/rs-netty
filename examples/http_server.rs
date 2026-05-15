use rs_netty::{
    codec::{HttpCodec, HttpRequest, HttpResponse},
    pipeline, Context, Handler, Result, TcpServer,
};

#[tokio::main]
async fn main() -> Result<()> {
    TcpServer::bind("127.0.0.1:9005")
        .pipeline(|| {
            pipeline()
                .codec(HttpCodec::server().allow_chunked(true))
                .handler(HttpHello)
        })
        .run()
        .await
}

struct HttpHello;

impl Handler<HttpRequest> for HttpHello {
    type Write = HttpResponse;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, request: HttpRequest) -> Result<()> {
        let body = format!(
            "hello from rs-netty http\nmethod={}\ntarget={}\nbody_len={}\n",
            request.method(),
            request.target(),
            request.body().len()
        );

        ctx.write(
            HttpResponse::new(200)
                .header("Content-Type", "text/plain; charset=utf-8")
                .body(body),
        )
        .await
    }
}
