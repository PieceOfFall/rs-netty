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
