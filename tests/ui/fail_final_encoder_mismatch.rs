use rs_netty::{codec::LineCodec, pipeline, Context, Handler, Result, TcpServer};

struct Response(String);

struct Router;

impl Handler<String> for Router {
    type Write = Response;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
        ctx.write(Response(msg)).await
    }
}

fn main() {
    let _server =
        TcpServer::bind("127.0.0.1:0").pipeline(|| pipeline().codec(LineCodec::new()).handler(Router));
}
