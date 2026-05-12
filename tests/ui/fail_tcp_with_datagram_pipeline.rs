use rs_netty::{
    codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Result,
    TcpServer,
};

struct Echo;

impl DatagramHandler<String> for Echo {
    type Write = String;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: String) -> Result<()> {
        ctx.write(msg).await
    }
}

fn main() {
    let _server = TcpServer::bind("127.0.0.1:0")
        .pipeline(|| datagram_pipeline().codec(Utf8DatagramCodec).handler(Echo));
}
