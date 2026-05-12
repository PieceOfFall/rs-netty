use rs_netty::{
    codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Result,
    UdpServer,
};

struct Response(String);

struct Route;

impl DatagramHandler<String> for Route {
    type Write = Response;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: String) -> Result<()> {
        ctx.write(Response(msg)).await
    }
}

fn main() {
    let _server = UdpServer::bind("127.0.0.1:0")
        .pipeline(|| datagram_pipeline().codec(Utf8DatagramCodec).handler(Route));
}
