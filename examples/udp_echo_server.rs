use rs_netty::{
    codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Result,
    UdpServer,
};

#[tokio::main]
async fn main() -> Result<()> {
    UdpServer::bind("127.0.0.1:9002")
        .pipeline(|| {
            datagram_pipeline()
                .codec(Utf8DatagramCodec)
                .handler(UdpEcho)
        })
        .run()
        .await
}

struct UdpEcho;

impl DatagramHandler<String> for UdpEcho {
    type Write = String;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: String) -> Result<()> {
        ctx.write(format!("echo: {msg}")).await
    }
}
