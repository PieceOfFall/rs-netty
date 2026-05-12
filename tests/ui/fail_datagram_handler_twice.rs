use rs_netty::{codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Result};

struct Echo;

impl DatagramHandler<String> for Echo {
    type Write = String;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: String) -> Result<()> {
        ctx.write(msg).await
    }
}

fn main() {
    let _ = datagram_pipeline()
        .codec(Utf8DatagramCodec)
        .handler(Echo)
        .handler(Echo);
}
