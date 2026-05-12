use rs_netty::{datagram_pipeline, DatagramContext, DatagramHandler, Result};

struct Echo;

impl DatagramHandler<String> for Echo {
    type Write = String;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: String) -> Result<()> {
        ctx.write(msg).await
    }
}

fn main() {
    let _ = datagram_pipeline().handler(Echo);
}
