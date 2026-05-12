use rs_netty::{codec::LineCodec, pipeline, Context, Handler, Result};

struct Echo;

impl Handler<String> for Echo {
    type Write = String;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
        ctx.write(msg).await
    }
}

fn main() {
    let _ = pipeline().codec(LineCodec::new()).handler(Echo).handler(Echo);
}
