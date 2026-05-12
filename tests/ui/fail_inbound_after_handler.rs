use rs_netty::{codec::LineCodec, pipeline, Context, Flow, Handler, Inbound, Result};

struct Echo;

impl Handler<String> for Echo {
    type Write = String;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
        ctx.write(msg).await
    }
}

struct Logging;

impl Inbound<String> for Logging {
    type Out = String;

    async fn read(
        &mut self,
        _ctx: &mut rs_netty::InboundContext,
        msg: String,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(msg))
    }
}

fn main() {
    let _ = pipeline().codec(LineCodec::new()).handler(Echo).inbound(Logging);
}
