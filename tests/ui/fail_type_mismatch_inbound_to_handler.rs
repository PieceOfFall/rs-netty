use rs_netty::{codec::LineCodec, pipeline, Context, Flow, Handler, Inbound, Result};

struct Request(String);

struct Parse;

impl Inbound<String> for Parse {
    type Out = Request;

    async fn read(
        &mut self,
        _ctx: &mut rs_netty::InboundContext,
        msg: String,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(Request(msg)))
    }
}

struct EchoString;

impl Handler<String> for EchoString {
    type Write = String;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
        ctx.write(msg).await
    }
}

fn main() {
    let _ = pipeline()
        .codec(LineCodec::new())
        .inbound(Parse)
        .handler(EchoString);
}
