use rs_netty::{codec::LineCodec, pipeline, Context, Flow, Handler, Outbound, Result};

struct Response(String);
struct Other(String);

struct Router;

impl Handler<String> for Router {
    type Write = Response;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
        ctx.write(Response(msg)).await
    }
}

struct RenderOther;

impl Outbound<Other> for RenderOther {
    type Out = String;

    async fn write(
        &mut self,
        _ctx: &mut rs_netty::OutboundContext,
        msg: Other,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(msg.0))
    }
}

fn main() {
    let _ = pipeline()
        .codec(LineCodec::new())
        .handler(Router)
        .outbound(RenderOther);
}
