use rs_netty::{
    codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Flow, Outbound,
    Result,
};

struct Response(String);
struct Other(String);

struct Route;

impl DatagramHandler<String> for Route {
    type Write = Response;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: String) -> Result<()> {
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
    let _ = datagram_pipeline()
        .codec(Utf8DatagramCodec)
        .handler(Route)
        .outbound(RenderOther);
}
