use rs_netty::{codec::Utf8DatagramCodec, datagram_pipeline, Flow, Outbound, Result};

struct Render;

impl Outbound<String> for Render {
    type Out = String;

    async fn write(
        &mut self,
        _ctx: &mut rs_netty::OutboundContext,
        msg: String,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(msg))
    }
}

fn main() {
    let _ = datagram_pipeline()
        .codec(Utf8DatagramCodec)
        .outbound(Render);
}
