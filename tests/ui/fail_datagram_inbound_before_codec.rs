use rs_netty::{datagram_pipeline, Flow, Inbound, Result};

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
    let _ = datagram_pipeline().inbound(Logging);
}
