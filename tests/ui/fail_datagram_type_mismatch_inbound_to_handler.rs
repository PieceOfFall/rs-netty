use rs_netty::{
    codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Flow, Inbound,
    Result,
};

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

impl DatagramHandler<String> for EchoString {
    type Write = String;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, msg: String) -> Result<()> {
        ctx.write(msg).await
    }
}

fn main() {
    let _ = datagram_pipeline()
        .codec(Utf8DatagramCodec)
        .inbound(Parse)
        .handler(EchoString);
}
