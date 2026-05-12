use rs_netty::{
    codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Flow, Inbound,
    Outbound, Result, UdpServer,
};

struct Request(String);
struct Response(String);

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

struct Route;

impl DatagramHandler<Request> for Route {
    type Write = Response;

    async fn read(&mut self, ctx: &mut DatagramContext<Self::Write>, req: Request) -> Result<()> {
        ctx.write(Response(req.0)).await
    }
}

struct Render;

impl Outbound<Response> for Render {
    type Out = String;

    async fn write(
        &mut self,
        _ctx: &mut rs_netty::OutboundContext,
        msg: Response,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(msg.0))
    }
}

fn main() {
    let _server = UdpServer::bind("127.0.0.1:0").pipeline(|| {
        datagram_pipeline()
            .codec(Utf8DatagramCodec)
            .inbound(Parse)
            .handler(Route)
            .outbound(Render)
    });
}
