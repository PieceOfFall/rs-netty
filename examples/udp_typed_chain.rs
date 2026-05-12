use rs_netty::{
    codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Flow, Inbound,
    Outbound, Result, UdpServer,
};

#[tokio::main]
async fn main() -> Result<()> {
    UdpServer::bind("127.0.0.1:9003")
        .pipeline(|| {
            datagram_pipeline()
                .codec(Utf8DatagramCodec)
                .inbound(Trim)
                .inbound(ParseRequest)
                .handler(Route)
                .outbound(RenderResponse)
        })
        .run()
        .await
}

struct Trim;

impl Inbound<String> for Trim {
    type Out = String;

    async fn read(
        &mut self,
        _ctx: &mut rs_netty::InboundContext,
        msg: String,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(msg.trim().to_string()))
    }
}

struct Request(String);
struct Response(String);

struct ParseRequest;

impl Inbound<String> for ParseRequest {
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
        ctx.write(Response(format!("response: {}", req.0))).await
    }
}

struct RenderResponse;

impl Outbound<Response> for RenderResponse {
    type Out = String;

    async fn write(
        &mut self,
        _ctx: &mut rs_netty::OutboundContext,
        msg: Response,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(msg.0))
    }
}
