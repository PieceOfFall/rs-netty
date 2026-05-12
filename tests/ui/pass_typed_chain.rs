use rs_netty::{
    codec::LineCodec, pipeline, Context, Flow, Handler, Inbound, Outbound, Result, TcpServer,
};

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

struct Router;

impl Handler<Request> for Router {
    type Write = Response;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, req: Request) -> Result<()> {
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
    let _server = TcpServer::bind("127.0.0.1:0").pipeline(|| {
        pipeline()
            .codec(LineCodec::new())
            .inbound(Trim)
            .inbound(Parse)
            .handler(Router)
            .outbound(Render)
    });
}
