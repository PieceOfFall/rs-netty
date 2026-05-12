use rs_netty::{
    codec::LineCodec, pipeline, Context, Flow, Handler, Inbound, Outbound, Result, TcpServer,
};

#[tokio::main]
async fn main() -> Result<()> {
    TcpServer::bind("127.0.0.1:9001")
        .pipeline(|| {
            pipeline()
                .codec(LineCodec::new())
                .inbound(Trim)
                .inbound(ParseRequest)
                .handler(Router)
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

struct Request {
    body: String,
}

struct Response {
    body: String,
}

struct ParseRequest;

impl Inbound<String> for ParseRequest {
    type Out = Request;

    async fn read(
        &mut self,
        _ctx: &mut rs_netty::InboundContext,
        msg: String,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(Request { body: msg }))
    }
}

struct Router;

impl Handler<Request> for Router {
    type Write = Response;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, req: Request) -> Result<()> {
        ctx.write(Response {
            body: format!("response: {}", req.body),
        })
        .await
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
        Ok(Flow::Next(msg.body))
    }
}
