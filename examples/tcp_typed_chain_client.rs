use rs_netty::{
    codec::LineCodec, pipeline, Context, Flow, Handler, Inbound, Outbound, Result, TcpClient,
};

#[tokio::main]
async fn main() -> Result<()> {
    let client = TcpClient::connect("127.0.0.1:9001")
        .pipeline(|| {
            pipeline()
                .codec(LineCodec::new())
                .inbound(ParseResponse)
                .handler(PrintResponse)
                .outbound(RenderRequest)
        })
        .run()
        .await?;

    client
        .write(Request {
            body: "hello typed tcp".to_string(),
        })
        .await?;
    client
        .write(Request {
            body: "second message".to_string(),
        })
        .await?;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    client.close().await?;
    client.wait().await
}

struct Request {
    body: String,
}

struct Response {
    body: String,
}

struct ParseResponse;

impl Inbound<String> for ParseResponse {
    type Out = Response;

    async fn read(
        &mut self,
        _ctx: &mut rs_netty::InboundContext,
        msg: String,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(Response { body: msg }))
    }
}

struct PrintResponse;

impl Handler<Response> for PrintResponse {
    type Write = Request;

    async fn read(&mut self, _ctx: &mut Context<Self::Write>, msg: Response) -> Result<()> {
        println!("tcp typed server -> {}", msg.body);
        Ok(())
    }
}

struct RenderRequest;

impl Outbound<Request> for RenderRequest {
    type Out = String;

    async fn write(
        &mut self,
        _ctx: &mut rs_netty::OutboundContext,
        msg: Request,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(msg.body))
    }
}
