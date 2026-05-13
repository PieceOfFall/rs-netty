use rs_netty::{
    codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Flow, Inbound,
    Outbound, Result, UdpClient,
};

#[tokio::main]
async fn main() -> Result<()> {
    let client = UdpClient::connect("127.0.0.1:9003")
        .pipeline(|| {
            datagram_pipeline()
                .codec(Utf8DatagramCodec)
                .inbound(ParseResponse)
                .handler(PrintResponse)
                .outbound(RenderRequest)
        })
        .run()
        .await?;

    client.write(Request("hello typed udp".to_string())).await?;
    client.write(Request("second datagram".to_string())).await?;

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    client.close().await?;
    client.wait().await
}

struct Request(String);
struct Response(String);

struct ParseResponse;

impl Inbound<String> for ParseResponse {
    type Out = Response;

    async fn read(
        &mut self,
        _ctx: &mut rs_netty::InboundContext,
        msg: String,
    ) -> Result<Flow<Self::Out>> {
        Ok(Flow::Next(Response(msg)))
    }
}

struct PrintResponse;

impl DatagramHandler<Response> for PrintResponse {
    type Write = Request;

    async fn read(&mut self, _ctx: &mut DatagramContext<Self::Write>, msg: Response) -> Result<()> {
        println!("udp typed server -> {}", msg.0);
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
        Ok(Flow::Next(msg.0))
    }
}
