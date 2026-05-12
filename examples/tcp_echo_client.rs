use rs_netty::{codec::LineCodec, pipeline, Context, Handler, Result, TcpClient};

#[tokio::main]
async fn main() -> Result<()> {
    let client = TcpClient::connect("127.0.0.1:9000")
        .pipeline(|| pipeline().codec(LineCodec::new()).handler(PrintResponse))
        .run()
        .await?;

    client.write("hello".to_string()).await?;
    client.write("world".to_string()).await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    client.close().await?;
    client.wait().await
}

struct PrintResponse;

impl Handler<String> for PrintResponse {
    type Write = String;

    async fn read(&mut self, _ctx: &mut Context<Self::Write>, msg: String) -> Result<()> {
        println!("server -> {msg}");
        Ok(())
    }
}
