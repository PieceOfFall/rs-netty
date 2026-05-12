use rs_netty::{
    codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Result,
    UdpClient,
};

#[tokio::main]
async fn main() -> Result<()> {
    let client = UdpClient::connect("127.0.0.1:9002")
        .pipeline(|| {
            datagram_pipeline()
                .codec(Utf8DatagramCodec)
                .handler(PrintReply)
        })
        .run()
        .await?;

    client.write("hello".to_string()).await?;
    client.write("world".to_string()).await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    client.close().await?;
    client.wait().await
}

struct PrintReply;

impl DatagramHandler<String> for PrintReply {
    type Write = String;

    async fn read(&mut self, _ctx: &mut DatagramContext<Self::Write>, msg: String) -> Result<()> {
        println!("udp server -> {msg}");
        Ok(())
    }
}
