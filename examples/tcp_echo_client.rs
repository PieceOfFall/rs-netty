use rs_netty::{codec::LineCodec, handler, pipeline, Result, TcpClient};

#[tokio::main]
async fn main() -> Result<()> {
    let client = TcpClient::connect("127.0.0.1:9000")
        .pipeline(|| {
            let pipeline = pipeline().codec(LineCodec::new()).handler(PrintResponse);
            pipeline
        })
        .run()
        .await?;

    client.write("hello".to_string()).await?;
    client.write("world".to_string()).await?;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    client.close().await?;
    client.wait().await
}

struct PrintResponse;

#[handler(PrintResponse, write = String)]
async fn print_response(msg: String) -> Result<()> {
    println!("server -> {msg}");
    Ok(())
}
