use rs_netty::{codec::LineCodec, handler, pipeline, Result, TcpServer};

#[tokio::main]
async fn main() -> Result<()> {
    TcpServer::bind("127.0.0.1:9000")
        .pipeline(|| {
            let pipeline = pipeline().codec(LineCodec::new()).handler(Echo);
            pipeline
        })
        .run()
        .await
}

struct Echo;

#[handler(Echo)]
async fn echo(msg: String) -> Result<String> {
    Ok(msg)
}
