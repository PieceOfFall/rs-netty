use rs_netty::{codec::LineCodec, handler, pipeline, Result, TcpServer};

struct Echo;

#[handler(Echo)]
async fn echo(msg: String) -> Result<String> {
    Ok(msg)
}

fn main() {
    let _server = TcpServer::bind("127.0.0.1:0").pipeline(|| {
        let pipeline = pipeline()
            .codec(LineCodec::new())
            .handler(Echo);
        pipeline
    });
}
