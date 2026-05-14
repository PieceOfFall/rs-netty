use rs_netty::{codec::Utf8DatagramCodec, datagram_pipeline, handler, Result, UdpServer};

#[tokio::main]
async fn main() -> Result<()> {
    UdpServer::bind("127.0.0.1:9002")
        .pipeline(|| {
            datagram_pipeline()
                .codec(Utf8DatagramCodec)
                .handler(UdpEcho)
        })
        .run()
        .await
}

struct UdpEcho;

#[handler(UdpEcho)]
async fn udp_echo(msg: String) -> Result<String> {
    Ok(format!("echo: {msg}"))
}
