use rs_netty::{
    codec::Utf8DatagramCodec, datagram_pipeline, DatagramContext, DatagramHandler, Result,
    UdpClient,
};

struct PrintReply;

impl DatagramHandler<String> for PrintReply {
    type Write = String;

    async fn read(&mut self, _ctx: &mut DatagramContext<Self::Write>, _msg: String) -> Result<()> {
        Ok(())
    }
}

fn main() {
    let _client = UdpClient::connect("127.0.0.1:0").pipeline(|| {
        datagram_pipeline()
            .codec(Utf8DatagramCodec)
            .handler(PrintReply)
    });
}
