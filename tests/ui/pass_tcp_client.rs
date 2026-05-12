use rs_netty::{codec::LineCodec, pipeline, Context, Handler, Result, TcpClient};

struct PrintResponse;

impl Handler<String> for PrintResponse {
    type Write = String;

    async fn read(&mut self, _ctx: &mut Context<Self::Write>, _msg: String) -> Result<()> {
        Ok(())
    }
}

fn main() {
    let _client = TcpClient::connect("127.0.0.1:0")
        .pipeline(|| pipeline().codec(LineCodec::new()).handler(PrintResponse));
}
