use rs_netty::{codec::LineCodec, pipeline, Context, Handler, Result, TcpClient};

struct PrintResponse {
    done: Option<tokio::sync::oneshot::Sender<()>>,
}

impl Handler<String> for PrintResponse {
    type Write = String;

    async fn read(&mut self, _ctx: &mut Context<Self::Write>, _msg: String) -> Result<()> {
        if let Some(done) = self.done.take() {
            let _ = done.send(());
        }
        Ok(())
    }
}

fn main() {
    let (done, _wait) = tokio::sync::oneshot::channel();

    let _client = TcpClient::connect("127.0.0.1:0").pipeline_instance(
        pipeline()
            .codec(LineCodec::new())
            .handler(PrintResponse { done: Some(done) }),
    );
}
