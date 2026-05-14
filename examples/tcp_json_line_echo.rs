use std::sync::{Arc, Mutex};

use rs_netty::{
    codec::{JsonDecode, JsonEncode, LineCodec},
    pipeline, Context, Handler, Result, TcpClient, TcpServer,
};
use tokio::sync::oneshot;

const DEFAULT_ADDR: &str = "127.0.0.1:9003";
const ADDR_ENV: &str = "RS_NETTY_TCP_JSON_ADDR";

#[derive(serde::Deserialize, serde::Serialize)]
struct Request {
    message: String,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct Response {
    echoed: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::args().nth(1).as_deref() == Some("client") {
        return run_client().await;
    }

    run_server().await
}

async fn run_server() -> Result<()> {
    TcpServer::bind(json_echo_addr())
        .pipeline(move || {
            pipeline()
                .codec(LineCodec::new())
                .inbound(JsonDecode::<Request>::new())
                .handler(EchoJson)
                .outbound(JsonEncode::<Response>::new())
        })
        .run()
        .await
}

async fn run_client() -> Result<()> {
    let addr = json_echo_addr();
    let (tx, rx) = oneshot::channel();
    let response_tx = Arc::new(Mutex::new(Some(tx)));

    let client = TcpClient::connect(addr)
        .pipeline(move || {
            let response_tx = response_tx.clone();

            pipeline()
                .codec(LineCodec::new())
                .inbound(JsonDecode::<Response>::new())
                .handler(PrintResponse { response_tx })
                .outbound(JsonEncode::<Request>::new())
        })
        .run()
        .await?;

    client
        .write_and_flush(Request {
            message: "hello json".to_string(),
        })
        .await?;

    let _ = rx.await;
    client.close().await?;
    client.wait().await
}

fn json_echo_addr() -> String {
    std::env::var(ADDR_ENV).unwrap_or_else(|_| DEFAULT_ADDR.to_string())
}

struct EchoJson;

impl Handler<Request> for EchoJson {
    type Write = Response;

    async fn read(&mut self, ctx: &mut Context<Self::Write>, req: Request) -> Result<()> {
        ctx.write(Response {
            echoed: req.message,
        })
        .await
    }
}

struct PrintResponse {
    response_tx: Arc<Mutex<Option<oneshot::Sender<()>>>>,
}

impl Handler<Response> for PrintResponse {
    type Write = Request;

    async fn read(&mut self, _ctx: &mut Context<Self::Write>, res: Response) -> Result<()> {
        println!("server -> {}", res.echoed);
        if let Some(tx) = self
            .response_tx
            .lock()
            .expect("response sender lock poisoned")
            .take()
        {
            let _ = tx.send(());
        }
        Ok(())
    }
}
