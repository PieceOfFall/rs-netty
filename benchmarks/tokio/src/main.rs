use std::{
    collections::VecDeque,
    env,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use bytes::BufMut;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::Barrier,
    time,
};

type BenchResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> BenchResult<()> {
    let args = Args::parse()?;

    match args.mode.as_str() {
        "server-tokio-line" => server_tokio_line(args.addr).await?,
        "server-tokio-len" => server_tokio_len(args.addr).await?,
        "server-tokio-udp" => server_tokio_udp(args.addr).await?,
        "client-line" => client_tcp_line(args).await?,
        "client-len" => client_tcp_len(args).await?,
        "client-udp" => client_udp(args).await?,
        _ => {
            eprintln!("{}", usage());
            return Err(format!("unknown mode: {}", args.mode).into());
        }
    }

    Ok(())
}

#[derive(Clone)]
struct Args {
    mode: String,
    addr: String,
    connections: usize,
    messages: usize,
    payload: usize,
    in_flight: usize,
}

impl Args {
    fn parse() -> BenchResult<Self> {
        let mut args = env::args().skip(1);
        let mode = args.next().ok_or_else(usage)?;

        let mut parsed = Self {
            mode,
            addr: "localhost:9000".to_string(),
            connections: 1,
            messages: 100_000,
            payload: 128,
            in_flight: 1,
        };

        while let Some(flag) = args.next() {
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {flag}"))?;
            match flag.as_str() {
                "--addr" => parsed.addr = value,
                "--connections" => parsed.connections = value.parse()?,
                "--messages" => parsed.messages = value.parse()?,
                "--payload" => parsed.payload = value.parse()?,
                "--in-flight" => parsed.in_flight = value.parse()?,
                _ => return Err(format!("unknown flag: {flag}").into()),
            }
        }

        if parsed.connections == 0 {
            return Err("--connections must be greater than zero".into());
        }
        if parsed.messages == 0 {
            return Err("--messages must be greater than zero".into());
        }
        if parsed.payload == 0 {
            return Err("--payload must be greater than zero".into());
        }
        if parsed.in_flight == 0 {
            return Err("--in-flight must be greater than zero".into());
        }

        Ok(parsed)
    }
}

fn usage() -> String {
    "usage:
  tokio-bench server-tokio-line --addr localhost:9000
  tokio-bench server-tokio-len  --addr localhost:9000
  tokio-bench server-tokio-udp  --addr localhost:9001

  tokio-bench client-line --addr localhost:9000 --connections 100 --messages 100000 --payload 128
  tokio-bench client-len  --addr localhost:9000 --connections 100 --messages 100000 --payload 128
  tokio-bench client-udp  --addr localhost:9001 --connections 100 --messages 100000 --payload 128"
        .to_string()
}

async fn server_tokio_line(addr: String) -> BenchResult<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(err) = handle_tokio_line(stream).await {
                eprintln!("tokio line connection error: {err}");
            }
        });
    }
}

async fn handle_tokio_line(stream: TcpStream) -> BenchResult<()> {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();

    loop {
        line.clear();
        let read = reader.read_line(&mut line).await?;
        if read == 0 {
            return Ok(());
        }
        writer.write_all(line.as_bytes()).await?;
    }
}

async fn server_tokio_len(addr: String) -> BenchResult<()> {
    let listener = TcpListener::bind(addr).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(err) = handle_tokio_len(stream).await {
                eprintln!("tokio length connection error: {err}");
            }
        });
    }
}

async fn handle_tokio_len(mut stream: TcpStream) -> BenchResult<()> {
    let mut len_buf = [0_u8; 4];
    loop {
        if stream.read_exact(&mut len_buf).await.is_err() {
            return Ok(());
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut payload = vec![0_u8; len];
        stream.read_exact(&mut payload).await?;
        stream.write_all(&len_buf).await?;
        stream.write_all(&payload).await?;
    }
}

async fn server_tokio_udp(addr: String) -> BenchResult<()> {
    let socket = UdpSocket::bind(addr).await?;
    let mut buf = vec![0_u8; 65_535];
    loop {
        let (len, peer) = socket.recv_from(&mut buf).await?;
        socket.send_to(&buf[..len], peer).await?;
    }
}

async fn client_tcp_line(args: Args) -> BenchResult<()> {
    run_tcp_clients(args, TcpProtocol::Line).await
}

async fn client_tcp_len(args: Args) -> BenchResult<()> {
    run_tcp_clients(args, TcpProtocol::Length).await
}

enum TcpProtocol {
    Line,
    Length,
}

async fn run_tcp_clients(args: Args, protocol: TcpProtocol) -> BenchResult<()> {
    let total = args.messages;
    let done = Arc::new(AtomicU64::new(0));
    let latencies = Latencies::new(total);
    let barrier = Arc::new(Barrier::new(args.connections + 1));
    let payload = make_payload(args.payload);

    for index in 0..args.connections {
        let messages = messages_for_worker(total, args.connections, index);
        let addr = args.addr.clone();
        let done = done.clone();
        let latencies = latencies.clone();
        let barrier = barrier.clone();
        let payload = payload.clone();
        let in_flight = args.in_flight;
        let protocol = match protocol {
            TcpProtocol::Line => TcpProtocol::Line,
            TcpProtocol::Length => TcpProtocol::Length,
        };

        tokio::spawn(async move {
            barrier.wait().await;
            let result = match protocol {
                TcpProtocol::Line => {
                    tcp_line_worker(
                        &addr,
                        messages,
                        in_flight,
                        &payload,
                        done.clone(),
                        latencies.clone(),
                    )
                    .await
                }
                TcpProtocol::Length => {
                    tcp_len_worker(
                        &addr,
                        messages,
                        in_flight,
                        &payload,
                        done.clone(),
                        latencies.clone(),
                    )
                    .await
                }
            };
            if let Err(err) = result {
                eprintln!("client worker error: {err}");
            }
        });
    }

    barrier.wait().await;
    let start = Instant::now();
    report_until_done("tcp", total, done, start).await;
    print_result("tcp", total, start, &latencies);
    Ok(())
}

async fn tcp_line_worker(
    addr: &str,
    messages: usize,
    in_flight: usize,
    payload: &[u8],
    done: Arc<AtomicU64>,
    latencies: Latencies,
) -> BenchResult<()> {
    let stream = TcpStream::connect(addr).await?;
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut line_payload = Vec::with_capacity(payload.len() + 1);
    line_payload.extend_from_slice(payload);
    line_payload.push(b'\n');

    let mut sent = 0;
    let mut received = 0;
    let mut sent_at = VecDeque::with_capacity(in_flight);
    let mut line = String::new();

    while received < messages {
        while sent < messages && sent.saturating_sub(received) < in_flight {
            sent_at.push_back(Instant::now());
            writer.write_all(&line_payload).await?;
            sent += 1;
        }

        line.clear();
        let read = reader.read_line(&mut line).await?;
        if read == 0 {
            return Err("server closed TCP line connection".into());
        }
        if let Some(started) = sent_at.pop_front() {
            latencies.record(started.elapsed());
        }
        received += 1;
        done.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}

async fn tcp_len_worker(
    addr: &str,
    messages: usize,
    in_flight: usize,
    payload: &[u8],
    done: Arc<AtomicU64>,
    latencies: Latencies,
) -> BenchResult<()> {
    let mut stream = TcpStream::connect(addr).await?;
    let mut frame = Vec::with_capacity(payload.len() + 4);
    frame.put_u32(payload.len() as u32);
    frame.extend_from_slice(payload);

    let mut sent = 0;
    let mut received = 0;
    let mut sent_at = VecDeque::with_capacity(in_flight);
    let mut len_buf = [0_u8; 4];

    while received < messages {
        while sent < messages && sent.saturating_sub(received) < in_flight {
            sent_at.push_back(Instant::now());
            stream.write_all(&frame).await?;
            sent += 1;
        }

        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0_u8; len];
        stream.read_exact(&mut body).await?;
        if let Some(started) = sent_at.pop_front() {
            latencies.record(started.elapsed());
        }
        received += 1;
        done.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}

async fn client_udp(args: Args) -> BenchResult<()> {
    let addr = resolve_addr(&args.addr).await?;
    let total = args.messages;
    let done = Arc::new(AtomicU64::new(0));
    let latencies = Latencies::new(total);
    let barrier = Arc::new(Barrier::new(args.connections + 1));
    let payload = make_payload(args.payload);

    for index in 0..args.connections {
        let messages = messages_for_worker(total, args.connections, index);
        let done = done.clone();
        let latencies = latencies.clone();
        let barrier = barrier.clone();
        let payload = payload.clone();

        tokio::spawn(async move {
            barrier.wait().await;
            if let Err(err) =
                udp_worker(addr, messages, &payload, done.clone(), latencies.clone()).await
            {
                eprintln!("udp worker error: {err}");
            }
        });
    }

    barrier.wait().await;
    let start = Instant::now();
    report_until_done("udp", total, done, start).await;
    print_result("udp", total, start, &latencies);
    Ok(())
}

async fn udp_worker(
    addr: SocketAddr,
    messages: usize,
    payload: &[u8],
    done: Arc<AtomicU64>,
    latencies: Latencies,
) -> BenchResult<()> {
    let socket = UdpSocket::bind(interface_bind_addr(addr)).await?;
    socket.connect(addr).await?;
    let mut buf = vec![0_u8; payload.len().max(1_500)];

    for _ in 0..messages {
        let started = Instant::now();
        socket.send(payload).await?;
        let len = socket.recv(&mut buf).await?;
        if len != payload.len() {
            return Err(format!("unexpected UDP echo length: {len}").into());
        }
        latencies.record(started.elapsed());
        done.fetch_add(1, Ordering::Relaxed);
    }

    Ok(())
}

async fn report_until_done(
    label: &str,
    requested_total: usize,
    done: Arc<AtomicU64>,
    start: Instant,
) {
    let mut last_count = 0;
    let mut last_time = start;
    let mut last_report = start;
    let total = requested_total as u64;

    loop {
        let now = Instant::now();
        let count = done.load(Ordering::Relaxed).min(total);
        let elapsed = now.duration_since(start).as_secs_f64();
        let interval = now.duration_since(last_time).as_secs_f64();

        if count >= total {
            break;
        }

        if now.duration_since(last_report) >= Duration::from_secs(1) {
            let current_rate = (count - last_count) as f64 / interval;
            let average_rate = count as f64 / elapsed;

            eprintln!(
                "{label}: done={count}/{total} current={current_rate:.0} msg/s average={average_rate:.0} msg/s elapsed={elapsed:.1}s"
            );

            last_count = count;
            last_time = now;
            last_report = now;
        }

        time::sleep(Duration::from_millis(10)).await;
    }
}

fn make_payload(size: usize) -> Vec<u8> {
    (0..size).map(|i| b'a' + (i % 26) as u8).collect()
}

fn messages_for_worker(total: usize, workers: usize, index: usize) -> usize {
    let base = total / workers;
    base + usize::from(index < total % workers)
}

async fn resolve_addr(addr: &str) -> BenchResult<SocketAddr> {
    tokio::net::lookup_host(addr)
        .await?
        .next()
        .ok_or_else(|| format!("could not resolve address: {addr}").into())
}

fn interface_bind_addr(remote: SocketAddr) -> SocketAddr {
    SocketAddr::new(remote.ip(), 0)
}

#[derive(Clone)]
struct Latencies {
    micros: Arc<Mutex<Vec<u64>>>,
}

impl Latencies {
    fn new(capacity: usize) -> Self {
        Self {
            micros: Arc::new(Mutex::new(Vec::with_capacity(capacity))),
        }
    }

    fn record(&self, duration: Duration) {
        let micros = duration.as_micros().min(u128::from(u64::MAX)) as u64;
        self.micros
            .lock()
            .expect("latency collection lock poisoned")
            .push(micros);
    }

    fn snapshot_sorted(&self) -> Vec<u64> {
        let mut values = self
            .micros
            .lock()
            .expect("latency collection lock poisoned")
            .clone();
        values.sort_unstable();
        values
    }
}

fn print_result(label: &str, total: usize, start: Instant, latencies: &Latencies) {
    let elapsed = start.elapsed().as_secs_f64();
    let throughput = total as f64 / elapsed;
    let values = latencies.snapshot_sorted();
    let p50 = percentile(&values, 50.0);
    let p90 = percentile(&values, 90.0);
    let p99 = percentile(&values, 99.0);
    let p999 = percentile(&values, 99.9);

    println!(
        "RESULT protocol={label} messages={total} elapsed_sec={elapsed:.6} throughput_msg_sec={throughput:.2} latency_count={} p50_us={p50} p90_us={p90} p99_us={p99} p999_us={p999}",
        values.len()
    );
}

fn percentile(sorted: &[u64], percentile: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }

    let rank = ((percentile / 100.0) * (sorted.len().saturating_sub(1) as f64)).ceil() as usize;
    sorted[rank.min(sorted.len() - 1)]
}
