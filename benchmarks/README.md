# Benchmarks

This directory contains comparable benchmark harnesses for:

- `rs-netty`
- bare `tokio`
- Java `netty`

The wire protocols are aligned across implementations:

- `line`: TCP line echo, `payload + "\n"`
- `len`: TCP length-field echo, `u32be length + payload`
- `udp`: UDP datagram echo

## Run all benchmarks

```bash
python3 benchmarks/run.py \
  --impls rs-netty tokio netty \
  --protocols line len udp \
  --connections 100 \
  --messages 1000000 \
  --payload 128 \
  --in-flight 16 \
  --output-dir benchmarks/results
```

By default, the runner auto-detects a non-loopback local IPv4 address and uses
that NIC address for both server bind and client connect. To pin a specific
interface, pass:

```bash
python3 benchmarks/run.py --host 192.168.1.20
```

The runner refuses `localhost`, `127.0.0.1`, and `::1` because those use the
loopback path rather than the network interface.

The runner builds all implementations, starts each server, samples server RSS,
runs the matching client, and writes:

```text
benchmarks/results/results.csv
benchmarks/results/*.log
```

CSV metrics include:

- `throughput_msg_sec`
- `server_max_rss_kb`
- `server_avg_rss_kb`
- `p50_us`
- `p90_us`
- `p99_us`
- `p999_us`

## Quick smoke run

```bash
python3 benchmarks/run.py \
  --impls rs-netty tokio netty \
  --protocols len \
  --connections 2 \
  --messages 100 \
  --payload 32 \
  --in-flight 4
```

## Notes

- Use release builds for Rust; the runner does this automatically.
- Netty is launched with `java -cp target/classes:target/dependency/*` instead
  of `mvn exec:java`, so server memory sampling is not inflated by Maven.
- UDP currently sends one request at a time per logical client socket.
