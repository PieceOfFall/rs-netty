# Tokio benchmark harness

This crate provides bare Tokio benchmark servers and clients for the same wire
protocols used by `benchmarks/rs-netty` and `benchmarks/netty`:

- TCP line echo
- TCP length-field echo
- UDP datagram echo

Run commands from the repository root. Replace `<NIC_IP>` with a non-loopback
address on your local network interface.

## Build

```bash
cargo build --manifest-path benchmarks/tokio/Cargo.toml --release --offline
```

## TCP line echo

Terminal 1:

```bash
cargo run --manifest-path benchmarks/tokio/Cargo.toml --release -- server-tokio-line --addr <NIC_IP>:9000
```

Terminal 2:

```bash
cargo run --manifest-path benchmarks/tokio/Cargo.toml --release -- client-line --addr <NIC_IP>:9000 --connections 100 --messages 1000000 --payload 128 --in-flight 1
```

## TCP length-field echo

Terminal 1:

```bash
cargo run --manifest-path benchmarks/tokio/Cargo.toml --release -- server-tokio-len --addr <NIC_IP>:9000
```

Terminal 2:

```bash
cargo run --manifest-path benchmarks/tokio/Cargo.toml --release -- client-len --addr <NIC_IP>:9000 --connections 100 --messages 1000000 --payload 128 --in-flight 16
```

## UDP echo

Terminal 1:

```bash
cargo run --manifest-path benchmarks/tokio/Cargo.toml --release -- server-tokio-udp --addr <NIC_IP>:9001
```

Terminal 2:

```bash
cargo run --manifest-path benchmarks/tokio/Cargo.toml --release -- client-udp --addr <NIC_IP>:9001 --connections 100 --messages 1000000 --payload 128
```
