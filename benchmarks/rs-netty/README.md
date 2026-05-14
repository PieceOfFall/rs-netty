# rs-netty benchmark harness

This crate provides rs-netty benchmark servers and matching clients for:

- rs-netty TCP line echo
- rs-netty TCP length-field echo
- rs-netty UDP datagram echo

Run commands from the repository root. Replace `<NIC_IP>` with a non-loopback
address on your local network interface.

## Build

```bash
cargo build --manifest-path benchmarks/rs-netty/Cargo.toml --release --offline
```

## TCP line echo

Terminal 1:

```bash
cargo run --manifest-path benchmarks/rs-netty/Cargo.toml --release -- server-rs-line --addr <NIC_IP>:9000
```

Terminal 2:

```bash
cargo run --manifest-path benchmarks/rs-netty/Cargo.toml --release -- client-line --addr <NIC_IP>:9000 --connections 100 --messages 1000000 --payload 128 --in-flight 1
```

## TCP length-field echo

Terminal 1:

```bash
cargo run --manifest-path benchmarks/rs-netty/Cargo.toml --release -- server-rs-len --addr <NIC_IP>:9000
```

Terminal 2:

```bash
cargo run --manifest-path benchmarks/rs-netty/Cargo.toml --release -- client-len --addr <NIC_IP>:9000 --connections 100 --messages 1000000 --payload 128 --in-flight 16
```

## UDP echo

Terminal 1:

```bash
cargo run --manifest-path benchmarks/rs-netty/Cargo.toml --release -- server-rs-udp --addr <NIC_IP>:9001
```

Terminal 2:

```bash
cargo run --manifest-path benchmarks/rs-netty/Cargo.toml --release -- client-udp --addr <NIC_IP>:9001 --connections 100 --messages 1000000 --payload 128
```

## Notes

- Use the same `--connections`, `--messages`, `--payload`, and `--in-flight`
  values when comparing with the Tokio and Netty harnesses.
- The line protocol sends `payload + "\n"` and waits for one echoed line.
- The length protocol sends `u32be length + payload` and waits for the same
  frame back.
- The UDP client uses one local socket per logical connection and sends one
  request at a time per socket.
