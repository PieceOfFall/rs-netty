# Netty benchmark harness

This project mirrors the Rust benchmark harness in `benchmarks/rust` with
Netty 4.x implementations of the same wire protocols:

- TCP line echo
- TCP length-field echo
- UDP datagram echo

Run commands from the repository root. Replace `<NIC_IP>` with a non-loopback
address on your local network interface.

## Build

```bash
mvn -f benchmarks/netty/pom.xml package
```

## TCP line echo

Terminal 1:

```bash
mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="server-netty-line --addr <NIC_IP>:9000"
```

Terminal 2:

```bash
mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="client-line --addr <NIC_IP>:9000 --connections 100 --messages 1000000 --payload 128 --in-flight 1"
```

## TCP length-field echo

Terminal 1:

```bash
mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="server-netty-len --addr <NIC_IP>:9000"
```

Terminal 2:

```bash
mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="client-len --addr <NIC_IP>:9000 --connections 100 --messages 1000000 --payload 128 --in-flight 16"
```

## UDP echo

Terminal 1:

```bash
mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="server-netty-udp --addr <NIC_IP>:9001"
```

Terminal 2:

```bash
mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="client-udp --addr <NIC_IP>:9001 --connections 100 --messages 1000000 --payload 128"
```

## Notes

- Use the same `--connections`, `--messages`, `--payload`, and `--in-flight`
  values as the Rust harness.
- The length protocol is `u32be length + payload`.
- The UDP client uses one `NioDatagramChannel` per logical connection and sends
  one request at a time per channel.
