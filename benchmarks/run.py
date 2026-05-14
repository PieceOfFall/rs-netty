#!/usr/bin/env python3
import argparse
import csv
import os
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
BENCH = ROOT / "benchmarks"


def main() -> int:
    args = parse_args()
    args.host = args.host or detect_nic_host()
    if is_loopback_host(args.host):
        raise SystemExit(
            f"refusing to benchmark through loopback host {args.host!r}; "
            "pass --host with a non-loopback interface address"
        )
    print(f"benchmark host: {args.host}")

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not args.no_build:
        build_all(args.impls)

    rows = []
    cases = list(make_cases(args))
    for case in cases:
        for impl in args.impls:
            port = args.port_base + len(rows)
            row = run_case(args, case, impl, port, out_dir)
            rows.append(row)
            append_csv(out_dir / "results.csv", rows[-1:], append=len(rows) > 1)

    chart_paths = plot_results(rows, out_dir)
    print(f"wrote {out_dir / 'results.csv'}")
    for path in chart_paths:
        print(f"wrote {path}")
    return 0


def parse_args():
    parser = argparse.ArgumentParser(description="Run rs-netty/Tokio/Netty throughput, memory, and latency benchmarks.")
    parser.add_argument("--impls", nargs="+", default=["rs-netty", "tokio", "netty"], choices=["rs-netty", "tokio", "netty"])
    parser.add_argument("--protocols", nargs="+", default=["line", "len", "udp"], choices=["line", "len", "udp"])
    parser.add_argument("--connections", type=int, default=100)
    parser.add_argument("--connection-values", nargs="+", type=int, default=None)
    parser.add_argument("--messages", type=int, default=100_000)
    parser.add_argument("--payload", type=int, default=128)
    parser.add_argument("--payload-values", nargs="+", type=int, default=None)
    parser.add_argument("--in-flight", type=int, default=16)
    parser.add_argument("--in-flight-values", nargs="+", type=int, default=None)
    parser.add_argument("--repeat", type=int, default=1)
    parser.add_argument("--host", default=None, help="Non-loopback local interface address. Defaults to auto-detected NIC IPv4.")
    parser.add_argument("--port-base", type=int, default=19000)
    parser.add_argument("--output-dir", default=str(BENCH / "results"))
    parser.add_argument("--no-build", action="store_true")
    args = parser.parse_args()
    positive_values = [args.connections, args.messages, args.payload, args.in_flight, args.repeat]
    for values in (args.connection_values, args.payload_values, args.in_flight_values):
        if values:
            positive_values.extend(values)
    if any(value <= 0 for value in positive_values):
        parser.error("connections, messages, payload, in-flight, repeat, and sweep values must be greater than zero")
    return args


@dataclass(frozen=True)
class Case:
    protocol: str
    connections: int
    messages: int
    payload: int
    in_flight: int
    repeat: int

    @property
    def key(self):
        return (
            self.protocol,
            str(self.connections),
            str(self.messages),
            str(self.payload),
            str(self.in_flight if self.protocol != "udp" else 1),
            str(self.repeat),
        )

    @property
    def label(self):
        suffix = f"{self.protocol}\\nc={self.connections}, p={self.payload}"
        if self.protocol != "udp":
            suffix += f", f={self.in_flight}"
        if self.repeat > 1:
            suffix += f"\\nr{self.repeat}"
        return suffix

    @property
    def slug(self):
        return (
            f"{self.protocol}-c{self.connections}-m{self.messages}-"
            f"p{self.payload}-f{self.in_flight if self.protocol != 'udp' else 1}-r{self.repeat}"
        )


def make_cases(args):
    connection_values = args.connection_values or [args.connections]
    payload_values = args.payload_values or [args.payload]
    in_flight_values = args.in_flight_values or [args.in_flight]

    for repeat in range(1, args.repeat + 1):
        for protocol in args.protocols:
            for connections in connection_values:
                for payload in payload_values:
                    for in_flight in in_flight_values:
                        yield Case(
                            protocol=protocol,
                            connections=connections,
                            messages=args.messages,
                            payload=payload,
                            in_flight=in_flight,
                            repeat=repeat,
                        )


def detect_nic_host():
    candidates = []

    # UDP connect does not send packets, but lets the OS choose the source
    # address it would use for normal outbound traffic.
    for target in ("8.8.8.8", "1.1.1.1", "192.0.2.1"):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
                sock.connect((target, 80))
                candidates.append(sock.getsockname()[0])
        except OSError:
            pass

    try:
        hostname = socket.gethostname()
        for info in socket.getaddrinfo(hostname, None, socket.AF_INET, socket.SOCK_DGRAM):
            candidates.append(info[4][0])
    except OSError:
        pass

    for candidate in candidates:
        if not is_loopback_host(candidate):
            return candidate

    raise SystemExit(
        "could not auto-detect a non-loopback local interface address; "
        "pass --host <your NIC IP>, for example --host 192.168.1.20"
    )


def is_loopback_host(host):
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except OSError:
        return False

    for family, _, _, _, sockaddr in infos:
        if family == socket.AF_INET and sockaddr[0].startswith("127."):
            return True
        if family == socket.AF_INET6 and sockaddr[0] == "::1":
            return True
    return False


def build_all(impls):
    if "rs-netty" in impls:
        run(["cargo", "build", "--manifest-path", str(BENCH / "rs-netty" / "Cargo.toml"), "--release"])
    if "tokio" in impls:
        run(["cargo", "build", "--manifest-path", str(BENCH / "tokio" / "Cargo.toml"), "--release"])
    if "netty" in impls:
        run([
            "mvn",
            "-f",
            str(BENCH / "netty" / "pom.xml"),
            "-q",
            "package",
            "dependency:copy-dependencies",
            "-DskipTests",
        ])


def run_case(args, case, impl, port, out_dir):
    addr = f"{args.host}:{port}"
    case_name = f"{impl}-{case.slug}"
    print(f"==> {case_name}")

    server_stdout = open(out_dir / f"{case_name}.server.out.log", "w", encoding="utf-8")
    server_stderr = open(out_dir / f"{case_name}.server.err.log", "w", encoding="utf-8")
    client_stdout_path = out_dir / f"{case_name}.client.out.log"
    client_stderr_path = out_dir / f"{case_name}.client.err.log"

    server = subprocess.Popen(
        server_command(impl, case.protocol, addr),
        cwd=ROOT,
        stdout=server_stdout,
        stderr=server_stderr,
        text=True,
    )
    rss_sampler = RssSampler(server.pid)
    rss_sampler.start()

    try:
        wait_for_server(case.protocol, args.host, port)
        client = subprocess.run(
            client_command(impl, case.protocol, addr, case),
            cwd=ROOT,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        client_stdout_path.write_text(client.stdout, encoding="utf-8")
        client_stderr_path.write_text(client.stderr, encoding="utf-8")

        result = parse_result(client.stdout)
        if client.returncode != 0:
            raise RuntimeError(f"client failed for {case_name}; see {client_stderr_path}")
        if result is None:
            raise RuntimeError(f"client produced no RESULT line for {case_name}; see {client_stdout_path}")

        row = {
            "impl": impl,
            "protocol": case.protocol,
            "connections": case.connections,
            "messages": case.messages,
            "payload_bytes": case.payload,
            "in_flight": case.in_flight if case.protocol != "udp" else 1,
            "repeat": case.repeat,
            "case": case.slug,
            "case_label": case.label,
            "elapsed_sec": result["elapsed_sec"],
            "throughput_msg_sec": result["throughput_msg_sec"],
            "latency_count": result["latency_count"],
            "p50_us": result["p50_us"],
            "p90_us": result["p90_us"],
            "p99_us": result["p99_us"],
            "p999_us": result["p999_us"],
            "server_max_rss_kb": rss_sampler.max_rss_kb,
            "server_avg_rss_kb": rss_sampler.avg_rss_kb(),
            "client_stdout": str(client_stdout_path),
            "client_stderr": str(client_stderr_path),
            "server_stdout": str(out_dir / f"{case_name}.server.out.log"),
            "server_stderr": str(out_dir / f"{case_name}.server.err.log"),
        }
        print(
            f"    throughput={float(row['throughput_msg_sec']):.0f} msg/s "
            f"p99={row['p99_us']}us max_rss={row['server_max_rss_kb']}KB"
        )
        return row
    finally:
        rss_sampler.stop()
        terminate(server)
        server_stdout.close()
        server_stderr.close()


def server_command(impl, protocol, addr):
    if impl == "rs-netty":
        binary = BENCH / "rs-netty" / "target" / "release" / "rs-netty-bench"
        mode = {"line": "server-rs-line", "len": "server-rs-len", "udp": "server-rs-udp"}[protocol]
        return [str(binary), mode, "--addr", addr]
    if impl == "tokio":
        binary = BENCH / "tokio" / "target" / "release" / "tokio-bench"
        mode = {"line": "server-tokio-line", "len": "server-tokio-len", "udp": "server-tokio-udp"}[protocol]
        return [str(binary), mode, "--addr", addr]
    classpath = netty_classpath()
    mode = {"line": "server-netty-line", "len": "server-netty-len", "udp": "server-netty-udp"}[protocol]
    return ["java", "-cp", classpath, "bench.BenchMain", mode, "--addr", addr]


def client_command(impl, protocol, addr, case):
    if impl == "rs-netty":
        binary = BENCH / "rs-netty" / "target" / "release" / "rs-netty-bench"
    elif impl == "tokio":
        binary = BENCH / "tokio" / "target" / "release" / "tokio-bench"
    else:
        binary = None

    mode = {"line": "client-line", "len": "client-len", "udp": "client-udp"}[protocol]
    common = [
        mode,
        "--addr",
        addr,
        "--connections",
        str(case.connections),
        "--messages",
        str(case.messages),
        "--payload",
        str(case.payload),
    ]
    if protocol != "udp":
        common += ["--in-flight", str(case.in_flight)]

    if binary is not None:
        return [str(binary)] + common
    return ["java", "-cp", netty_classpath(), "bench.BenchMain"] + common


def netty_classpath():
    sep = os.pathsep
    return sep.join([
        str(BENCH / "netty" / "target" / "classes"),
        str(BENCH / "netty" / "target" / "dependency" / "*"),
    ])


def wait_for_server(protocol, host, port):
    deadline = time.time() + 10.0
    if protocol == "udp":
        time.sleep(1.0)
        return

    last_error = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                return
        except OSError as err:
            last_error = err
            time.sleep(0.05)
    raise RuntimeError(f"server did not become ready on {host}:{port}: {last_error}")


def parse_result(stdout):
    for line in stdout.splitlines():
        if not line.startswith("RESULT "):
            continue
        result = {}
        for part in line.removeprefix("RESULT ").split():
            key, value = part.split("=", 1)
            result[key] = value
        return result
    return None


class RssSampler:
    def __init__(self, pid):
        self.pid = pid
        self.samples = []
        self.max_rss_kb = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join(timeout=1.0)

    def avg_rss_kb(self):
        if not self.samples:
            return 0
        return round(sum(self.samples) / len(self.samples))

    def _run(self):
        while not self._stop.is_set():
            rss = read_rss_kb(self.pid)
            if rss is not None:
                self.samples.append(rss)
                self.max_rss_kb = max(self.max_rss_kb, rss)
            time.sleep(0.1)


def read_rss_kb(pid):
    result = subprocess.run(
        ["ps", "-o", "rss=", "-p", str(pid)],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def terminate(process):
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def run(command):
    print("+", " ".join(command))
    subprocess.run(command, cwd=ROOT, check=True)


def append_csv(path, rows, append):
    fieldnames = [
        "impl",
        "protocol",
        "connections",
        "messages",
        "payload_bytes",
        "in_flight",
        "repeat",
        "case",
        "case_label",
        "elapsed_sec",
        "throughput_msg_sec",
        "latency_count",
        "p50_us",
        "p90_us",
        "p99_us",
        "p999_us",
        "server_max_rss_kb",
        "server_avg_rss_kb",
        "client_stdout",
        "client_stderr",
        "server_stdout",
        "server_stderr",
    ]
    with path.open("a" if append else "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not append:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def plot_results(rows, out_dir):
    if not rows:
        return []

    cases = ordered_unique(row["case"] for row in rows)
    case_labels = [find_case_label(rows, case) for case in cases]
    impls = ordered_unique(row["impl"] for row in rows)
    colors = {
        "rs-netty": "#2563eb",
        "tokio": "#16a34a",
        "netty": "#dc2626",
    }
    markers = {
        "rs-netty": "o",
        "tokio": "s",
        "netty": "^",
    }

    paths = [
        plot_metric(
            rows,
            out_dir,
            cases,
            case_labels,
            impls,
            colors,
            markers,
            metric="throughput_msg_sec",
            title="Throughput by Benchmark Case",
            ylabel="messages / second",
            filename="throughput.png",
        ),
        plot_metric(
            rows,
            out_dir,
            cases,
            case_labels,
            impls,
            colors,
            markers,
            metric="p99_us",
            title="P99 Latency by Benchmark Case",
            ylabel="microseconds",
            filename="p99_latency.png",
        ),
        plot_metric(
            rows,
            out_dir,
            cases,
            case_labels,
            impls,
            colors,
            markers,
            metric="server_max_rss_kb",
            title="Peak Server RSS by Benchmark Case",
            ylabel="KB",
            filename="server_memory.png",
        ),
    ]
    paths.append(plot_latency_percentiles(rows, out_dir, cases, case_labels, impls, colors, markers))
    return paths


def plot_metric(rows, out_dir, cases, case_labels, impls, colors, markers, metric, title, ylabel, filename):
    width = max(11, min(24, 1.4 * len(cases) + 4))
    plt.figure(figsize=(width, 6.4))
    x = list(range(len(cases)))
    for impl in impls:
        values = []
        for case in cases:
            row = find_row(rows, impl, case)
            values.append(float(row[metric]) if row else float("nan"))
        plt.plot(
            x,
            values,
            label=impl,
            color=colors.get(impl),
            marker=markers.get(impl, "o"),
            linewidth=2.2,
            markersize=7,
        )

    plt.title(title)
    plt.xlabel("benchmark case")
    plt.ylabel(ylabel)
    plt.xticks(x, case_labels, rotation=30, ha="right")
    plt.grid(True, axis="y", alpha=0.3)
    plt.legend()
    plt.tight_layout()
    path = out_dir / filename
    plt.savefig(path, dpi=180)
    plt.close()
    return path


def plot_latency_percentiles(rows, out_dir, cases, case_labels, impls, colors, markers):
    percentiles = ["p50_us", "p90_us", "p99_us", "p999_us"]
    labels = ["p50", "p90", "p99", "p999"]
    shown_cases = cases[: min(len(cases), 6)]
    shown_labels = case_labels[: len(shown_cases)]

    fig, axes = plt.subplots(
        1,
        len(shown_cases),
        figsize=(5.0 * len(shown_cases), 5.4),
        sharey=False,
    )
    if len(shown_cases) == 1:
        axes = [axes]

    for axis, case, label in zip(axes, shown_cases, shown_labels):
        x = list(range(len(percentiles)))
        for impl in impls:
            row = find_row(rows, impl, case)
            if not row:
                continue
            values = [float(row[metric]) for metric in percentiles]
            axis.plot(
                x,
                values,
                label=impl,
                color=colors.get(impl),
                marker=markers.get(impl, "o"),
                linewidth=2.0,
                markersize=6,
            )
        axis.set_title(label)
        axis.set_xlabel("percentile")
        axis.set_ylabel("microseconds")
        axis.set_xticks(x)
        axis.set_xticklabels(labels)
        axis.grid(True, axis="y", alpha=0.3)

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, loc="upper center", ncol=max(1, len(impls)))
    title = "Latency Percentiles"
    if len(cases) > len(shown_cases):
        title += f" (first {len(shown_cases)} cases)"
    fig.suptitle(title, y=1.02)
    fig.tight_layout()
    path = out_dir / "latency_percentiles.png"
    fig.savefig(path, dpi=180, bbox_inches="tight")
    plt.close(fig)
    return path


def ordered_unique(values):
    seen = set()
    result = []
    for value in values:
        if value not in seen:
            seen.add(value)
            result.append(value)
    return result


def find_case_label(rows, case):
    for row in rows:
        if row["case"] == case:
            return row["case_label"]
    return case


def find_row(rows, impl, case):
    for row in rows:
        if row["impl"] == impl and row["case"] == case:
            return row
    return None


if __name__ == "__main__":
    sys.exit(main())
