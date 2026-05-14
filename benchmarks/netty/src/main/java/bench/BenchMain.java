package bench;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;

public final class BenchMain {
    private BenchMain() {
    }

    public static void main(String[] rawArgs) throws Exception {
        Args args = Args.parse(rawArgs);

        switch (args.mode) {
            case "server-netty-line" -> serverNettyLine(args);
            case "server-netty-len" -> serverNettyLen(args);
            case "server-netty-udp" -> serverNettyUdp(args);
            case "client-line" -> clientTcpLine(args);
            case "client-len" -> clientTcpLen(args);
            case "client-udp" -> clientUdp(args);
            default -> {
                System.err.println(usage());
                throw new IllegalArgumentException("unknown mode: " + args.mode);
            }
        }
    }

    private static void serverNettyLine(Args args) throws InterruptedException {
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup workers = new NioEventLoopGroup();

        try {
            Channel channel = new ServerBootstrap()
                    .group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new LineBasedFrameDecoder(8 * 1024 * 1024))
                                    .addLast(new StringDecoder(CharsetUtil.UTF_8))
                                    .addLast(new StringEncoder(CharsetUtil.UTF_8))
                                    .addLast(new SimpleChannelInboundHandler<String>() {
                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
                                            ctx.writeAndFlush(msg + "\n");
                                        }
                                    });
                        }
                    })
                    .bind(args.host(), args.port())
                    .sync()
                    .channel();

            channel.closeFuture().sync();
        } finally {
            boss.shutdownGracefully();
            workers.shutdownGracefully();
        }
    }

    private static void serverNettyLen(Args args) throws InterruptedException {
        EventLoopGroup boss = new NioEventLoopGroup(1);
        EventLoopGroup workers = new NioEventLoopGroup();

        try {
            Channel channel = new ServerBootstrap()
                    .group(boss, workers)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline()
                                    .addLast(new LengthFieldBasedFrameDecoder(
                                            8 * 1024 * 1024,
                                            0,
                                            4,
                                            0,
                                            4
                                    ))
                                    .addLast(new LengthFieldPrepender(4))
                                    .addLast(new SimpleChannelInboundHandler<ByteBuf>() {
                                        @Override
                                        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
                                            ctx.writeAndFlush(msg.retainedDuplicate());
                                        }
                                    });
                        }
                    })
                    .bind(args.host(), args.port())
                    .sync()
                    .channel();

            channel.closeFuture().sync();
        } finally {
            boss.shutdownGracefully();
            workers.shutdownGracefully();
        }
    }

    private static void serverNettyUdp(Args args) throws InterruptedException {
        EventLoopGroup group = new NioEventLoopGroup();

        try {
            Channel channel = new Bootstrap()
                    .group(group)
                    .channel(NioDatagramChannel.class)
                    .handler(new SimpleChannelInboundHandler<DatagramPacket>() {
                        @Override
                        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
                            ctx.writeAndFlush(new DatagramPacket(packet.content().retainedDuplicate(), packet.sender()));
                        }
                    })
                    .bind(args.host(), args.port())
                    .sync()
                    .channel();

            channel.closeFuture().sync();
        } finally {
            group.shutdownGracefully();
        }
    }

    private static void clientTcpLine(Args args) throws Exception {
        runTcpClients(args, Protocol.LINE);
    }

    private static void clientTcpLen(Args args) throws Exception {
        runTcpClients(args, Protocol.LENGTH);
    }

    private static void runTcpClients(Args args, Protocol protocol) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        AtomicLong done = new AtomicLong();
        Latencies latencies = new Latencies();
        CountDownLatch connected = new CountDownLatch(args.connections);
        CountDownLatch finished = new CountDownLatch(args.connections);
        CompletableFuture<Void> startSignal = new CompletableFuture<>();
        byte[] payload = makePayload(args.payload);

        try {
            for (int i = 0; i < args.connections; i++) {
                int messages = messagesForWorker(args.messages, args.connections, i);
                Bootstrap bootstrap = new Bootstrap()
                        .group(group)
                        .channel(NioSocketChannel.class)
                        .option(ChannelOption.TCP_NODELAY, true)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel ch) {
                                if (protocol == Protocol.LINE) {
                                    ch.pipeline()
                                            .addLast(new LineBasedFrameDecoder(8 * 1024 * 1024))
                                            .addLast(new StringDecoder(CharsetUtil.UTF_8))
                                            .addLast(new StringEncoder(CharsetUtil.UTF_8))
                                            .addLast(new TcpLineClientHandler(
                                                    payload,
                                                    messages,
                                                    args.inFlight,
                                                    done,
                                                    latencies,
                                                    connected,
                                                    finished,
                                                    startSignal
                                            ));
                                } else {
                                    ch.pipeline()
                                            .addLast(new LengthFieldBasedFrameDecoder(
                                                    8 * 1024 * 1024,
                                                    0,
                                                    4,
                                                    0,
                                                    4
                                            ))
                                            .addLast(new LengthFieldPrepender(4))
                                            .addLast(new TcpLengthClientHandler(
                                                    payload,
                                                    messages,
                                                    args.inFlight,
                                                    done,
                                                    latencies,
                                                    connected,
                                                    finished,
                                                    startSignal
                                            ));
                                }
                            }
                        });

                bootstrap.connect(args.host(), args.port()).sync();
            }

            connected.await();
            long start = System.nanoTime();
            startSignal.complete(null);
            reportUntilDone("tcp", args.messages, done, start, finished);
            finished.await();
            printResult("tcp", args.messages, start, latencies);
        } finally {
            group.shutdownGracefully().sync();
        }
    }

    private static void clientUdp(Args args) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        AtomicLong done = new AtomicLong();
        Latencies latencies = new Latencies();
        CountDownLatch connected = new CountDownLatch(args.connections);
        CountDownLatch finished = new CountDownLatch(args.connections);
        CompletableFuture<Void> startSignal = new CompletableFuture<>();
        byte[] payload = makePayload(args.payload);
        InetSocketAddress remote = new InetSocketAddress(args.host(), args.port());

        try {
            for (int i = 0; i < args.connections; i++) {
                int messages = messagesForWorker(args.messages, args.connections, i);
                Bootstrap bootstrap = new Bootstrap()
                        .group(group)
                        .channel(NioDatagramChannel.class)
                        .handler(new UdpClientHandler(
                                remote,
                                payload,
                                messages,
                                done,
                                latencies,
                                connected,
                                finished,
                                startSignal
                        ));

                bootstrap.bind(args.host(), 0).sync();
            }

            connected.await();
            long start = System.nanoTime();
            startSignal.complete(null);
            reportUntilDone("udp", args.messages, done, start, finished);
            finished.await();
            printResult("udp", args.messages, start, latencies);
        } finally {
            group.shutdownGracefully().sync();
        }
    }

    private static final class TcpLineClientHandler extends SimpleChannelInboundHandler<String> {
        private final String payload;
        private final int messages;
        private final int inFlight;
        private final AtomicLong done;
        private final Latencies latencies;
        private final CountDownLatch connected;
        private final CountDownLatch finished;
        private final CompletableFuture<Void> startSignal;
        private final Queue<Long> sentAt = new ArrayDeque<>();
        private int sent;
        private int received;

        private TcpLineClientHandler(
                byte[] payload,
                int messages,
                int inFlight,
                AtomicLong done,
                Latencies latencies,
                CountDownLatch connected,
                CountDownLatch finished,
                CompletableFuture<Void> startSignal
        ) {
            this.payload = new String(payload, StandardCharsets.UTF_8);
            this.messages = messages;
            this.inFlight = inFlight;
            this.done = done;
            this.latencies = latencies;
            this.connected = connected;
            this.finished = finished;
            this.startSignal = startSignal;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            connected.countDown();
            startSignal.thenRun(() -> ctx.executor().execute(() -> writeMore(ctx)));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
            Long started = sentAt.poll();
            if (started != null) {
                latencies.record(System.nanoTime() - started);
            }
            received++;
            done.incrementAndGet();
            if (received >= messages) {
                finished.countDown();
                ctx.close();
                return;
            }
            writeMore(ctx);
        }

        private void writeMore(ChannelHandlerContext ctx) {
            while (sent < messages && sent - received < inFlight) {
                sentAt.add(System.nanoTime());
                ctx.write(payload + "\n");
                sent++;
            }
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            finished.countDown();
            ctx.close();
        }
    }

    private static final class TcpLengthClientHandler extends SimpleChannelInboundHandler<ByteBuf> {
        private final byte[] payload;
        private final int messages;
        private final int inFlight;
        private final AtomicLong done;
        private final Latencies latencies;
        private final CountDownLatch connected;
        private final CountDownLatch finished;
        private final CompletableFuture<Void> startSignal;
        private final Queue<Long> sentAt = new ArrayDeque<>();
        private int sent;
        private int received;

        private TcpLengthClientHandler(
                byte[] payload,
                int messages,
                int inFlight,
                AtomicLong done,
                Latencies latencies,
                CountDownLatch connected,
                CountDownLatch finished,
                CompletableFuture<Void> startSignal
        ) {
            this.payload = payload;
            this.messages = messages;
            this.inFlight = inFlight;
            this.done = done;
            this.latencies = latencies;
            this.connected = connected;
            this.finished = finished;
            this.startSignal = startSignal;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            connected.countDown();
            startSignal.thenRun(() -> ctx.executor().execute(() -> writeMore(ctx)));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) {
            Long started = sentAt.poll();
            if (started != null) {
                latencies.record(System.nanoTime() - started);
            }
            received++;
            done.incrementAndGet();
            if (received >= messages) {
                finished.countDown();
                ctx.close();
                return;
            }
            writeMore(ctx);
        }

        private void writeMore(ChannelHandlerContext ctx) {
            while (sent < messages && sent - received < inFlight) {
                sentAt.add(System.nanoTime());
                ctx.write(Unpooled.wrappedBuffer(payload));
                sent++;
            }
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            finished.countDown();
            ctx.close();
        }
    }

    private static final class UdpClientHandler extends SimpleChannelInboundHandler<DatagramPacket> {
        private final InetSocketAddress remote;
        private final byte[] payload;
        private final int messages;
        private final AtomicLong done;
        private final Latencies latencies;
        private final CountDownLatch connected;
        private final CountDownLatch finished;
        private final CompletableFuture<Void> startSignal;
        private int sent;
        private int received;
        private long sentAt;

        private UdpClientHandler(
                InetSocketAddress remote,
                byte[] payload,
                int messages,
                AtomicLong done,
                Latencies latencies,
                CountDownLatch connected,
                CountDownLatch finished,
                CompletableFuture<Void> startSignal
        ) {
            this.remote = remote;
            this.payload = payload;
            this.messages = messages;
            this.done = done;
            this.latencies = latencies;
            this.connected = connected;
            this.finished = finished;
            this.startSignal = startSignal;
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            connected.countDown();
            startSignal.thenRun(() -> ctx.executor().execute(() -> sendNext(ctx)));
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) {
            if (packet.content().readableBytes() != payload.length) {
                throw new IllegalStateException("unexpected UDP echo length: " + packet.content().readableBytes());
            }
            latencies.record(System.nanoTime() - sentAt);
            received++;
            done.incrementAndGet();
            if (received >= messages) {
                finished.countDown();
                ctx.close();
                return;
            }
            sendNext(ctx);
        }

        private void sendNext(ChannelHandlerContext ctx) {
            if (sent < messages) {
                sentAt = System.nanoTime();
                ctx.writeAndFlush(new DatagramPacket(Unpooled.wrappedBuffer(payload), remote));
                sent++;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            finished.countDown();
            ctx.close();
        }
    }

    private static void reportUntilDone(
            String label,
            long total,
            AtomicLong done,
            long startNanos,
            CountDownLatch finished
    ) throws InterruptedException {
        long lastCount = 0;
        long lastTime = startNanos;
        long lastReport = startNanos;

        while (finished.getCount() > 0) {
            long now = System.nanoTime();
            long count = Math.min(done.get(), total);
            double elapsed = Duration.ofNanos(now - startNanos).toNanos() / 1_000_000_000.0;
            double interval = Duration.ofNanos(now - lastTime).toNanos() / 1_000_000_000.0;

            if (finished.getCount() == 0) {
                break;
            }

            if (Duration.ofNanos(now - lastReport).toMillis() >= 1_000) {
                double currentRate = (count - lastCount) / interval;
                double averageRate = count / elapsed;

                System.err.printf(
                        Locale.ROOT,
                        "%s: done=%d/%d current=%.0f msg/s average=%.0f msg/s elapsed=%.1fs%n",
                        label,
                        count,
                        total,
                        currentRate,
                        averageRate,
                        elapsed
                );

                lastCount = count;
                lastTime = now;
                lastReport = now;
            }

            TimeUnit.MILLISECONDS.sleep(10);
        }
    }

    private static byte[] makePayload(int size) {
        byte[] payload = new byte[size];
        for (int i = 0; i < size; i++) {
            payload[i] = (byte) ('a' + (i % 26));
        }
        return payload;
    }

    private static int messagesForWorker(int total, int workers, int index) {
        int base = total / workers;
        return base + (index < total % workers ? 1 : 0);
    }

    private static final class Latencies {
        private final ConcurrentLinkedQueue<Long> micros = new ConcurrentLinkedQueue<>();

        void record(long nanos) {
            micros.add(TimeUnit.NANOSECONDS.toMicros(nanos));
        }

        ArrayList<Long> sortedSnapshot() {
            ArrayList<Long> values = new ArrayList<>(micros);
            Collections.sort(values);
            return values;
        }
    }

    private static void printResult(String label, int total, long startNanos, Latencies latencies) {
        long elapsedNanos = System.nanoTime() - startNanos;
        double elapsed = elapsedNanos / 1_000_000_000.0;
        double throughput = total / elapsed;
        ArrayList<Long> values = latencies.sortedSnapshot();
        long p50 = percentile(values, 50.0);
        long p90 = percentile(values, 90.0);
        long p99 = percentile(values, 99.0);
        long p999 = percentile(values, 99.9);

        System.out.printf(
                Locale.ROOT,
                "RESULT protocol=%s messages=%d elapsed_sec=%.6f throughput_msg_sec=%.2f latency_count=%d p50_us=%d p90_us=%d p99_us=%d p999_us=%d%n",
                label,
                total,
                elapsed,
                throughput,
                values.size(),
                p50,
                p90,
                p99,
                p999
        );
    }

    private static long percentile(ArrayList<Long> sorted, double percentile) {
        if (sorted.isEmpty()) {
            return 0;
        }

        int rank = (int) Math.ceil((percentile / 100.0) * (sorted.size() - 1));
        return sorted.get(Math.min(rank, sorted.size() - 1));
    }

    private enum Protocol {
        LINE,
        LENGTH
    }

    private record Args(
            String mode,
            String addr,
            int connections,
            int messages,
            int payload,
            int inFlight
    ) {
        static Args parse(String[] rawArgs) {
            if (rawArgs.length == 0) {
                throw new IllegalArgumentException(usage());
            }

            String mode = rawArgs[0];
            String addr = "localhost:9000";
            int connections = 1;
            int messages = 100_000;
            int payload = 128;
            int inFlight = 1;

            for (int i = 1; i < rawArgs.length; i += 2) {
                if (i + 1 >= rawArgs.length) {
                    throw new IllegalArgumentException("missing value for " + rawArgs[i]);
                }

                String flag = rawArgs[i];
                String value = rawArgs[i + 1];
                switch (flag) {
                    case "--addr" -> addr = value;
                    case "--connections" -> connections = Integer.parseInt(value);
                    case "--messages" -> messages = Integer.parseInt(value);
                    case "--payload" -> payload = Integer.parseInt(value);
                    case "--in-flight" -> inFlight = Integer.parseInt(value);
                    default -> throw new IllegalArgumentException("unknown flag: " + flag);
                }
            }

            if (connections <= 0) {
                throw new IllegalArgumentException("--connections must be greater than zero");
            }
            if (messages <= 0) {
                throw new IllegalArgumentException("--messages must be greater than zero");
            }
            if (payload <= 0) {
                throw new IllegalArgumentException("--payload must be greater than zero");
            }
            if (inFlight <= 0) {
                throw new IllegalArgumentException("--in-flight must be greater than zero");
            }

            return new Args(mode, addr, connections, messages, payload, inFlight);
        }

        String host() {
            int index = addr.lastIndexOf(':');
            if (index < 0) {
                return addr;
            }
            return addr.substring(0, index);
        }

        int port() {
            int index = addr.lastIndexOf(':');
            if (index < 0) {
                throw new IllegalArgumentException("addr must include port: " + addr);
            }
            return Integer.parseInt(addr.substring(index + 1));
        }
    }

    private static String usage() {
        return """
                usage:
                  mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="server-netty-line --addr localhost:9000"
                  mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="server-netty-len --addr localhost:9000"
                  mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="server-netty-udp --addr localhost:9001"

                  mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="client-line --addr localhost:9000 --connections 100 --messages 1000000 --payload 128 --in-flight 1"
                  mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="client-len --addr localhost:9000 --connections 100 --messages 1000000 --payload 128 --in-flight 16"
                  mvn -f benchmarks/netty/pom.xml exec:java -Dexec.args="client-udp --addr localhost:9001 --connections 100 --messages 1000000 --payload 128"
                """;
    }
}
