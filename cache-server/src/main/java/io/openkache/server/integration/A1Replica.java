package io.openkache.server.integration;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.openkache.core.api.*;
import io.openkache.core.failure.*;
import io.openkache.core.model.*;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.replication.raft.RaftEngine;
import io.openkache.core.replication.vr.VrEngine;
import io.openkache.core.replication.chain.ChainEngine;
import io.openkache.core.replication.rose.RoseEngine;
import io.openkache.core.runtime.*;
import io.openkache.core.spi.*;
import io.openkache.server.ionia.IoniaFileStorage;
import io.openkache.server.metrics.ServerMetrics;
import io.openkache.server.netty.*;
import io.openkache.server.replication.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

/** A1 엔진에 A2와 동일한 append-file KV를 주입한다. 기존 PB 서버와 알고리즘은 변경하지 않는다. */
final class A1Replica implements AutoCloseable {
    private final IntegrationConfig config;
    private final NodeId self;
    private final EventLoopGroup io = new MultiThreadIoEventLoopGroup(2, NioIoHandler.newFactory());
    private final EventLoopGroup accept = new MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory());
    private final OwnerWorker owner = new OwnerWorker(new WorkerId(100), 65536);
    private final ScheduledExecutorService ticks = Executors.newSingleThreadScheduledExecutor();
    private ServerMetrics metrics;
    private PbTransport transport;
    private IoniaFileStorage file;
    private CacheEngine engine;
    private RaftEngine raft;
    private VrEngine vr;
    private CrashOnlyPrimaryBackupEngine pb;
    private ChainEngine chain;
    private RoseEngine rose;
    private final java.util.concurrent.atomic.AtomicLong heartbeatAt = new java.util.concurrent.atomic.AtomicLong(-1);
    private Channel client, peer;
    A1Replica(IntegrationConfig config, int group, int index, Path directory) throws IOException, InterruptedException {
        this.config = config; self = new NodeId(index);
        try {
            TimeSource clock = TimeSource.system();
            file = new IoniaFileStorage(self, directory, config.number("storage.workers", 2), clock, false);
            metrics = ServerMetrics.start("127.0.0.1", 0);
            transport = new PbTransport(io, config.addresses(group, 1), switch (config.protocol()) {
                case "RAFT" -> RaftWire::encode; case "VR" -> VrWire::encode; case "CHAIN", "CRAQ" -> ChainWire::encode; case "ROSE" -> RoseWire::encode; default -> PbWire::encode;
            });
            List<NodeId> members = new ArrayList<>(config.addresses(group, 0).keySet());
            Duration heartbeat = Duration.ofMillis(50), timeout = Duration.ofMillis(config.number("failure.timeout.ms", 1000));
            if (config.protocol().equals("CHAIN") || config.protocol().equals("CRAQ")) {
                chain = new ChainEngine(self, members, 1, file, owner, transport, clock, Duration.ofMillis(config.number("replication.retry.ms", 50)),
                        config.protocol().equals("CRAQ") ? ChainEngine.ReadMode.CRAQ : ChainEngine.ReadMode.TAIL_ONLY); engine = chain;
            } else if (config.protocol().equals("ROSE")) {
                rose = new RoseEngine(self, members, members.getFirst(), 1, config.number("rose.limit", 4), file, owner, transport, clock,
                        Duration.ofMillis(config.number("replication.retry.ms", 50))); engine = rose;
            } else if (config.protocol().equals("RAFT")) {
                raft = new RaftEngine(self, members, file, owner, transport, clock, heartbeat, timeout,
                        Long.parseLong(config.text("failure.seed", "1")) + index * 104729L, config.externalFd(), Duration.ofMillis(Math.max(0, config.nativeWatchdogMillis() - config.number("failure.timeout.ms", 1000)))); engine = raft;
            } else if (config.protocol().equals("VR")) {
                vr = new VrEngine(self, members, file, owner, transport, clock, heartbeat, config.externalFd() ? Duration.ofMillis(config.nativeWatchdogMillis()) : timeout, VrEngine.Mode.valueOf(config.text("vr.mode", "INCREMENTAL_CATCHUP"))); engine = vr;
            } else {
                // PB 내부 COUNT는 데이터 접근이 아니라 앞선 비동기 KV 적용을 배출하는 barrier다.
                CacheEngine storage = new BarrierStorage(file);
                var pc = new PrimaryBackupConfig(self, members.getFirst(), members.subList(1, 3), new OwnershipEpoch(1));
                pb = config.protocol().equals("PB_FENCED") ? new CrashOnlyPrimaryBackupEngine(pc, storage, owner, transport, clock, heartbeat, timeout, timeout)
                        : new CrashOnlyPrimaryBackupEngine(pc, storage, owner, transport, clock, heartbeat, timeout, !config.externalFd()); engine = pb;
            }
            peer = bind(config.hosts(group).get(index), config.number("replica.port", 17101) + 1, new PeerHandler(Set.copyOf(members)));
            client = bind(config.hosts(group).get(index), config.number("replica.port", 17101), new ClientRequestHandler(metrics, engine, Duration.ofSeconds(3)));
            String tick = raft != null ? RaftEngine.TICK : vr != null ? VrEngine.TICK : chain != null ? ChainEngine.TICK : rose != null ? RoseEngine.TICK : CrashOnlyPrimaryBackupEngine.TICK;
            ticks.scheduleAtFixedRate(() -> {
                long now = clock.monotonicNanos();
                if (pb != null && config.protocol().equals("PB_FENCED") && !config.externalFd() && heartbeatAt.get() >= 0 && now - heartbeatAt.get() >= timeout.toNanos()) pb.onPrimaryFailure(new NodeId(0), now);
                if (pb == null || config.protocol().equals("PB_FENCED") || self.value() == 0 || heartbeatAt.get() >= 0 || config.externalFd()) engine.onTimer(new TimerEvent(tick, now));
            }, 10, 10, TimeUnit.MILLISECONDS);
        } catch (IOException | RuntimeException | InterruptedException e) { close(); throw e; }
    }
    @ChannelHandler.Sharable
    private final class PeerHandler extends SimpleChannelInboundHandler<Frame> {
        private final Set<NodeId> members;
        PeerHandler(Set<NodeId> members) { this.members = members; }
        @Override protected void channelRead0(ChannelHandlerContext context, Frame frame) {
            try {
                PeerEnvelope envelope = raft != null ? RaftWire.decode(frame) : vr != null ? VrWire.decode(frame) : chain != null ? ChainWire.decode(frame) : rose != null ? RoseWire.decode(frame) : PbWire.decode(frame);
                if (!envelope.to().equals(self) || !members.contains(envelope.from()) || envelope.from().equals(self)) throw new IllegalArgumentException("peer identity mismatch");
                if (envelope.from().value() == 0 && envelope.message() instanceof CrashOnlyPrimaryBackupEngine.Control c
                        && c.kind() == CrashOnlyPrimaryBackupEngine.Kind.HEARTBEAT && c.epoch().value() == 1) heartbeatAt.set(System.nanoTime());
                if (config.protocol().equals("PB_FENCED") && envelope.from().value() == 0 && frame.opcode() == PbWire.FENCE) heartbeatAt.set(System.nanoTime());
                engine.onPeerMessage(envelope.from(), envelope.message());
            } catch (RuntimeException e) { context.close(); }
        }
    }
    private Channel bind(String host, int port, ChannelHandler handler) throws InterruptedException {
        return new ServerBootstrap().group(accept, io).channel(NioServerSocketChannel.class).childOption(ChannelOption.TCP_NODELAY, true)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override protected void initChannel(SocketChannel channel) { channel.pipeline().addLast(new FrameDecoder(), new FrameEncoder(), handler); }
                }).bind(host, port).sync().channel();
    }
    void membership(MembershipEvent event) {
        if (raft != null) raft.onMembershipChanged(event); else if (vr != null) vr.onMembershipChanged(event); else if (pb != null) pb.onMembershipChanged(event);
    }
    String json() {
        long leader;
        String role;
        if (raft != null) { var o = raft.observation(); leader = o.leader() == null ? -1 : o.leader().value(); role = o.role().name(); }
        else if (vr != null) { var o = vr.observation(); leader = o.view() % 3; role = o.state().name(); }
        else if (chain != null) { var o = chain.observation(); leader = o.chain().getFirst().value(); role = o.state().name(); }
        else if (rose != null) { var o = rose.observation(); leader = o.primary().value(); role = o.state().name(); }
        else { var f = pb.fenceObservation(); leader = f == null ? (pb.readyAtNanos() >= 0 ? 1 : 0) : f.primary().value(); role = f == null ? self.value() == leader ? "PRIMARY" : "BACKUP" : f.state(); }
        return "{\"node\":" + self.value() + ",\"leader\":" + leader + ",\"role\":" + JsonLine.quote(role)
                + ",\"epoch\":" + engine.snapshot().ownershipEpoch().value() + ",\"peerSentBytes\":" + transport.sentBytes() + extra() + "}";
    }
    private String extra() {
        if (chain != null) {
            var o = chain.observation(); var r = chain.readObservation();
            return ",\"length\":" + o.logLength() + ",\"applied\":" + o.applied() + ",\"acknowledged\":" + o.acknowledged()
                    + ",\"chain\":" + o.chain().stream().map(n -> "" + n.value()).collect(java.util.stream.Collectors.joining(",", "[", "]"))
                    + ",\"localReads\":" + r.localReads() + ",\"dirtyReads\":" + r.dirtyReads();
        }
        if (rose != null) { var o = rose.observation(); return ",\"length\":" + o.logLength() + ",\"applied\":" + o.applied() + ",\"minAck\":" + o.minAck() + ",\"limit\":" + config.number("rose.limit", 4); }
        if (pb != null && pb.fenceObservation() != null) {
            var f = pb.fenceObservation(); return ",\"fenceState\":" + JsonLine.quote(f.state()) + ",\"leaseDeadlineNanos\":" + f.leaseDeadline()
                    + ",\"promotionVotes\":" + f.votes() + ",\"fdDeadNanos\":" + f.deadAt() + ",\"fdSuspectNanos\":" + f.suspectAt();
        }
        return "";
    }
    /** 신뢰하는 실험 master 전용. 영구 crash 확인은 호출자가 보장하고 각 단계 future가 barrier다. */
    CompletionStage<String> master(Map<String, String> p) {
        if (chain == null && rose == null) return CompletableFuture.failedFuture(new IllegalArgumentException("master control requires Chain/Rose"));
        long epoch = Long.parseLong(p.get("epoch"));
        return switch (p.get("op")) {
            case "freeze" -> {
                List<NodeId> survivors = Arrays.stream(p.get("nodes").split(",")).map(x -> new NodeId(Long.parseLong(x))).toList();
                yield (chain != null ? chain.installConfiguration(epoch, survivors) : rose.freezeAfterCrash(epoch)).thenApply(n -> "\"length\":" + n);
            }
            case "repair" -> (chain != null ? chain.beginRepair(epoch) : rose.beginRepair(epoch, new NodeId(Long.parseLong(p.get("primary"))),
                    Arrays.stream(p.get("nodes").split(",")).map(x -> new NodeId(Long.parseLong(x))).toList())).thenApply(v -> "\"repaired\":true");
            case "open" -> (chain != null ? chain.openConfiguration(epoch, Integer.parseInt(p.get("length"))) : rose.openAfterRepair(epoch, Integer.parseInt(p.get("length")))).thenApply(v -> "\"opened\":true");
            default -> CompletableFuture.failedFuture(new IllegalArgumentException("unknown master operation"));
        };
    }
    @Override public void close() {
        ticks.shutdownNow(); if (client != null) client.close().awaitUninterruptibly(); if (peer != null) peer.close().awaitUninterruptibly();
        if (engine != null) engine.close(); if (transport != null) transport.close();
        io.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly(); accept.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
        owner.close(); if (file != null) file.close(); if (metrics != null) metrics.close();
    }
    private static final class BarrierStorage implements CacheEngine {
        private final CacheEngine delegate;
        private final Set<CompletableFuture<Result>> pending = ConcurrentHashMap.newKeySet();
        private final Set<Bytes> keys = ConcurrentHashMap.newKeySet();
        BarrierStorage(CacheEngine delegate) { this.delegate = delegate; }
        @Override public CompletionStage<Result> execute(Command command, RequestContext context) {
            if (command instanceof Command.Count) return CompletableFuture.allOf(pending.toArray(CompletableFuture<?>[]::new))
                    .thenApply(ignored -> Result.ok(Bytes.copyOf(ByteBuffer.allocate(8).putLong(keys.size()).array()), 0, RoutingVersion.ZERO, OwnershipEpoch.ZERO));
            var f = delegate.execute(command, context).toCompletableFuture(); pending.add(f);
            return f.whenComplete((r, e) -> { if (e == null && r.status() == ResultStatus.OK && command instanceof Command.Set s) keys.add(s.key()); pending.remove(f); });
        }
        @Override public void onPeerMessage(NodeId from, PeerMessage message) { delegate.onPeerMessage(from, message); }
        @Override public void onTimer(TimerEvent event) { delegate.onTimer(event); }
        @Override public EngineSnapshot snapshot() { return delegate.snapshot(); }
        @Override public void close() { }
    }
}
