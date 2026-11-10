package io.openkache.server.replication;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.openkache.core.api.*;
import io.openkache.core.failure.SwimConfig;
import io.openkache.core.kv.KvEngine;
import io.openkache.core.model.*;
import io.openkache.core.replication.primarybackup.*;
import io.openkache.core.runtime.*;
import io.openkache.core.replication.vr.*;
import io.openkache.core.replication.raft.*;
import io.openkache.core.replication.chain.*;
import io.openkache.core.replication.rose.*;
import io.openkache.core.spi.*;
import io.openkache.server.failure.SwimRuntime;
import io.openkache.server.failure.netty.SwimDatagramHandler;
import io.openkache.server.failure.netty.SwimUdpTransport;
import io.openkache.server.metrics.ServerMetrics;
import io.openkache.server.netty.*;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * PB 전용 실서버 조립부. 실제 TCP·owner worker·system monotonic 시계를 사용한다.
 * 최초 Primary의 영구 crash 한 번만 지원하며, startup은 최초 heartbeat 수신 뒤 감지를 시작한다.
 * peer 진단은 read-only지만 인증이 없으므로 전용 보안 그룹에만 노출한다.
 */
public final class PbServer implements AutoCloseable {
    private final PbNodeConfig config;
    private final TimeSource time=TimeSource.system();
    private final long startNano=System.nanoTime(),startEpochMs=System.currentTimeMillis();
    private final AtomicBoolean closed=new AtomicBoolean(),heartbeatSeen=new AtomicBoolean();
    private EventLoopGroup io,accept;
    private OwnerWorker protocolWorker,kvWorker;
    private KvEngine kv;
    private PbTransport transport;
    private CacheEngine engine;
    private CrashOnlyPrimaryBackupEngine pbEngine;
    private VrEngine vrEngine;
    private RaftEngine raftEngine;
    private ChainEngine chainEngine;
    private RoseEngine roseEngine;
    /** 외부 master의 FREEZE 제어를 받은 이 JVM의 monotonic 시각. Chain/Rosé는 자체 장애 감지가 없다. */
    private volatile long masterDetectedNs=-1;
    private ServerMetrics metrics;
    private ScheduledExecutorService timer;
    private SwimRuntime swim;
    private Channel client,peer;
    private PbServer(PbNodeConfig config) { this.config=config; }
    /** 동기 bind는 호출 thread에서만 수행한다. 실패하면 이미 시작한 구성 요소도 닫는다. */
    public static PbServer start(PbNodeConfig config) throws InterruptedException {
        PbServer server=new PbServer(config);
        try { server.open(); return server; } catch(RuntimeException | InterruptedException e) { server.close(); throw e; }
    }
    private void open() throws InterruptedException {
        boolean fenced = config.protocol().equals("PB_FENCED");
        if (fenced && config.addresses().values().stream().anyMatch(a -> a.isUnresolved() || !a.getAddress().isLoopbackAddress())) {
            throw new IllegalArgumentException("PB_FENCED prototype requires loopback peers");
        }
        metrics=ServerMetrics.start("127.0.0.1",config.adminPort());
        protocolWorker=new OwnerWorker(new WorkerId(100),4096); kvWorker=new OwnerWorker(new WorkerId(0),4096);
        kv=new KvEngine(new NodeId(config.node()),List.of(kvWorker),time);
        io=new MultiThreadIoEventLoopGroup(2,NioIoHandler.newFactory());
        accept=new MultiThreadIoEventLoopGroup(1,NioIoHandler.newFactory());
        transport=new PbTransport(io,config.addresses(),switch(config.protocol()) {
            case "RAFT" -> RaftWire::encode; case "VR" -> VrWire::encode; case "CHAIN" -> ChainWire::encode; case "ROSE" -> RoseWire::encode; default -> PbWire::encode; });
        List<NodeId> members=List.of(new NodeId(0),new NodeId(1),new NodeId(2));
        if(config.protocol().equals("CHAIN")) {
            chainEngine=new ChainEngine(new NodeId(config.node()),members,1,kv,protocolWorker,transport,time,
                    Duration.ofMillis(config.retryMs()),ChainEngine.ReadMode.valueOf(config.chainReadMode()));
            engine=chainEngine;
        } else if(config.protocol().equals("ROSE")) {
            roseEngine=new RoseEngine(new NodeId(config.node()),members,new NodeId(0),1,config.roseLimit(),kv,protocolWorker,transport,time,
                    Duration.ofMillis(config.retryMs()));
            engine=roseEngine;
        } else if(config.protocol().equals("RAFT")) {
            raftEngine=new RaftEngine(new NodeId(config.node()),List.of(new NodeId(0),new NodeId(1),new NodeId(2)),kv,protocolWorker,transport,time,
                    Duration.ofMillis(config.heartbeatMs()),Duration.ofMillis(config.timeoutMs()),config.electionSeed()+104729L*config.node());
            engine=raftEngine;
        } else if(config.protocol().equals("VR")) {
            vrEngine=new VrEngine(new NodeId(config.node()),List.of(new NodeId(0),new NodeId(1),new NodeId(2)),kv,protocolWorker,transport,time,
                    Duration.ofMillis(config.heartbeatMs()),Duration.ofMillis(config.timeoutMs()),VrEngine.Mode.valueOf(config.vrMode()));
            engine=vrEngine;
        } else {
            pbEngine=new CrashOnlyPrimaryBackupEngine(new PrimaryBackupConfig(new NodeId(config.node()),new NodeId(0),
                List.of(new NodeId(1),new NodeId(2)),new OwnershipEpoch(1)),kv,protocolWorker,transport,time,
                Duration.ofMillis(config.heartbeatMs()),Duration.ofMillis(config.timeoutMs()),
                fenced ? Duration.ofMillis(config.timeoutMs()) : null);
            engine=pbEngine;
        }
        peer=bind(config.peerPort(),new PeerHandler());
        client=bind(config.clientPort(),new ClientRequestHandler(metrics,engine,Duration.ofSeconds(1)));
        if (fenced) {
            NodeId self = new NodeId(config.node());
            var udp = new SwimUdpTransport(self, config.addresses(), io);
            var fdConfig = new SwimConfig(Duration.ofMillis(200), Duration.ofMillis(60), Duration.ofMillis(100),
                    Duration.ofMillis(600), 1, 3, 4);
            swim = new SwimRuntime(self, members, fdConfig, udp, pbEngine::onMembershipChanged, 101, config.electionSeed() + config.node());
            udp.bind(config.hosts().get(config.node()), config.peerPort(), new SwimDatagramHandler(swim, metrics));
            swim.start();
        }
        timer=Executors.newSingleThreadScheduledExecutor(r->new Thread(r,"pb-timer-"+config.node()));
        timer.scheduleAtFixedRate(()->{
            if(fenced || raftEngine!=null || chainEngine!=null || roseEngine!=null || config.node()==0 || heartbeatSeen.get()) engine.onTimer(new TimerEvent(tickName(),time.monotonicNanos()));
        },0,config.tickMs(),TimeUnit.MILLISECONDS);
    }
    private String tickName() {
        return raftEngine!=null?RaftEngine.TICK:chainEngine!=null?ChainEngine.TICK:roseEngine!=null?RoseEngine.TICK:vrEngine!=null?VrEngine.TICK:CrashOnlyPrimaryBackupEngine.TICK;
    }
    private Channel bind(int port,ChannelHandler handler) throws InterruptedException {
        return new ServerBootstrap().group(accept,io).channel(NioServerSocketChannel.class).childOption(ChannelOption.TCP_NODELAY,true)
            .childHandler(new ChannelInitializer<SocketChannel>() {
                @Override protected void initChannel(SocketChannel ch) { ch.pipeline().addLast(new FrameDecoder(),new FrameEncoder(),handler); }
            }).bind(config.hosts().get(config.node()),port).sync().channel();
    }
    @ChannelHandler.Sharable
    private final class PeerHandler extends SimpleChannelInboundHandler<Frame> {
        @Override protected void channelRead0(ChannelHandlerContext ctx,Frame frame) {
            if(frame.messageClass()!=Protocol.CLASS_PEER || frame.flags()!=0 || frame.status()!=0) { respond(ctx,frame,ResultStatus.BAD_REQUEST,"invalid peer header"); return; }
            try {
                if((pbEngine!=null && frame.opcode()>=PbWire.REPLICATE && frame.opcode()<=PbWire.CONTROL)
                        || (config.protocol().equals("PB_FENCED") && frame.opcode()==PbWire.FENCE)
                        || (vrEngine!=null && frame.opcode()>=VrWire.PREPARE && frame.opcode()<=VrWire.CHANGE_ACK)
                        || (raftEngine!=null && frame.opcode()>=RaftWire.VOTE && frame.opcode()<=RaftWire.APPEND_REPLY)
                        || (chainEngine!=null && frame.opcode()>=ChainWire.FORWARD && frame.opcode()<=ChainWire.VERSION_REPLY)
                        || (roseEngine!=null && frame.opcode()>=RoseWire.APPEND && frame.opcode()<=RoseWire.APPLIED)) {
                    PeerEnvelope e=raftEngine!=null?RaftWire.decode(frame):vrEngine!=null?VrWire.decode(frame)
                            :chainEngine!=null?ChainWire.decode(frame):roseEngine!=null?RoseWire.decode(frame):PbWire.decode(frame);
                    if(e.to().value()!=config.node() || e.from().value()>2 || e.from().equals(e.to()))
                        throw new IllegalArgumentException("unknown/misdirected PB peer");
                    engine.onPeerMessage(e.from(),e.message());
                    if(e.message() instanceof RaftMessage.Append) heartbeatSeen.set(true);
                    if(e.from().value()==0 && e.message() instanceof VrMessage m && m.view()==0
                            && (m instanceof VrMessage.Commit || m instanceof VrMessage.Prepare || m instanceof VrMessage.DeltaPrepare || m instanceof VrMessage.StartView)) heartbeatSeen.set(true);
                    if(e.from().value()==0 && e.message() instanceof CrashOnlyPrimaryBackupEngine.Control c
                            && c.kind()==CrashOnlyPrimaryBackupEngine.Kind.HEARTBEAT && c.epoch().value()==1) heartbeatSeen.set(true);
                } else if(frame.opcode()==PbWire.STATUS && frame.body().isEmpty()) {
                    ctx.writeAndFlush(Frame.response(frame.requestId(),frame.opcode(),
                        Result.ok(Bytes.utf8(statusJson()),0,RoutingVersion.ZERO,engine.snapshot().ownershipEpoch()),""));
                } else if(frame.opcode()==PbWire.LOCAL_GET) {
                    if (config.protocol().equals("PB_FENCED")) {
                        respond(ctx,frame,ResultStatus.NOT_IMPLEMENTED,"local diagnostic reads disabled in fenced mode");
                        return;
                    }
                    var context=new RequestContext(new ClientId(0),new OperationId(frame.requestId()),RoutingVersion.ZERO,
                            engine.snapshot().ownershipEpoch(),Deadline.after(time,Duration.ofSeconds(1)));
                    kv.execute(new Command.Get(frame.body()),context).whenComplete((value,error)->{
                        if(error==null) ctx.writeAndFlush(Frame.response(frame.requestId(),frame.opcode(),value,""));
                        else respond(ctx,frame,ResultStatus.UNAVAILABLE,"diagnostic read failed");
                    });
                } else if(frame.opcode()==PbWire.LOG_DIGEST && frame.body().isEmpty()) {
                    if(chainEngine==null && roseEngine==null) { respond(ctx,frame,ResultStatus.NOT_IMPLEMENTED,"log digest only for CHAIN/ROSE"); return; }
                    var snapshot=chainEngine!=null?chainEngine.logSnapshot().thenApply(log->digest(log.size(),log.stream().map(r->new RequestCodec.Decoded(r.client(),r.operation(),r.command())).toList()))
                            :roseEngine.logSnapshot().thenApply(log->digest(log.size(),log.stream().map(r->new RequestCodec.Decoded(r.client(),r.operation(),r.command())).toList()));
                    snapshot.whenComplete((value,error)->{
                        if(error==null) ctx.writeAndFlush(Frame.response(frame.requestId(),frame.opcode(),Result.ok(value,0,RoutingVersion.ZERO,engine.snapshot().ownershipEpoch()),""));
                        else respond(ctx,frame,ResultStatus.UNAVAILABLE,"log digest failed");
                    });
                } else if(frame.opcode()==PbWire.MASTER_CONTROL) {
                    masterControl(ctx,frame);
                } else if(frame.opcode()==Protocol.OP_PING && frame.body().isEmpty()) respond(ctx,frame,ResultStatus.OK,"");
                else respond(ctx,frame,ResultStatus.NOT_IMPLEMENTED,"unknown PB peer opcode");
            } catch(IllegalArgumentException e) { respond(ctx,frame,ResultStatus.BAD_REQUEST,e.getMessage()); }
        }
        @Override public void exceptionCaught(ChannelHandlerContext ctx,Throwable error) { ctx.close(); }
        private void respond(ChannelHandlerContext ctx,Frame f,ResultStatus status,String detail) {
            ctx.writeAndFlush(Frame.response(f.requestId(),f.opcode(),status,detail));
        }
        /**
         * 신뢰하는 외부 master(실험 controller) 전용 제어다. kind 0=FREEZE(newEpoch, count, survivors[u64]),
         * 1=REPAIR(epoch[, newPrimary, count, survivors]), 2=OPEN(epoch, length). 엔진 future 완료 후 응답하며 EventLoop를 막지 않는다.
         * 정확한 영구 crash 전제를 controller가 보장해야 하며 분단 fencing이나 제품 API가 아니다.
         */
        private void masterControl(ChannelHandlerContext ctx,Frame frame) {
            if(chainEngine==null && roseEngine==null) { respond(ctx,frame,ResultStatus.NOT_IMPLEMENTED,"master control only for CHAIN/ROSE"); return; }
            java.util.concurrent.CompletionStage<String> outcome;
            try {
                var in=new java.io.DataInputStream(new java.io.ByteArrayInputStream(frame.body().toByteArray()));
                int kind=in.readUnsignedByte(); long epoch=in.readLong();
                List<NodeId> survivors=List.of(); NodeId newPrimary=null; int length=0;
                // 형식 전체를 검증한 뒤 owner mailbox에 상태 변경을 넣는다.
                if(kind==0) {
                    if(chainEngine!=null) survivors=nodes(in);
                } else if(kind==1) {
                    if(roseEngine!=null) { newPrimary=new NodeId(in.readLong()); survivors=nodes(in); }
                } else if(kind==2) length=in.readInt();
                else throw new IllegalArgumentException("unknown master control kind");
                if(in.available()!=0) throw new IllegalArgumentException("trailing master control bytes");
                if(kind==0) {
                    long receivedNs=time.monotonicNanos();
                    boolean newDetection=masterDetectedNs<0 || epoch>engine.snapshot().ownershipEpoch().value();
                    var frozen=chainEngine!=null?chainEngine.installConfiguration(epoch,survivors):roseEngine.freezeAfterCrash(epoch);
                    outcome=frozen.thenApply(applied->{
                        if(newDetection) masterDetectedNs=receivedNs;
                        return "applied="+applied;
                    });
                } else if(kind==1) {
                    if(chainEngine!=null) outcome=chainEngine.beginRepair(epoch).thenApply(v->"repairing");
                    else outcome=roseEngine.beginRepair(epoch,newPrimary,survivors).thenApply(v->"repairing");
                } else {
                    outcome=chainEngine!=null?chainEngine.openConfiguration(epoch,length).thenApply(v->"open"):roseEngine.openAfterRepair(epoch,length).thenApply(v->"open");
                }
            } catch(java.io.IOException | RuntimeException e) { respond(ctx,frame,ResultStatus.BAD_REQUEST,"invalid master control: "+e.getMessage()); return; }
            outcome.whenComplete((detail,error)->{
                if(error==null) respond(ctx,frame,ResultStatus.OK,detail);
                else respond(ctx,frame,ResultStatus.UNAVAILABLE,"master control rejected: "+(error.getCause()==null?error:error.getCause()).getMessage());
            });
        }
        private List<NodeId> nodes(java.io.DataInputStream in) throws java.io.IOException {
            int count=in.readInt(); if(count<1 || count>3) throw new IllegalArgumentException("invalid survivor count");
            var list=new java.util.ArrayList<NodeId>(count);
            for(int i=0;i<count;i++) { long id=in.readLong(); if(id<0 || id>2) throw new IllegalArgumentException("unknown survivor"); list.add(new NodeId(id)); }
            return list;
        }
    }
    /** length(u32) + SHA-256(client,op,commandTag,key,value 순서 직렬화). 항목별로 digest에 넣으므로 로그 전체가 frame 한도를 넘어도 된다. 생존자 로그 일치 검사 전용이다. */
    private static Bytes digest(int length,List<RequestCodec.Decoded> log) {
        try {
            var sha=java.security.MessageDigest.getInstance("SHA-256"); var bytes=new java.io.ByteArrayOutputStream();
            for(var r:log) {
                bytes.reset(); RequestCodec.request(new java.io.DataOutputStream(bytes),r.client(),r.operation(),r.command()); sha.update(bytes.toByteArray());
            }
            var result=java.nio.ByteBuffer.allocate(36).putInt(length).put(sha.digest()).array();
            return Bytes.copyOf(result);
        } catch(java.io.IOException | java.security.NoSuchAlgorithmException e) { throw new IllegalStateException(e); }
    }
    /** 이 JVM의 monotonic 시각이다. 다른 호스트의 nanoTime과 직접 차감하면 안 된다. */
    public String statusJson() {
        return "{\"protocol\":\""+config.protocol()+"\",\"node\":"+config.node()+",\"epoch\":"+engine.snapshot().ownershipEpoch().value()+",\"heartbeatSeen\":"+heartbeatSeen.get()
                +",\"nanoTime\":"+time.monotonicNanos()+",\"detectedNs\":"+detectedAt()+",\"readyNs\":"+readyAt()
                +",\"bootEpochMs\":"+startEpochMs+",\"uptimeNs\":"+(System.nanoTime()-startNano)
                +vrStatus()+raftStatus()+chainStatus()+roseStatus()+fenceStatus()+",\"peerTx\":"+transport.trafficJson()+",\"sentBytes\":"+transport.sentBytes()+",\"transportFailures\":"+transport.failures()+"}";
    }
    private String fenceStatus() {
        var o = pbEngine == null ? null : pbEngine.fenceObservation();
        if (o == null) return "";
        return ",\"primary\":" + o.primary().value() + ",\"suspectNs\":" + o.suspectAt() + ",\"quorumNs\":" + o.quorumAt()
                + ",\"promotionAuthorizedNs\":" + o.authorizedAt()
                + ",\"installedNs\":" + o.installedAt() + ",\"peerReadyNs\":" + o.peerReadyAt()
                + ",\"leaseDeadlineNs\":" + o.leaseDeadline() + ",\"promotionVotes\":" + o.votes()
                + ",\"fenceState\":\"" + o.state() + "\"";
    }
    /** mainの待機に使うclient listenerのclose future。 */
    private long detectedAt() {
        if(chainEngine!=null || roseEngine!=null) return masterDetectedNs;
        return raftEngine!=null?raftEngine.electionObservation().detectedAt():vrEngine==null?pbEngine.detectedAtNanos():vrEngine.observation().detectedAt();
    }
    private long readyAt() {
        if(chainEngine!=null) return chainEngine.observation().readyAt();
        if(roseEngine!=null) return roseEngine.observation().readyAt();
        return raftEngine!=null?raftEngine.electionObservation().readyAt():vrEngine==null?pbEngine.readyAtNanos():vrEngine.observation().readyAt();
    }
    private String chainStatus() {
        if(chainEngine==null) return "";
        var o=chainEngine.observation(); var r=chainEngine.readObservation();
        String chain=o.chain().stream().map(n->Long.toString(n.value())).collect(java.util.stream.Collectors.joining(",","[","]"));
        return ",\"readMode\":\""+config.chainReadMode()+"\",\"state\":\""+o.state()+"\",\"chain\":"+chain+",\"logLength\":"+o.logLength()+",\"applied\":"+o.applied()
                +",\"acknowledged\":"+o.acknowledged()+",\"sent\":"+o.sent()+",\"localReads\":"+r.localReads()+",\"queryAttempts\":"+r.queryAttempts()
                +",\"dirtyReads\":"+r.dirtyReads()+",\"retainedVersions\":"+r.retainedVersions()+",\"pendingReads\":"+r.pendingReads();
    }
    private String roseStatus() {
        if(roseEngine==null) return "";
        var o=roseEngine.observation();
        return ",\"limit\":"+config.roseLimit()+",\"state\":\""+o.state()+"\",\"primary\":"+o.primary().value()+",\"logLength\":"+o.logLength()+",\"applied\":"+o.applied()
                +",\"minAck\":"+o.minAck()+",\"peakLag\":"+o.peakLag()+",\"backpressureCount\":"+o.backpressureCount()+",\"deferred\":"+o.deferred();
    }
    private String vrStatus() {
        if(vrEngine==null) return "";
        var o=vrEngine.observation();
        String reasons=vrEngine.trafficCounts().entrySet().stream().map(e->"\""+e.getKey()+"\":"+e.getValue()).collect(java.util.stream.Collectors.joining(",","{","}"));
        return ",\"vrMode\":\""+config.vrMode()+"\",\"state\":\""+o.state()+"\",\"logLength\":"+o.logLength()+",\"commit\":"+o.commit()+",\"applied\":"+o.applied()+",\"vrTraffic\":"+reasons;
    }
    private String raftStatus() {
        if(raftEngine==null) return "";
        var o=raftEngine.observation(); var election=raftEngine.electionObservation();
        return ",\"role\":\""+o.role()+"\",\"leader\":"+(o.leader()==null?-1:o.leader().value())
                +",\"logLength\":"+o.logLength()+",\"commit\":"+o.commit()+",\"applied\":"+o.applied()
                +",\"elections\":"+o.elections()+",\"electionTerm\":"+election.term()+",\"electionSeed\":"+config.electionSeed();
    }
    public void awaitTermination() throws InterruptedException { client.closeFuture().sync(); }
    @Override public void close() {
        if(!closed.compareAndSet(false,true)) return;
        if(timer!=null) timer.shutdownNow();
        if(swim!=null) swim.close();
        if(client!=null) client.close().awaitUninterruptibly();
        if(peer!=null) peer.close().awaitUninterruptibly();
        if(engine!=null) engine.close();
        if(transport!=null) transport.close();
        if(io!=null) io.shutdownGracefully(0,2,TimeUnit.SECONDS).awaitUninterruptibly();
        if(accept!=null) accept.shutdownGracefully(0,2,TimeUnit.SECONDS).awaitUninterruptibly();
        if(kv!=null) kv.close();
        if(protocolWorker!=null) protocolWorker.close();
        if(kvWorker!=null) kvWorker.close();
        if(metrics!=null) metrics.close();
    }
}
