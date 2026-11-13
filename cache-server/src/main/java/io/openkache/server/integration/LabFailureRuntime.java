package io.openkache.server.integration;

import com.openkache.failuredetector.core.*;
import com.openkache.failuredetector.deadline.FixedDeadlineFailureDetector;
import com.openkache.failuredetector.adaptive.AdaptiveDeadlineFailureDetector;
import com.openkache.failuredetector.phi.*;
import com.openkache.failuredetector.swim.SwimLifeguardDetector;
import com.openkache.failuredetector.redis.RedisClusterFailureDetector;
import com.openkache.failuredetector.ignite.IgniteTcpRingFailureDetector;
import com.openkache.failuredetector.aerospike.AerospikeMeshFailureDetector;
import io.openkache.core.failure.*;
import io.openkache.core.model.NodeId;
import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Real-socket, group-wide adapter for the original Lab decision machines. All decisions,
 * correlation tables, heartbeat histories and topology changes belong to one state thread.
 * UDP handles probes/reports/gossip; Ignite and Aerospike retain persistent TCP heartbeats.
 * A received failure notification triggers the native replication protocol, never grants ownership.
 */
final class LabFailureRuntime implements FailureRuntime {
    private enum Type { HEARTBEAT, GOSSIP, PING, ACK, RELAY, NACK, SUSPECT, ALIVE, REDIS_REPORT, REDIS_FAIL,
        IGNITE_REPORT, IGNITE_COMMIT, AERO_PROPOSE, AERO_ACK, AERO_COMMIT }
    private enum Purpose { SWIM, VERIFY, IGNITE_LOCAL, IGNITE_REMOTE }
    private record Frame(Type type, int from, int to, int origin, int target, long sequence, long incarnation, long epoch, int evidence) { }
    private static final class Probe {
        final int target, reporter; final long sequence; final Purpose purpose;
        final Set<Integer> helpers = new HashSet<>(), negatives = new HashSet<>();
        long due; boolean indirect;
        Probe(int target, int reporter, long sequence, Purpose purpose, long due) { this.target=target; this.reporter=reporter; this.sequence=sequence; this.purpose=purpose; this.due=due; }
    }
    private record Relay(int origin, int target, long sequence) { }
    private final IntegrationConfig config;
    private final int self, coordinator;
    private final String method, identity;
    private final List<String> names = List.of("node-0", "node-1", "node-2");
    private final List<InetSocketAddress> addresses;
    private final FailureDetector detector;
    private final Random random;
    private final MembershipListener listener;
    private final JsonLine journal;
    private final DatagramSocket udp;
    private final ServerSocket tcp;
    private final ScheduledExecutorService owner = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService io = Executors.newVirtualThreadPerTaskExecutor();
    private final Semaphore pendingInput = new Semaphore(1024);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<Socket> accepted = ConcurrentHashMap.newKeySet();
    private final Map<Integer, TcpLink> links = new HashMap<>();
    private final Map<Long, Probe> probes = new HashMap<>();
    private final Map<Relay, Long> relays = new HashMap<>();
    private final Map<Integer, MemberStatus> memberStates = new TreeMap<>();
    private final Map<Integer, Long> incarnations = new HashMap<>(), nextVerification = new HashMap<>();
    private final Map<Long, Integer> proposalTargets = new HashMap<>(), proposalVoters = new HashMap<>();
    private final AtomicLong udpSent = new AtomicLong(), udpBytes = new AtomicLong(), tcpSent = new AtomicLong(), tcpBytes = new AtomicLong(), received = new AtomicLong(), failures = new AtomicLong(), rejected = new AtomicLong();
    private final long began = System.nanoTime(), graceNanos, localGeneration = System.currentTimeMillis();
    private final Map<Long, Long> proposalSentAt = new HashMap<>();
    private long incarnation = 1, sequence, nextTraffic, lastTick, buddyNotifications, helperNegatives, topologyVersion, latestProposal;
    private volatile String fatal, snapshot = "{}";

    LabFailureRuntime(IntegrationConfig config, int group, int index, MembershipListener listener, JsonLine journal) throws IOException {
        this.config=config; self=index; coordinator=config.number("fd.coordinator.index",2); this.listener=listener; this.journal=journal; method=config.failureMethod();
        random=new Random(Long.parseLong(config.text("failure.seed","1"))+index*7919L);
        identity=config.runId()+":"+config.protocol()+":"+group+":"+method;
        addresses=List.copyOf(config.addresses(group,config.ionia()?1:2).values());
        graceNanos=TimeUnit.MILLISECONDS.toNanos(config.number("failure.grace.ms",3000));
        detector=FailureMethods.create(config,self,names,method);
        publish();
        udp=new DatagramSocket(null); ServerSocket server=null;
        try {
            udp.bind(addresses.get(self));
            if (usesTcp()) { server=new ServerSocket(); server.setReuseAddress(true); server.bind(addresses.get(self)); }
            tcp=server;
            for (int target : observed()) {
                if (detector instanceof FixedDeadlineFailureDetector d) d.registerTarget(0,names.get(target),0);
                if (detector instanceof AdaptiveDeadlineFailureDetector d) d.registerTarget(0,names.get(target),0);
                if (detector instanceof RedisClusterFailureDetector d) d.registerTarget(0,names.get(target));
            }
            if (usesTcp()) for (int target=0;target<3;target++) if(target!=self) links.put(target,new TcpLink(target));
            io.execute(this::receiveUdp);
            if(tcp!=null) io.execute(this::acceptTcp);
            owner.scheduleAtFixedRate(this::tickSafely,0,20,TimeUnit.MILLISECONDS);
            journal.event("lab-fd-started","\"method\":"+JsonLine.quote(method)+",\"node\":"+self+",\"transport\":"+JsonLine.quote(usesTcp()?"persistent-tcp+udp":"udp")
                    +",\"heartbeatMs\":"+heartbeatMs()+",\"deadlineMs\":"+FailureMethods.deadline(config.values())+",\"nativeWatchdogMs\":"+config.nativeWatchdogMillis());
        } catch (IOException | RuntimeException e) {
            udp.close(); if(server!=null) try { server.close(); } catch(IOException ignored) { } owner.shutdownNow(); io.shutdownNow(); throw e;
        }
    }
    private boolean usesTcp() { return method.equals("IGNITE_TCP_RING")||method.equals("AEROSPIKE_MESH"); }
    private long now() { return Math.max(0,System.nanoTime()-began-graceNanos); }
    private int heartbeatMs() { return FailureMethods.heartbeat(config.values()); }
    private long probeTimeout() { return TimeUnit.MILLISECONDS.toNanos(config.number("fd.probe.timeout.ms",100)); }
    private Set<Integer> currentMembers() {
        if(detector instanceof IgniteTcpRingFailureDetector d) return ids(d.members());
        if(detector instanceof AerospikeMeshFailureDetector d) return ids(d.members());
        return Set.of(0,1,2);
    }
    private Set<Integer> ids(Set<String> members) { var out=new TreeSet<Integer>(); for(int i=0;i<3;i++)if(members.contains(names.get(i)))out.add(i); return out; }
    private List<Integer> observed() {
        if(!FailureMethods.ring(method))return List.of((self+1)%3,(self+2)%3);
        return java.util.stream.IntStream.rangeClosed(1,config.number("fd.ring.successors",2)).map(i->(self+i)%3).boxed().toList();
    }
    private int next(int node) {
        List<Integer> members=new ArrayList<>(currentMembers()); int at=members.indexOf(node);
        return at<0||members.size()<2?-1:members.get((at+1)%members.size());
    }
    private Frame frame(Type type,int destination,int origin,int target,long seq,long inc,long epoch,int evidence) { return new Frame(type,self,destination,origin,target,seq,inc,epoch,evidence); }
    private Frame simple(Type type,int destination,int target) { return frame(type,destination,self,target,++sequence,incarnation,0,0); }
    private byte[] encode(Frame f) throws IOException {
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes);
        out.writeInt(0x4f4b4644);out.writeByte(1);out.writeUTF(identity);out.writeByte(f.type().ordinal());
        out.writeInt(f.from());out.writeInt(f.to());out.writeInt(f.origin());out.writeInt(f.target());
        out.writeLong(f.sequence());out.writeLong(f.incarnation());out.writeLong(f.epoch());out.writeInt(f.evidence());out.flush();
        if(bytes.size()>512)throw new IOException("FD frame capacity");return bytes.toByteArray();
    }
    private Frame decode(byte[] data) throws IOException {
        if(data.length>512)throw new IOException("FD frame capacity");var in=new DataInputStream(new ByteArrayInputStream(data));
        if(in.readInt()!=0x4f4b4644||in.readUnsignedByte()!=1||!in.readUTF().equals(identity))throw new IOException("FD version/run/group/layer mismatch");
        int type=in.readUnsignedByte();if(type>=Type.values().length)throw new IOException("FD type");
        Frame f=new Frame(Type.values()[type],in.readInt(),in.readInt(),in.readInt(),in.readInt(),in.readLong(),in.readLong(),in.readLong(),in.readInt());
        if(in.available()!=0||f.from()<0||f.from()>2||f.from()==self||f.to()!=self||f.origin()<0||f.origin()>2||f.target()<0||f.target()>2||f.sequence()<1||f.incarnation()<0||f.epoch()<0||f.evidence()<0||f.evidence()>7)throw new IOException("FD frame fields");
        return f;
    }
    private void receiveUdp() {
        byte[] buffer=new byte[513];
        while(!closed.get())try {
            DatagramPacket packet=new DatagramPacket(buffer,buffer.length);udp.receive(packet);Frame f=decode(Arrays.copyOf(packet.getData(),packet.getLength()));
            var address=addresses.get(f.from());
            if(!address.getAddress().equals(packet.getAddress())||address.getPort()!=packet.getPort())throw new IOException("FD UDP source mismatch");
            enqueue(f);
        } catch(IOException|RuntimeException e) { if(!closed.get())rejected.incrementAndGet(); }
    }
    private void acceptTcp() {
        while(!closed.get())try { Socket socket=tcp.accept();socket.setSoTimeout(5000);socket.setTcpNoDelay(true);accepted.add(socket);io.execute(()->readTcp(socket)); }
        catch(IOException|RejectedExecutionException e) { if(!closed.get())failures.incrementAndGet(); }
    }
    private void readTcp(Socket socket) {
        try(socket) {
            var in=new DataInputStream(socket.getInputStream());
            while(!closed.get()) {
                int length=in.readInt();if(length<1||length>512)throw new IOException("FD TCP frame length");byte[] bytes=in.readNBytes(length);if(bytes.length!=length)throw new EOFException();
                Frame f=decode(bytes);if(!addresses.get(f.from()).getAddress().equals(socket.getInetAddress())||f.type()!=Type.HEARTBEAT||f.target()!=f.from())throw new IOException("FD TCP source/type");enqueue(f);
            }
        } catch(IOException|RuntimeException e) { if(!closed.get())failures.incrementAndGet(); }
        finally { accepted.remove(socket); }
    }
    private void enqueue(Frame f) {
        if(!pendingInput.tryAcquire()){rejected.incrementAndGet();return;}
        try {owner.execute(()->{try{received.incrementAndGet();onMessage(f);}catch(RuntimeException e){fatal(e);}finally{pendingInput.release();}});}
        catch(RejectedExecutionException e){pendingInput.release();}
    }
    private void send(Frame f) {
        if(closed.get())return;
        try {byte[] data=encode(f);udp.send(new DatagramPacket(data,data.length,addresses.get(f.to())));udpSent.incrementAndGet();udpBytes.addAndGet(data.length);}
        catch(IOException e){if(!closed.get())failures.incrementAndGet();}
    }
    private void broadcast(Type type,int target,long inc,long epoch,int evidence) {
        long seq=++sequence;for(int peer=0;peer<3;peer++)if(peer!=self)send(frame(type,peer,self,target,seq,inc,epoch,evidence));
    }
    private final class TcpLink implements AutoCloseable {
        private final int target; private final ArrayBlockingQueue<byte[]> queue=new ArrayBlockingQueue<>(16); private volatile Socket socket;
        TcpLink(int target){this.target=target;io.execute(this::run);}
        void heartbeat(Frame f){try{if(!queue.offer(encode(f)))rejected.incrementAndGet();}catch(IOException e){failures.incrementAndGet();}}
        private void run(){
            DataOutputStream out=null;
            while(!closed.get())try {
                byte[] payload=queue.poll(200,TimeUnit.MILLISECONDS);if(payload==null)continue;
                if(socket==null){Socket opening=new Socket();socket=opening;opening.setTcpNoDelay(true);opening.bind(new InetSocketAddress(addresses.get(self).getAddress(),0));opening.connect(addresses.get(target),300);out=new DataOutputStream(opening.getOutputStream());}
                out.writeInt(payload.length);out.write(payload);out.flush();tcpSent.incrementAndGet();tcpBytes.addAndGet(payload.length+4L);
            }catch(InterruptedException e){Thread.currentThread().interrupt();break;}catch(IOException|RuntimeException e){failures.incrementAndGet();closeSocket();out=null;}
            closeSocket();
        }
        private void closeSocket(){Socket old=socket;socket=null;if(old!=null)try{old.close();}catch(IOException ignored){}}
        @Override public void close(){closeSocket();queue.clear();}
    }
    private void tickSafely(){try{tick();}catch(RuntimeException e){fatal(e);}}
    private void fatal(RuntimeException e){if(fatal==null){fatal=e.toString();journal.event("lab-fd-error","\"method\":"+JsonLine.quote(method)+",\"error\":"+JsonLine.quote(fatal));}}
    private void tick(){
        if(closed.get()||fatal!=null)return;
        if(System.nanoTime()-began<graceNanos){publish();return;}
        long now=now();
        if(detector instanceof SwimLifeguardDetector d){if(lastTick>0)d.observeLocalDelay(now-lastTick);}
        lastTick=now;
        for(var entry:new ArrayList<>(relays.entrySet()))if(now>=entry.getValue()){
            Relay r=entry.getKey();relays.remove(r);send(frame(Type.NACK,r.origin(),r.origin(),r.target(),r.sequence(),0,0,0));
        }
        for(Probe p:new ArrayList<>(probes.values()))if(now>=p.due){
            if(p.purpose==Purpose.SWIM&&!p.indirect){p.indirect=true;p.due=now+probeTimeout();for(int helper:helpers(p.target)){p.helpers.add(helper);send(frame(Type.RELAY,helper,self,p.target,p.sequence,0,0,0));}}
            else finish(p,false);
        }
        detector.tick(now);drain();
        if(now>=nextTraffic){
            if(detector instanceof SwimLifeguardDetector d){int target=(self+1+random.nextInt(2))%3;startProbe(target,Purpose.SWIM,self,false);nextTraffic=now+d.effectiveProbeIntervalNanos();}
            else{traffic();nextTraffic=now+TimeUnit.MILLISECONDS.toNanos(heartbeatMs());}
        }
        if(detector instanceof FixedDeadlineFailureDetector d&&method.equals("F6"))for(int target:observed()){
            if(detector.status(names.get(target))==NodeStatus.SUSPECT&&now>=nextVerification.getOrDefault(target,0L)&&d.beginVerification(names.get(target))){startProbe(target,Purpose.VERIFY,self,true);nextVerification.put(target,now+probeTimeout());}
        }
        if(detector instanceof RedisClusterFailureDetector)for(int target:observed())if(detector.status(names.get(target))==NodeStatus.SUSPECT)broadcast(Type.REDIS_REPORT,target,0,0,0);
        if(detector instanceof IgniteTcpRingFailureDetector d && self==coordinator) for(int target:currentMembers()) if(target!=self&&d.status(names.get(target))==NodeStatus.SUSPECT) startProbe(target,Purpose.IGNITE_LOCAL,self,true);
        if(detector instanceof AerospikeMeshFailureDetector d)for(int target:new TreeSet<>(currentMembers()))if(target!=self&&d.status(names.get(target))==NodeStatus.SUSPECT){
            boolean pending=proposalTargets.entrySet().stream().anyMatch(e->e.getValue()==target&&e.getKey()>d.viewVersion());
            if(!pending){long epoch=Math.max(d.viewVersion()+1,++latestProposal*3+self+1);proposalTargets.put(epoch,target);proposalVoters.put(epoch,1<<self);if(d.proposeMembershipRemoval(epoch,names.get(target))){broadcast(Type.AERO_PROPOSE,target,0,epoch,1<<self);proposalSentAt.put(epoch,now);}}
        }
        if(detector instanceof AerospikeMeshFailureDetector d) {
            for(var entry:new ArrayList<>(proposalTargets.entrySet())) {
                long epoch=entry.getKey();int target=entry.getValue();
                if(epoch<=d.viewVersion()||!d.members().contains(names.get(target))){proposalTargets.remove(epoch);proposalVoters.remove(epoch);proposalSentAt.remove(epoch);}
                else if(now-proposalSentAt.getOrDefault(epoch,0L)>=TimeUnit.MILLISECONDS.toNanos(heartbeatMs())){broadcast(Type.AERO_PROPOSE,target,0,epoch,1<<self);proposalSentAt.put(epoch,now);}
            }
        }
        drain();publish();
    }
    private void traffic(){
        long version=++sequence;
        if(detector instanceof IgniteTcpRingFailureDetector){
            for(int peer:currentMembers())if(next(peer)==self)links.get(peer).heartbeat(frame(Type.HEARTBEAT,peer,self,self,version,localGeneration,0,0));
            int neighbor=next(self);if(self!=coordinator&&neighbor>=0&&detector.status(names.get(neighbor))==NodeStatus.SUSPECT)send(simple(Type.IGNITE_REPORT,coordinator,neighbor));return;
        }
        if(detector instanceof AerospikeMeshFailureDetector){for(int peer:currentMembers())if(peer!=self)links.get(peer).heartbeat(frame(Type.HEARTBEAT,peer,self,self,version,localGeneration,0,0));return;}
        if(detector instanceof RedisClusterFailureDetector d){for(int target:observed()){d.recordPingSent(now(),names.get(target));send(frame(Type.PING,target,self,target,version,incarnation,0,0));}return;}
        List<Integer> destinations=detector instanceof PhiAccrualFailureDetector ? java.util.stream.IntStream.rangeClosed(1,config.number("fd.gossip.fanout",1)).map(i->(self+i)%3).boxed().toList() : FailureMethods.ring(method)?java.util.stream.IntStream.rangeClosed(1,config.number("fd.ring.successors",2)).map(i->Math.floorMod(self-i,3)).boxed().toList():observed();
        for(int peer:destinations)send(frame(Type.HEARTBEAT,peer,self,self,version,localGeneration,0,0));
    }
    private List<Integer> helpers(int target){return java.util.stream.IntStream.range(0,3).filter(i->i!=self&&i!=target).limit(config.number("fd.verify.helpers",1)).boxed().toList();}
    private void startProbe(int target,Purpose purpose,int reporter,boolean indirect){
        if(probes.values().stream().anyMatch(p->p.target==target&&p.purpose==purpose))return;
        if(probes.size()>=32){rejected.incrementAndGet();return;}
        Probe p=new Probe(target,reporter,++sequence,purpose,now()+probeTimeout()*(indirect?3:1));p.indirect=indirect;probes.put(p.sequence,p);
        if(indirect)for(int helper:helpers(target)){p.helpers.add(helper);send(frame(Type.RELAY,helper,self,target,p.sequence,0,0,0));}
        else send(frame(Type.PING,target,self,target,p.sequence,incarnation,0,0));
    }
    private void finish(Probe p,boolean success){
        if(probes.remove(p.sequence)==null)return;
        if(p.purpose==Purpose.SWIM)((SwimLifeguardDetector)detector).recordProbe(now(),names.get(p.target),success,incarnations.getOrDefault(p.target,0L));
        else if(p.purpose==Purpose.VERIFY){FixedDeadlineFailureDetector d=(FixedDeadlineFailureDetector)detector;
            if(success||p.negatives.size()>=config.number("fd.verify.quorum",1))d.recordVerification(now(),names.get(p.target),success);else d.cancelVerification(names.get(p.target));
        }else if(!success){
            IgniteTcpRingFailureDetector d=(IgniteTcpRingFailureDetector)detector;
            int reporter=p.purpose==Purpose.IGNITE_REMOTE?p.reporter:p.negatives.stream().findFirst().orElse(-1);
            if(p.purpose==Purpose.IGNITE_REMOTE)d.recordCoordinatorValidation(now(),names.get(reporter),names.get(p.target));
            if(reporter>=0&&d.coordinatorConfirmFailure(now(),names.get(reporter),names.get(p.target))){topologyVersion=d.viewVersion();broadcast(Type.IGNITE_COMMIT,p.target,0,topologyVersion,(1<<self)|(1<<reporter));}
        }
        drain();
    }
    private void onMessage(Frame f){
        long now=now();
        switch(f.type()){
            case HEARTBEAT,GOSSIP -> {
                int origin=f.origin();if(f.target()!=origin||origin==self)return;
                if(detector instanceof PhiAccrualFailureDetector d){
                    boolean accepted=d.recordHeartbeat(new GossipHeartbeat(names.get(origin),f.incarnation(),f.sequence(),now),true);
                    if(accepted&&f.type()==Type.HEARTBEAT)for(int peer=0;peer<3;peer++)if(peer!=self&&peer!=origin)send(frame(Type.GOSSIP,peer,origin,origin,f.sequence(),f.incarnation(),0,0));
                }else if(f.type()==Type.HEARTBEAT&&f.from()==origin){
                    if(detector instanceof FixedDeadlineFailureDetector d&&observed().contains(origin))d.recordHeartbeat(now,names.get(origin),f.incarnation());
                    if(detector instanceof AdaptiveDeadlineFailureDetector d)d.recordHeartbeat(now,names.get(origin),f.incarnation());
                    if(detector instanceof IgniteTcpRingFailureDetector d)d.recordNeighborMessage(now,names.get(origin));
                    if(detector instanceof AerospikeMeshFailureDetector d)d.recordHeartbeat(now,names.get(origin));
                }
            }
            case PING -> {if(f.target()!=self)return;send(frame(Type.ACK,f.from(),f.origin(),self,f.sequence(),incarnation,0,0));}
            case RELAY -> {
                if(f.origin()!=f.from()||f.target()==self||f.target()==f.origin()||relays.size()>=64)return;
                Relay r=new Relay(f.origin(),f.target(),f.sequence());relays.putIfAbsent(r,now+probeTimeout());send(frame(Type.PING,f.target(),f.origin(),f.target(),f.sequence(),incarnation,0,0));
            }
            case ACK,NACK -> {
                if(f.origin()!=self){
                    Relay r=new Relay(f.origin(),f.target(),f.sequence());
                    if(f.type()==Type.ACK&&f.from()==f.target()&&relays.remove(r)!=null)send(frame(Type.ACK,f.origin(),f.origin(),f.target(),f.sequence(),f.incarnation(),0,0));
                }else if(detector instanceof RedisClusterFailureDetector d){if(f.type()==Type.ACK&&f.from()==f.target())d.recordPong(now,names.get(f.target()));}
                else{
                    Probe p=probes.get(f.sequence());if(p==null||p.target!=f.target())return;
                    if(f.type()==Type.ACK&&(f.from()==p.target||p.helpers.contains(f.from()))){incarnations.merge(p.target,f.incarnation(),Math::max);finish(p,true);}
                    else if(f.type()==Type.NACK&&p.helpers.contains(f.from())){p.negatives.add(f.from());helperNegatives++;}
                }
            }
            case SUSPECT -> {
                if(!(detector instanceof SwimLifeguardDetector d))return;
                if(f.target()==self){if(d.config().buddySystemEnabled()){incarnation=Math.max(incarnation+1,f.incarnation()+1);buddyNotifications++;broadcast(Type.ALIVE,self,incarnation,0,0);}return;}
                if(d.config().suspicionConfirmationsEnabled()){
                    d.suspect(now,names.get(f.target()),names.get(f.from()),f.incarnation());startProbe(f.target(),Purpose.SWIM,f.from(),false);
                }
            }
            case ALIVE -> {if(detector instanceof SwimLifeguardDetector d&&f.from()==f.target()&&f.target()!=self){incarnations.merge(f.target(),f.incarnation(),Math::max);d.recordProbe(now,names.get(f.target()),true,f.incarnation());}}
            case REDIS_REPORT -> {if(detector instanceof RedisClusterFailureDetector d&&f.from()!=f.target())d.recordFailureReport(now,names.get(f.target()),names.get(f.from()));}
            case REDIS_FAIL -> {if(detector instanceof RedisClusterFailureDetector d&&f.from()!=f.target())d.recordFailMessage(now,names.get(f.target()),names.get(f.from()));}
            case IGNITE_REPORT -> {if(detector instanceof IgniteTcpRingFailureDetector&&self==coordinator&&f.target()!=self&&next(f.from())==f.target())startProbe(f.target(),Purpose.IGNITE_REMOTE,f.from(),false);}
            case IGNITE_COMMIT -> {
                if(detector instanceof IgniteTcpRingFailureDetector d&&f.from()==coordinator&&f.target()!=coordinator&&f.epoch()>topologyVersion&&Integer.bitCount(f.evidence())>=2){
                    var updated=new ArrayList<>(d.members());updated.remove(names.get(f.target()));if(d.applyTopologyUpdate(f.epoch(),updated)){topologyVersion=f.epoch();signal(f.target(),MemberStatus.DEAD,"coordinator-topology-commit");}
                }
            }
            case AERO_PROPOSE -> {
                if(detector instanceof AerospikeMeshFailureDetector d&&f.from()!=f.target()&&f.target()!=self&&d.status(names.get(f.target()))==NodeStatus.SUSPECT&&f.epoch()>d.viewVersion()){
                    proposalTargets.put(f.epoch(),f.target());proposalVoters.put(f.epoch(),(1<<self)|(1<<f.from()));
                    if(d.proposeMembershipRemoval(f.epoch(),names.get(f.target()))){d.acknowledgeMembershipProposal(now,f.epoch(),names.get(f.from()));send(frame(Type.AERO_ACK,f.from(),self,f.target(),f.sequence(),0,f.epoch(),1<<self));aeroCommit(d,f.epoch(),f.target());}
                }
            }
            case AERO_ACK -> {
                if(detector instanceof AerospikeMeshFailureDetector d&&Objects.equals(proposalTargets.get(f.epoch()),f.target())&&f.from()!=f.target()&&d.acknowledgeMembershipProposal(now,f.epoch(),names.get(f.from()))){proposalVoters.merge(f.epoch(),1<<f.from(),(a,b)->a|b);aeroCommit(d,f.epoch(),f.target());}
            }
            case AERO_COMMIT -> {
                if(detector instanceof AerospikeMeshFailureDetector d&&f.epoch()>d.viewVersion()&&d.members().contains(names.get(f.target()))&&f.target()!=self){
                    int allowed=0;for(int member:currentMembers())if(member!=f.target())allowed|=1<<member;
                    if((f.evidence()&~allowed)==0&&Integer.bitCount(f.evidence())>=Integer.bitCount(allowed)/2+1&&d.status(names.get(f.target()))==NodeStatus.SUSPECT){
                        d.proposeMembershipRemoval(f.epoch(),names.get(f.target()));for(int voter=0;voter<3;voter++)if((f.evidence()&(1<<voter))!=0)d.acknowledgeMembershipProposal(now,f.epoch(),names.get(voter));
                    }
                }
            }
        }
        drain();
    }
    private void aeroCommit(AerospikeMeshFailureDetector d,long epoch,int target){if(d.viewVersion()==epoch)broadcast(Type.AERO_COMMIT,target,0,epoch,proposalVoters.getOrDefault(epoch,0));}
    private void drain(){
        for(DetectorEvent event:detector.drainEvents()){
            int target=names.indexOf(event.target());if(target<0||target==self)continue;
            journal.event("lab-fd-decision","\"method\":"+JsonLine.quote(method)+",\"node\":"+self+",\"target\":"+target+",\"state\":"+JsonLine.quote(event.newStatus().name())
                    +",\"detectorElapsedNanos\":"+event.atNanos()+",\"reason\":"+JsonLine.quote(event.reason()));
            signal(target,MemberStatus.valueOf(event.newStatus().name()),"local-detector");
            if(event.newStatus()==NodeStatus.SUSPECT && detector.status(event.target())==NodeStatus.SUSPECT){
                if(detector instanceof SwimLifeguardDetector d){
                    if(d.config().suspicionConfirmationsEnabled())for(int peer=0;peer<3;peer++)if(peer!=self&&peer!=target)send(frame(Type.SUSPECT,peer,self,target,++sequence,incarnations.getOrDefault(target,0L),0,0));
                    if(d.config().buddySystemEnabled())send(frame(Type.SUSPECT,target,self,target,++sequence,incarnations.getOrDefault(target,0L),0,0));
                }
                if(detector instanceof IgniteTcpRingFailureDetector){if(self==coordinator)startProbe(target,Purpose.IGNITE_LOCAL,self,true);else send(simple(Type.IGNITE_REPORT,coordinator,target));}
            }
            if(event.newStatus()==NodeStatus.DEAD&&detector instanceof RedisClusterFailureDetector)broadcast(Type.REDIS_FAIL,target,0,0,0);
        }
    }
    private void signal(int target,MemberStatus status,String source){
        MemberStatus previous=memberStates.getOrDefault(target,MemberStatus.ALIVE);if(previous==status)return;memberStates.put(target,status);
        journal.event("membership","\"method\":"+JsonLine.quote(method)+",\"member\":"+(target+(config.ionia()?1:0))+",\"state\":"+JsonLine.quote(status.name())+",\"evidence\":"+JsonLine.quote(source));
        listener.onMembershipChanged(new MembershipEvent(new NodeId(target+(config.ionia()?1:0)),previous,status,incarnations.getOrDefault(target,0L),System.nanoTime()));
    }
    private void publish(){
        int awareness=detector instanceof SwimLifeguardDetector d?d.awarenessScore():0;
        String dead=memberStates.entrySet().stream().filter(e->e.getValue()==MemberStatus.DEAD).map(e->""+(e.getKey()+(config.ionia()?1:0))).collect(java.util.stream.Collectors.joining(",","[","]"));
        snapshot="{\"method\":"+JsonLine.quote(method)+",\"sourceAlgorithm\":"+JsonLine.quote(detector.algorithm())+",\"runtimeHealthy\":"+(fatal==null)+",\"error\":"+(fatal==null?"null":JsonLine.quote(fatal))
                +",\"sentFrames\":"+(udpSent.get()+tcpSent.get())+",\"sentDatagramBytes\":"+udpBytes.get()+",\"udpSentFrames\":"+udpSent.get()+",\"tcpSentFrames\":"+tcpSent.get()+",\"tcpSentStreamBytes\":"+tcpBytes.get()
                +",\"receivedFrames\":"+received.get()+",\"failures\":"+failures.get()+",\"rejectedFrames\":"+rejected.get()+",\"awareness\":"+awareness+",\"deadMembers\":"+dead
                +",\"buddyNotifications\":"+buddyNotifications+",\"helperNegativeEvidence\":"+helperNegatives+",\"pendingProbes\":"+probes.size()+",\"pendingRelays\":"+relays.size()
                +",\"heartbeatMs\":"+heartbeatMs()+",\"deadlineMs\":"+FailureMethods.deadline(config.values())+",\"nativeWatchdogMs\":"+config.nativeWatchdogMillis()
                +",\"coordinatorIndex\":"+coordinator+",\"coordinatorReelectionSupported\":false,\"ringSuccessors\":"+config.number("fd.ring.successors",2)+",\"verifyHelpers\":"+config.number("fd.verify.helpers",1)+",\"verifyQuorum\":"+config.number("fd.verify.quorum",1)+"}";
    }
    @Override public String json(){return snapshot;}
    @Override public void close(){
        if(!closed.compareAndSet(false,true))return;udp.close();if(tcp!=null)try{tcp.close();}catch(IOException ignored){}
        links.values().forEach(TcpLink::close);accepted.forEach(s->{try{s.close();}catch(IOException ignored){}});owner.shutdownNow();io.shutdownNow();
        try{owner.awaitTermination(2,TimeUnit.SECONDS);io.awaitTermination(2,TimeUnit.SECONDS);}catch(InterruptedException e){Thread.currentThread().interrupt();}
    }
}
