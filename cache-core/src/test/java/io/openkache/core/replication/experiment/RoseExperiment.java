package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** Rosé 단일 파티션 변형의 L별 지연/성공률/유실 비교. 0~2초 open-loop, crash는 1.2초다. */
final class RoseExperiment {
    enum Scenario { NORMAL, SLOW_BACKUP, PARTITION_REPAIR, PRIMARY_CRASH, ISOLATED_CRASH }
    record Condition(int limit,Scenario scenario,int intervalMs,int valueBytes,int mibps) {
        Condition {
            if(!List.of(-1,0,1,10,100,1000).contains(limit) || scenario==null || !List.of(1,5).contains(intervalMs)
                    || !List.of(64,8192).contains(valueBytes) || !List.of(1,10).contains(mibps)) throw new IllegalArgumentException();
        }
        String id() { return "L"+limit+"-"+scenario+"-i"+intervalMs+"-v"+valueBytes+"-b"+mibps; }
        boolean crash() { return scenario==Scenario.PRIMARY_CRASH || scenario==Scenario.ISOLATED_CRASH; }
    }
    record Sample(long timeNs,int ok,int primary,int lag,int pending,int backup1Applied,int backup2Applied) {}
    record Measurement(Condition condition,Map<String,Long> values,List<Sample> samples) {
        Measurement { values=Collections.unmodifiableMap(new LinkedHashMap<>(values)); samples=List.copyOf(samples); }
    }
    static final List<String> FIELDS=List.of(("offered,ok,busy,timeout,unavailable,stale,redirect,pending,backpressureCount,peakLag,preCrashOk,lostPreCrash,retainedOk,"
            +"firstPostCrashOkNs,crashAtNs,readyAtNs,drainNs,converged,primary,p50Ns,p99Ns,attemptedBytes,enqueuedBytes,deliveredBytes,droppedBytes,pendingBytes,messages").split(","));
    static List<Condition> conditions(boolean quick) {
        List<Condition> result=new ArrayList<>();
        for(int limit:quick?new int[]{0,10,-1}:new int[]{0,1,10,100,1000,-1}) for(var s:Scenario.values())
            for(int interval:quick?new int[]{1}:new int[]{1,5}) for(int value:new int[]{64,8192}) for(int rate:quick?new int[]{1}:new int[]{1,10})
                result.add(new Condition(limit,s,interval,value,rate));
        return List.copyOf(result);
    }
    private static Bytes key(int op) { return Bytes.utf8(String.format(Locale.ROOT,"rose-%011d",op)); }
    private static final class Client {
        int route; long epoch=1,firstPost=-1;
        final EnumMap<ResultStatus,Integer> outcomes=new EnumMap<>(ResultStatus.class);
        final Set<Integer> successful=new LinkedHashSet<>(),preCrash=new LinkedHashSet<>();
        final List<Long> latencies=new ArrayList<>();
        final List<CompletableFuture<Result>> futures=new ArrayList<>();
        void submit(RoseCluster c,int operation,Bytes value,boolean crash) {
            int destination=route; long start=c.now();
            var f=c.nodes.get(destination).execute(new Command.Set(key(operation),value),new RequestContext(new ClientId(7),new OperationId(operation),RoutingVersion.ZERO,
                    new OwnershipEpoch(epoch),new Deadline(start+ProtocolCluster.ms(500)))).toCompletableFuture(); futures.add(f);
            f.whenComplete((r,error)->{
                require(error==null,"request failed exceptionally"); outcomes.merge(r.status(),1,Integer::sum);
                long previousEpoch=epoch; epoch=Math.max(epoch,r.ownershipEpoch().value());
                if(r.status()==ResultStatus.OK) {
                    successful.add(operation); latencies.add(c.now()-start);
                    if(crash && start>=ProtocolCluster.ms(1200) && firstPost<0) firstPost=c.now();
                } else if(r.status()==ResultStatus.NOT_PRIMARY && r.ownershipEpoch().value()>=previousEpoch && route==destination)
                    route=(int)r.redirect().orElseThrow().target().value();
                else if((r.status()==ResultStatus.UNAVAILABLE || r.status()==ResultStatus.STALE_EPOCH) && route==destination) route=(destination+1)%3;
                else require(r.status()==ResultStatus.BUSY || r.status()==ResultStatus.TIMEOUT || r.status()==ResultStatus.NOT_PRIMARY
                        || r.status()==ResultStatus.UNAVAILABLE || r.status()==ResultStatus.STALE_EPOCH,"unexpected outcome "+r.status());
            });
        }
        int count(ResultStatus status) { return outcomes.getOrDefault(status,0); }
        int pending() { return (int)futures.stream().filter(f->!f.isDone()).count(); }
    }
    static Measurement run(Condition condition) {
        try(var c=new RoseCluster(condition.limit(),1,200,condition.mibps()*1_048_576L)) {
            Client client=new Client(); byte[] bytes=new byte[condition.valueBytes()]; Arrays.fill(bytes,(byte)11); Bytes value=Bytes.copyOf(bytes);
            switch(condition.scenario()) {
                case SLOW_BACKUP -> {
                    c.scheduler.schedule(ProtocolCluster.ms(500),()->c.slowData=200);
                    c.scheduler.schedule(ProtocolCluster.ms(1500),()->c.slowData=0);
                }
                case PARTITION_REPAIR -> {
                    c.scheduler.schedule(ProtocolCluster.ms(500),()->c.isolate(0));
                    c.scheduler.schedule(ProtocolCluster.ms(1000),c::heal);
                }
                case ISOLATED_CRASH -> c.scheduler.schedule(ProtocolCluster.ms(1000),()->c.isolate(0));
                default -> { }
            }
            if(condition.crash()) c.scheduler.schedule(ProtocolCluster.ms(1200),()->{
                client.preCrash.addAll(client.successful); c.crashPrimary(); c.heal();
            });
            for(int at=0;at<2000;at+=condition.intervalMs()) {
                int operation=at/condition.intervalMs()+1;
                c.scheduler.schedule(ProtocolCluster.ms(at),()->client.submit(c,operation,value,condition.crash()));
            }
            List<Sample> samples=new ArrayList<>();
            for(int at=0;at<=2000;at+=10) { c.until(ProtocolCluster.ms(at)); samples.add(sample(c,client)); }
            while(c.now()<ProtocolCluster.ms(32000) && (client.pending()>0 || !c.converged())) { c.advance(10); samples.add(sample(c,client)); }
            var live=c.nodes.get(c.primary).logSnapshot().toCompletableFuture().join();
            Set<Integer> present=new HashSet<>();
            for(var r:live) if(r.client().value()==7) {
                int op=Math.toIntExact(r.operation().value()); require(r.command().equals(new Command.Set(key(op),value)),"retained command differs"); present.add(op);
            }
            int retained=0,lost=0;
            for(int op:client.successful) {
                if(present.contains(op)) {
                    retained++;
                    var read=c.stores.get(c.primary).execute(new Command.Get(key(op)),new RequestContext(new ClientId(99),new OperationId(1),RoutingVersion.ZERO,
                            OwnershipEpoch.ZERO,new Deadline(Long.MAX_VALUE))).toCompletableFuture().join();
                    require(read.value().filter(value::equals).isPresent(),"retained success not applied at current Primary");
                } else { require(client.preCrash.contains(op),"post-crash or normal success lost"); lost++; }
            }
            if(c.converged()) for(int n=0;n<3;n++) if(!c.dead.contains(n)) require(live.equals(c.nodes.get(n).logSnapshot().toCompletableFuture().join()),"final prefixes differ");
            int peak=c.nodes.stream().mapToInt(n->n.observation().peakLag()).max().orElseThrow();
            if(condition.limit()>=0) require(peak<=Math.max(1,condition.limit()) && lost<=condition.limit(),"window/loss bound exceeded");
            var stats=c.network.stats(); var latency=client.latencies.stream().sorted().toList();
            Map<String,Long> f=new LinkedHashMap<>();
            long[] numbers={2000/condition.intervalMs(),client.successful.size(),client.count(ResultStatus.BUSY),client.count(ResultStatus.TIMEOUT),client.count(ResultStatus.UNAVAILABLE),
                    client.count(ResultStatus.STALE_EPOCH),client.count(ResultStatus.NOT_PRIMARY),client.pending(),c.nodes.stream().mapToInt(n->n.observation().backpressureCount()).sum(),peak,
                    client.preCrash.size(),lost,retained,client.firstPost,condition.crash()?ProtocolCluster.ms(1200):-1,c.readyAt,c.now()-ProtocolCluster.ms(2000),c.converged()?1:0,c.primary,
                    percentile(latency,.5),percentile(latency,.99),stats.attemptedBytes(),stats.enqueuedBytes(),stats.deliveredBytes(),stats.droppedBytes(),stats.pendingBytes(),stats.messages()};
            for(int i=0;i<numbers.length;i++) f.put(FIELDS.get(i),numbers[i]);
            return new Measurement(condition,f,samples);
        }
    }
    private static Sample sample(RoseCluster c,Client client) {
        var o=c.nodes.get(c.primary).observation();
        int lag=c.dead.contains(c.primary) || o.state()!=io.openkache.core.replication.rose.RoseEngine.State.RUNNING || o.minAck()<0?-1:o.logLength()-o.minAck();
        return new Sample(c.now(),client.successful.size(),c.primary,lag,client.pending(),c.nodes.get(1).observation().applied(),c.nodes.get(2).observation().applied());
    }
    private static long percentile(List<Long> values,double p) { return values.isEmpty()?-1:values.get((int)Math.ceil(values.size()*p)-1); }
}
