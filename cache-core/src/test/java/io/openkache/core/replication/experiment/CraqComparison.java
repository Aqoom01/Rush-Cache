package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.Bytes;
import io.openkache.core.replication.chain.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 읽기 경로만 비교하는 가상 시간 모델. CPU/클라이언트 링크가 없으므로 처리량을 산출하지 않는다. */
final class CraqComparison {
    enum Scenario { CLEAN, IN_FLIGHT, ACK_DELAY, TAIL_CRASH }
    record Condition(ChainEngine.ReadMode mode,Scenario scenario,int valueBytes,int mibps) {
        Condition {
            Objects.requireNonNull(mode); Objects.requireNonNull(scenario);
            if(!List.of(64,8192).contains(valueBytes) || !List.of(1,10).contains(mibps)) throw new IllegalArgumentException();
        }
        String id() { return mode+"-"+scenario+"-v"+valueBytes+"-b"+mibps; }
    }
    record Measurement(Condition condition,long reads,long correct,long node0,long node1,long node2,
                       long localReads,long dirtyReads,long queryAttempts,long peerBytes,long peerMessages,long p50Ns,long p99Ns) {}
    static List<Condition> conditions(boolean quick) {
        List<Condition> result=new ArrayList<>();
        for(var mode:ChainEngine.ReadMode.values()) for(var scenario:Scenario.values())
            for(int value:quick?new int[]{64}:new int[]{64,8192}) for(int rate:quick?new int[]{1}:new int[]{1,10})
                result.add(new Condition(mode,scenario,value,rate));
        return List.copyOf(result);
    }
    private static Bytes value(int size,int marker) { byte[] data=new byte[size]; Arrays.fill(data,(byte)marker); return Bytes.copyOf(data); }
    static Measurement run(Condition condition) {
        try(var c=new ChainCluster(1,200,condition.mibps()*1_048_576L,condition.mode())) {
            Bytes old=value(condition.valueBytes(),1),next=value(condition.valueBytes(),2),expected=old;
            var first=c.execute(0,1,new Command.Set(Bytes.utf8("hot"),old),5000); c.advance(50);
            require(first.join().status()==ResultStatus.OK,"warmup failed");
            if(condition.scenario()==Scenario.IN_FLIGHT || condition.scenario()==Scenario.ACK_DELAY) {
                c.drop=condition.scenario()==Scenario.IN_FLIGHT
                        ?e->e.message() instanceof ChainMessage.Forward && e.to().value()==2
                        :e->e.message() instanceof ChainMessage.Ack;
                c.execute(0,2,new Command.Set(Bytes.utf8("hot"),next),5000); c.advance(50);
                if(condition.scenario()==Scenario.ACK_DELAY) expected=next;
            } else if(condition.scenario()==Scenario.TAIL_CRASH) {
                c.crash(2); c.advance(1500); require(c.readyHead()>=0,"tail repair failed");
            }
            long bytes=c.network.enqueuedBytes,messages=c.network.messages,start=c.now(); long[] routed=new long[3];
            List<CompletableFuture<Result>> futures=new ArrayList<>(); List<Long> latencies=new ArrayList<>();
            List<Integer> live=new ArrayList<>(); for(int n=0;n<3;n++) if(!c.dead.contains(n)) live.add(n);
            for(int i=0;i<90;i++) {
                int target=condition.mode()==ChainEngine.ReadMode.TAIL_ONLY?c.tail():live.get(i%live.size()); routed[target]++;
                var f=c.execute(target,1000+i,new Command.Get(Bytes.utf8("hot")),5000);
                f.whenComplete((r,e)->latencies.add(c.now()-start)); futures.add(f);
            }
            long limit=start+ProtocolCluster.ms(5000);
            while(futures.stream().anyMatch(f->!f.isDone()) && c.now()<limit) c.advance(1);
            long correct=0;
            for(var f:futures) { require(f.isDone(),"read not completed"); if(f.join().status()==ResultStatus.OK && f.join().value().filter(expected::equals).isPresent()) correct++; }
            require(correct==90,"uncommitted/stale read observed");
            long local=0,dirty=0,queries=0;
            for(int n:live) { var o=c.nodes.get(n).readObservation(); local+=o.localReads(); dirty+=o.dirtyReads(); queries+=o.queryAttempts(); }
            latencies.sort(Long::compare);
            var m=new Measurement(condition,90,correct,routed[0],routed[1],routed[2],local,dirty,queries,
                    c.network.enqueuedBytes-bytes,c.network.messages-messages,latencies.get(44),latencies.get(89));
            validate(m); return m;
        }
    }
    static void validate(Measurement m) {
        require(m.reads()==90 && m.correct()==90 && m.node0()+m.node1()+m.node2()==90,"read totals differ");
        require(m.p50Ns()>=0 && m.p99Ns()>=m.p50Ns() && m.peerBytes()>=0 && m.peerMessages()>=0,"cost bounds differ");
        if(m.condition().mode()==ChainEngine.ReadMode.TAIL_ONLY) {
            require(m.localReads()==0 && m.dirtyReads()==0 && m.queryAttempts()==0 && m.peerBytes()==0 && m.peerMessages()==0,"baseline path differs");
            require(m.node0()==0 && (m.condition().scenario()==Scenario.TAIL_CRASH?m.node1()==90:m.node2()==90),"baseline routing differs");
        } else {
            boolean dirty=m.condition().scenario()==Scenario.IN_FLIGHT || m.condition().scenario()==Scenario.ACK_DELAY;
            require(m.localReads()==(dirty?30:90) && m.dirtyReads()==(dirty?60:0) && m.queryAttempts()==(dirty?60:0),"CRAQ path differs");
            require(m.node0()==(m.condition().scenario()==Scenario.TAIL_CRASH?45:30) && m.node1()==m.node0(),"CRAQ routing differs");
            require(m.peerMessages()==(dirty?120:0),"CRAQ query message count differs");
        }
    }
}
