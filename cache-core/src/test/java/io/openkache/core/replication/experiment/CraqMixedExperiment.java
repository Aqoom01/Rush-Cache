package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.Bytes;
import io.openkache.core.replication.chain.ChainEngine;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** 고정 open-loop 요청열로 쓰기 비율/충돌 범위/ACK 지연에 따른 읽기 경로를 측정한다. */
final class CraqMixedExperiment {
    record Condition(ChainEngine.ReadMode mode,int writePercent,int keys,int valueBytes,int ackDelayMs,int seed) {
        Condition {
            Objects.requireNonNull(mode);
            if(!List.of(0,1,5,10,25,50).contains(writePercent) || !List.of(1,64).contains(keys)
                    || !List.of(64,8192).contains(valueBytes) || !List.of(0,20).contains(ackDelayMs) || seed<1 || seed>3)
                throw new IllegalArgumentException("invalid mixed condition");
        }
        String id() { return mode+"-w"+writePercent+"-k"+keys+"-v"+valueBytes+"-a"+ackDelayMs+"-s"+seed; }
    }
    record Row(Condition condition,Map<String,Long> metrics) {
        Row { metrics=Map.copyOf(metrics); }
        long get(String name) { return metrics.get(name); }
    }
    static final List<String> METRICS=List.of("offered","readOffered","readOk","readBusy","readTimeout","readOther",
            "writeOffered","writeOk","writeBusy","writeTimeout","writeOther","correctReads","invalidReads","pending",
            "node0Reads","node1Reads","node2Reads","localReads","dirtyReads","queryAttempts","peerBytes","peerMessages",
            "readP50Ns","readP99Ns","writeP50Ns","writeP99Ns","writeAccepted","tailApplied","retainedVersions");
    static List<Condition> conditions(boolean quick) {
        List<Condition> result=new ArrayList<>();
        for(var mode:ChainEngine.ReadMode.values()) for(int percent:quick?new int[]{0,10,50}:new int[]{0,1,5,10,25,50})
            for(int keys:new int[]{1,64}) for(int value:quick?new int[]{8192}:new int[]{64,8192})
                for(int ack:new int[]{0,20}) for(int seed=1;seed<=(quick?1:3);seed++) result.add(new Condition(mode,percent,keys,value,ack,seed));
        return List.copyOf(result);
    }
    record Commit(long order,Bytes value) {}
    /** 단일 key GET의 선형화 가능 구간 검사. 명령 제출/응답 및 tail 적용의 전체 순서를 기록한다. */
    static final class Trace {
        long order;
        final Map<Bytes,List<Commit>> histories=new HashMap<>();
        final Set<Command> committed=new HashSet<>();
        void applied(ChainCluster.AppliedWrite event) {
            if(event.node()!=2 || !(event.command() instanceof Command.Set s)) return;
            histories.computeIfAbsent(s.key(),k->new ArrayList<>()).add(new Commit(++order,s.value())); committed.add(s);
        }
        boolean valid(Bytes key,Bytes value,long begin,long end) {
            Commit atStart=null;
            for(var c:histories.getOrDefault(key,List.of())) {
                if(c.order()<=begin) atStart=c;
                else if(c.order()<=end && c.value().equals(value)) return true;
            }
            return atStart!=null && atStart.value().equals(value);
        }
    }
    private static Bytes key(int key) { return Bytes.utf8("mixed-"+key); }
    private static Bytes value(int size,long operation) {
        byte[] data=new byte[size]; Arrays.fill(data,(byte)37); ByteBuffer.wrap(data).putLong(operation); return Bytes.copyOf(data);
    }
    static Row run(Condition condition) {
        Trace trace=new Trace();
        try(var c=new ChainCluster(1,200,1_048_576,condition.mode(),trace::applied)) {
            for(int i=0;i<condition.keys();i++) {
                var f=c.execute(0,1+i,new Command.Set(key(i),value(condition.valueBytes(),i)),1000); c.advance(30);
                require(f.join().status()==ResultStatus.OK,"mixed warmup failed");
            }
            c.slowAck=condition.ackDelayMs();
            long beforeBytes=c.network.enqueuedBytes,beforeMessages=c.network.messages,start=c.now();
            int baseline=c.nodes.get(2).observation().applied();
            Map<String,Long> m=new LinkedHashMap<>(); METRICS.forEach(k->m.put(k,0L));
            List<Long> readLatency=new ArrayList<>(),writeLatency=new ArrayList<>();
            List<CompletableFuture<Result>> futures=new ArrayList<>();
            Random random=new Random(condition.seed()); int readSequence=0;
            for(int i=0;i<1500;i++) {
                long at=start+ProtocolCluster.ms(i*2L); c.until(at);
                boolean write=random.nextInt(100)<condition.writePercent(); int selected=random.nextInt(condition.keys());
                Bytes k=key(selected); long operation=10_000L+i;
                Command command=write?new Command.Set(k,value(condition.valueBytes(),operation)):new Command.Get(k);
                int target=write?0:condition.mode()==ChainEngine.ReadMode.TAIL_ONLY?2:readSequence%3;
                if(!write) { readSequence++; add(m,"node"+target+"Reads"); }
                add(m,"offered"); add(m,write?"writeOffered":"readOffered");
                long begin=++trace.order;
                var f=c.execute(target,operation,command,1000); futures.add(f);
                f.whenComplete((result,error)->{
                    long end=++trace.order;
                    if(error!=null) { add(m,write?"writeOther":"readOther"); return; }
                    String outcome=switch(result.status()) { case OK -> "Ok"; case BUSY -> "Busy"; case TIMEOUT -> "Timeout"; default -> "Other"; };
                    add(m,(write?"write":"read")+outcome);
                    if(result.status()==ResultStatus.OK) {
                        (write?writeLatency:readLatency).add(c.now()-at);
                        if(write) require(trace.committed.contains(command),"write OK without tail apply");
                        else if(result.value().isPresent() && trace.valid(k,result.value().orElseThrow(),begin,end)) add(m,"correctReads");
                        else add(m,"invalidReads");
                    }
                });
            }
            c.until(start+ProtocolCluster.ms(3000));
            long drainLimit=c.now()+ProtocolCluster.ms(10000);
            while((futures.stream().anyMatch(f->!f.isDone()) || c.nodes.stream().anyMatch(n->n.observation().sent()!=0)) && c.now()<drainLimit) c.advance(1);
            m.put("pending",futures.stream().filter(f->!f.isDone()).count());
            for(var node:c.nodes) {
                var observation=node.readObservation();
                m.merge("localReads",observation.localReads(),Long::sum); m.merge("dirtyReads",observation.dirtyReads(),Long::sum);
                m.merge("queryAttempts",observation.queryAttempts(),Long::sum); m.merge("retainedVersions",(long)observation.retainedVersions(),Long::sum);
                require(node.observation().sent()==0 && node.observation().applied()==c.nodes.get(2).observation().applied(),"mixed did not converge");
            }
            m.put("peerBytes",c.network.enqueuedBytes-beforeBytes); m.put("peerMessages",c.network.messages-beforeMessages);
            m.put("readP50Ns",quantile(readLatency,.5)); m.put("readP99Ns",quantile(readLatency,.99));
            m.put("writeP50Ns",quantile(writeLatency,.5)); m.put("writeP99Ns",quantile(writeLatency,.99));
            m.put("writeAccepted",(long)c.nodes.get(0).observation().logLength()-baseline);
            m.put("tailApplied",(long)c.nodes.get(2).observation().applied()-baseline);
            c.assertPrefix(); Row row=new Row(condition,m); validate(row); return row;
        }
    }
    private static void add(Map<String,Long> metrics,String key) { metrics.merge(key,1L,Long::sum); }
    private static long quantile(List<Long> values,double q) {
        if(values.isEmpty()) return 0; values.sort(Long::compare); return values.get((int)Math.ceil(q*values.size())-1);
    }
    static void validate(Row r) {
        require(r.metrics().keySet().equals(new HashSet<>(METRICS)) && r.metrics().values().stream().allMatch(v->v>=0),"mixed metric fields differ");
        require(r.get("offered")==1500 && r.get("readOffered")+r.get("writeOffered")==1500,"mixed offers differ");
        require(r.get("readOffered")==r.get("readOk")+r.get("readBusy")+r.get("readTimeout")+r.get("readOther"),"mixed read outcomes incomplete");
        require(r.get("writeOffered")==r.get("writeOk")+r.get("writeBusy")+r.get("writeTimeout")+r.get("writeOther"),"mixed write outcomes incomplete");
        require(r.get("correctReads")==r.get("readOk") && r.get("invalidReads")==0 && r.get("pending")==0,"invalid/incomplete read observed");
        require(r.get("node0Reads")+r.get("node1Reads")+r.get("node2Reads")==r.get("readOffered"),"mixed read routing differs");
        require(r.get("writeAccepted")==r.get("tailApplied") && r.get("writeOk")<=r.get("writeAccepted"),"mixed accepted write mismatch");
        require(r.get("readP99Ns")>=r.get("readP50Ns") && r.get("writeP99Ns")>=r.get("writeP50Ns"),"mixed percentiles differ");
        if(r.condition().writePercent()==0) require(r.get("writeOffered")==0 && r.get("dirtyReads")==0,"read-only is dirty");
        if(r.condition().mode()==ChainEngine.ReadMode.CRAQ)
            require(r.get("localReads")+r.get("dirtyReads")==r.get("readOk") && r.get("queryAttempts")>=r.get("dirtyReads"),"mixed CRAQ paths incomplete");
        else require(r.get("node2Reads")==r.get("readOffered") && r.get("localReads")==0 && r.get("dirtyReads")==0 && r.get("queryAttempts")==0,"mixed baseline paths differ");
    }
}
