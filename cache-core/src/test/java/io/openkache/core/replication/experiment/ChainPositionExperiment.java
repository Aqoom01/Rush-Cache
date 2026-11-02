package io.openkache.core.replication.experiment;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;
import static io.openkache.core.replication.experiment.ReplicationExperiment.require;

/** Chain head/middle/tail 각각의 crash에서 prefix·Sent suffix 보존을 별도 측정한다. */
final class ChainPositionExperiment {
    record Condition(int failed,int pending,int valueBytes,int mibps) {
        Condition { if(failed<0 || failed>2 || pending<0 || pending>1 || !List.of(64,8192).contains(valueBytes) || !List.of(1,10).contains(mibps)) throw new IllegalArgumentException(); }
        String id() { return "node"+failed+"-pending"+pending+"-v"+valueBytes+"-b"+mibps; }
    }
    record Measurement(Condition condition,long faultAtNs,long detectedAtNs,long readyAtNs,long firstOkNs,
                       int preOk,int retainedPre,int preKvCopies,int pendingRetained,int sentAfter,long postFaultEnqueuedBytes) {}
    private static final String HEADER="failed,pending,valueBytes,mibps,faultAtNs,detectedAtNs,readyAtNs,firstOkNs,preOk,retainedPre,preKvCopies,pendingRetained,sentAfter,postFaultEnqueuedBytes";
    private static final List<String> NAMES=List.of(HEADER.split(","));
    static List<Condition> conditions() {
        List<Condition> result=new ArrayList<>();
        for(int failed=0;failed<3;failed++) for(int pending=0;pending<2;pending++) for(int value:new int[]{64,8192}) for(int rate:new int[]{1,10})
            result.add(new Condition(failed,pending,value,rate));
        return List.copyOf(result);
    }
    private static Bytes key(int op) { return Bytes.utf8("position-key-"+op); }
    static Measurement run(Condition condition) {
        try(var c=new ChainCluster(1,200,condition.mibps()*1_048_576L)) {
            byte[] data=new byte[condition.valueBytes()]; Arrays.fill(data,(byte)9); Bytes value=Bytes.copyOf(data);
            for(int op=1;op<=16;op++) {
                var f=c.execute(0,op,new Command.Set(key(op),value),1000); c.advance(30); require(f.join().status()==ResultStatus.OK,"warmup failed");
            }
            java.util.concurrent.CompletableFuture<Result> pending=null;
            if(condition.pending()==1) {
                c.drop=e->e.message() instanceof io.openkache.core.replication.chain.ChainMessage.Forward;
                pending=c.execute(0,17,new Command.Set(key(17),value),1000); c.advance(5); require(!pending.isDone(),"pending already completed");
            }
            long fault=c.now(),bytesBefore=c.network.enqueuedBytes; c.crash(condition.failed()); c.drop=e->false;
            long epoch=1,first=-1; int route=0;
            while(c.now()<fault+ProtocolCluster.ms(5000) && first<0) {
                var f=c.nodes.get(route).execute(new Command.Set(key(100),value),new RequestContext(new ClientId(7),new OperationId(100),RoutingVersion.ZERO,
                        new OwnershipEpoch(epoch),new Deadline(c.now()+ProtocolCluster.ms(1000)))).toCompletableFuture();
                long[] callback={-1}; f.whenComplete((r,e)->callback[0]=c.now());
                while(!f.isDone()) c.advance(1);
                var r=f.join(); epoch=Math.max(epoch,r.ownershipEpoch().value());
                if(r.status()==ResultStatus.OK) first=callback[0];
                else { route=r.redirect().isPresent()?(int)r.redirect().orElseThrow().target().value():(route+1)%3; c.advance(10); }
            }
            require(first>=0,"position failover did not resume"); c.advance(100);
            int retained=0,copies=0,uncertain=0,sent=0;
            for(int op=1;op<=16;op++) {
                boolean any=false;
                for(int n=0;n<3;n++) if(!c.dead.contains(n)) {
                    final int operation=op;
                    if(c.nodes.get(n).logSnapshot().toCompletableFuture().join().stream().anyMatch(r->r.operation().value()==operation && r.command().equals(new Command.Set(key(operation),value)))) any=true;
                    var read=c.stores.get(n).execute(new Command.Get(key(op)),new RequestContext(new ClientId(99),new OperationId(1),RoutingVersion.ZERO,OwnershipEpoch.ZERO,
                            new Deadline(Long.MAX_VALUE))).toCompletableFuture().join();
                    if(read.value().filter(value::equals).isPresent()) copies++;
                }
                if(any) retained++;
            }
            for(int n=0;n<3;n++) if(!c.dead.contains(n)) {
                sent+=c.nodes.get(n).observation().sent();
                if(c.nodes.get(n).logSnapshot().toCompletableFuture().join().stream().anyMatch(r->r.operation().value()==17 && r.command().equals(new Command.Set(key(17),value)))) uncertain=1;
            }
            if(pending!=null) require(pending.isDone() && pending.join().status()==ResultStatus.UNAVAILABLE,"pending client outcome differs");
            c.assertPrefix();
            Measurement m=new Measurement(condition,fault,c.detectedAt,c.readyAt,first,16,retained,copies,uncertain,sent,c.network.enqueuedBytes-bytesBefore);
            validate(m); return m;
        }
    }
    static void validate(Measurement m) {
        require(m.faultAtNs()>=0 && m.detectedAtNs()-m.faultAtNs()==ProtocolCluster.ms(200) && m.readyAtNs()>=m.detectedAtNs()
                && m.firstOkNs()>=m.readyAtNs(),"position timing differs");
        require(m.preOk()==16 && m.retainedPre()==16 && m.preKvCopies()==32 && m.sentAfter()==0 && m.postFaultEnqueuedBytes()>0,"position preservation differs");
        require(m.pendingRetained()==(m.condition().pending()==1 && m.condition().failed()!=0?1:0),"pending suffix outcome differs");
    }
    static String encode(Measurement m) {
        var c=m.condition(); long[] numbers={c.failed(),c.pending(),c.valueBytes(),c.mibps(),m.faultAtNs(),m.detectedAtNs(),m.readyAtNs(),m.firstOkNs(),m.preOk(),m.retainedPre(),m.preKvCopies(),m.pendingRetained(),m.sentAfter(),m.postFaultEnqueuedBytes()};
        StringBuilder b=new StringBuilder("{\n  \"resultSchemaVersion\": \"a1-chain-position-json-1\",\n  \"timeDomain\": \"virtual\",\n  \"timeUnit\": \"ns\"");
        for(int i=0;i<numbers.length;i++) b.append(",\n  \"").append(NAMES.get(i)).append("\": ").append(numbers[i]);
        return b.append("\n}\n").toString();
    }
    static Measurement decode(String original) {
        Map<String,Long> f=new HashMap<>(); String json=original.replace("\r\n","\n");
        Matcher matcher=Pattern.compile("\"([A-Za-z]+)\": (-?[0-9]+)").matcher(json);
        while(matcher.find()) require(f.put(matcher.group(1),Long.parseLong(matcher.group(2)))==null,"duplicate field");
        require(f.keySet().equals(new HashSet<>(NAMES)),"position fields differ");
        var c=new Condition(Math.toIntExact(f.get("failed")),Math.toIntExact(f.get("pending")),Math.toIntExact(f.get("valueBytes")),Math.toIntExact(f.get("mibps")));
        var m=new Measurement(c,f.get("faultAtNs"),f.get("detectedAtNs"),f.get("readyAtNs"),f.get("firstOkNs"),Math.toIntExact(f.get("preOk")),Math.toIntExact(f.get("retainedPre")),
                Math.toIntExact(f.get("preKvCopies")),Math.toIntExact(f.get("pendingRetained")),Math.toIntExact(f.get("sentAfter")),f.get("postFaultEnqueuedBytes"));
        require(encode(m).equals(json),"position schema or syntax differs"); validate(m); return m;
    }
    static void writeOriginals(Path folder) throws IOException {
        Files.createDirectory(folder.resolve("position-results"));
        for(var c:conditions()) Files.writeString(folder.resolve("position-results/"+c.id()+".json"),encode(run(c)));
    }
    static void regenerate(Path folder) throws IOException {
        List<Path> files; try(var paths=Files.list(folder.resolve("position-results"))) { files=paths.filter(p->p.toString().endsWith(".json")).sorted().toList(); }
        require(files.size()==24,"expected 24 position conditions"); var expected=new HashSet<>(conditions()); Set<Condition> seen=new HashSet<>();
        StringBuilder csv=new StringBuilder(HEADER+",failoverNs,repairNs\n"),md=new StringBuilder("# Chain 위치별 crash 24조건\n\n초기 head 0·middle 1·tail 2. 정상 쓰기 16건 이후 지정 노드 종료, master 감지 200ms.\n\n"
                +"pending=1은 쓰기 17을 head에만 적용한 뒤 종료한다. 모든 Forward를 막았다가 crash 직후 해제하여 미확정 suffix의 유실/복구를 구분한다.\n\n"
                +"|종료 노드|미완료 쓰기|값 B|MiB/s|첫 OK ms|master 복구 ms|성공 보존|생존 KV 복사본|미완료 보존|\n|---|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for(Path file:files) {
            Measurement m=decode(Files.readString(file)); var c=m.condition(); require(expected.contains(c) && seen.add(c) && file.getFileName().toString().equals(c.id()+".json"),"position coverage differs");
            String json=encode(m); Matcher matcher=Pattern.compile("\"([A-Za-z]+)\": (-?[0-9]+)").matcher(json); List<String> numbers=new ArrayList<>();
            while(matcher.find()) numbers.add(matcher.group(2));
            long failover=m.firstOkNs()-m.faultAtNs(),repair=m.readyAtNs()-m.detectedAtNs();
            csv.append(String.join(",",numbers)).append(',').append(failover).append(',').append(repair).append('\n');
            md.append('|').append(List.of("head","middle","tail").get(c.failed())).append('|').append(c.pending()).append('|').append(c.valueBytes()).append('|').append(c.mibps())
                    .append('|').append(String.format(Locale.ROOT,"%.3f",failover/1e6)).append('|').append(String.format(Locale.ROOT,"%.3f",repair/1e6))
                    .append('|').append(m.retainedPre()).append('|').append(m.preKvCopies()).append('|').append(m.pendingRetained()).append("|\n");
        }
        require(seen.equals(expected),"position coverage incomplete");
        md.append("\n위치별 case의 master는 단일 신뢰 코디네이터이며 영구 crash만 제외한다. 분단 오판과 master 합의를 모델링하지 않았다. pending 요청의 UNAVAILABLE는 취소가 아니다.\n");
        Files.writeString(folder.resolve("position-summary.csv"),csv); Files.writeString(folder.resolve("position-results.md"),md);
    }
}
