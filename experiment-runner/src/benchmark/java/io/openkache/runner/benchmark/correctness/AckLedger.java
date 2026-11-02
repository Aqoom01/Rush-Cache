package io.openkache.runner.benchmark.correctness;

import io.openkache.core.api.*;
import io.openkache.core.model.Bytes;
import io.openkache.runner.benchmark.LogicalRequest;
import io.openkache.runner.benchmark.client.ClientOutcome;
import io.openkache.runner.benchmark.execution.DrainProbe;
import java.util.*;

/** 별도로 예약한 배타적 audit 키의 단일 acknowledged mutation만 검사한다. 덮어쓰기/불확실한 mutation은 제외한다. */
public final class AckLedger {
    private AckLedger() {}
    public record Entry(LogicalRequest.RequestId id,Bytes key,Bytes expectedValue,long acknowledgedNanos,boolean deletion) {}
    public record Ledger(String historyToken,String runId,String loadSlot,String clockId,boolean exclusiveNamespace,
            List<Entry> entries,Map<Bytes,String> exclusions) {
        public Ledger {entries=List.copyOf(entries);exclusions=Map.copyOf(exclusions);}
    }
    /** 초기 DELETE용 값은 ledger 이전에 준비할 수 있다. ledger window 중에는 key당 mutation1개만 허용한다. */
    public static Ledger build(HistoryRecorder.Snapshot h,Set<Bytes> reservedKeys,boolean exclusiveNamespaceAttested){
        Objects.requireNonNull(reservedKeys);Map<Bytes,List<HistoryRecorder.Operation>> byKey=new LinkedHashMap<>();
        for(var key:reservedKeys){byKey.put(key,new ArrayList<>());}
        for(var op:h.operations()){
            if(!(op.request().command() instanceof Command.Get)&&!op.definitelyNotAdmitted()&&byKey.containsKey(op.request().command().key())){byKey.get(op.request().command().key()).add(op);}
        }
        List<Entry> entries=new ArrayList<>();Map<Bytes,String> excluded=new LinkedHashMap<>();
        for(var key:reservedKeys){
            var writes=byKey.get(key);
            if(!exclusiveNamespaceAttested){excluded.put(key,"NAMESPACE_NOT_ATTESTED_EXCLUSIVE");continue;}
            if(!h.failures().isEmpty()){excluded.put(key,"INVALID_COLLECTION");continue;}
            if(writes.size()!=1){excluded.put(key,writes.isEmpty()?"NO_ACKNOWLEDGED_MUTATION":"MULTIPLE_OR_UNCERTAIN_MUTATIONS");continue;}
            var op=writes.getFirst();var outcome=op.outcome();
            if(outcome==null||outcome.category()!=ClientOutcome.Category.SUCCESS||op.uncertain()){
                excluded.put(key,"WRITE_NOT_ACKNOWLEDGED");continue;
            }
            var command=op.request().command();var result=outcome.result();
            boolean valid=result!=null&&result.value().isEmpty()&&(result.status()==ResultStatus.OK
                    ||command instanceof Command.Delete&&result.status()==ResultStatus.MISS);
            if(!valid){excluded.put(key,"MALFORMED_ACK");continue;}
            entries.add(new Entry(outcome.id(),key,command instanceof Command.Set set?set.value():null,outcome.terminalNanos(),command instanceof Command.Delete));
        }
        return new Ledger(h.token(),h.runId(),h.loadSlot(),h.clockId(),exclusiveNamespaceAttested,entries,excluded);
    }
    public record Read(LogicalRequest request,ClientOutcome outcome,String clockId) {}
    public record Item(Entry entry,String status,String reason,Read read) {}
    public record Audit(String historyToken,HistoryChecker.Verdict verdict,String reason,String barrierLabel,long barrierNanos,
            DrainProbe.Observation readiness,List<Item> items,int excludedKeys) {
        public Audit {items=List.copyOf(items);}
    }
    /** READY와 barrier는 호출자가 관측한 같은 clock의 quiescence 증거다. 자동 장애 주입/원격 clock 보정은 하지 않는다. */
    public static Audit audit(Ledger ledger,String label,long barrierNanos,DrainProbe.Observation readiness,List<Read> reads){
        List<Item> items=new ArrayList<>();boolean bad=false,unknown=!ledger.exclusions().isEmpty()||ledger.entries().isEmpty();
        Map<Bytes,List<Read>> byKey=new HashMap<>();for(var r:reads){byKey.computeIfAbsent(r.request().command().key(),ignored->new ArrayList<>()).add(r);}
        for(var entry:ledger.entries()){
            var candidates=byKey.getOrDefault(entry.key(),List.of());Read read=candidates.size()==1?candidates.getFirst():null;
            String reason=null,status="MATCH";
            if(readiness==null||readiness.status()!=DrainProbe.Status.READY){reason="BARRIER_NOT_READY";}
            else if(barrierNanos-entry.acknowledgedNanos()<0){reason="BARRIER_PRECEDES_ACK";}
            else if(read==null){reason="MISSING_OR_DUPLICATE_AUDIT_READ";}
            else if(!(read.request().command() instanceof Command.Get)||!ledger.clockId().equals(read.clockId())
                    ||read.outcome()==null||read.outcome().operation()!=ClientOutcome.Operation.GET
                    ||read.outcome().id().clientId()!=read.request().clientId()||read.outcome().id().sequence()!=read.request().sequence()
                    ||read.outcome().invokedNanos()-barrierNanos<0||read.outcome().terminalNanos()-read.outcome().invokedNanos()<0){reason="INVALID_AUDIT_READ_OR_CLOCK";}
            else if(read.outcome().category()!=ClientOutcome.Category.SUCCESS||read.outcome().result()==null){reason="AUDIT_READ_NOT_SUCCESSFUL";}
            if(reason!=null){status="INCONCLUSIVE";unknown=true;}
            else{
                var result=read.outcome().result();boolean match=entry.deletion()
                        ?result.status()==ResultStatus.MISS&&result.value().isEmpty()
                        :result.status()==ResultStatus.OK&&result.value().equals(Optional.of(entry.expectedValue()));
                if(!match){bad=true;status="MISMATCH";reason=entry.deletion()?"ACKNOWLEDGED_DELETE_NOT_ABSENT":"ACKNOWLEDGED_VALUE_MISSING_OR_DIFFERENT";}
            }
            items.add(new Item(entry,status,reason,read));
        }
        var verdict=bad?HistoryChecker.Verdict.VIOLATION:unknown?HistoryChecker.Verdict.INCONCLUSIVE:HistoryChecker.Verdict.PASS;
        return new Audit(ledger.historyToken(),verdict,bad?"ACK_AUDIT_MISMATCH":unknown?"AUDIT_COVERAGE_INCOMPLETE":"RESERVED_ACK_KEYS_MATCH",label,barrierNanos,readiness,items,ledger.exclusions().size());
    }
}
