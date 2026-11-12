package io.openkache.runner.benchmark.correctness;

import io.openkache.core.replication.ionia.IoniaCommands;
import io.openkache.core.api.Command;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * correctness 전용 전수 이력. 모든 변경은 synchronized이며 disk I/O/사용자 callback은 lock 밖에서 한다.
 * 요청/제출 수와 보관 payload bytes를 명시 제한하고, 초과를 숨기거나 표본으로 바꾸지 않는다.
 */
public final class HistoryRecorder {
    public enum Kind { PENDING, OUTCOME, NOT_ADMITTED, SUBMIT_EXCEPTION, INVALID }
    public record Limits(int maxOperations,int maxSubmissions,long maxPayloadBytes) {
        public Limits { if(maxOperations<1||maxOperations>100_000||maxSubmissions<maxOperations||maxSubmissions>1_000_000
                ||maxPayloadBytes<1||maxPayloadBytes>1_073_741_824L){throw new IllegalArgumentException("history limits out of range");} }
    }
    public record Ticket(LogicalRequest.RequestId id,long submission) {}
    public record Call(long submission,long invokedNanos,Long observedNanos,Kind kind,ClientOutcome outcome,String errorKind) {}
    public record Operation(LogicalRequest request,List<Call> calls) {
        public Operation { calls=List.copyOf(calls); }
        public ClientOutcome outcome(){return calls.stream().filter(c->c.kind()==Kind.OUTCOME).map(Call::outcome).findFirst().orElse(null);}
        public boolean definitelyNotAdmitted(){return !calls.isEmpty()&&calls.stream().allMatch(c->c.kind()==Kind.NOT_ADMITTED);}
        public long invokedNanos(){var o=outcome();return o==null?calls.getFirst().invokedNanos():o.invokedNanos();}
        public boolean uncertain(){var o=outcome();return !definitelyNotAdmitted()&&(o==null||o.category()!=ClientOutcome.Category.SUCCESS);}
    }
    public record Snapshot(String token,String runId,String loadSlot,String clockId,long sealedNanos,Limits limits,
            List<Operation> operations,List<String> failures,long submissions,long retainedPayloadBytes,long pendingCalls) {
        public Snapshot {operations=List.copyOf(operations);failures=List.copyOf(failures);}
        public boolean collectionComplete(){return failures.isEmpty()&&pendingCalls==0;}
    }
    private static final class MutableCall {
        final long submission,start;Long end;Kind kind=Kind.PENDING;ClientOutcome outcome;String error;
        MutableCall(long submission,long start){this.submission=submission;this.start=start;}
        Call snapshot(){return new Call(submission,start,end,kind,outcome,error);}
    }
    private static final class Entry {
        final LogicalRequest request;final List<MutableCall> calls=new ArrayList<>();ClientOutcome first;
        Entry(LogicalRequest request){this.request=request;}
    }
    private final String runId,slot,clockId;
    private final Limits limits;
    private final Map<LogicalRequest.RequestId,Entry> entries=new LinkedHashMap<>();
    private final Map<Long,MutableCall> calls=new HashMap<>();
    private final Set<String> failures=new LinkedHashSet<>();
    private long submissions,payloadBytes,lateAfterSeal;
    private Snapshot sealed;
    public HistoryRecorder(String runId,String loadSlot,String clockId,Limits limits){
        if(Objects.requireNonNull(runId).isBlank()||Objects.requireNonNull(loadSlot).isBlank()||Objects.requireNonNull(clockId).isBlank()){throw new IllegalArgumentException("history identity required");}
        this.runId=runId;slot=loadSlot;this.clockId=clockId;this.limits=Objects.requireNonNull(limits);
    }
    public synchronized Ticket begin(LogicalRequest request,long now){
        if(sealed!=null){throw new IllegalStateException("history sealed");}Objects.requireNonNull(request);
        var id=request.id(runId,slot);var entry=entries.get(id);
        if(entry!=null&&!entry.request.equals(request)){failures.add("LOGICAL_ID_REUSED_WITH_DIFFERENT_REQUEST");throw new IllegalArgumentException("history ID reused");}
        if(submissions>=limits.maxSubmissions()||entry==null&&entries.size()>=limits.maxOperations()){
            failures.add("HISTORY_CAPACITY_EXCEEDED");throw new IllegalStateException("history capacity");
        }
        if(entry==null){
            long bytes=IoniaCommands.key(request.command()).size()+(request.command() instanceof Command.Set set?(long)set.value().size():0L);
            retain(bytes);entry=new Entry(request);entries.put(id,entry);
        }
        var call=new MutableCall(++submissions,now);entry.calls.add(call);calls.put(call.submission,call);return new Ticket(id,call.submission);
    }
    /** 오류/완료를 같은 ticket에 한 번만 기록하며 seal 뒤의 불변 snapshot은 바꾸지 않는다. */
    public synchronized void finish(Ticket ticket,ClientOutcome outcome,Throwable failure,long now){
        if(sealed!=null){lateAfterSeal++;return;}
        var call=calls.get(ticket.submission());var entry=entries.get(ticket.id());
        if(call==null||entry==null||!entry.calls.contains(call)){failures.add("INVALID_HISTORY_TICKET");throw new IllegalArgumentException("history ticket");}
        if(call.kind!=Kind.PENDING){failures.add("DUPLICATE_TERMINAL_CALLBACK");throw new IllegalStateException("history terminal twice");}
        call.end=now;
        if(now-call.start<0){invalid(call,"HISTORY_CLOCK_ORDER");throw new IllegalArgumentException("history clock");}
        Throwable cause=unwrap(failure);
        if(cause!=null){call.kind=cause instanceof RejectedExecutionException?Kind.NOT_ADMITTED:Kind.SUBMIT_EXCEPTION;call.error=cause.getClass().getName();return;}
        if(outcome==null||!outcome.id().equals(ticket.id())||outcome.operation()!=ClientOutcome.Operation.of(entry.request.command())
                ||outcome.invokedNanos()-entry.calls.getFirst().start<0||outcome.decidedNanos()-outcome.invokedNanos()<0
                ||outcome.terminalNanos()-outcome.decidedNanos()<0||now-outcome.terminalNanos()<0){
            invalid(call,"OUTCOME_IDENTITY_OR_CLOCK");throw new IllegalArgumentException("history outcome mismatch");
        }
        if(entry.first!=null&&!entry.first.equals(outcome)){invalid(call,"CONFLICTING_LOGICAL_OUTCOME");throw new IllegalArgumentException("conflicting logical result");}
        try{retain(outcome.result()==null?0:outcome.result().value().map(v->(long)v.size()).orElse(0L));}
        catch(RuntimeException full){invalid(call,"OUTCOME_PAYLOAD_CAPACITY");throw full;}
        call.kind=Kind.OUTCOME;call.outcome=outcome;entry.first=outcome;
    }
    private void invalid(MutableCall call,String reason){call.kind=Kind.INVALID;call.error=reason;failures.add(reason);}
    private void retain(long bytes){if(bytes>limits.maxPayloadBytes()-payloadBytes){failures.add("HISTORY_PAYLOAD_CAPACITY");throw new IllegalStateException("history payload capacity");}payloadBytes+=bytes;}
    private static Throwable unwrap(Throwable error){while((error instanceof CompletionException||error instanceof ExecutionException)&&error.getCause()!=null){error=error.getCause();}return error;}
    public synchronized Snapshot seal(long now){
        if(sealed==null){
            List<Operation> out=new ArrayList<>();long pending=0;
            for(var e:entries.values()){
                List<Call> events=e.calls.stream().map(MutableCall::snapshot).toList();
                for(var c:events){if(c.kind()==Kind.PENDING){pending++;}if(now-(c.observedNanos()==null?c.invokedNanos():c.observedNanos())<0){failures.add("SEAL_CLOCK_ORDER");}}
                out.add(new Operation(e.request,events));
            }
            sealed=new Snapshot(UUID.randomUUID().toString(),runId,slot,clockId,now,limits,out,List.copyOf(failures),submissions,payloadBytes,pending);
        }return sealed;
    }
    public synchronized long lateCallbacksAfterSeal(){return lateAfterSeal;}
}
