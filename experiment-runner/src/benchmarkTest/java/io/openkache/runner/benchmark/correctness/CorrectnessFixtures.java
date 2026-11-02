package io.openkache.runner.benchmark.correctness;

import io.openkache.core.api.*;
import io.openkache.core.model.*;
import io.openkache.runner.benchmark.*;
import io.openkache.runner.benchmark.client.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

public final class CorrectnessFixtures {
    private CorrectnessFixtures() {}
    public static final Bytes KEY=Bytes.utf8("k"),VALUE=Bytes.utf8("a");
    public static HistoryRecorder recorder(){return new HistoryRecorder("run","slot","clock",new HistoryRecorder.Limits(100,200,1_000_000));}
    public static HistoryChecker.Limits limits(){return new HistoryChecker.Limits(24,100_000,TimeUnit.SECONDS.toNanos(2));}
    public static LogicalRequest request(long i,Command command){return new LogicalRequest(TracePhase.MEASURE,i,0,2,i,command);}
    public static Result plain(ResultStatus status){return Result.of(status,RoutingVersion.ZERO,OwnershipEpoch.ZERO);}
    public static Result value(Bytes value){return Result.ok(value,1,RoutingVersion.ZERO,OwnershipEpoch.ZERO);}
    public static ClientOutcome outcome(LogicalRequest r,ClientOutcome.Category category,Result result,long start,long end){
        return new ClientOutcome(r.id("run","slot"),ClientOutcome.Operation.of(r.command()),category,
                category==ClientOutcome.Category.SUCCESS?ClientOutcome.Reason.SUCCESS:ClientOutcome.Reason.DEADLINE,result,start,end,end,r.clientId(),r.sequence()+1,1,
                List.of(new ClientOutcome.Attempt(1,start,end,result==null?null:result.status(),null)));
    }
    public static void record(HistoryRecorder h,long i,Command command,ClientOutcome.Category category,Result result,long start,long end){
        var request=request(i,command);var ticket=h.begin(request,start-1);h.finish(ticket,outcome(request,category,result,start,end),null,end+1);
    }
    public static void success(HistoryRecorder h,long i,Command command,Result result,long start,long end){record(h,i,command,ClientOutcome.Category.SUCCESS,result,start,end);}
    public static final class Memory implements BenchmarkClient {
        public final AtomicLong clock=new AtomicLong(10);public final ClientAccounting ledger=new ClientAccounting();
        public final Map<Bytes,Bytes> values=new ConcurrentHashMap<>();public int calls,closes,cancels;public boolean rejectFirst,hold;
        public final CompletableFuture<ClientOutcome> held=new CompletableFuture<>();private LogicalRequest heldRequest;private long heldStart;
        @Override public CompletionStage<ClientOutcome> submit(LogicalRequest r){
            calls++;if(rejectFirst&&calls==1){ledger.rejected();return CompletableFuture.failedFuture(new RejectedExecutionException("fixture"));}
            if(hold&&heldRequest!=null){return held.minimalCompletionStage();}
            var op=ClientOutcome.Operation.of(r.command());ledger.admitted(op);ledger.attempt(op);long start=clock.incrementAndGet();
            if(hold){heldRequest=r;heldStart=start;return held.minimalCompletionStage();}
            var result=apply(r.command());ledger.terminal(op,ClientOutcome.Category.SUCCESS,1);
            return CompletableFuture.completedFuture(outcome(r,ClientOutcome.Category.SUCCESS,result,start,clock.incrementAndGet()));
        }
        public void release(){var op=ClientOutcome.Operation.of(heldRequest.command());ledger.terminal(op,ClientOutcome.Category.SUCCESS,1);held.complete(outcome(heldRequest,ClientOutcome.Category.SUCCESS,apply(heldRequest.command()),heldStart,clock.incrementAndGet()));}
        private Result apply(Command command){
            if(command instanceof Command.Get){var found=values.get(command.key());return found==null?plain(ResultStatus.MISS):value(found);}
            if(command instanceof Command.Set set){values.put(set.key(),set.value());return plain(ResultStatus.OK);}
            return plain(values.remove(command.key())==null?ResultStatus.MISS:ResultStatus.OK);
        }
        @Override public boolean cancel(LogicalRequest.RequestId id){cancels++;return false;}
        @Override public ClientAccounting.Snapshot snapshot(){return ledger.snapshot();}
        @Override public void close(){closes++;}
    }
}
