package io.openkache.runner.benchmark.correctness;

import io.openkache.runner.benchmark.LogicalRequest;
import io.openkache.runner.benchmark.client.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;

/** correctness 전용 decorator. 원래 결과/ID/취소/계수를 유지하고 이력을 기록한 뒤 lock 밖에서 결과를 게시한다. */
public final class RecordingBenchmarkClient implements BenchmarkClient {
    private final BenchmarkClient delegate;private final HistoryRecorder history;private final LongSupplier clock;
    private final AtomicBoolean closed=new AtomicBoolean();
    public RecordingBenchmarkClient(BenchmarkClient delegate,HistoryRecorder history){this(delegate,history,System::nanoTime);}
    public RecordingBenchmarkClient(BenchmarkClient delegate,HistoryRecorder history,LongSupplier sameDomainClock){
        this.delegate=Objects.requireNonNull(delegate);this.history=Objects.requireNonNull(history);clock=Objects.requireNonNull(sameDomainClock);
    }
    @Override public CompletionStage<ClientOutcome> submit(LogicalRequest request){
        HistoryRecorder.Ticket ticket;
        try{ticket=history.begin(request,clock.getAsLong());}catch(RuntimeException error){return CompletableFuture.failedFuture(error);}
        CompletableFuture<ClientOutcome> observed=new CompletableFuture<>();CompletionStage<ClientOutcome> stage;
        try{stage=Objects.requireNonNull(delegate.submit(request),"client stage");}
        catch(RuntimeException error){stage=CompletableFuture.failedFuture(error);}
        stage.whenComplete((outcome,error)->{
            try{history.finish(ticket,outcome,error,clock.getAsLong());}
            catch(RuntimeException recordingFailure){observed.completeExceptionally(recordingFailure);return;}
            if(error!=null){observed.completeExceptionally(error);}else{observed.complete(outcome);}
        });
        return observed.minimalCompletionStage();
    }
    @Override public boolean cancel(LogicalRequest.RequestId id){return delegate.cancel(id);}
    @Override public ClientAccounting.Snapshot snapshot(){return delegate.snapshot();}
    @Override public void close(){if(closed.compareAndSet(false,true)){delegate.close();}}
}
