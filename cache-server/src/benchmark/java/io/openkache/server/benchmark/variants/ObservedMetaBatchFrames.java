package io.openkache.server.benchmark.variants;

import io.openkache.core.model.NodeId;
import io.openkache.core.replication.ionia.*;
import io.openkache.core.replication.ionia_eighth.IoniaMetaBatchTransport;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** V07의 실제 batch frame stage를 별도로 제한/관찰한다. logical timeout/close로 permit을 반환하지 않는다. */
public final class ObservedMetaBatchFrames implements IoniaMetaBatchTransport, AutoCloseable {
    public record Snapshot(long calls, long dispatched, long completed, long failed, long rejected,
            int outstanding, int peak, boolean closed) {}
    private final IoniaMetaBatchTransport raw;
    private final int limit;
    private long calls, dispatched, completed, failed, rejected;
    private int outstanding, peak;
    private boolean closed;
    public ObservedMetaBatchFrames(IoniaClientTransport raw, int limit) {
        if (!(raw instanceof IoniaMetaBatchTransport batch) || limit < 1) { throw new IllegalArgumentException("V07 requires a batch transport/positive cap"); }
        this.raw=batch;this.limit=limit;
    }
    @Override public CompletionStage<List<Answer>> metaBatch(NodeId to,List<Item> requests) {
        synchronized(this){calls++;if(closed||outstanding>=limit){rejected++;return CompletableFuture.failedFuture(new RejectedExecutionException("batch closed/cap"));}dispatched++;peak=Math.max(peak,++outstanding);}
        CompletableFuture<List<Answer>> output=new CompletableFuture<>();AtomicBoolean once=new AtomicBoolean();
        java.util.function.BiConsumer<List<Answer>,Throwable> finish=(value,error)->{
            if(!once.compareAndSet(false,true)){return;}
            synchronized(this){outstanding--;completed++;if(error!=null){failed++;}}
            if(error!=null){output.completeExceptionally(error);}else{output.complete(value);}
        };
        try{Objects.requireNonNull(raw.metaBatch(to,List.copyOf(requests)),"batch stage").whenComplete(finish);}
        catch(RuntimeException error){finish.accept(null,error);}
        return output.minimalCompletionStage();
    }
    public synchronized Snapshot snapshot(){return new Snapshot(calls,dispatched,completed,failed,rejected,outstanding,peak,closed);}
    /** 일반 RPC는 runtime의 ObservedRpc가 제한한다. batch cap과 일반 cap은 별개이며 metadata에 기록한다. */
    public IoniaMetaBatchTransport through(IoniaClientTransport observedNormal) {
        return new IoniaMetaBatchTransport(){
            @Override public CompletionStage<WriteAck> record(NodeId to,Request r){return observedNormal.record(to,r);}
            @Override public CompletionStage<Reply> execute(NodeId to,Request r){return observedNormal.execute(to,r);}
            @Override public CompletionStage<ReadReply> followerRead(NodeId to,Request r){return observedNormal.followerRead(to,r);}
            @Override public CompletionStage<MetaReply> metaQuery(NodeId to,Request r){return observedNormal.metaQuery(to,r);}
            @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){return observedNormal.probe(to);}
            @Override public CompletionStage<List<Answer>> metaBatch(NodeId to,List<Item> r){return ObservedMetaBatchFrames.this.metaBatch(to,r);}
        };
    }
    @Override public CompletionStage<WriteAck> record(NodeId to,Request r){return raw.record(to,r);}
    @Override public CompletionStage<Reply> execute(NodeId to,Request r){return raw.execute(to,r);}
    @Override public CompletionStage<ReadReply> followerRead(NodeId to,Request r){return raw.followerRead(to,r);}
    @Override public CompletionStage<MetaReply> metaQuery(NodeId to,Request r){return raw.metaQuery(to,r);}
    @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to){return raw.probe(to);}
    @Override public void close(){
        synchronized(this){if(closed){return;}closed=true;}
        if(raw instanceof AutoCloseable owner){try{owner.close();}catch(Exception error){if(error instanceof InterruptedException){Thread.currentThread().interrupt();}throw new IllegalStateException("batch raw close",error);}}
    }
}
