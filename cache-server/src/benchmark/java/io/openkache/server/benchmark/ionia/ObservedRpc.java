package io.openkache.server.benchmark.ionia;

import io.openkache.core.replication.ionia.IoniaClientTransport;
import io.openkache.core.replication.ionia.IoniaEngine;
import io.openkache.core.model.NodeId;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * 실제 delegate RPC stage의 관찰·상한. socket/message/byte 수가 아닌 호출 수다.
 * 취소·logical timeout으로 raw permit을 조기 반환하지 않는다.
 */
public final class ObservedRpc implements IoniaClientTransport {
    public enum Kind { RECORD, EXECUTE, READ, META, PROBE }
    public record Counts(long calls, long dispatched, long completed, long failed, long rejected) {}
    public record Snapshot(Map<Kind, Counts> byKind, int outstanding, int peak, boolean closed) {
        public Snapshot { byKind = Map.copyOf(byKind); }
    }
    private final IoniaClientTransport delegate;
    private final int limit;
    private final Map<Kind, long[]> counts = new EnumMap<>(Kind.class);
    private int outstanding;
    private int peak;
    private boolean closed;

    ObservedRpc(IoniaClientTransport delegate, int limit) {
        this.delegate = Objects.requireNonNull(delegate);
        if (limit < 1) { throw new IllegalArgumentException("RPC limit must be positive"); }
        this.limit = limit;
        for (Kind kind : Kind.values()) { counts.put(kind, new long[5]); }
    }

    private <T> CompletionStage<T> call(Kind kind, Supplier<CompletionStage<T>> action) {
        synchronized (this) {
            long[] c = counts.get(kind);
            c[0]++;
            if (closed || outstanding >= limit) {
                c[4]++;
                return CompletableFuture.failedFuture(new RejectedExecutionException("RPC closed/limit"));
            }
            c[1]++; outstanding++; peak = Math.max(peak, outstanding);
        }
        CompletableFuture<T> result = new CompletableFuture<>();
        AtomicBoolean finished = new AtomicBoolean();
        java.util.function.BiConsumer<T, Throwable> finish = (value, error) -> {
            if (!finished.compareAndSet(false, true)) { return; }
            synchronized (this) {
                outstanding--; counts.get(kind)[2]++;
                if (error != null) { counts.get(kind)[3]++; }
            }
            if (error == null) { result.complete(value); } else { result.completeExceptionally(error); }
        };
        try { Objects.requireNonNull(action.get(), "RPC stage").whenComplete(finish); }
        catch (RuntimeException error) { finish.accept(null, error); }
        return result.minimalCompletionStage();
    }

    synchronized Snapshot snapshot() {
        Map<Kind, Counts> result = new EnumMap<>(Kind.class);
        counts.forEach((kind, c) -> result.put(kind, new Counts(c[0], c[1], c[2], c[3], c[4])));
        return new Snapshot(result, outstanding, peak, closed);
    }

    synchronized void stopAdmission() { closed = true; }

    @Override public CompletionStage<WriteAck> record(NodeId to, Request r) {
        return call(Kind.RECORD, () -> delegate.record(to, r));
    }
    @Override public CompletionStage<Reply> execute(NodeId to, Request r) {
        return call(Kind.EXECUTE, () -> delegate.execute(to, r));
    }
    @Override public CompletionStage<ReadReply> followerRead(NodeId to, Request r) {
        return call(Kind.READ, () -> delegate.followerRead(to, r));
    }
    @Override public CompletionStage<MetaReply> metaQuery(NodeId to, Request r) {
        return call(Kind.META, () -> delegate.metaQuery(to, r));
    }
    @Override public CompletionStage<IoniaEngine.ViewInfo> probe(NodeId to) {
        return call(Kind.PROBE, () -> delegate.probe(to));
    }
}

