package io.openkache.core.kv;

import io.openkache.core.api.CacheEngine;
import io.openkache.core.api.Command;
import io.openkache.core.api.EngineSnapshot;
import io.openkache.core.api.RequestContext;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.Bytes;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.runtime.Mailbox;
import io.openkache.core.runtime.TimerEvent;
import io.openkache.core.spi.PeerMessage;
import io.openkache.core.spi.TimeSource;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 단일 노드 인메모리 KV {@link CacheEngine} 구현체.
 *
 * <p>Key마다 {@code Math.floorMod(key.hashCode(), workers.size())}로 소유 worker를 정하고, 그 worker의 mailbox로
 * 연산을 넘긴다. 각 worker는 자신만의 {@link KvStore}를 가지므로 store 변경은 항상 owner worker thread에서만 일어난다.
 *
 * <p>복제·라우팅·Tablet이 없으므로 {@link #snapshot()}의 routingVersion과 ownershipEpoch는 항상
 * {@link RoutingVersion#ZERO}, {@link OwnershipEpoch#ZERO}다. {@link #onPeerMessage}와 {@link #onTimer}는 아직
 * 정의된 동작이 없어 아무 일도 하지 않는다. {@code EXISTS}, {@code COUNT}, TTL, eviction은 {@link Command}에 아직
 * 정의되지 않아 지원하지 않는다.
 */
public final class KvEngine implements CacheEngine {

    private final NodeId nodeId;
    private final List<? extends Mailbox> workers;
    private final KvStore[] stores;
    private final TimeSource timeSource;
    private final Set<CompletableFuture<Result>> pending = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    public KvEngine(NodeId nodeId, List<? extends Mailbox> workers, TimeSource timeSource) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.workers = List.copyOf(Objects.requireNonNull(workers, "workers"));
        if (this.workers.isEmpty()) {
            throw new IllegalArgumentException("workers must not be empty");
        }
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
        this.stores = new KvStore[this.workers.size()];
        for (int i = 0; i < stores.length; i++) {
            stores[i] = new KvStore();
        }
    }

    @Override
    public CompletionStage<Result> execute(Command command, RequestContext context) {
        if (closed.get()) {
            return CompletableFuture.completedFuture(unavailable());
        }
        if (context.deadline().isExpired(timeSource)) {
            return CompletableFuture.completedFuture(timeout());
        }

        int index = workerIndex(command.key());
        CompletableFuture<Result> future = new CompletableFuture<>();
        future.whenComplete((result, error) -> pending.remove(future));
        pending.add(future);

        boolean accepted = workers.get(index).offer(() -> runOnOwner(index, command, context, future));
        if (!accepted) {
            future.complete(busy());
        }
        return future;
    }

    private void runOnOwner(int index, Command command, RequestContext context, CompletableFuture<Result> future) {
        if (closed.get()) {
            future.complete(unavailable());
            return;
        }
        if (context.deadline().isExpired(timeSource)) {
            future.complete(timeout());
            return;
        }
        KvStore store = stores[index];
        Result result =
                switch (command) {
                    case Command.Get get -> doGet(store, get);
                    case Command.Set set -> doSet(store, set);
                    case Command.Delete delete -> doDelete(store, delete);
                };
        future.complete(result);
    }

    private Result doGet(KvStore store, Command.Get get) {
        return store.get(get.key())
                .map(entry -> Result.ok(entry.value(), entry.version(), RoutingVersion.ZERO, OwnershipEpoch.ZERO))
                .orElseGet(() -> Result.of(ResultStatus.MISS, RoutingVersion.ZERO, OwnershipEpoch.ZERO));
    }

    private Result doSet(KvStore store, Command.Set set) {
        long version = store.set(set.key(), set.value());
        return new Result(
                ResultStatus.OK, Optional.empty(), version, RoutingVersion.ZERO, OwnershipEpoch.ZERO, Optional.empty());
    }

    private Result doDelete(KvStore store, Command.Delete delete) {
        boolean existed = store.delete(delete.key());
        return Result.of(existed ? ResultStatus.OK : ResultStatus.MISS, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
    }

    @Override
    public void onPeerMessage(NodeId from, PeerMessage message) {
        // 복제·장애 감지 peer 프로토콜이 없는 단일 노드 엔진이라 아무 일도 하지 않는다.
    }

    @Override
    public void onTimer(TimerEvent event) {
        // 이 엔진은 timer를 예약하지 않으므로 아무 일도 하지 않는다.
    }

    @Override
    public EngineSnapshot snapshot() {
        return new EngineSnapshot(nodeId, timeSource.monotonicNanos(), RoutingVersion.ZERO, OwnershipEpoch.ZERO);
    }

    /** 새 요청을 거절하고, 아직 완료되지 않은 요청(대기 중·처리 중)을 UNAVAILABLE로 완료한다. 여러 번 호출해도 안전하다. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (CompletableFuture<Result> future : pending) {
            if (pending.remove(future)) {
                future.complete(unavailable());
            }
        }
    }

    private int workerIndex(Bytes key) {
        return Math.floorMod(key.hashCode(), workers.size());
    }

    private Result busy() {
        return Result.of(ResultStatus.BUSY, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
    }

    private Result timeout() {
        return Result.of(ResultStatus.TIMEOUT, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
    }

    private Result unavailable() {
        return Result.of(ResultStatus.UNAVAILABLE, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
    }
}
