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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

/**
 * 단일 노드 인메모리 KV {@link CacheEngine} 구현체.
 *
 * <p>Key마다 {@code Math.floorMod(key.hashCode(), workers.size())}로 소유 worker를 정하고, 그 worker의 mailbox로
 * 연산을 넘긴다. 각 worker는 자신만의 {@link KvStore}를 가지므로 store 접근(읽기 포함)은 항상 owner worker thread에서만
 * 일어난다.
 *
 * <p>{@link Command.Count}만 Key가 없어 모든 worker에 한 번씩 보낸다. 각 worker가 자기 store 크기를 더하고 마지막 worker가
 * 결과를 완료한다. worker마다 세는 시점이 다르므로, 세는 동안 들어온 SET·DELETE는 반영될 수도 안 될 수도 있다(한 순간의
 * snapshot이 아니다). 어느 한 worker의 mailbox라도 가득 차면 {@link ResultStatus#BUSY}다.
 *
 * <p>복제·라우팅·Tablet이 없으므로 {@link #snapshot()}의 routingVersion과 ownershipEpoch는 항상
 * {@link RoutingVersion#ZERO}, {@link OwnershipEpoch#ZERO}다. {@link #onPeerMessage}와 {@link #onTimer}는 아직
 * 정의된 동작이 없어 아무 일도 하지 않는다. TTL, eviction, SCAN/RANGE, 영속화는 지원하지 않는다.
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
        return switch (command) {
            case Command.Get get -> onOwner(get.key(), context, store -> doGet(store, get));
            case Command.Set set -> onOwner(set.key(), context, store -> doSet(store, set));
            case Command.Delete delete -> onOwner(delete.key(), context, store -> doDelete(store, delete));
            case Command.Exists exists -> onOwner(exists.key(), context, store -> doExists(store, exists));
            case Command.Count count -> onAllOwners(context);
        };
    }

    /** {@code key}의 owner worker에서 {@code operation}을 실행한다. mailbox가 가득 차면 BUSY. */
    private CompletionStage<Result> onOwner(Bytes key, RequestContext context, Function<KvStore, Result> operation) {
        int index = workerIndex(key);
        CompletableFuture<Result> future = track(new CompletableFuture<>());
        boolean accepted =
                workers.get(index)
                        .offer(
                                () -> {
                                    if (!stillRunnable(context, future)) {
                                        return;
                                    }
                                    future.complete(operation.apply(stores[index]));
                                });
        if (!accepted) {
            future.complete(busy());
        }
        return future;
    }

    /** 모든 worker에서 자기 store 크기를 더해 COUNT 결과를 만든다. 한 worker라도 mailbox가 가득 차면 BUSY. */
    private CompletionStage<Result> onAllOwners(RequestContext context) {
        CompletableFuture<Result> future = track(new CompletableFuture<>());
        AtomicLong total = new AtomicLong();
        AtomicInteger remaining = new AtomicInteger(stores.length);
        for (int i = 0; i < stores.length; i++) {
            int index = i;
            boolean accepted =
                    workers.get(index)
                            .offer(
                                    () -> {
                                        if (!stillRunnable(context, future)) {
                                            return;
                                        }
                                        total.addAndGet(stores[index].size());
                                        if (remaining.decrementAndGet() == 0) {
                                            future.complete(countResult(total.get()));
                                        }
                                    });
            if (!accepted) {
                future.complete(busy());
                return future;
            }
        }
        return future;
    }

    /** owner worker에서 실행 직전에 다시 확인한다. 닫혔거나 deadline이 지났으면 future를 끝내고 {@code false}. */
    private boolean stillRunnable(RequestContext context, CompletableFuture<Result> future) {
        if (closed.get()) {
            future.complete(unavailable());
            return false;
        }
        if (context.deadline().isExpired(timeSource)) {
            future.complete(timeout());
            return false;
        }
        return true;
    }

    private CompletableFuture<Result> track(CompletableFuture<Result> future) {
        future.whenComplete((result, error) -> pending.remove(future));
        pending.add(future);
        return future;
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

    private Result doExists(KvStore store, Command.Exists exists) {
        return store.get(exists.key())
                .map(
                        entry ->
                                new Result(
                                        ResultStatus.OK,
                                        Optional.empty(),
                                        entry.version(),
                                        RoutingVersion.ZERO,
                                        OwnershipEpoch.ZERO,
                                        Optional.empty()))
                .orElseGet(() -> Result.of(ResultStatus.MISS, RoutingVersion.ZERO, OwnershipEpoch.ZERO));
    }

    /** COUNT 결과. 개수를 8-byte big-endian으로 {@code value}에 담는다({@link Command.Count} 참고). */
    private static Result countResult(long total) {
        byte[] encoded = ByteBuffer.allocate(Long.BYTES).putLong(total).array();
        return Result.ok(Bytes.copyOf(encoded), 0, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
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
