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
import io.openkache.core.spi.MetricsSink;
import io.openkache.core.spi.PeerMessage;
import io.openkache.core.spi.TimeSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;

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
 * 정의된 동작이 없어 아무 일도 하지 않는다. SCAN/RANGE는 {@link Command}가 아니라 {@link #scan}·{@link #range}로 제공한다.
 * TTL과 영속화는 지원하지 않는다. 저장소는 {@link KvStorage}로 고른다. {@link KvStorage.Nvme}만 용량이 차면 축출한다.
 */
public final class KvEngine implements CacheEngine {

    private final NodeId nodeId;
    private final List<? extends Mailbox> workers;
    private final KvStore[] stores;
    private final TimeSource timeSource;
    private final Set<CompletableFuture<Result>> pending = ConcurrentHashMap.newKeySet();
    private final Set<CompletableFuture<ScanResult>> pendingScans = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    /** 메모리 저장소로 만든다. */
    public KvEngine(NodeId nodeId, List<? extends Mailbox> workers, TimeSource timeSource) {
        this(nodeId, workers, timeSource, KvStorage.memory(), MetricsSink.noop());
    }

    /**
     * {@code storage}로 worker마다 저장소를 연다. 호출한 thread에서 파일을 열므로 EventLoop에서 호출하지 않는다.
     *
     * @param metrics 저장소 지표(NVMe 읽기·쓰기 횟수 등)를 받는다. owner worker thread에서 호출된다
     * @throws UncheckedIOException NVMe 저장소 디렉터리를 만들거나 파일을 열 수 없을 때(direct I/O 미지원 포함)
     */
    public KvEngine(
            NodeId nodeId, List<? extends Mailbox> workers, TimeSource timeSource, KvStorage storage, MetricsSink metrics) {
        this.nodeId = Objects.requireNonNull(nodeId, "nodeId");
        this.workers = List.copyOf(Objects.requireNonNull(workers, "workers"));
        if (this.workers.isEmpty()) {
            throw new IllegalArgumentException("workers must not be empty");
        }
        this.timeSource = Objects.requireNonNull(timeSource, "timeSource");
        Objects.requireNonNull(storage, "storage");
        Objects.requireNonNull(metrics, "metrics");
        this.stores = new KvStore[this.workers.size()];
        try {
            for (int i = 0; i < stores.length; i++) {
                stores[i] = switch (storage) {
                    case KvStorage.Memory memory -> new MemoryKvStore();
                    case KvStorage.Nvme nvme -> NvmeKvStore.open(nvme, i, stores.length, metrics);
                };
            }
        } catch (IOException e) {
            closeStores();
            throw new UncheckedIOException("failed to open storage " + storage, e);
        } catch (RuntimeException e) {
            closeStores();
            throw e;
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

    /**
     * SCAN: {@code keyFilter}를 통과한 항목을 {@link KvEntry#KEY_ORDER}로 정렬해 앞에서 {@code limit}개까지 돌려준다.
     *
     * <p>{@code Command}/{@code Result}는 값 하나만 다루므로 SCAN/RANGE는 {@link #execute}가 아니라 이 메서드로 제공한다. TCP
     * opcode는 없다. 노드 안에서 Tablet 이동(B3) 같은 용도로 호출한다. 예: Tablet 해시 범위에 속한 항목만 고르려면
     * {@code key -> tabletRange.contains(KeyHash.of(key))}를 넘긴다. KV는 Tablet·해시를 모른다.
     *
     * <p>COUNT처럼 모든 worker에 보내 각자 걸러낸 뒤 합친다. worker마다 보는 시점이 달라 노드 전체의 한 순간 snapshot은 아니다.
     * 한 worker 안에서는 그 worker의 다른 연산과 끼어들지 않는다. 어느 한 worker의 mailbox라도 가득 차면
     * {@link ResultStatus#BUSY}. 결과 합치기와 정렬은 마지막으로 끝난 worker thread에서 한다.
     *
     * @param keyFilter owner worker thread에서 Key마다 호출된다. 저장소 전체를 훑으므로 가볍고 순수해야 하며 blocking·예외를
     *     던지면 안 된다. 예외를 던지면 결과 stage가 예외로 완료된다
     * @param limit 돌려줄 최대 항목 수(1 이상). 걸리면 {@link ScanResult#truncated()}가 {@code true}
     * @param context deadline 확인에만 쓴다
     */
    public CompletionStage<ScanResult> scan(Predicate<Bytes> keyFilter, int limit, RequestContext context) {
        Objects.requireNonNull(keyFilter, "keyFilter");
        Objects.requireNonNull(context, "context");
        if (limit < 1) {
            throw new IllegalArgumentException("limit must be >= 1: " + limit);
        }
        if (closed.get()) {
            return CompletableFuture.completedFuture(ScanResult.of(ResultStatus.UNAVAILABLE));
        }
        if (context.deadline().isExpired(timeSource)) {
            return CompletableFuture.completedFuture(ScanResult.of(ResultStatus.TIMEOUT));
        }

        CompletableFuture<ScanResult> future = new CompletableFuture<>();
        future.whenComplete((result, error) -> pendingScans.remove(future));
        pendingScans.add(future);

        List<KvStore.Partial> partials = new CopyOnWriteArrayList<>();
        AtomicInteger remaining = new AtomicInteger(stores.length);
        for (int i = 0; i < stores.length; i++) {
            int index = i;
            boolean accepted =
                    workers.get(index)
                            .offer(
                                    () -> {
                                        if (closed.get()) {
                                            future.complete(ScanResult.of(ResultStatus.UNAVAILABLE));
                                            return;
                                        }
                                        if (context.deadline().isExpired(timeSource)) {
                                            future.complete(ScanResult.of(ResultStatus.TIMEOUT));
                                            return;
                                        }
                                        try {
                                            partials.add(stores[index].scan(keyFilter, limit));
                                        } catch (StoreException e) {
                                            future.complete(ScanResult.of(e.status()));
                                            return;
                                        } catch (RuntimeException e) {
                                            future.completeExceptionally(e);
                                            return;
                                        }
                                        if (remaining.decrementAndGet() == 0) {
                                            future.complete(merge(partials, limit));
                                        }
                                    });
            if (!accepted) {
                future.complete(ScanResult.of(ResultStatus.BUSY));
                return future;
            }
        }
        return future;
    }

    /**
     * RANGE: Key 순서({@link KvEntry#KEY_ORDER})로 {@code startInclusive} 이상 {@code endExclusive} 미만인 항목을 돌려준다.
     * 비어 있는 경계는 그 방향으로 제한이 없다는 뜻이다. 나머지 동작은 {@link #scan}과 같다.
     */
    public CompletionStage<ScanResult> range(
            Optional<Bytes> startInclusive, Optional<Bytes> endExclusive, int limit, RequestContext context) {
        Objects.requireNonNull(startInclusive, "startInclusive");
        Objects.requireNonNull(endExclusive, "endExclusive");
        if (startInclusive.isPresent()
                && endExclusive.isPresent()
                && KvEntry.KEY_ORDER.compare(startInclusive.get(), endExclusive.get()) >= 0) {
            throw new IllegalArgumentException("start must be < end: " + startInclusive.get() + " .. " + endExclusive.get());
        }
        Predicate<Bytes> inRange =
                key ->
                        (startInclusive.isEmpty() || KvEntry.KEY_ORDER.compare(startInclusive.get(), key) <= 0)
                                && (endExclusive.isEmpty() || KvEntry.KEY_ORDER.compare(key, endExclusive.get()) < 0);
        return scan(inRange, limit, context);
    }

    /** worker별 결과를 합쳐 Key 순서로 정렬하고 앞 {@code limit}개만 남긴다. */
    private static ScanResult merge(List<KvStore.Partial> partials, int limit) {
        List<KvEntry> all = new ArrayList<>();
        boolean anyTruncated = false;
        for (KvStore.Partial partial : partials) {
            all.addAll(partial.entries());
            anyTruncated |= partial.truncated();
        }
        all.sort((a, b) -> KvEntry.KEY_ORDER.compare(a.key(), b.key()));
        boolean truncated = anyTruncated || all.size() > limit;
        List<KvEntry> kept = all.size() > limit ? all.subList(0, limit) : all;
        return new ScanResult(ResultStatus.OK, kept, truncated);
    }

    private Result doGet(KvStore store, Command.Get get) {
        try {
            return store.get(get.key())
                    .map(entry -> Result.ok(entry.value(), entry.version(), RoutingVersion.ZERO, OwnershipEpoch.ZERO))
                    .orElseGet(() -> Result.of(ResultStatus.MISS, RoutingVersion.ZERO, OwnershipEpoch.ZERO));
        } catch (StoreException e) {
            return Result.of(e.status(), RoutingVersion.ZERO, OwnershipEpoch.ZERO);
        }
    }

    private Result doSet(KvStore store, Command.Set set) {
        try {
            long version = store.set(set.key(), set.value());
            return new Result(
                    ResultStatus.OK, Optional.empty(), version, RoutingVersion.ZERO, OwnershipEpoch.ZERO, Optional.empty());
        } catch (StoreException e) {
            return Result.of(e.status(), RoutingVersion.ZERO, OwnershipEpoch.ZERO);
        }
    }

    private Result doDelete(KvStore store, Command.Delete delete) {
        boolean existed = store.delete(delete.key());
        return Result.of(existed ? ResultStatus.OK : ResultStatus.MISS, RoutingVersion.ZERO, OwnershipEpoch.ZERO);
    }

    private Result doExists(KvStore store, Command.Exists exists) {
        try {
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
        } catch (StoreException e) {
            return Result.of(e.status(), RoutingVersion.ZERO, OwnershipEpoch.ZERO);
        }
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

    /**
     * 새 요청을 거절하고, 아직 완료되지 않은 요청(대기 중·처리 중)을 UNAVAILABLE로 완료한 뒤 저장소 파일을 닫는다. 여러 번
     * 호출해도 안전하다.
     *
     * <p>저장소는 owner worker가 아닌 이 thread에서 닫는다. 그 순간 worker가 실행 중이던 연산은 I/O 오류로 끝나지만, 그 요청은
     * 이미 UNAVAILABLE로 완료됐으므로 응답이 바뀌지 않는다.
     */
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
        for (CompletableFuture<ScanResult> future : pendingScans) {
            if (pendingScans.remove(future)) {
                future.complete(ScanResult.of(ResultStatus.UNAVAILABLE));
            }
        }
        closeStores();
    }

    private void closeStores() {
        for (KvStore store : stores) {
            if (store != null) {
                store.close();
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
