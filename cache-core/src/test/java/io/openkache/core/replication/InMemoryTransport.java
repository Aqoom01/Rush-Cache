package io.openkache.core.replication;

import io.openkache.core.api.CacheEngine;
import io.openkache.core.model.NodeId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.Transport;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/**
 * 네트워크 없이 노드 사이 메시지를 큐에 쌓아 두고, 테스트가 골라서 전달하는 {@link Transport}.
 *
 * <p>{@link #send}는 목적지 큐에 넣기만 하고 즉시 완료한다. 수신 엔진의 {@code onPeerMessage}는 테스트가 {@link #deliverNext},
 * {@link #deliverAll}을 부를 때만 호출되므로, 특정 메시지를 보류·유실·중복 전달하는 시나리오를 결정적으로 재현할 수 있다.
 * 전달 자체는 lock을 잡지 않은 채 호출 thread에서 한다.
 *
 * <p>A1 variant 공통 테스트 도구다. 지연·분단 주입은 아직 없다.
 */
public final class InMemoryTransport implements Transport {

    private final Map<NodeId, CacheEngine> engines = new HashMap<>();
    private final Map<NodeId, Deque<PeerEnvelope>> queues = new HashMap<>();
    private final List<PeerEnvelope> sentLog = new ArrayList<>();

    /** {@code node}로 가는 메시지를 받을 엔진을 등록한다. */
    public synchronized void register(NodeId node, CacheEngine engine) {
        engines.put(Objects.requireNonNull(node, "node"), Objects.requireNonNull(engine, "engine"));
    }

    @Override
    public synchronized CompletionStage<Void> send(PeerEnvelope envelope) {
        queues.computeIfAbsent(envelope.to(), ignored -> new ArrayDeque<>()).addLast(envelope);
        sentLog.add(envelope);
        notifyAll();
        return CompletableFuture.completedFuture(null);
    }

    /** {@code to}에 도착했지만 아직 전달하지 않은 메시지 수. */
    public synchronized int pendingCount(NodeId to) {
        return queue(to).size();
    }

    /** {@code to}에 아직 전달하지 않은 메시지의 복사본(도착 순서). */
    public synchronized List<PeerEnvelope> pending(NodeId to) {
        return List.copyOf(queue(to));
    }

    /** 지금까지 {@link #send}된 모든 메시지의 복사본(전송 순서). 흐름 로그용. */
    public synchronized List<PeerEnvelope> sentLog() {
        return List.copyOf(sentLog);
    }

    /** {@code to}의 가장 오래된 메시지 하나를 꺼내 전달한다. */
    public PeerEnvelope deliverNext(NodeId to) {
        return deliverNext(to, ignored -> true);
    }

    /** {@code to}의 메시지 중 {@code filter}에 맞는 가장 오래된 것을 꺼내 전달한다. 없으면 예외. */
    public PeerEnvelope deliverNext(NodeId to, Predicate<PeerEnvelope> filter) {
        PeerEnvelope envelope = take(to, filter);
        deliver(envelope);
        return envelope;
    }

    /** {@code to}의 메시지를 비어 있을 때까지 도착 순서대로 전달하고 전달한 개수를 돌려준다. */
    public int deliverAll(NodeId to) {
        int delivered = 0;
        while (true) {
            PeerEnvelope envelope;
            synchronized (this) {
                envelope = queue(to).pollFirst();
            }
            if (envelope == null) {
                return delivered;
            }
            deliver(envelope);
            delivered++;
        }
    }

    /**
     * 큐를 거치지 않고 {@code envelope}를 수신 엔진에 바로 전달한다. 이미 전달한 메시지를 다시 넣어 중복 전달을 재현할 때 쓴다.
     */
    public void deliver(PeerEnvelope envelope) {
        CacheEngine engine;
        synchronized (this) {
            engine = engines.get(envelope.to());
        }
        if (engine == null) {
            throw new IllegalStateException("no engine registered for " + envelope.to());
        }
        engine.onPeerMessage(envelope.from(), envelope.message());
    }

    /** {@code to}의 메시지 중 {@code filter}에 맞는 가장 오래된 것을 꺼내 버린다(유실). 없으면 예외. */
    public PeerEnvelope drop(NodeId to, Predicate<PeerEnvelope> filter) {
        return take(to, filter);
    }

    /** {@code to}에 {@code count}개 이상 쌓일 때까지 기다린다. 실제 worker thread와 함께 쓸 때만 필요하다. */
    public synchronized void awaitPending(NodeId to, int count, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (queue(to).size() < count) {
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new AssertionError(
                        "expected " + count + " pending message(s) for " + to + " but had " + queue(to).size());
            }
            wait(Math.max(1, remainingNanos / 1_000_000));
        }
    }

    private synchronized PeerEnvelope take(NodeId to, Predicate<PeerEnvelope> filter) {
        Iterator<PeerEnvelope> iterator = queue(to).iterator();
        while (iterator.hasNext()) {
            PeerEnvelope candidate = iterator.next();
            if (filter.test(candidate)) {
                iterator.remove();
                return candidate;
            }
        }
        throw new NoSuchElementException("no matching pending message for " + to);
    }

    private Deque<PeerEnvelope> queue(NodeId to) {
        return queues.computeIfAbsent(to, ignored -> new ArrayDeque<>());
    }
}
