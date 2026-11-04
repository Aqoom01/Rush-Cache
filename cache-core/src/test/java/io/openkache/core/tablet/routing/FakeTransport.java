package io.openkache.core.tablet.routing;

import io.openkache.core.model.NodeId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.PeerMessage;
import io.openkache.core.spi.Transport;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;

/**
 * 네트워크 없이 메시지를 큐에 쌓아 두고 테스트가 골라서 전달하는 {@link Transport}. A1의 {@code InMemoryTransport}와 같은 생각이지만
 * 수신자가 {@code CacheEngine}이 아니라 아무 콜백이라 Controller·LocalRouting을 바로 꽂을 수 있다.
 *
 * <p>유실({@link #drop})·순서 바꾸기({@link #deliverNext})를 결정적으로 재현한다.
 */
final class FakeTransport implements Transport {

    interface Receiver {
        void receive(NodeId from, PeerMessage message);
    }

    private final Map<NodeId, Receiver> receivers = new HashMap<>();
    private final Map<NodeId, Deque<PeerEnvelope>> queues = new HashMap<>();
    private final List<PeerEnvelope> sent = new ArrayList<>();

    void register(NodeId node, Receiver receiver) {
        receivers.put(node, receiver);
    }

    @Override
    public CompletionStage<Void> send(PeerEnvelope envelope) {
        queue(envelope.to()).addLast(envelope);
        sent.add(envelope);
        return CompletableFuture.completedFuture(null);
    }

    List<PeerEnvelope> sent() {
        return List.copyOf(sent);
    }

    List<PeerEnvelope> pending(NodeId to) {
        return List.copyOf(queue(to));
    }

    /** 모든 노드의 큐가 빌 때까지 전달한다. 전달 중에 생긴 응답도 전달한다. */
    int deliverAll() {
        int delivered = 0;
        boolean progress = true;
        while (progress) {
            progress = false;
            for (NodeId to : List.copyOf(queues.keySet())) {
                int n = deliverAll(to);
                delivered += n;
                progress |= n > 0;
            }
        }
        return delivered;
    }

    int deliverAll(NodeId to) {
        int delivered = 0;
        PeerEnvelope e;
        while ((e = queue(to).pollFirst()) != null) {
            deliver(e);
            delivered++;
        }
        return delivered;
    }

    /** {@code to}의 메시지 중 {@code filter}에 맞는 가장 오래된 것을 꺼내 전달한다. */
    PeerEnvelope deliverNext(NodeId to, Predicate<PeerEnvelope> filter) {
        PeerEnvelope e = take(to, filter);
        deliver(e);
        return e;
    }

    /** {@code to}의 메시지 중 {@code filter}에 맞는 가장 오래된 것을 버린다(유실). */
    PeerEnvelope drop(NodeId to, Predicate<PeerEnvelope> filter) {
        return take(to, filter);
    }

    private void deliver(PeerEnvelope e) {
        Receiver r = receivers.get(e.to());
        if (r == null) {
            throw new IllegalStateException("no receiver for " + e.to());
        }
        r.receive(e.from(), e.message());
    }

    private PeerEnvelope take(NodeId to, Predicate<PeerEnvelope> filter) {
        Iterator<PeerEnvelope> it = queue(to).iterator();
        while (it.hasNext()) {
            PeerEnvelope e = it.next();
            if (filter.test(e)) {
                it.remove();
                return e;
            }
        }
        throw new NoSuchElementException("no matching message for " + to);
    }

    private Deque<PeerEnvelope> queue(NodeId to) {
        return queues.computeIfAbsent(to, k -> new ArrayDeque<>());
    }
}
