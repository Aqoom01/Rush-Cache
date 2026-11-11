package io.openkache.runner;

import io.openkache.core.api.Command;
import io.openkache.core.model.Bytes;
import io.openkache.core.tablet.KeyHash;
import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * B1 클러스터용 요청 client. 여러 thread(가상 thread 포함)가 함께 쓴다. 노드마다 연결 pool을 두고, 요청 한 번 보낼 때만 연결을
 * 빌린다. 그래서 BUSY 재시도로 기다리는 요청이 많아도 연결을 붙잡지 않는다.
 *
 * <p>표에서 주인을 찾아 바로 보내고(hop 0), MOVED면 §6-1 redirect 노드로 다시 보낸다(hop 1, 2, ...). redirect 주소가 없거나 노드
 * 사이를 오가면(commit 직후 표 전파 중) 1ms 기다린 뒤 표로 다시 고른다. BUSY(이동 중 freeze)면 간격을 두고 다시 보낸다.
 */
final class ClusterClient implements AutoCloseable {

    /** 결과 상태 코드: wire 상태 또는 아래의 Runner 전용 값. */
    static final int REDIRECT_EXHAUSTED = -1;
    static final int IO_FAILURE = -2;

    /**
     * @param status 마지막 wire 상태 코드, 또는 {@link #REDIRECT_EXHAUSTED}·{@link #IO_FAILURE}
     * @param node 마지막으로 응답한(또는 실패한) 노드 번호
     * @param hops 따라간 MOVED 수
     * @param busyRetries BUSY 뒤 다시 보낸 수
     * @param value GET HIT의 값(그 외 null)
     * @param firstSendNanos 처음 보낸 시각(연결을 빌린 직후, {@link System#nanoTime()})
     */
    record Result(int status, int node, int hops, int busyRetries, byte[] value, long firstSendNanos) {}

    /** 노드 하나의 연결 pool. 연결 수 상한까지 필요할 때 연다. */
    private static final class Pool {
        final RunnerConfig.Target target;
        final long socketTimeoutMs;
        final Semaphore permits;
        final ConcurrentLinkedQueue<WireClient> idle = new ConcurrentLinkedQueue<>();

        Pool(RunnerConfig.Target target, long socketTimeoutMs, int size) {
            this.target = target;
            this.socketTimeoutMs = socketTimeoutMs;
            this.permits = new Semaphore(size);
        }

        WireClient acquire() throws IOException {
            permits.acquireUninterruptibly();
            WireClient c = idle.poll();
            if (c != null) return c;
            try {
                return new WireClient(target.host(), target.port(), socketTimeoutMs);
            } catch (IOException e) {
                permits.release();
                throw e;
            }
        }

        void release(WireClient c, boolean broken) {
            if (broken) c.close();
            else idle.offer(c);
            permits.release();
        }

        void close() {
            WireClient c;
            while ((c = idle.poll()) != null) c.close();
        }
    }

    private final ClusterRouting routing;
    private final ClusterConfig cluster;
    private final long requestTimeoutMs;
    private final Pool[] pools;
    private final AtomicLong operationId = new AtomicLong();

    /** @param connectionsPerNode 노드마다 동시에 열 연결 수 상한 */
    ClusterClient(ClusterRouting routing, ClusterConfig cluster, long requestTimeoutMs, long socketTimeoutMs,
                  int connectionsPerNode) {
        this.routing = routing;
        this.cluster = cluster;
        this.requestTimeoutMs = requestTimeoutMs;
        this.pools = new Pool[routing.nodeCount()];
        for (int i = 0; i < pools.length; i++) pools[i] = new Pool(routing.node(i), socketTimeoutMs, connectionsPerNode);
    }

    Result send(Command command, Bytes key) {
        long hash = KeyHash.of(key);
        int node = routing.owner(hash);
        int hops = 0;
        int busy = 0;
        long first = 0;
        while (true) {
            WireClient.Response r;
            WireClient c;
            try {
                c = pools[node].acquire();
            } catch (IOException e) {
                return new Result(IO_FAILURE, node, hops, busy, null, first == 0 ? System.nanoTime() : first);
            }
            if (first == 0) first = System.nanoTime();
            long op = operationId.incrementAndGet();
            try {
                r = c.send(command, 1000 + (op & 0xffff), op, 0, requestTimeoutMs, routing.version());
                pools[node].release(c, false);
            } catch (IOException e) {
                pools[node].release(c, true);
                return new Result(IO_FAILURE, node, hops, busy, null, first);
            }
            if (r.routingVersion() > routing.version()) routing.requestRefresh();
            if (r.status() == WireProtocol.STATUS_MOVED) {
                routing.requestRefresh();
                if (++hops > cluster.redirectMax()) return new Result(REDIRECT_EXHAUSTED, node, hops - 1, busy, null, first);
                int next = routing.redirectTarget(r.tail());
                if (next < 0 || next == node || hops >= 2) LockSupport.parkNanos(1_000_000);
                if (next < 0 || next == node) next = routing.owner(hash);
                node = next;
                continue;
            }
            if (r.status() == WireProtocol.STATUS_BUSY && busy < cluster.busyRetryMax()) {
                long wait = Math.min(cluster.busyRetryMaxIntervalMs(), cluster.busyRetryIntervalMs() << Math.min(busy, 20));
                busy++;
                LockSupport.parkNanos(wait * 1_000_000);
                continue;
            }
            byte[] value = r.status() == WireProtocol.STATUS_OK && command instanceof Command.Get ? r.value() : null;
            return new Result(r.status(), node, hops, busy, value, first);
        }
    }

    @Override
    public void close() {
        for (Pool p : pools) p.close();
    }
}
