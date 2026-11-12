package io.openkache.runner;

import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.routing.TableMessageCodec;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Runner가 들고 있는 B1 라우팅 표. node 0의 peer listener에서 TABLE_DUMP(opcode 9)로 받는다.
 *
 * <p>MOVED나 더 새 표 버전을 본 thread는 {@link #requestRefresh()}만 부르고, 표 조회는 background thread 하나가 한다(요청
 * thread가 표 조회로 막히지 않게). 어느 thread에서나 읽을 수 있다.
 */
final class ClusterRouting implements AutoCloseable {

    static final int OP_TABLE_DUMP = 9;

    private final List<RunnerConfig.Target> nodes;
    private final int peerPort;
    private final long timeoutMs;
    private final Map<String, Integer> nodeByAddress = new HashMap<>();
    private final AtomicBoolean refreshWanted = new AtomicBoolean();
    private final AtomicLong refreshes = new AtomicLong();
    private final Thread refresher;
    private volatile RoutingTable table;
    private volatile boolean closed;

    ClusterRouting(List<RunnerConfig.Target> nodes, int peerPort, long timeoutMs) throws IOException {
        this.nodes = List.copyOf(nodes);
        this.peerPort = peerPort;
        this.timeoutMs = timeoutMs;
        for (int i = 0; i < nodes.size(); i++) {
            nodeByAddress.put(nodes.get(i).host() + ":" + nodes.get(i).port(), i);
        }
        this.table = fetch();
        this.refresher = new Thread(this::refreshLoop, "cluster-table-refresh");
        refresher.setDaemon(true);
        refresher.start();
    }

    RoutingTable table() {
        return table;
    }

    long version() {
        return table.version().value();
    }

    int peerPort() {
        return peerPort;
    }

    int nodeCount() {
        return nodes.size();
    }

    RunnerConfig.Target node(int index) {
        return nodes.get(index);
    }

    /** Tablet 주인 NodeId = target.hosts 순서. 표에 target.hosts 밖의 노드가 있으면 예외. */
    int owner(long keyHash) {
        long owner = table.lookup(keyHash).owner().value();
        if (owner >= nodes.size()) throw new IllegalStateException("table names node " + owner + " outside target.hosts");
        return (int) owner;
    }

    /** §6-1 redirect가 가리키는 노드 번호. 주소가 없거나 모르는 주소면 -1. */
    int redirectTarget(byte[] tail) {
        if (tail.length == 0) return -1;
        try {
            return TableMessageCodec.decodeRedirect(tail)
                    .map(a -> a.host() + ":" + a.port())
                    .map(a -> nodeByAddress.getOrDefault(a, -1))
                    .orElse(-1);
        } catch (IllegalArgumentException e) {
            return -1;
        }
    }

    void requestRefresh() {
        refreshWanted.set(true);
    }

    /** 지금 바로 표를 받아 교체한다(이동 지시 직전처럼 최신 주인이 필요할 때). */
    RoutingTable refreshNow() throws IOException {
        RoutingTable fetched = fetch();
        accept(fetched);
        return table;
    }

    long refreshCount() {
        return refreshes.get();
    }

    private synchronized void accept(RoutingTable candidate) {
        if (candidate.version().isNewerThan(table.version())) table = candidate;
    }

    private RoutingTable fetch() throws IOException {
        RunnerConfig.Target controller = nodes.get(0);
        try (WireClient client = new WireClient(controller.host(), peerPort, timeoutMs)) {
            WireClient.Response r = client.peer(OP_TABLE_DUMP, new byte[0]);
            if (r.status() != WireProtocol.STATUS_OK) throw new IOException("TABLE_DUMP status " + r.status());
            refreshes.incrementAndGet();
            return TableMessageCodec.decodeTable(r.value());
        }
    }

    private void refreshLoop() {
        while (!closed) {
            if (refreshWanted.getAndSet(false)) {
                try {
                    accept(fetch());
                } catch (IOException | RuntimeException e) {
                    refreshWanted.set(true);
                }
            }
            LockSupport.parkNanos(2_000_000);
        }
    }

    @Override
    public void close() {
        closed = true;
        refresher.interrupt();
    }
}
