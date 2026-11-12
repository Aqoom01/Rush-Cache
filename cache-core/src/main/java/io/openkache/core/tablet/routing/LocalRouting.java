package io.openkache.core.tablet.routing;

import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.PeerMessage;
import io.openkache.core.spi.Transport;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import io.openkache.core.tablet.TabletRange;
import io.openkache.core.tablet.routing.OwnershipCheck.Decision;
import io.openkache.core.tablet.routing.TableMessage.GetTable;
import io.openkache.core.tablet.routing.TableMessage.TablePush;
import io.openkache.core.tablet.routing.TableMessage.TableReply;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 노드 하나가 들고 있는 라우팅 상태: 표의 사본과, 표에는 없는 노드 로컬 상태("이 Tablet은 지금 받지 않는다").
 *
 * <p>표 사본은 {@link Controller}가 push한 것이나 {@link GetTable}로 받아 온 것이며, 버전이 더 클 때만 교체한다. 그래서 늦게 도착한
 * 옛 push로 표가 뒤로 가지 않는다. 요청 판정은 {@link OwnershipCheck}에 맡기고, 여기서는 그 위에 거절 플래그만 얹는다.
 *
 * <p>거절 플래그는 B3 cutover 순서의 1단계다: (1) 옛 주인이 {@link #stopServing}으로 그 Tablet의 요청을 {@code MOVED}로 거절하기
 * 시작하고 → (2) 마지막 변경분을 복사하고 → (3) Controller가 주인을 바꾸면 → (4) 새 주인이 받는다. (1)~(3) 사이에 옛 주인으로 온 쓰기가
 * 전부 거절되므로 어느 순간에도 쓰기를 받는 주인이 둘이 되지 않는다. 플래그는 그때의 epoch에 묶이며, 주인이 바뀐 표를 받으면 자동으로
 * 풀린다(이동이 취소돼 다시 받으려면 {@link #resumeServing}).
 *
 * <p>어느 thread에서나 호출할 수 있다. 표 교체와 플래그 변경은 짧은 lock 안에서, 판정은 lock 없이 하되 판정 중에 표가 바뀌었으면
 * 다시 판정한다.
 */
public final class LocalRouting {

    /** 거절을 건 순간의 epoch·범위. 범위는 그 뒤 Tablet이 split돼 id가 늘어나도 거절을 승계하기 위해 둔다. */
    private record Refusal(OwnershipEpoch epoch, TabletRange range, Optional<NodeId> destination, Tablet origin) {}

    private final NodeId self;
    private final Transport transport;
    private volatile RoutingTable table;
    private final Map<TabletId, Refusal> refusing = new ConcurrentHashMap<>();
    private volatile boolean staleHint;

    public LocalRouting(NodeId self, RoutingTable initial, Transport transport) {
        this.self = Objects.requireNonNull(self, "self");
        this.table = Objects.requireNonNull(initial, "initial");
        this.transport = Objects.requireNonNull(transport, "transport");
    }

    public NodeId self() {
        return self;
    }

    public RoutingTable table() {
        return table;
    }

    /** 응답에 실어 보낼 내 표 버전(피기백). */
    public RoutingVersion version() {
        return table.version();
    }

    /**
     * {@code candidate}가 내 표보다 새 버전이면 교체하고 {@code true}. 주인이나 epoch가 바뀐 Tablet의 거절 플래그는 푼다.
     * 거절 중인 Tablet이 split됐으면(같은 주인·같은 epoch로 범위만 나뉨) 새 id의 조각에도 거절을 승계한다. 그래야 cutover 중에
     * B2가 split해도 옛 주인이 오른쪽 조각의 쓰기를 받지 않는다.
     */
    public synchronized boolean accept(RoutingTable candidate) {
        if (!candidate.version().isNewerThan(table.version())) {
            return false;
        }
        // 승계할 플래그는 표를 바꾸기 전에 넣는다(옛 표에는 없는 id라 판정에 영향이 없다). 지우는 것은 표를 바꾼 뒤에 한다.
        // check()는 플래그를 읽은 뒤 표가 바뀌었는지 재확인하므로 이 순서에 의존한다
        for (Refusal r : List.copyOf(refusing.values())) {
            for (Tablet t : candidate.tablets()) {
                if (t.owner().equals(self) && t.epoch().equals(r.epoch()) && within(t.range(), r.range())) {
                    refusing.putIfAbsent(t.id(), new Refusal(r.epoch(), t.range(), r.destination(), r.origin()));
                }
            }
        }
        table = candidate;
        staleHint = false;
        refusing.entrySet().removeIf(e -> {
            TabletId id = e.getKey();
            if (!candidate.contains(id)) {
                return true;
            }
            Tablet t = candidate.get(id);
            return !t.owner().equals(self) || !t.epoch().equals(e.getValue().epoch());
        });
        return true;
    }

    /** {@code inner}가 {@code outer} 안에 들어가는가. end 0은 2^64. */
    private static boolean within(TabletRange inner, TabletRange outer) {
        if (!outer.contains(inner.start())) {
            return false;
        }
        long innerEnd = inner.end() == 0 ? -1L : inner.end() - 1; // 마지막 해시 지점
        return outer.contains(innerEnd);
    }

    /**
     * 내가 주인인 Tablet {@code id}의 요청을 지금부터 {@code MOVED}로 거절한다. {@code destination}을 주면 redirect 힌트로 쓴다
     * (새 주인이 아직 표를 못 받았으면 되돌아올 수 있으므로 클라이언트는 표도 함께 갱신한다).
     *
     * @throws IllegalArgumentException 내가 주인이 아닌 Tablet
     */
    public synchronized void stopServing(TabletId id, Optional<NodeId> destination) {
        Tablet t = table.get(id);
        if (!t.owner().equals(self)) {
            throw new IllegalArgumentException(id + " is owned by " + t.owner() + ", not " + self);
        }
        refusing.put(id, new Refusal(t.epoch(), t.range(), Objects.requireNonNull(destination, "destination"), t));
    }

    /** 이동이 취소됐을 때 거절을 푼다. */
    public void resumeServing(TabletId id) {
        refusing.remove(id);
    }

    /** 취소된 이동이 건 거절과 split으로 승계된 거절을 함께 푼다. */
    public synchronized void resumeServing(Tablet origin) {
        refusing.entrySet().removeIf(e -> e.getValue().origin().equals(origin));
    }

    public boolean isRefusing(TabletId id) {
        return refusing.containsKey(id);
    }

    /**
     * 요청 하나를 판정한다. {@link OwnershipCheck#decide}에 거절 플래그를 더한 것이다. 요청이 나보다 새 표를 들고 왔으면
     * {@link #needsRefresh()}가 켜진다.
     */
    public Decision check(long keyHash, RoutingVersion clientVersion, OwnershipEpoch clientEpoch) {
        while (true) {
            RoutingTable t = table;
            Decision d = OwnershipCheck.decide(self, t, keyHash, clientVersion, clientEpoch);
            if (d.myTableIsStale()) {
                staleHint = true;
            }
            if (!d.allowed()) {
                return d;
            }
            Refusal r = refusing.get(d.tablet().id());
            // 옛 표로 "내가 주인"이라 판정한 사이에 accept()가 표를 바꾸고 플래그를 지웠을 수 있다. 그러면 새 표로 다시 판정한다
            if (table != t) {
                continue;
            }
            if (r != null) {
                return new Decision(ResultStatus.MOVED, d.tablet(), r.destination(), d.serverVersion(), d.myTableIsStale());
            }
            return d;
        }
    }

    /** 나보다 새 표를 들고 온 요청을 본 뒤 아직 새 표를 못 받았으면 {@code true}. 호출자는 {@link #requestTable}을 부른다. */
    public boolean needsRefresh() {
        return staleHint;
    }

    /** Controller에 내 버전보다 새 표를 달라고 요청한다. 응답은 {@link #onPeerMessage}로 돌아온다. */
    public CompletionStage<Void> requestTable(NodeId controller) {
        return transport.send(new PeerEnvelope(self, controller, new GetTable(version())));
    }

    /**
     * 표 메시지를 처리한다. 엔진의 {@code onPeerMessage}가 위임한다. {@link TablePush}·{@link TableReply}는 표를 받아들이고,
     * {@link GetTable}은 내 표로 응답한다(Controller가 없을 때 새 클라이언트가 노드에게 물을 수 있게).
     *
     * @return 표 메시지였으면 {@code true}. 아니면 아무것도 하지 않고 {@code false}
     */
    public boolean onPeerMessage(NodeId from, PeerMessage message) {
        if (!(message instanceof TableMessage m)) {
            return false;
        }
        switch (m) {
            case TablePush p -> accept(p.table());
            case TableReply r -> r.table().ifPresent(this::accept);
            case GetTable g -> {
                RoutingTable t = table;
                Optional<RoutingTable> newer = t.version().isNewerThan(g.since()) ? Optional.of(t) : Optional.empty();
                transport.send(new PeerEnvelope(self, from, new TableReply(newer, t.version())));
            }
        }
        return true;
    }
}
