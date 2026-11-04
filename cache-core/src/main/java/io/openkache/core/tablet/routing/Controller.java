package io.openkache.core.tablet.routing;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.model.TabletId;
import io.openkache.core.spi.PeerEnvelope;
import io.openkache.core.spi.PeerMessage;
import io.openkache.core.spi.Transport;
import io.openkache.core.tablet.Change;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.routing.TableMessage.GetTable;
import io.openkache.core.tablet.routing.TableMessage.TablePush;
import io.openkache.core.tablet.routing.TableMessage.TableReply;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.UnaryOperator;

/**
 * 라우팅 표의 원본을 가진 쪽. 표를 바꾸는 유일한 곳이고, 바뀐 표를 노드들에 push한다.
 *
 * <p>모든 변경은 {@link #apply(RoutingVersion, UnaryOperator)}를 지나며, 요청자가 본 표 버전({@code expected})이 현재 버전과 다르면
 * 거절한다(compare-and-set). B2(균형)와 B3(이동)가 서로 모르고 같은 Tablet을 건드리는 사고를 막는다. B3의
 * {@code MigrationCommitter.commit(job, expectedTable)}은 {@code move(expectedTable.version(), job.change())}로 구현한다.
 *
 * <p>push는 변경 직후 한 번이고 실패해도 재시도하지 않는다. 못 받은 노드는 다음 요청의 피기백으로 낡음을 알고 {@link GetTable}로
 * 받아 간다(design.md §4.3). {@link RoutingVersion}은 여기서만 오른다. Controller가 죽어도 노드들은 들고 있는 표로 계속 서비스하고
 * 표 변경만 멈춘다. Controller 복제와 재시작 시 버전 영속화는 범위 밖이다.
 *
 * <p>Controller 역할을 겸하는 노드는 자기 {@link LocalRouting}에 {@link #apply} 결과를 직접 {@code accept}한다(자기 자신에게는
 * push하지 않는다). 모든 메서드는 동기화되어 변경이 직렬화된다.
 */
public final class Controller {

    private final NodeId self;
    private final Transport transport;
    private RoutingTable table;
    private final Set<NodeId> members = new LinkedHashSet<>();

    /** push 대상은 처음 표에 나타나는 노드들이다. 표에 없는 노드(막 들어온 노드)는 {@link #register}로 더한다. */
    public Controller(NodeId self, RoutingTable initial, Transport transport) {
        this.self = Objects.requireNonNull(self, "self");
        this.table = Objects.requireNonNull(initial, "initial");
        this.transport = Objects.requireNonNull(transport, "transport");
        members.addAll(initial.nodes());
    }

    public NodeId self() {
        return self;
    }

    public synchronized RoutingTable table() {
        return table;
    }

    public synchronized RoutingVersion version() {
        return table.version();
    }

    public synchronized Set<NodeId> members() {
        return Set.copyOf(members);
    }

    /** {@code node}를 push 대상에 더하고 현재 표를 바로 보낸다. 노드가 들어올 때 Tablet을 받기 전에 먼저 표부터 갖게 한다. */
    public synchronized void register(NodeId node) {
        if (members.add(node) && !node.equals(self)) {
            transport.send(new PeerEnvelope(self, node, new TablePush(table)));
        }
    }

    /** {@code node}를 push 대상에서 뺀다. 떠난 노드에 계속 보내지 않기 위해서다. 표에서 Tablet을 빼는 것은 별도 변경이다. */
    public synchronized void unregister(NodeId node) {
        members.remove(node);
    }

    /**
     * 표를 바꾼다. {@code expected}가 현재 버전이 아니면 아무것도 바꾸지 않고 empty를 돌려준다. 성공하면 새 표를 모든 노드에 push하고
     * 그 표를 돌려준다.
     *
     * @param change 현재 표에서 새 표를 만드는 함수. 버전이 올라가야 한다
     * @throws IllegalArgumentException {@code change}가 버전을 올리지 않았을 때
     */
    public synchronized Optional<RoutingTable> apply(RoutingVersion expected, UnaryOperator<RoutingTable> change) {
        if (!table.version().equals(expected)) {
            return Optional.empty();
        }
        RoutingTable next = change.apply(table);
        if (!next.version().isNewerThan(table.version())) {
            throw new IllegalArgumentException("change did not advance the version: " + next.version());
        }
        table = next;
        push();
        return Optional.of(next);
    }

    /** 이동 한 건의 cutover. B3가 복사를 끝내고 옛 주인의 freeze를 확인한 뒤 부른다. */
    public Optional<RoutingTable> move(RoutingVersion expected, Change change) {
        return apply(expected, t -> t.apply(change));
    }

    public Optional<RoutingTable> split(RoutingVersion expected, TabletId id, long at) {
        return apply(expected, t -> t.withSplit(id, at));
    }

    public Optional<RoutingTable> merge(RoutingVersion expected, TabletId left, TabletId right) {
        return apply(expected, t -> t.withMerge(left, right));
    }

    /**
     * 노드나 클라이언트의 {@link GetTable}에 응답한다.
     *
     * @return 표 메시지였으면 {@code true}
     */
    public synchronized boolean onPeerMessage(NodeId from, PeerMessage message) {
        if (!(message instanceof GetTable g)) {
            return false;
        }
        Optional<RoutingTable> newer = table.version().isNewerThan(g.since()) ? Optional.of(table) : Optional.empty();
        transport.send(new PeerEnvelope(self, from, new TableReply(newer, table.version())));
        return true;
    }

    private void push() {
        TablePush message = new TablePush(table);
        for (NodeId node : members) {
            if (!node.equals(self)) {
                transport.send(new PeerEnvelope(self, node, message));
            }
        }
    }
}
