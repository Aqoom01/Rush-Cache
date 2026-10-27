package io.openkache.core.tablet.routing;

import io.openkache.core.model.RoutingVersion;
import io.openkache.core.spi.PeerMessage;
import io.openkache.core.tablet.RoutingTable;
import java.util.Objects;
import java.util.Optional;

/**
 * 라우팅 표를 주고받는 peer 메시지. Controller와 노드 사이에서만 쓴다.
 *
 * <p>표는 통째로 보낸다. 노드 10대 × Tablet 64개면 20 KB 정도라 변경분(delta) 전파는 하지 않는다. wire 인코딩은 아직 없고
 * ({@code protocol-v1} §6 예약), 지금은 in-process {@link io.openkache.core.spi.Transport}로만 오간다.
 */
public sealed interface TableMessage extends PeerMessage {

    /** Controller → 노드. 표가 바뀐 직후 한 번 보낸다. 받는 쪽은 자기 것보다 버전이 클 때만 교체한다. */
    record TablePush(RoutingTable table) implements TableMessage {
        public TablePush {
            Objects.requireNonNull(table, "table");
        }
    }

    /** 노드 → Controller. {@code since}보다 새 표가 있으면 달라는 요청. 피기백으로 낡음을 알았을 때 보낸다. */
    record GetTable(RoutingVersion since) implements TableMessage {
        public GetTable {
            Objects.requireNonNull(since, "since");
        }
    }

    /**
     * {@link GetTable}의 응답. {@code since}보다 새 표가 있으면 {@code table}에 담고, 아니면 비워서 "변경 없음"을 뜻한다.
     * {@code current}는 응답자의 표 버전이다.
     */
    record TableReply(Optional<RoutingTable> table, RoutingVersion current) implements TableMessage {
        public TableReply {
            Objects.requireNonNull(table, "table");
            Objects.requireNonNull(current, "current");
            if (table.isPresent() && !table.get().version().equals(current)) {
                throw new IllegalArgumentException("table version " + table.get().version() + " != current " + current);
            }
        }
    }
}
