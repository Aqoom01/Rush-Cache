package io.openkache.core.tablet.routing;

import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import io.openkache.core.spi.Transport;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.routing.OwnershipCheck.Decision;
import io.openkache.core.tablet.routing.RoutingClient.Route;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 노드 하나의 최소 모형: {@link LocalRouting}으로 판정하고, 허용되면 메모리 map에 쓴다. KvEngine·worker는 없다.
 * Tablet별로 받아들인 쓰기 수를 세어 "두 주인" 검사에 쓴다.
 */
final class FakeNode {

    private final NodeId id;
    private final LocalRouting routing;
    private final Map<Long, Long> store = new HashMap<>();
    private final Map<TabletId, Integer> acceptedWrites = new HashMap<>();

    FakeNode(NodeId id, RoutingTable initial, Transport transport) {
        this.id = id;
        this.routing = new LocalRouting(id, initial, transport);
    }

    NodeId id() {
        return id;
    }

    LocalRouting routing() {
        return routing;
    }

    /** 요청 하나를 처리한다. {@code route}의 버전·epoch가 요청 헤더에 해당한다. */
    Result handle(long keyHash, boolean write, Route route) {
        Decision d = routing.check(keyHash, route.version(), route.epoch());
        Optional<Result> rejected = d.rejection();
        if (rejected.isPresent()) {
            return rejected.get();
        }
        if (write) {
            store.merge(keyHash, 1L, Long::sum);
            acceptedWrites.merge(d.tablet().id(), 1, Integer::sum);
        }
        ResultStatus status = write || store.containsKey(keyHash) ? ResultStatus.OK : ResultStatus.MISS;
        return Result.of(status, routing.version(), d.tablet().epoch());
    }

    int acceptedWrites(TabletId tablet) {
        return acceptedWrites.getOrDefault(tablet, 0);
    }
}
