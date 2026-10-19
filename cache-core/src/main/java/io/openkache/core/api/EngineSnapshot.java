package io.openkache.core.api;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import java.util.Objects;

/**
 * 특정 시점에 관측한 엔진 상태. 불변 복사본이므로 어느 thread에서나 읽을 수 있다.
 *
 * <p>관측 이후 엔진 상태가 바뀌어도 이 객체는 바뀌지 않는다. 연구별로 필요한 관측값은 엔진을 구현할 때 추가한다.
 *
 * @param nodeId 관측한 노드
 * @param observedAtNanos 관측 시각 ({@code TimeSource.monotonicNanos()})
 * @param routingVersion 관측 시점의 라우팅 버전
 * @param ownershipEpoch 관측 시점의 소유권 epoch
 */
public record EngineSnapshot(
        NodeId nodeId,
        long observedAtNanos,
        RoutingVersion routingVersion,
        OwnershipEpoch ownershipEpoch) {

    public EngineSnapshot {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(routingVersion, "routingVersion");
        Objects.requireNonNull(ownershipEpoch, "ownershipEpoch");
    }
}
