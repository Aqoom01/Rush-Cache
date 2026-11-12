package io.openkache.core.tablet.routing;

import io.openkache.core.balance.TabletLoad;
import io.openkache.core.balance.split.SplitDecision;
import io.openkache.core.balance.split.SplitPolicy;
import io.openkache.core.model.TabletId;
import io.openkache.core.tablet.RoutingTable;
import java.util.Objects;
import java.util.Optional;

/**
 * B2 분할 정책의 결정을 {@link Controller}에 반영한다.
 *
 * <p>분할을 시작할지(부하 기준·지속 기간·유예)는 호출자가 판단하고, 여기서는 그 단계를 통과한
 * Tablet 하나의 관찰 결과를 정책에 묻는다. {@code Split}이면 {@link Controller#split}을 호출해
 * 표를 바꾸고 노드들에 push한다.
 *
 * <p>관찰이 낡았을 수 있으므로 두 번 방어한다. load의 구간이 표의 현재 구간과 다르면(이미 분할·병합됨)
 * 호출하지 않고, 정책 판단과 적용 사이에 표가 바뀌면 Controller의 CAS가 거절한다. 두 경우 모두 empty다.
 */
public final class SplitRunner {

    private final Controller controller;
    private final SplitPolicy policy;

    public SplitRunner(Controller controller, SplitPolicy policy) {
        this.controller = Objects.requireNonNull(controller, "controller");
        this.policy = Objects.requireNonNull(policy, "policy");
    }

    /**
     * Tablet {@code id}의 관찰 결과로 분할을 시도한다.
     *
     * @return 분할이 적용된 새 표. 정책이 보류했거나 관찰·표가 낡았으면 empty
     */
    public Optional<RoutingTable> trySplit(TabletId id, TabletLoad load) {
        RoutingTable table = controller.table();
        if (!table.get(id).range().equals(load.range())) {
            return Optional.empty();
        }
        return switch (policy.decide(load)) {
            case SplitDecision.Split s -> controller.split(table.version(), id, s.at());
            case SplitDecision.NoSplit ns -> Optional.empty();
        };
    }
}
