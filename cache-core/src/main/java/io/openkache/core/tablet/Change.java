package io.openkache.core.tablet;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.TabletId;
import java.util.Objects;

/**
 * Tablet 하나를 {@code from}에서 {@code to}로 옮기는 계획 한 건.
 *
 * <p>{@link DistributionScheme}과 {@code balance}가 만들고, {@code migration}이 데이터를 옮긴 뒤
 * {@link RoutingTable#apply(Change)}로 표에 반영한다. 계획 자체는 표를 바꾸지 않는다.
 *
 * @param unit 옮길 Tablet
 * @param from 현재 주인. 적용 시점에 표의 주인과 다르면 낡은 계획이므로 거절된다
 * @param to 새 주인
 */
public record Change(TabletId unit, NodeId from, NodeId to) {

    public Change {
        Objects.requireNonNull(unit, "unit");
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        if (from.equals(to)) {
            throw new IllegalArgumentException("from and to are the same node: " + from);
        }
    }
}
