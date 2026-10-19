package io.openkache.core.tablet;

import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.TabletId;
import java.util.Objects;

/**
 * Tablet 메타데이터: 식별자, 맡는 해시 구간, 주인 노드, 소유권 epoch. 불변이다.
 *
 * <p>이동 중 상태나 부하 통계는 여기 두지 않는다. 표는 "어디로 가나"만 답하고, 이동 상태는 {@code migration}의 노드 로컬 상태,
 * 부하는 {@code balance}의 별도 구조가 맡는다.
 *
 * @param id Tablet 식별자. 표를 가진 쪽(Controller)만 발급한다
 * @param range 맡는 해시 구간
 * @param owner 담당 노드
 * @param epoch 이 Tablet의 주인이 바뀔 때마다 오르는 번호. 같은 Tablet을 두 노드가 주장하면 큰 쪽이 이긴다
 */
public record Tablet(TabletId id, TabletRange range, NodeId owner, OwnershipEpoch epoch) {

    public Tablet {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(range, "range");
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(epoch, "epoch");
    }

    /** 주인을 {@code newOwner}로 바꾸고 epoch를 올린 사본. */
    public Tablet movedTo(NodeId newOwner) {
        return new Tablet(id, range, newOwner, epoch.next());
    }
}
