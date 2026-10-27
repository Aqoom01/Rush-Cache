package io.openkache.core.tablet.routing;

import io.openkache.core.api.Redirect;
import io.openkache.core.api.Result;
import io.openkache.core.api.ResultStatus;
import io.openkache.core.model.NodeId;
import io.openkache.core.model.OwnershipEpoch;
import io.openkache.core.model.RoutingVersion;
import io.openkache.core.tablet.RoutingTable;
import io.openkache.core.tablet.Tablet;
import java.util.Objects;
import java.util.Optional;

/**
 * 노드가 요청을 받았을 때 "이 key를 내가 처리해도 되나"를 판정한다. 상태가 없는 순수 함수다.
 *
 * <ul>
 *   <li>내 표에서 내가 주인이면 {@link ResultStatus#OK}. 응답에 내 표 버전을 실어 보낸다(피기백).
 *   <li>내가 주인이 아니면 {@link ResultStatus#MOVED} + 새 주인({@link Redirect}) + 내 표 버전. Redis Cluster의 MOVED와 같다.
 *       {@code ASK}는 쓰지 않는다. 이동이 끝나는 순간까지 표상 주인이 처리한다(design.md §4.1).
 *   <li>요청의 epoch가 내 Tablet epoch보다 작으면 {@link ResultStatus#STALE_EPOCH}. 옛 주인 정보로 온 요청을 막는다.
 *   <li>요청의 epoch가 내 것보다 크면 나보다 새 주인을 아는 것이므로 내 표가 낡았다. 안전 쪽으로 {@code MOVED}로 거절하되 새 주인은
 *       모르므로 redirect 없이 보내고, {@link Decision#myTableIsStale}로 호출자에게 표를 받아 오라고 알린다.
 *   <li>요청의 표 버전이 내 것보다 새로우면 역시 내 표가 낡았다는 신호다. 판정은 그대로 하고 {@code myTableIsStale}만 켠다.
 * </ul>
 *
 * <p>[한계] 이 판정만으로는 "두 주인"을 막지 못한다. 옛 주인이 새 표를 아직 못 받았으면 자기가 주인이라고 판정한다. 막는 것은
 * {@link LocalRouting#stopServing}을 먼저 부르는 B3의 cutover 순서다.
 */
public final class OwnershipCheck {

    private OwnershipCheck() {}

    /**
     * 판정 결과.
     *
     * @param status OK, MOVED, STALE_EPOCH 중 하나
     * @param tablet 해당 key의 Tablet(내 표 기준)
     * @param redirect MOVED일 때 다시 보낼 노드. 모르면 empty
     * @param serverVersion 응답에 실을 내 표 버전(피기백)
     * @param myTableIsStale 요청이 나보다 새 표나 epoch를 들고 왔다. 호출자는 Controller에서 표를 받아 와야 한다
     */
    public record Decision(
            ResultStatus status,
            Tablet tablet,
            Optional<NodeId> redirect,
            RoutingVersion serverVersion,
            boolean myTableIsStale) {

        public Decision {
            Objects.requireNonNull(status, "status");
            Objects.requireNonNull(tablet, "tablet");
            Objects.requireNonNull(redirect, "redirect");
            Objects.requireNonNull(serverVersion, "serverVersion");
        }

        public boolean allowed() {
            return status == ResultStatus.OK;
        }

        /** 거절일 때 클라이언트에 보낼 {@link Result}. 허용이면 empty(호출자가 실제 연산을 수행한다). */
        public Optional<Result> rejection() {
            if (allowed()) {
                return Optional.empty();
            }
            if (redirect.isPresent()) {
                return Optional.of(Result.redirect(status, new Redirect(redirect.get()), serverVersion, tablet.epoch()));
            }
            return Optional.of(Result.of(status, serverVersion, tablet.epoch()));
        }
    }

    /**
     * @param self 판정하는 노드
     * @param table 이 노드가 알고 있는 라우팅 표
     * @param keyHash {@link io.openkache.core.tablet.KeyHash#of}의 결과
     * @param clientVersion 요청이 들고 온 표 버전. 모르면 {@link RoutingVersion#ZERO}
     * @param clientEpoch 요청이 들고 온 소유권 epoch. 모르면 {@link OwnershipEpoch#ZERO}(검사하지 않음)
     */
    public static Decision decide(
            NodeId self, RoutingTable table, long keyHash, RoutingVersion clientVersion, OwnershipEpoch clientEpoch) {
        Tablet t = table.lookup(keyHash);
        boolean stale = clientVersion.isNewerThan(table.version());
        if (!t.owner().equals(self)) {
            return new Decision(ResultStatus.MOVED, t, Optional.of(t.owner()), table.version(), stale);
        }
        if (!clientEpoch.equals(OwnershipEpoch.ZERO)) {
            if (t.epoch().isNewerThan(clientEpoch)) {
                return new Decision(ResultStatus.STALE_EPOCH, t, Optional.empty(), table.version(), stale);
            }
            if (clientEpoch.isNewerThan(t.epoch())) {
                return new Decision(ResultStatus.MOVED, t, Optional.empty(), table.version(), true);
            }
        }
        return new Decision(ResultStatus.OK, t, Optional.empty(), table.version(), stale);
    }
}
