package io.openkache.core.migration;

import io.openkache.core.tablet.RoutingTable;
import java.util.Objects;

/** 같은 controller에서 서로 다른 Tablet을 병렬 이동할 때의 게시 검증. 실제 CAS는 호출자가 수행한다. */
public final class MigrationPublication {
    private MigrationPublication() {}

    /**
     * 시작 뒤 다른 Tablet의 전환은 허용하되 이동 대상의 owner·epoch·범위 변경은 거절한다.
     * before는 CAS 직전 실제 표, published는 그 표에 job의 Change 하나만 적용한 실제 게시 표여야 한다.
     * owner/controller thread에서 readiness 확인·CAS·protocol 통지를 연속해서 호출한다.
     */
    public static void verify(MigrationJob job, RoutingTable started, RoutingTable before, RoutingTable published) {
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(started, "started");
        Objects.requireNonNull(before, "before");
        Objects.requireNonNull(published, "published");
        if (before.version().value() < started.version().value()
                || !before.get(job.change().unit()).equals(started.get(job.change().unit()))) {
            throw new IllegalArgumentException("moving tablet changed since begin");
        }
        RoutingTable planned = before.apply(job.change());
        if (!planned.version().equals(published.version()) || !planned.tablets().equals(published.tablets())
                || planned.nextTabletId() != published.nextTabletId()) {
            throw new IllegalArgumentException("ownership was not committed as planned");
        }
    }
}
