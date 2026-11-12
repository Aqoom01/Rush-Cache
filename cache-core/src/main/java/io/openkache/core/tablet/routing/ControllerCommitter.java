package io.openkache.core.tablet.routing;

import io.openkache.core.migration.JobState;
import io.openkache.core.migration.MigrationCommitter;
import io.openkache.core.migration.MigrationJob;
import io.openkache.core.tablet.RoutingTable;
import java.util.Objects;

/**
 * B3 이동의 cutover를 {@link Controller}로 수행하는 {@link MigrationCommitter} 구현.
 *
 * <p>MigrationCommitter 계약대로 source freeze와 snapshot ACK 확인은 호출자
 * ({@code MigrationProtocol.readyToCommit()}) 책임이고, 여기서는 job 상태와 표 버전 CAS만 검사한다.
 * {@link Controller#move}가 성공하면 표 버전과 이동 Tablet의 epoch가 각각 1 오르고 노드들에 push된다.
 * 실패(버전 불일치)하면 표는 바뀌지 않고 false를 돌려준다.
 */
public final class ControllerCommitter implements MigrationCommitter {

    private final Controller controller;

    public ControllerCommitter(Controller controller) {
        this.controller = Objects.requireNonNull(controller, "controller");
    }

    @Override
    public boolean commit(MigrationJob job, RoutingTable expectedTable) {
        if (job.state() != JobState.SWITCHING || !job.progress().snapshotComplete()) {
            return false;
        }
        return controller.move(expectedTable.version(), job.change()).isPresent();
    }
}
