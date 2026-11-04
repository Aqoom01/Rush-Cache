package io.openkache.core.migration.experiment;

import io.openkache.core.migration.JobState;
import io.openkache.core.migration.stopcopy.StopAndCopyProtocol;

/** 외부 실행기. tick마다 복사, 조건부 commit, cleanup 중 하나만 수행한다. */
public final class TickRunner {
    private final MigrationFixture fixture;
    private final StopAndCopyProtocol protocol;
    private final long maxPreCommitTicks;
    private long ticks;
    private boolean timedOut;

    public TickRunner(MigrationFixture fixture, long maxPreCommitTicks) {
        if (maxPreCommitTicks < 1) throw new IllegalArgumentException("nonpositive timeout");
        this.fixture = fixture;
        this.protocol = fixture.protocol;
        this.maxPreCommitTicks = maxPreCommitTicks;
    }

    /** 실제로 이 실행기의 시간 예산이 소진되어 취소했는지. 다른 경로의 취소는 포함하지 않는다. */
    public boolean timedOut() { return timedOut; }

    /** ACK 누락 등으로 commit 전에 시간이 만료되면 abort. commit 뒤에는 cleanup을 재시도한다. */
    public JobState step(long budgetBytes) {
        if (budgetBytes < 0) throw new IllegalArgumentException("negative budget");
        if (protocol.job() == null) throw new IllegalStateException("begin first");
        if ((protocol.state() == JobState.COPYING || protocol.state() == JobState.SWITCHING)
                && ticks++ >= maxPreCommitTicks) {
            timedOut = true;
            return protocol.abort();
        }
        if (protocol.state() == JobState.SWITCHING) {
            if (!protocol.readyToCommit()) return protocol.abort();
            if (!fixture.coordinator.commit(protocol.job(), protocol.expectedTable())) return protocol.abort();
            protocol.committed(fixture.coordinator.current());
            return protocol.state();
        }
        return protocol.step(budgetBytes);
    }
}
