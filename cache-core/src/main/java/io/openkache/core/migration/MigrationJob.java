package io.openkache.core.migration;

import io.openkache.core.tablet.Change;
import java.util.Objects;

/**
 * Tablet 하나의 이동 job 스냅샷. 설계 §3의 Change·variant·상태·진행도를 보관한다.
 *
 * <p>B1·B2가 제공한 Change는 바꾸지 않는다. 실행 주체가 owner/controller thread에서
 * 다음 상태의 스냅샷을 만든다. 변경 가능한 상태를 외부에 노출하지 않는다.
 *
 * @param change 이동할 Tablet과 source/destination
 * @param variant 실험 결과에 기록할 복사 방식 이름
 * @param state 현재 job 상태
 * @param progress 현재 snapshot 전송 진행도
 */
public record MigrationJob(Change change, String variant, JobState state, TransferProgress progress) {
    public MigrationJob {
        Objects.requireNonNull(change, "change");
        Objects.requireNonNull(variant, "variant");
        Objects.requireNonNull(state, "state");
        Objects.requireNonNull(progress, "progress");
        if (variant.isBlank()) throw new IllegalArgumentException("blank variant");
    }

    /** 주어진 계획과 복사 방식의 PENDING job. */
    public static MigrationJob pending(Change change, String variant) {
        return new MigrationJob(change, variant, JobState.PENDING, TransferProgress.initial());
    }
}
