package io.openkache.core.migration;

/**
 * Source-owned 이동 상태. 설계 §3을 따른다.
 *
 * <p>정상 순서는 PENDING → COPYING → (pre-copy의 CATCHING_UP) → SWITCHING → CLEANING_UP → DONE이다.
 * commit 전 실패는 ABORTED, commit 후 정리 실패는 CLEANING_UP에서 재시도한다.
 * 실제 상태 전이는 각 MigrationProtocol 구현이 담당한다.
 */
public enum JobState {
    PENDING,
    COPYING,
    CATCHING_UP,
    SWITCHING,
    CLEANING_UP,
    DONE,
    ABORTED
}
