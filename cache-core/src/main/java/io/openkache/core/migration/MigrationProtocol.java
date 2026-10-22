package io.openkache.core.migration;

/**
 * Source-owned 복사 방식의 공통 계약. 설계 §4의 다섯 연산을 정의한다.
 *
 * <p>job 실행기와 저장소는 같은 owner/controller thread에서 직렬 실행한다.
 * 시간은 호출자가 tick별 전송 예산으로 제공하며 protocol은 실제 시계를 읽지 않는다.
 * 소유권 commit은 protocol 밖의 MigrationCommitter가 담당한다.
 */
public interface MigrationProtocol {
    /** 실험 결과의 variant 이름. */
    String name();

    /** PENDING job을 시작한다. stop-and-copy는 여기서 이동 Tablet의 write를 freeze한다. */
    JobState begin(MigrationJob job);

    /**
     * 이번 tick의 byte 예산 안에서 전송·ACK 확인을 진행하고 다음 상태를 반환한다.
     *
     * @param budgetBytes 이번 시간 단위에 전송할 수 있는 bytes
     */
    JobState step(long budgetBytes);

    /** 이동 Tablet의 source가 현재 write를 받아도 되는지 반환한다. */
    boolean acceptsWrite();

    /** commit 전에는 복사본을 폐기하고 freeze를 해제한다. commit 뒤에는 취소를 거부한다. */
    JobState abort();
}
