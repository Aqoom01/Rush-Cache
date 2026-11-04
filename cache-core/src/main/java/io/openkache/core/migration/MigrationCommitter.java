package io.openkache.core.migration;

import io.openkache.core.tablet.RoutingTable;

/**
 * 조건부 소유권 전환 경계. 설계 §3·§4의 coordinator 책임이다.
 *
 * <p>호출 전 실행기는 source freeze와 모든 snapshot batch의 ACK를 확인해야 한다.
 * 구현체는 예상 table/version이 여전히 현재 값인지 검사하고 RoutingTable.apply(Change)의
 * 결과를 직렬화 또는 CAS로 원자 교체해야 한다. apply 호출만으로 commit이 되지는 않는다.
 * 1단계에서는 단일 controller thread에서 이 동작을 모사한다.
 */
public interface MigrationCommitter {
    /**
     * job의 소유권 전환을 조건부로 한 번 수행한다.
     *
     * <p>성공 시 routing version과 이동 Tablet의 epoch만 각각 1 증가한다.
     * 실패하면 routing table을 바꾸지 않는다. 성공 뒤에는 abort하지 않고 cleanup한다.
     *
     * @param job freeze·전송·ACK 확인이 끝난 이동 job
     * @param expectedTable job 시작 시의 예상 routing snapshot
     * @return 조건부 원자 교체 성공 여부
     */
    boolean commit(MigrationJob job, RoutingTable expectedTable);
}
