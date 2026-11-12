/**
 * Live Migration과 Elastic Scaling. B3가 사용한다.
 *
 * <p>책임: Tablet snapshot 복사, 복사 중 변경분(delta) 동기화, cutover, 노드 추가·제거 시 rebalancing 실행.
 *
 * <p>상태: stop-and-copy, pre-copy, destination-owned post-copy를 in-process로 구현했다.
 * pre-copy는 dirty generation·tombstone·final seq ACK를, post-copy는 소유권 선전환·priority pull·
 * 새 write 보호·source 장애 후 재개를 제공한다. 외부 controller가 routing 조건부 commit을 담당한다.
 * test의 experiment 패키지에서 최소 저장소·전송·coordinator와 논리 tick으로 검증한다.
 * 실제 KvEngine/peer 연계, variant 비교 실험 backend, scaling은 후속 범위다.
 */
package io.openkache.core.migration;
