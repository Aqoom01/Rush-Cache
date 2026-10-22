/**
 * Live Migration과 Elastic Scaling. B3가 사용한다.
 *
 * <p>책임: Tablet snapshot 복사, 복사 중 변경분(delta) 동기화, cutover, 노드 추가·제거 시 rebalancing 실행.
 *
 * <p>상태: 설계 §7의 구조로 1단계 stop-and-copy를 구현했다. source freeze,
 * snapshot batch 전송·검증·ACK, 외부 조건부 commit, abort와 cleanup 재시도를 제공한다.
 * test의 experiment 패키지에서 최소 저장소·전송·coordinator와 논리 tick으로 검증한다.
 * production KvEngine/peer 연계, pre-copy, post-copy, scaling은 후속 범위다.
 */
package io.openkache.core.migration;
