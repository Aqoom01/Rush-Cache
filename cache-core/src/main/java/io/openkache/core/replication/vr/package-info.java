/**
 * 고정 3노드의 Viewstamped Replication 연구 구현.
 *
 * <p>근거: Liskov and Cowling, Viewstamped Replication Revisited (2012) §4.1–4.2.
 * SET/GET/DELETE/EXISTS/COUNT를 순서 있는 로그로 처리하고 과반수 저장 후에만 KV에 적용한다.
 * STARTVIEWCHANGE/DOVIEWCHANGE/STARTVIEW로 로그를 선택하고 미확정 suffix를 복구한다.
 * peer identity는 주입된 신뢰 가능한 Transport가 제공한다고 가정한다.
 *
 * <p>모든 상태 변경은 owner mailbox에서 수행한다. 프로세스 재시작 없이 메모리를 유지한 분단 복구를 지원한다.
 * f=1의 가용성은 결국 두 노드가 통신하고 timer가 정상 동작하는 조건에서만 기대한다.
 * 비잔틴 장애, 디스크 영속화, 재시작 recovery, 멤버 변경, checkpoint, 실제 TCP codec은 범위 밖이다.
 * 상세 실행과 모델 차이는 experiments/a1-active-passive-failover/protocol-comparison/README.md를 따른다.
 */
package io.openkache.core.replication.vr;
