/**
 * 고정 leader/view, 메모리 3노드의 IONIA 1차 상태 머신.
 *
 * <p>Client SET → 세 노드 기록 ACK와, leader Prepare → 과반수 PrepareAck → Commit → 순차 KV 적용을 분리한다.
 * GET은 leader에서 앞선 쓰기의 적용을 기다리고, DELETE는 leader 적용 결과(OK/MISS)를 반환한다.
 * Client 전송 구현은 IoniaClientTransport, 노드 간 전송 구현은 공통 Transport로 외부에서 주입한다.
 * Client/Engine의 deadline 처리와 재전송에는 외부에서 주기적인 ionia.tick TimerEvent를 전달해야 한다.
 *
 * <p>실행 예제는 test의 IoniaStateMachineTest(수동 메시지/가상 시계)와 IoniaWorkerIntegrationTest(실제 worker)다.
 * TCP/Runner 조립, SSD 영속화, follower meta-query 읽기, 병렬 KV 적용, 로그 압축, quorum fallback,
 * leader 변경과 재시작/partition 복구는 구현하지 않았다. 메모리 ACK는 내구성 보장이 아니다.
 */
package io.openkache.core.replication.ionia;
