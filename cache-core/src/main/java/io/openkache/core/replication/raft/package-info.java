/**
 * A1 고정 3노드 Raft 연구 구현. 모든 상태 변경은 owner mailbox에서 수행한다.
 * currentTerm, votedFor, log는 메모리에만 유지한다. 프로세스 재시작 recovery를 주장하지 않는다.
 * 외부 실행기가 가상/실제 TimeSource와 timer를 주입하며 peer codec은 제공하지 않는다.
 */
package io.openkache.core.replication.raft;
