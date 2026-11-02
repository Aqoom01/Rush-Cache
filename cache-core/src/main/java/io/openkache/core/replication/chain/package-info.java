/**
 * A1 Chain Replication: head 쓰기, tail 확정·읽기, 역방향 Sent ACK와 suffix 재전송.
 * owner mailbox 규칙을 따르며 외부 master가 영구 crash 노드 제거를 직렬화한다.
 * 분단 노드의 강제 제외·master 합의·재시작·CRAQ·신규 노드 상태 복사는 범위 밖이다.
 */
package io.openkache.core.replication.chain;
