/**
 * IONIA O00: 복제 Durability Log, 배경 합의, 키별 병렬 적용, 검증된 Follower 읽기,
 * VR View Change 및 Skyros DAG 복구를 구현한다.
 *
 * <p>공통 API/model/spi/runtime을 사용하며 실제 TCP·디스크 I/O 조립은 server.ionia에 둔다.
 * Replica RAM 기록 ACK의 내구성은 정족수와 독립 장애 모델에 기반한다.
 * 매 요청 WAL fsync는 원본 필수가 아닌 별도 강화 옵션이며 기본 모드에서 활성화하지 않는다.
 */
package io.openkache.core.replication.ionia;
