/**
 * Rosé에서 착안한 단일 파티션·op 단위 bounded-lag Primary-Backup 연구 변형.
 * 다중 파티션 coordinated apply·MVCC·지역 복제·영속 WAL을 재현한 구현이 아니다.
 * 정확한 영구 Primary crash를 전제로 외부 master가 가장 긴 생존 prefix를 선택한다.
 */
package io.openkache.core.replication.rose;
