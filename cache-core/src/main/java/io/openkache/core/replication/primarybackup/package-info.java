/**
 * A1 기준형 {@code primary-backup}: 고정 Primary 1대가 쓰기를 받아 고정 Backup들에 순서대로 전달하는 동기 복제.
 *
 * <p>참고 문헌은 Budhiraja et al., "The Primary-Backup Approach" (1993)이며 정리는
 * {@code experiments/a1-active-passive-failover/references/primary-backup.md}, 전체 설계는 같은 디렉터리의
 * {@code design.md}를 따른다.
 *
 * <p>고정 구성의 동기 복제와 {@link io.openkache.core.replication.primarybackup.CrashOnlyPrimaryBackupEngine}의
 * 단일 Primary crash 감지·고정 승계·생존 Backup 준비 확인 후 쓰기 재개를 구현했다.
 *
 * <ul>
 *   <li>구현: SET 복제({@link io.openkache.core.replication.primarybackup.PrimaryBackupMessage.Replicate}) → Backup
 *       로컬 적용 → {@link io.openkache.core.replication.primarybackup.PrimaryBackupMessage.ReplicateAck} → 모든 Backup
 *       ACK 수집 → Primary 로컬 적용 → 클라이언트 OK. deadline까지 ACK가 모이지 않으면 TIMEOUT. Backup으로 온 클라이언트
 *       쓰기는 NOT_PRIMARY + Redirect.
 *   <li>미구현: 분단 안전성, 복귀 후 따라잡기(recovery), DELETE 복제, 비동기
 *       모드, 메시지 재전송·순서 역전 처리, 클라이언트 재시도 중복 제거, 동시 진행 쓰기(진행 중 SET이 있으면 BUSY).
 * </ul>
 *
 * <p>Crash-only 조립부는 승격 시 epoch를 올리고 stale 클라이언트 요청을 거절한다. 새 epoch를 모르는 옛
 * Primary를 외부에서 차단하지 않으므로 split brain 방지를 보장하는 fencing은 아니다.
 */
package io.openkache.core.replication.primarybackup;
