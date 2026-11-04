# Pre-Copy / Post-Copy 구현

2026-10-28 기준. 기존 stop-and-copy와 같은 in-process·단일 controller thread 범위다. S1~S5·S3 민감도·S6 취소/장애 비교 backend는 [비교 실행 안내](migration-comparison/README.md)에 있다. 실제 `KvEngine`·peer 통신, 노드 생성/종료는 후속이다. 기존 SC1~SC6 결과는 stop-and-copy 단독 결과이며 새 variant의 측정값으로 재사용하지 않는다.

## Pre-Copy

`core.migration.precopy.PreCopyProtocol`은 기존 `MigrationProtocol`을 구현한다.

1. destination staging을 준비하고 source의 쓰기 hook을 등록한 **뒤** snapshot을 읽는다.
2. `COPYING` 중 source는 계속 요청을 처리한다. 모든 snapshot batch ACK 뒤에만 delta를 전송한다.
3. `CATCHING_UP` 라운드마다 collecting/sending dirty generation을 교체한다. 이미 보내는 key에 새 write가 와도 다음 generation에 남는다. 값은 batch를 만들 때 source에서 읽으며 DELETE는 tombstone이다.
4. dirty 직렬화 예상량(헤더 포함)이 256 KiB 이하, live delta 라운드가 10회, 또는 dirty bytes가 줄지 않을 때 `SWITCHING`으로 간다. `Settings`로 변경할 수 있다.
5. 이동 Tablet만 freeze하고 마지막 delta와 final seq ACK를 확인한다. `readyToCommit()` 확인 → 외부 expected-table CAS → `committed(publishedTable)`을 한 controller 작업에서 수행한다.
6. source 범위를 cleanup한다. 실패하면 재시도하고 commit 뒤 abort는 거부한다. commit 전 abort는 성공한 source write를 보존하고 staging·hook·자신의 freeze만 해제한다.

`Source.startTracking` 등록과 snapshot 사이, SET/DELETE 적용과 hook 사이에 다른 controller 작업이 끼어들면 안 된다. snapshot은 write를 허용한 상태에서도 일관된 고정 목록을 반환해야 한다. `Source`의 hook·read·freeze는 실제 KV adapter가 제공할 경계다.

`PreCopyReceiver`는 in-process staging 구현이다. session identity, Tablet epoch/범위, 연속 batch ID/seq, checksum·건수·크기를 전체 검증한 뒤 적용한다. snapshot 완료 barrier를 지키고, key별 마지막 seq와 삭제를 기억한다. 최종 batch 뒤 추가 데이터를 받지 않으며 중복에는 원본 ACK만 반환한다. `data()`는 검증/게시 adapter용 불변 스냅샷이고 commit 전 client에 노출하면 안 된다.

## Post-Copy

`core.migration.postcopy.PostCopyProtocol`은 source-owned abort 계약과 분리된 `PostCopyState`를 사용한다.

`PENDING → SWITCHING → PULLING → CLEANING_UP → DONE`

`begin`에서 source의 이동 Tablet만 freeze하고 전체 key manifest를 얻는다. 값 전체를 snapshot으로 읽거나 전송하지 않는다. `readyToCommit()` 확인 → 외부 expected-table CAS → `committed(publishedTable)`을 한 controller 작업에서 수행한다. post-copy commit은 복사 완료를 전제하는 기존 `MigrationCommitter`와 별도 controller 경계를 사용한다. 실제 게시된 표는 예상 표에 `Change`를 적용한 값과 같아야 한다.

commit 직후 destination의 `execute(GET/SET/DELETE)`를 사용할 수 있다. caller는 현재 routing으로 owner를 먼저 판정하고 이전 source 요청에는 `MOVED`를 응답해야 한다. commit 전 destination은 source로 `MOVED`를 반환한다.

- 미도착 key의 GET: priority pull을 예약하고 `BUSY`로 재시도를 요청한다. 아직 가져오지 않은 값을 `MISS`로 처리하지 않는다.
- manifest에 없는 key 또는 삭제 완료한 key의 GET: `MISS`다.
- SET/DELETE: 즉시 적용하고 resolved/tombstone을 남긴다. 늦은 baseline pull은 새 값·삭제를 덮지 못한다. 아직 가져오지 않은 기존 key의 DELETE도 성공이다.
- background pull: priority key가 없으면 manifest 순서로 진행한다. 진행 중인 한 응답 전송은 preempt하지 않는다. session·epoch·request ID·key·CRC를 검증하고 tick 예산만큼만 전송한다.
- source 장애/응답 timeout: `fail()` 또는 생존 검사로 `FAILED`가 된다. 소유권·성공한 destination 쓰기를 보존하고 source를 정리하지 않는다. 미도착 GET은 `UNAVAILABLE`이다. manifest 내 key의 source 부재도 데이터 손실로 취급하며 조용히 `DONE`으로 바꾸지 않는다.
- 복구: 동일 source 데이터와 freeze/fence가 복구된 뒤 `resume()`으로 다시 진행한다. 외부 controller가 프로세스/논리 tick timeout을 판단해 `fail()`을 호출한다.
- commit 전 abort: source 재개. commit 후 abort: 거부. cleanup 부분 실패는 destination 데이터를 보존하고 재시도한다.

source freeze는 baseline 데이터를 고정하는 fence다. commit 뒤 요청은 destination으로 라우팅하므로 source freeze가 전체 복사 동안 유지되어도 destination 쓰기를 막지 않는다. key manifest 수집·CAS에 드는 실제 지연은 이 모델에서 측정하지 않는다. freeze를 잃거나 source를 영구 손실한 경우 자동 rollback/데이터 복구를 제공하지 않는다.

## 검증

```powershell
.\gradlew.bat build
.\gradlew.bat :cache-core:test --tests '*PreCopyProtocolTest' --tests '*PostCopyProtocolTest'
```

두 테스트와 `LiveMigrationFixture`는 실제 프로토콜·수신기를 호출한다. snapshot/delta barrier, 라운드 중 반복 쓰기, 삭제/빈 값, final ACK 누락, checksum·session·epoch 오류, byte 예산, 취소와 cleanup, Tablet 격리, 우선 pull, late-pull 쓰기 보호, source 장애와 복구를 검증한다. 이 검증은 실제 서버 성능 실험이나 S1~S6 비교 보고서를 대체하지 않는다.
