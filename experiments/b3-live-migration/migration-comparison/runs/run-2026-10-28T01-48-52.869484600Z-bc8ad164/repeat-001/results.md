# S6 취소·장애 경계 비교

실행 정보: `run-2026-10-28T01-48-52.869484600Z-bc8ad164` · 반복 1/1 · 설정 `full` · commit `a32ef79ba80e85f167a5faed4e7ddd96330a7ce1` · source hash `d75a4f9842ab`

9개 경계 × seed 5개 × 세 방식 = 135개 결과 행. 실제 실행 85회, 미지원 50행. 실행된 시나리오의 정합성 위반 0건.

예상 종결: 취소 ABORTED 40회, 이동 DONE 40회, source 미복구 FAILED 5회. FAILED는 이동 성공이 아니라 source 의존 데이터가 남아 정상 완료를 거부한 결과다. ABORTED는 source 전체, DONE은 destination 전체, FAILED는 보존된 source 전체와 destination의 가용 데이터 전체를 각각 검사한다.

| 경계 | 방식 | 결과 | 쓰기 차단 tick | 주입 후 종료 tick | 전송 bytes | 취소 거부 | 미도착 UNAVAILABLE |
|---|---|---|---:|---:|---:|---:|---:|
| COPYING_ABORT | stop-and-copy | ABORTED | 1 | 0 | 65536 | 0 | 0 |
| COPYING_ABORT | pre-copy | ABORTED | 0 | 0 | 65536 | 0 | 0 |
| COPYING_ABORT | post-copy | 미지원 | — | — | — | — | — |
| BEFORE_COMMIT_ABORT | stop-and-copy | ABORTED | 24 | 0 | 1520696 | 0 | 0 |
| BEFORE_COMMIT_ABORT | pre-copy | ABORTED | 3~4 | 0 | 2200932~2219081 | 0 | 0 |
| BEFORE_COMMIT_ABORT | post-copy | ABORTED | 0 | 0 | 0 | 0 | 0 |
| FINAL_ACK_WAIT_ABORT | stop-and-copy | ABORTED | 24 | 0 | 1520696 | 0 | 0 |
| FINAL_ACK_WAIT_ABORT | pre-copy | ABORTED | 3~4 | 0 | 2200932~2219081 | 0 | 0 |
| FINAL_ACK_WAIT_ABORT | post-copy | 미지원 | — | — | — | — | — |
| AFTER_COMMIT_ABORT | stop-and-copy | DONE | 25 | 1 | 1520696 | 5 | 0 |
| AFTER_COMMIT_ABORT | pre-copy | DONE | 4~5 | 1 | 2200932~2219081 | 5 | 0 |
| AFTER_COMMIT_ABORT | post-copy | DONE | 1 | 22~23 | 1375290~1387326 | 5 | 0 |
| PARTIAL_CLEANUP_ABORT | stop-and-copy | DONE | 25 | 2 | 1520696 | 5 | 0 |
| PARTIAL_CLEANUP_ABORT | pre-copy | DONE | 4~5 | 2 | 2200932~2219081 | 5 | 0 |
| PARTIAL_CLEANUP_ABORT | post-copy | DONE | 1 | 2 | 1375290~1387326 | 5 | 0 |
| CATCHING_UP_ABORT | stop-and-copy | 미지원 | — | — | — | — | — |
| CATCHING_UP_ABORT | pre-copy | ABORTED | 0 | 0 | 1520696 | 0 | 0 |
| CATCHING_UP_ABORT | post-copy | 미지원 | — | — | — | — | — |
| SOURCE_DOWN_RESUME | stop-and-copy | 미지원 | — | — | — | — | — |
| SOURCE_DOWN_RESUME | pre-copy | 미지원 | — | — | — | — | — |
| SOURCE_DOWN_RESUME | post-copy | DONE | 1 | 23~24 | 1307676~1319181 | 5 | 222 |
| PULL_TIMEOUT_RESUME | stop-and-copy | 미지원 | — | — | — | — | — |
| PULL_TIMEOUT_RESUME | pre-copy | 미지원 | — | — | — | — | — |
| PULL_TIMEOUT_RESUME | post-copy | DONE | 1 | 26 | 1245903~1255815 | 5 | 213 |
| SOURCE_DOWN_UNRECOVERED | stop-and-copy | 미지원 | — | — | — | — | — |
| SOURCE_DOWN_UNRECOVERED | pre-copy | 미지원 | — | — | — | — | — |
| SOURCE_DOWN_UNRECOVERED | post-copy | FAILED | 1 | 3 | 65536 | 5 | 292 |

## 경계와 해석

COPYING_ABORT는 복사에 byte를 소비한 뒤 취소한다. BEFORE_COMMIT_ABORT는 readyToCommit에서 취소한다. FINAL_ACK_WAIT_ABORT는 마지막 ACK를 보류해 commit을 막은 뒤 취소한다. 이때 stop-and-copy의 실제 상태는 COPYING, pre-copy는 SWITCHING이다. AFTER_COMMIT_ABORT는 소유권 전환 직후 취소를 거부하고 마무리한다. PARTIAL_CLEANUP_ABORT는 source 일부를 지운 정리 실패 뒤 취소 거부와 두 번의 실패·세 번째 성공을 검증한다. CATCHING_UP_ABORT는 pre-copy의 실제 delta 라운드 진입 뒤 취소한다.

SOURCE_DOWN_RESUME는 post-copy source를 3 tick 사용할 수 없게 한 뒤 동일 데이터·fence로 복구한다. PULL_TIMEOUT_RESUME는 pull 응답을 막고 3 tick 뒤 controller가 fail을 호출하며 FAILED를 3 tick 관찰한 뒤 복구한다. 장애 중에도 destination 쓰기·삭제를 성공시키고 미도착 GET의 UNAVAILABLE와 취소 거부를 확인한다. SOURCE_DOWN_UNRECOVERED는 source를 복구하지 않은 채 FAILED를 관찰하고 source·소유권·성공 쓰기가 보존됨을 검사한다. source의 물리적 영구 삭제와 자동 데이터 복구를 구현한 실험은 아니다.

가상 시간·단일 controller·인메모리 모델이다. 모든 경계/방식에 같은 seed의 초기 데이터·tick별 요청 시도를 재생하지만 경계에 도달하는 시점·성공 요청 집합은 다르다. 주입 후 종료 tick은 시나리오 동작을 포함하며 실제 장애 감지 latency나 동일 작업량의 속도 비교가 아니다. 미지원에는 0이나 차이값을 채우지 않는다. workloadHash와 inputHash는 요청 생성 및 경계 규격 hash이며 materialized trace 전체 hash가 아니다. S6 전용 b3-s6-json-1이며 S1~S5/S3 결과 및 계획된 dirty 라운드 상세 v2와 합산하지 않는다.

## 미지원 이유

- COPYING_ABORT: post-copy는 복사 전에 소유권을 넘겨 source-owned COPYING 취소 경계가 없다
- FINAL_ACK_WAIT_ABORT: post-copy는 소유권 전환 전 전체 복사 final ACK를 기다리지 않는다
- CATCHING_UP_ABORT: CATCHING_UP은 pre-copy 전용 상태다
- SOURCE_DOWN_RESUME: 소유권 선전환 뒤 source 의존성은 post-copy 별도 계약이다
- PULL_TIMEOUT_RESUME: 소유권 선전환 뒤 source 의존성은 post-copy 별도 계약이다
- SOURCE_DOWN_UNRECOVERED: 소유권 선전환 뒤 source 의존성은 post-copy 별도 계약이다
