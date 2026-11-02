# Primary-Backup / VR 비교

이 실험의 VR은 보관된 FULL_LOG 모드를 사용한다. 이후 추가한 증분 전송 모드와 비용 비교는 [network-optimization](../network-optimization/README.md)에 있다.

## 구현

`primarybackup/CrashOnlyPrimaryBackupEngine` 기준형과 `vr/VrEngine`을 동일한 Transport·KV·가상 시계·timer 실행기에서 비교한다.
공통 `core.api/model/spi/runtime` 계약은 변경하지 않았다.

| 항목 | Primary-Backup 기준형 | VR |
|---|---|---|
| 쓰기 확정 | 현재 구성의 모든 Backup ACK | 고정 3노드 중 과반수(자신 + Backup 1대) |
| 미확정 연산 | Backup KV에 먼저 적용 | 메모리 로그에만 보관, commit 후 KV 적용 |
| Primary 교체 | 최초 Primary 장애 후 최소 Backup ID 승계 | 정렬된 멤버의 `view mod 3` |
| 이전 연산 이어받기 | 완료된 고유 키 확인, 미완료 로그 합의 없음 | DOVIEWCHANGE의 last-normal-view → 로그 길이 순으로 선택 |
| 분단 | crash-only 범위를 벗어난 한계 관측 | 과반수와 증가하는 view로 쓰기 확정 제한 |
| 읽기 | Primary의 로컬 KV | GET/EXISTS/COUNT도 로그와 과반수 경유 |
| 재시도 | SET 중복 제거 없음 | ClientId + OperationId로 중복 응답 재사용 |

VR의 `STARTVIEWCHANGE`는 같은 view의 노드 둘이 참여해야 DOVIEWCHANGE로 진행한다. 새 Primary는 DOVIEWCHANGE 둘의 로그 중 최신 것을 채택하며 알려진 commit-number의 최댓값까지 보존한다.
새 로그가 다시 과반수에 전달된 뒤 미확정 suffix도 commit한다. commit된 연산만 KV에 순차 적용한다.
따라서 응답을 받지 못한 연산도 새 view에서 실행될 수 있다. TIMEOUT은 취소가 아니다. 로그와 함께 재구성되는 client table이 같은 요청의 재실행을 막는다.

성공 응답 직후 Primary가 죽고 COMMIT 통지가 유실되어도 과반수에 있던 로그를 view change가 이어받는다.
옛 Primary는 분단 중 자기 view가 오래됐음을 몰라도 새 요청의 과반수를 얻지 못하므로 확정할 수 없다. 분단 전에 과반수에 도착한 진행 중 연산은 나중에 확정될 수 있으며 이는 새 요청을 분단 후 확정하는 것과 구분한다.

## 논문과의 대응·범위

근거는 [MIT 공식 VR Revisited](https://dspace.mit.edu/entities/publication/80846d94-fcd3-40e6-87fb-8d91fe99a5d1)의 §4.1~4.2다.
설계 정리는 [참고 문서](../references/viewstamped-replication.md)를 따른다.

- 초기 view는 0이며 NodeId 정렬 순서 0·1·2를 사용한다. PB의 초기 epoch는 1이라 절대 view 숫자를 직접 비교하지 않는다.
- PREPARE에 전체 로그와 commit-number를 넣고 heartbeat도 같은 메시지로 보낸다. 별도 COMMIT 최적화·증분 상태 전달은 구현하지 않았다.
- 새 Primary는 채택한 로그가 과반수에 다시 설치되고 로컬 적용이 끝나야 새 요청을 받는다.
- 모든 클라이언트 연산을 로그 순서로 실행한다. 동시에 새 연산 하나만 허용하며 미확정 suffix가 있으면 새 요청은 BUSY다.
- client table은 전체 로그 이력에 대해 유지한다. 같은 식별자에 다른 명령을 보내면 BAD_REQUEST다.
- 메시지 재전송·중복·순서 역전·분단 후 따라잡기를 지원한다. 상태 전달은 새 Primary가 보낸 전체 로그로 수행한다.
- 보존된 commit prefix와 다른 로그는 채택하지 않는다. KV 적용 실패 시 FAILED로 멈추고 성공 응답을 만들지 않는다.
- 메모리를 유지한 노드가 다시 연결되는 경우를 다룬다. 영구 종료된 JVM의 재시작, 디스크 복구, 동적 membership, checkpoint는 구현하지 않았다.
- 실제 cache-server wire 연결·SSD 비용은 이번 구현과 실험에 포함하지 않는다. 외부가 timer를 전달한다.

전체 로그 전송과 무제한 메모리 이력은 소규모 연구용 선택이다. 아래 네트워크 모델은 메시지별 지연을 부여하며 entry 수·bytes에 비례한 전송 시간은 반영하지 않는다. 따라서 VR의 실제 통신 비용이나 실제 서버 성능 우위를 이 결과로 주장할 수 없다.

## 공통 실험 모델

| 항목 | 설정 |
|---|---|
| 방식 | PB / VR |
| 시나리오 | 아래 8개 |
| 기본 편도 지연 | 1 / 5ms |
| 추가 jitter | 메시지마다 0~3ms, 고정 seed |
| seed | full 1~5 / quick 1 |
| heartbeat / 감지 timeout | 10 / 100ms |
| timer tick | 1ms |
| 주 요청 deadline | 60ms |
| 장애 이후 클라이언트 재시도 간격 | 5ms |

full은 `2방식 × 8시나리오 × 2지연 × 5seed = 160조건`이다. quick은 32조건을 두 번 반복한다.
각 조건에서 marker 키 두 개와 x=old를 성공시킨 뒤 장애/지연을 주입한다. 준비 구간은 동일한 90ms이고 seed % 7ms만큼 추가 진행한다.
동일 seed가 같은 무작위 생성기를 재현하지만 프로토콜별 메시지 수가 다르므로 두 방식의 packet별 지연열을 1:1로 대응시키지는 않는다.

| 시나리오 | 주입·관측 |
|---|---|
| NORMAL | 고유 키 SET 1건의 응답과 복사본 비교 |
| SLOW_BACKUP | Backup 2 ACK에 100ms 추가. PB TIMEOUT, VR의 과반수 완료 관측 |
| CRASH | 최초 Primary 영구 종료 후 첫 OK까지 측정 |
| PRIMARY_PARTITION | Primary 0과 Backup 1·2 사이 양방향 차단 후 남은 쪽에서 첫 OK |
| BACKUP_PARTITION | Backup 2 격리. Primary 0·Backup 1의 쓰기 진행 여부 |
| PARTIAL_CRASH | x=pending을 Backup 2에만 전달하고 ACK를 버린 뒤 Primary 종료 |
| PARTIAL_PARTITION | 같은 일부 전달 후 Primary 0을 격리, 미완료 쓰기와 새 구성 비교 |
| LOST_ACK | 양 Backup에 로그를 전달하되 ACK를 버린 뒤 Primary 종료 |

crash된 노드에서 나오는 미전달 메시지와 격리 링크의 메시지는 폐기한다. KV는 DirectMailbox로 즉시 적용한다.
클라이언트는 장애 후 알고 있는 Backup 2에서 시작해 Redirect를 따르고 응답의 epoch를 갱신한다. 클라이언트는 각 노드에 접속 가능하며 노드 간 분단만 주입한다.
UNAVAILABLE/TIMEOUT/BUSY로 옛 Primary 재시도가 실패하면 Backup 2로 다시 탐색한다. socket timeout과 client↔server 전송 지연은 모델링하지 않는다.

## 원본 지표와 해석

- `responseMs`: 정상·느린 Backup·Backup 분단에서 주 SET 제출부터 callback까지. 해당 없으면 -1.
- `recoveryMs`: crash/Primary 분단 주입부터 클라이언트 첫 OK callback까지. 해당 없으면 -1.
- `ok`: 주 SET 또는 복구 후 첫 SET의 성공 여부(0/1). 준비 쓰기와 재시도 횟수는 합산하지 않는다.
- `timeout`: 주 SET 또는 의도적으로 진행 중이던 쓰기의 TIMEOUT 수. 프로토콜 전체의 성공률이 아니다.
- `divergent`: 해당 audit key의 선택된 복제본 값이 다른 조건이면 1. 모든 키·이력의 위반 개수가 아니다.
- `preserved`: marker 두 키의 일치한 복사본 수. 정상/느린 Backup에서는 3노드(6), 나머지는 비교 노드 둘(4)이다.
- `messages`: 재전송과 버려진 메시지를 포함한 전송 시도 수. 요청이 끝난 뒤 고정 관측 시간의 heartbeat도 포함한다.
- `logEntries`: VR의 PREPARE/STARTVIEW/DOVIEWCHANGE에 실린 entry 수의 합. PB의 값 0은 데이터 전송량 0이 아니라 해당 로그 메시지가 없다는 뜻이다.
- JSON의 `trace`는 장애 시각·요청 결과·audit 값·VR 최종 view/commit/applied 상태를 기록한다.

PB의 SLOW_BACKUP audit은 노드 0·1·2, BACKUP_PARTITION은 연결된 0·1을 비교한다.
crash/Primary 분단 계열은 Backup 1·2를 비교하며 PARTIAL_*와 LOST_ACK는 x, 나머지는 복구 후 새 키를 검사한다.
의도된 PB TIMEOUT이나 미완료 쓰기의 복제본 불일치는 기준형의 한계 관측이며 실험 프로세스의 실패가 아니다.

별도 JUnit은 성공한 쓰기 보존, COMMIT 통지 유실, 로그 없는 후보 승격, 단일 복제본 미확정 suffix, primary-only suffix 폐기, 중복 요청 응답, GET의 과반수 경유, 비동기 KV callback, 반복 분단·유실·중복·순서 역전, 마지막 정상 view 우선 선택을 검사한다.
작은 단일 키 이력은 TIMEOUT 쓰기를 미확정으로 둔 순열 탐색으로 읽기/쓰기 순서를 검사한다. 이는 유한한 주입 조건의 검증이며 프로토콜 전체의 형식 증명은 아니다.

## 실행·재현

저장소 루트, JDK 21:

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/protocol-comparison/comparison-quick.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/a1-active-passive-failover/protocol-comparison/comparison.properties
```

공통 Runner가 실행 명령·설정·source/class hash·제한 시간·반복을 기록한다. 원본 JSON에서 조건·결과 불변식을 검증한 뒤 CSV·Markdown·완료 표식을 만든다.
원본 누락·중복·지표 변조·schema 불일치는 완료 표식을 제거하고 실패한다. backend의 고정 JSON 형식만 지원한다.

```powershell
java -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.replication.experiment.ProtocolResearchMain --rebuild "<repeat-001 폴더>"
```

JSON·manifest는 로컬 보관하며 CSV/보고서는 기존 Git 정책의 보관 대상이다. [결과 해석](results.md)을 함께 읽는다.
