# 여러 job의 이동 · M1~M5

2026-11-02. **다중 이동 Scheduler·다중 노드 fixture·공통 Runner backend 구현과 정식 210회 실험을 완료했다.** 소규모 두 반복 168회도 통과했다. [핵심 결과와 공식 run](results.md) · [전체 설계 §6.4](../design.md) · [B3 안내](../README.md)

B1·B2가 만든 고정 `List<Change>`를 받아 동시 실행 수·노드 경합·전송 예산이 이동에 주는 영향을 비교한다. 노드 증감 판단 및 placement 최적화와 구별한다. 기존 S1~S6 단일 이동 결과를 M 결과로 재사용하지 않았다.

여기의 M1~M5는 FIFO 한 정책의 조건 변화 실험이다. MongoDB·TiDB·iter-GWIN·single-MCs·SLAMIG·VMbuddies·FPTA에서 옮긴 실행 방법끼리의 비교는 [선행 Scheduler 비교](../scheduler-comparison/README.md)에서 별도 입력·schema·run으로 실행한다. 두 실험의 순위를 합치지 않는다.

## 구현

- production `core.migration.MigrationScheduler`: 실행 가능한 가장 오래된 job을 고르는 FIFO, 전체/노드별 동시 상한, 회전 progressive filling byte 할당, 완료·실패·job timeout 수집. 동일 Tablet의 중복 계획은 시작 전에 거절한다. 단일 controller thread에서 호출한다.
- `MigrationPublication`과 세 protocol의 `committed(before, published)`: 다른 Tablet의 선행 전환을 보존하는 최신 라우팅 CAS를 검증한다. 이동 대상의 owner·epoch·범위가 시작 이후 달라졌으면 거절한다. 기존 단일 인자 overload는 기존처럼 시작 표 기준의 엄격한 전환만 허용한다.
- test `MultiMigrationFixture`: 실제 Stop/Pre/Post-copy protocol 호출, 여러 노드의 Tablet별 저장소·freeze·dirty hook·staging, 하나의 공유 라우팅 CAS. 공유 source/destination에서도 job 데이터와 상태를 분리한다. 같은 source 노드에 남는 비이동 guard Tablet도 검증한다.
- `MovementExperiment`: 요청 시도 생성, 매 tick 예산·동시 상한, 성공 요청 oracle, 최종 데이터 exact compare, owner/epoch·이전 source MOVED·source 정리를 검증한다.
- `MovementResearchMain`: 공통 Runner가 호출하는 backend. JSON 원본 검증·CSV/Markdown 재생성과 누락/중복·변조 거절을 담당한다. 예상 개수 전체가 검증된 뒤에만 완료 표시를 쓴다.

이는 Java 프로토콜을 실행하는 다중 노드 **인메모리·가상 시간 실험**이다. 실제 KvEngine·TCP·SSD·노드 생성/종료·멀티스레드 통합은 후속이다. timeout은 관측·예약 해제이며 자동 rollback이나 commit 이후 데이터 복구를 제공하지 않는다. 장애·복구의 세부 계약은 [protocols.md](../protocols.md)를 따른다.

## 실험 조건

| 시나리오 | 조건 수 | 바꾸는 조건 | 복사 방식 |
|---|---:|---|---|
| M1 | 4 | job 1/4/16/64, 동시 상한 4, 독립 endpoint | Stop-copy |
| M2 | 4 | job 16, 동시 상한 1/2/4/8, 전체 예산 고정 | Stop-copy |
| M3 | 6 | job 16·동시 4, 독립/source 공유/destination 공유 × 노드 동시 상한 1/4 | Stop-copy |
| M4 | 4 | job 16·동시 4, rho 0/0.5/0.9/1.2 | Pre-copy |
| M5 | 24 | M2와 M4의 조건을 각각 세 복사 방식으로 반복 | Stop/Pre/Post-copy |

M4는 dirty 재전송과 수렴 종료가 드러나는 Pre-copy를 기준으로 삼았다. M5가 같은 쓰기 조건을 세 방식 모두로 비교한다. M1~M3/M2 계열의 rho는 0.1이다.

| 설정 | full | quick |
|---|---:|---:|
| Tablet당 초기 key 수 | 2,048 | 128 |
| seed | 1~5 | 1~2 |
| 공통 Runner 반복 | 1 | 2 |
| 반복당 실행 | 210 | 84 |
| 전체 전송 예산 | 65,536 bytes/tick | 4,096 bytes/tick |
| 노드 송수신 합산 예산 | 16,384 bytes/tick | 1,024 bytes/tick |
| M2/M5-M2의 노드 예산 | 65,536 bytes/tick | 4,096 bytes/tick |
| job당 쓰기 생성의 reference 예산 / batch 목표 | 16,384 bytes/tick | 1,024 bytes/tick |
| Pre-copy 종료 임계 / live 라운드 상한 | 4,096 bytes / 10 | 256 bytes / 10 |

key 16 bytes·value 128 bytes, snapshot record는 길이 필드 포함 152 bytes다. 기존 protocol의 snapshot/delta/pull 직렬화 bytes를 실제로 차감한다. ACK·job control·실제 network framing 비용은 포함하지 않는다. M2의 노드 예산을 전체 예산과 같게 둬 단일 endpoint 제한보다 전체 대역폭 공유가 드러나게 했다.

## 입력·예산·관측

모든 job은 tick 0에 대기열에 들어온다. source와 destination의 송수신을 같은 노드 예산으로 합산하고, 전송 1 byte는 양 endpoint에 각각 1 byte씩, 전체 예산에는 한 번 차감한다. 미사용 할당을 같은 tick에 회수·재분배하거나 다음 tick으로 적립하지 않는다. 완료·제어 단계에도 slot과 할당을 주기 때문에 동시성 증가가 제어 tick 비용을 겹치게 할 수 있다.

각 tick은 **요청 시도 → 신규 job 시작 → byte 할당·protocol step·준비된 소유권 CAS → 관측** 순서다. Post-copy의 freeze와 commit이 같은 controller 작업 안에서 끝나므로 요청이 차단된 tick이 0일 수 있다. 이것을 실제 서비스의 중단 시간 0으로 해석하지 않는다.

쓰기는 job당 `rho × referenceBytesPerTick / 152`회/tick의 누적 소수 부분까지 고정해서 생성한다. SET 70%·DELETE 30%, GET은 job당 2회/tick이다. 실제 할당량이 작아져도 쓰기 생성률을 줄이지 않는다. seed·job·tick에서 요청 key/연산을 결정하므로 같은 입력에서는 variant와 scheduler를 바꿔도 요청 생성 규격이 같다. queued/DONE job에도 suite 종료까지 요청을 보낸다. 방식에 따른 BUSY·완료 시각 차이로 성공 요청과 최종 데이터량은 다르며 각 방식의 성공 요청만 oracle에 반영한다.

- makespan: tick 0의 첫 begin부터 마지막 DONE까지. 단위는 tick이며 ms가 아니다.
- aggregate throughput: 총 전송 bytes / makespan. 재전송도 포함한다.
- useful throughput: **suite 종료 시** destination 유효 데이터 bytes / makespan. 이동 후 성공한 쓰기도 포함하므로 순수 전송 처리량과 구별한다.
- 전송 비율: 초기 데이터 대비와 최종 유효 데이터 대비를 모두 JSON에 보관한다. 대기 중 DELETE·Post-copy의 쓰기 해결 때문에 초기 대비 비율이 1보다 작을 수 있다.
- 대기: 입력부터 begin까지의 job별 p50/p95/p99/max. 분위수는 nearest-rank이며 CSV/표의 p99는 run 내부 분위수의 seed 평균이다.
- 요청 영향: job별 write 차단 tick·쓰기/읽기 BUSY. 실제 p99 latency는 측정하지 않는다.
- 공정성: job별 전송량 최소/최대·시작 대기·미시작 job을 기록한다. 이번 유한 입력의 starvation 0을 지속 도착 환경의 보장으로 확장하지 않는다.

## 실행과 재생성

저장소 루트에서 실행한다.

```powershell
.\gradlew.bat build :experiment-runner:installDist
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/b3-live-migration/movement-scaling-comparison/movement.properties
.\experiment-runner\build\install\openkache-experiment-runner\bin\openkache-experiment-runner.bat experiments/b3-live-migration/movement-scaling-comparison/movement-quick.properties
```

JSON 원본을 보유한 반복 폴더에서 재집계한다. 이 작업은 실험을 다시 실행하지 않는다.

```powershell
java -Xmx1g -cp "cache-core/build/classes/java/main;cache-core/build/classes/java/test" io.openkache.core.migration.experiment.MovementResearchMain --rebuild experiments/b3-live-migration/movement-scaling-comparison/runs/<run-id>/repeat-001
```

schema는 `b3-movement-json-1`이다. 각 JSON은 condition·고정 Change 목록·seed·생성 규격 workloadHash·inputHash·evidenceHash, source/backend/runner hash, job별 lifecycle·정합성 hash·상태, tick/job/node 예산, phase/round 이벤트를 담는다. workloadHash는 실제 trace 전체 파일이 아닌 생성 규격의 SHA-256이다.

파생 파일은 `cases.csv`, `runs.csv`, `summary.csv`, `jobs.csv`, `budgets.csv`, `rounds.csv`, `results.md`다. `rounds.csv`는 job ID가 있는 phase·라운드 번호 전환이며 v2의 전체 dirty generation 상세 형식을 구현한 것은 아니다. Runner의 manifest·로그·완료 표시·원본과 상세 CSV는 로컬에 남고, Git에는 정식 반복의 `summary.csv`·`results.md`만 보관한다. [공통 Runner 계약](../../research-backends.md)을 따른다.


## 새 실행의 dirty 상세 (2026-11-04)

Pre-copy generation의 시작·ACK·취소 관측을 추가했다. 현재 config는 이 backend의 schema 2로 새 결과를 생성하고 이전 schema 1 원본도 재생성할 수 있다. 검증된 원본에서 dirty-rounds.csv와 dirty-rounds.md를 생성한다. 기존 단계 기록과 정식 run은 보존한다. [필드·시간·종료 판단·호환성 안내](../migration-comparison/dirty-rounds.md)를 따른다.
