# Open-loop 실행기 — fixed-rate·bounded admission

기준일: 2026-10-30. **정해진 도착률과 예정 시각을 따르는 open-loop 코어 구현·로컬 검증 완료.** 기존 closed-loop/알고리즘은 보존했다. EC2·서버 처리 한계·성능 비교·표준 결과 export는 이번 범위가 아니다.

## 실제 코드

- experiment-runner/src/benchmark/java/io/openkache/runner/benchmark/execution/
  - FixedRateSchedule: 정수 ops/s의 fixed-N 예정 시각과 due 요청 수 계산.
  - OpenLoopPlan: phase별 rate·요청 수·미완료 상한·허용 발행 지연·budget.
  - OpenLoopRunner: phase 실행·단일 제출·비동기 결과 수집·정리.
  - OpenLoopResult: 수집/발행/서비스 상태, phase/op별 보존식, 지연 요약·미발행 계수.
- 대응 검사는 runner benchmarkTest의 FixedRateScheduleTest/OpenLoopRunnerTest와 server benchmarkTest의 OpenLoopO00TcpTest다.
- 기존 TraceGenerator, BenchmarkClient/ClientOutcome/ClientAccounting, DrainProbe/LocalO00DrainProbe와 O00 client/TCP를 재사용한다. production 의존성·wire·원본/변형·기존 Java/Gradle 수정은 없다.

호출은 new OpenLoopRunner(generator, plan, client, probe[, observer]).run()이다. 한 JVM/한 load slot의 Java API이며 client 소유권을 받아 한 번 실행하고 닫는다. 현재 remote launcher/manifest loader는 없고 plan.template.json은 draft/disabled 상태를 유지한다.

## 세 phase와 고정 예정열

PREFLIGHT → LOAD → LOAD_DRAIN → WARMUP → WARMUP_DRAIN → MEASURE → FINAL_DRAIN → CLEANUP을 유지한다. LOAD/WARMUP도 각각 명시한 rate로 발행하며 모든 초기/워밍업 요청이 정상 완료돼야 측정으로 진행한다. setup의 손실을 허용해 불완전한 데이터셋으로 측정하지 않는다.

rate r은 정수1..1,000,000,000 ops/s이며, phase 요청 index i의 예정 시각은 다음과 같다.

`due(i) = phaseStart + floor(i × 1,000,000,000 / r)`

첫 요청은 index0/offset0이다. 나눗셈을 분해해 long 곱셈 overflow와 부동소수점·누적 sleep drift를 피한다. 응답 완료, 상한 도달, 거절, driver 지연에 따라 phaseStart/다음 due를 다시 잡지 않는다.

- rate는 일정한 요청 간격을 정의한다. fixed-N의 마지막 예정 offset은 (N−1)/r의 내림값이며 N/r과 다르다.
- runtime에서는 절대 due를 확인하며 sleep/poll한다. OS/JVM이 ns 정밀도로 발행한다는 보장은 없다. 실제 지연은 계수한다.
- 고려한 후보는 항상 globalIndex 순서다. logical stream 수를 바꾸거나 이전 거절 ID를 나중에 재제출하지 않는다.
- 일정 horizon은 최대1일, 총 phase 예정 span은 work budget 이내여야 한다. 요청 수 합계 overflow와 streams>1024도 전송 전에 거절한다.
- LOAD 수는 key.count, WARMUP/MEASURE 수는 계획값이다. minimumWarmup/Measure는 예정 span으로 사전 검증한다. 부족한 계획에 임의 요청·종료 대기 시간을 덧붙이지 않는다.

## 상한·과부하 정책

스케줄러는 이전 응답을 기다리지 않는다. 후보를 생성한 뒤 허용 지연과 maxInFlight를 검사한다.

| 상태 | 계수·행동 |
| --- | --- |
| 허용 지연 초과 | timingMiss. 해당 예정 요청을 보내지 않고 다음 고정 due로 진행 |
| 미완료 상한 도달 | capacityDrop. 큐를 무한히 늘리거나 응답을 기다려 rate를 낮추지 않음 |
| 제출 호출 후 admission 거절 | clientRejected. 동일 요청을 재시도하지 않음 |
| 수락 후 오류/무응답 | errorResponse/noResponse. 측정 phase는 나머지 예정열을 계속 발행 |
| 예기치 않은 제출/collector 예외·잘못된 outcome | 수집 실패로 중단하고 아직 발행하지 않은 계획을 보존 |
| work 한도·caller interrupt | 새 발행 중단, cancel/close·bounded cleanup 수행 |

maxInFlight는 결과 관찰/observer 처리를 포함한 driver slot 상한이다. 실제 서버 미완료 RPC/socket 수와 다르다. backend 자체의 상한·raw stage 상한도 유지된다. 드롭은 backend capacity의 직접 증거가 아니므로 실제 backend 수락률·driver/collector 오버헤드 적합성을 함께 확인해야 한다.

측정 phase의 과부하 결과를 삭제하거나 빠른 성공만 골라 재실행하지 않는다. write 오류/timeout으로 실제 적용 여부가 불분명하면 WRITE_EFFECTS_UNCERTAIN을 남겨 복제 drain 적합성을 통과시키지 않는다. 미수락된 요청이나 GET 실패와 구분한다.

## 결과 판정의 의미

| 필드 | 의미 |
| --- | --- |
| allArrivalsConsidered | 세 phase의 예정 후보 N개를 모두 고려했음. 모두 서버로 보냈다는 뜻은 포함하지 않음 |
| collectionValid | ID/결과·계수·정리/적용 관찰에 수집 실패가 없음 |
| scheduleQualified | 전체 후보 고려 및 설정한 허용 지연 충족, timingMiss/지연 합계 overflow 없음 |
| serviceHealthy | 모든 예정 요청이 정상 완료. 드롭·거절·오류·미응답 없이 전부 성공 |
| completed() | 전체 후보 고려와 collectionValid |
| passed() | 정상 무손실 인수 조건: completed + scheduleQualified + serviceHealthy |

예를 들어 상한에 따른 드롭이 정확히 기록된 과부하 모델은 collectionValid=true, serviceHealthy=false일 수 있다. driver가 늦어 timingMiss가 발생하면 scheduleQualified=false다. 성능 비교에는 실제 부하 전달률과 계측 비용의 추가 적합성 확인도 필요하다.

현재 validation 기록의 passed는 **이 계약을 확인하는 JUnit 기능 검사 통과**다. 운영 서비스의 성공률·SLO·성능 우위를 보고한 것이 아니다.

## 계수와 미발행 보존식

각 phase·GET/SET/DELETE별로 다음을 보관한다.

`considered = success + errorResponse + noResponse + capacityDrop + timingMiss + clientRejected + submitFailed + unresolved`

phase 전체는 다음도 만족해야 한다.

`planned N = considered + unissued`

- unissued는 중단/작업 한도 때문에 후보를 생성하지 못한 계획이다.
- dueButUnissued: 발행 루프 종료 시점에 이미 due가 지났지만 고려하지 못한 수.
- futureCancelled: 그 시점 이후에 예정돼 있던 나머지.
- unissued=dueButUnissued+futureCancelled. 정수 일정의 역함수로 O(1)에 계산한다.
- 생성하지 않은 unissued를 연산별로0 또는 mix 비율 추정치로 채우지 않는다.
- unresolved는 고려한 요청의 결과를 정리 grace 이후에도 확인/검증하지 못한 경우다. 무응답을 발명해 보존식을 맞추지 않는다.

제출 호출, client attempt/재시도 logical 수, backend lifetime snapshot delta도 대조한다. root는 admission 거절을 재시도하지 않지만 기존 O00 adapter 내부의 같은-ID bounded retry는 유지하며 그 client attempts를 따로 센다. protocol RPC/message·복제 수를 logical 요청 수로 세지 않는다.

## 발행 지연의 관측 경계

현재는 histogram 대신 phase별 count/sum/min/max ns를 보관한다.

| 요약 | population/정의 |
| --- | --- |
| decisionLag | 모든 considered 후보의 decisionNanos−due, drop 후보 포함 |
| submissionLag | 실제 client.submit 호출 직전−due, admission 거절 호출 포함 |
| admittedLag | 실제 outcome의 client invokedNanos−due, 수락된 요청만 |

sample0은 sum/min/max=null이고, sum overflow도 null+overflow flag로 남긴다. 미발행 요청의 지연을 성공 latency0으로 넣지 않는다. 전체 지연 histogram·p50/p95/p99/p99.9·scheduled/service latency 분포는 후속이다.

phase의 emissionEndedNanos는 발행 루프 종료, outcomeWaitEndedNanos는 결과 대기 종료 관찰이다. 후자가 모든 결과를 확보했다는 단독 증거는 아니므로 unresolved와 상태 필드를 함께 본다. 정상 발행 창과 마지막 응답·drain/cleanup 시간을 혼합해 throughput을 부풀리지 않는다.

## Observer와 bounded 상태

선택적 Observer는 Decision과 ScheduledOutcome을 받는다. due/driver submit/실제 client 시각·요청·outcome을 후속 collector에 연결할 수 있다.

- decision은 dispatcher thread, outcome은 backend 완료 thread에서 올 수 있다.
- callback은 lock 밖에서 호출하며 비차단이어야 한다. 비동기 응답 때문에 decision/outcome의 전달 순서는 ID별로 달라질 수 있다. 시각과 ID로 결합해야 한다.
- collector 예외는 실패로 남기고 새 발행을 멈춘다. event drop을 숨기지 않는다.
- 요청 리스트 전체를 저장하지 않고 최대 maxInFlight 요청/결과 관찰과 현재 후보1개만 유지한다. 느린 observer도 slot을 점유하므로 드롭/부하 전달률에 영향을 준다.

work/outcomeDrain/barrier/cleanup/poll budget은 협력적인 비차단 client/probe/observer를 전제로 한다. block한 callback/JVM을 강제 종료하거나 realtime 마감 정확도를 보장하는 도구가 아니다.

## 적용·실행 범위

기존 O00 로컬 관찰기를 재사용해 빈 격리 그룹·고정 정상 View·전 replica의 recorded/ordered/commit/applied 목표와 client raw/publication 정리를 확인한다. apply는 기존 KV API 완료이며 SSD fsync·동시 전원 손실 내구성·전체 peer traffic 종료의 증명이 아니다. 독립 replica RAM log와 stable View/KV force 구분은 유지한다.

이 rate와 요청열은 **한 JVM/load slot 전체** 기준이다. 다중 host에 같은 계획을 각각 실행하는 방식은 아직 제공하지 않는다. EC2 launcher·전역 load 분할/clock·원격 apply 관찰·변형 adapter·분산 checker는 후속이다. 실제 파일/인스턴스에 적용할 준비가 된 실행 manifest로 표시하지 않는다.

## 로컬 검증

프로젝트 최상위에서 JAVA_HOME=Java21로 설정한다.

```powershell
# 비네트워크 일정/과부하·종료 모델
.\gradlew.bat :experiment-runner:benchmarkTest --tests "*FixedRateScheduleTest" --tests "*OpenLoopRunnerTest" --offline --console=plain
# 로컬 포트 gate 포함 전체 회귀
.\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1
```

- 일정 역함수·경계/overflow, 보류 응답으로 상한3을 채워도 예정20개를 모두 고려하고17개를 capacityDrop으로 기록하는 모델.
- backend 거절/오류/무응답, 명시적 driver pause에 따른 timingMiss, work cutoff의 due/future 미발행, collector/ID 오류·unresolved·interrupt·쓰기 결과 불명확 음성 대조.
- 실제 같은 JVM의 로컬TCP3/5노드 × 읽기100%/GET80·SET15·DELETE5. LOAD4/WARMUP3/MEASURE10, rates20/20/30ops/s·상한8. replica 적용 목표는 읽기4/혼합6이며 읽기 전용은 별도 audit으로 값도 대조.
- 이 rate/지연 허용값은 작은 기능 fixture의 설정이다. 최적값·서버 처리 한계·논문 성능 재현 수치가 아니다.
- 추가22검사(일정4/runner14/TCP4), 최종 전체607검사, 실패·오류·skip0. 기존 src246개 hash 동일·새Java7파일, 기존 application JAR의 benchmark class0.

명령·XML/log·한계·보존 목록은 B4_VALIDATION.json/B4_SOURCE_PRESERVATION.json을 따른다. 전수 지연 histogram과 로컬 표준 결과 파일 저장은 후속 구현을 완료했다(LATENCY_EXPORT.md 참조). 현재 남은 것은 변형 adapter·정확성/자원/원격 관찰·분산 실행 적합성이다.

