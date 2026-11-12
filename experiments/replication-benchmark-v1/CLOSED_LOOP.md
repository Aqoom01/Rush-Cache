# 단계3-A — fixed-N closed-loop·phase coordinator

기준일: 2026-10-30. **closed-loop 코어 구현·로컬 기능 검증 완료.** open-loop/histogram/표준 result 파일 export와 EC2 실행은 후속이다. 이 문서는 실제 성능 비교 결과가 아니다.

## 실제 코드와 호출

- 공통 코어: experiment-runner/src/benchmark/java/io/openkache/runner/benchmark/execution/
  - ClosedLoopPlan: 요청 수·동시성·최소 시간·작업/종료 budget.
  - ClosedLoopRunner: 생성기와 BenchmarkClient를 연결하는 one-shot coordinator.
  - ClosedLoopResult: phase/연산별 불변 집계와 전환·종료 관찰.
  - DrainProbe: adapter별 비차단 적용/미완료 관찰 경계.
- O00 관찰: cache-server/src/benchmark/java/io/openkache/server/benchmark/ionia/LocalO00DrainProbe.java.
- 검사는 각 모듈 src/benchmarkTest에 있다. 기존 Java/Gradle/production API·의존성·알고리즘은 수정하지 않았다.

호출은 new ClosedLoopRunner(generator, plan, client, probe).run()이다. run은 client의 배타적 소유권을 받아 성공/실패 모두 close하며, 두 번째 호출은 거절한다. 현재 운영 CLI/manifest loader가 아니라 Java API와 로컬 fixture로 제공한다. 기존 plan.template.json을 읽어 자동 실행하지 않는다.

## 실행 순서와 정상 진행 조건

| 순서 | 실행·확인 |
| --- | --- |
| PREFLIGHT | 미사용 client, 빈 격리 그룹, 정상 View·모든 replica, 기존 작업 없음 확인 |
| LOAD | key.count개 초기 SET을 closed-loop 실행 |
| LOAD_DRAIN | client 결과 전달·RPC 및 전체 replica의 누적 쓰기 적용 완료 |
| WARMUP | 별도 namespace/요청열, 별도 연산 집계 |
| WARMUP_DRAIN | 워밍업 쓰기와 raw/publication backlog를 정리한 뒤 측정 시작 |
| MEASURE | 고정 N개 측정 요청, 적재/워밍업과 분리 |
| FINAL_DRAIN | 측정 종료 후 미완료 RPC·복제 적용 확인 |
| CLEANUP | client close, worker 종료, 미완료 이력·상태 보존 |

각 phase가 전부 정상 완료하고 backend 계수와 대조된 뒤에만 다음 단계로 간다. 정상 시험에서 오류/미응답/잘못된 outcome, 관찰 불가·View 변경·barrier timeout은 해당 실행을 FAILED로 남긴다. 실패 후 warm-up/측정을 계속해 성공 결과처럼 합치지 않는다.

고정 N이 minimumWarmup/Measure 시간을 채우지 못하면 **INSUFFICIENT_DURATION**으로 실패한다. 요청을 더 보내거나 대기 시간을 측정 시간에 더해 통과시키지 않는다. 로컬 인수 fixture는 minimum=0을 명시하며 명세 후보120초/300초를 검증·대체한 것이 아니다.

## 동시성·메모리·재현성

- logical stream 수 S는 고정하고 concurrency C만 바꾼다. 1≤C≤S≤1024.
- worker w는 streamId% C=w인 stream을 round-robin으로 순회하며 **logical 요청 하나의 결과를 기다린 후** 다음 요청을 생성한다. 실제 protocol RPC 수는 C보다 많을 수 있다.
- 요청을 전체 리스트로 만들지 않는다. 커서는 O(S), driver 미완료 상태는 최대 O(C), phase/연산 집계와 stage events는 고정 크기다. byte 크기·backend 자체 메모리 예산은 별도로 검증해야 한다.
- 같은 stream의 sequence 순서를 유지하고, 고정 stream 분할의 모든 globalIndex를 중복 없이 실행한다. C가 바뀌어도 생성된 요청 집합/내용을 바꾸지 않으며 동시 실행의 interleaving은 달라질 수 있다.
- runId/loadSlot은 client 설정과 같아야 한다. 응답의 ID·연산·monotonic 시각·attempt 순서·성공 모양을 대조한다. 잘못된 응답을 성공으로 집계하지 않는다.

## admission·시간 한도·결과 집계

ClosedLoopPlan은 warmup/measured request 수, concurrency, 두 최소 시간과 Limits를 받는다. LOAD 수는 generator의 key.count다. 전체 요청 수의 long overflow도 실행 전에 거절한다.

| Limits | 의미 |
| --- | --- |
| workNanos | preflight부터 phase/복제 drain까지 작업 budget |
| completionWaitNanos | 한 제출의 terminal outcome 관찰을 기다리는 한도 |
| admissionWaitNanos/maxSubmissionAttempts | admission 거절 시 동일 요청을 다시 제출할 수 있는 시간/횟수 |
| barrierWaitNanos | 각 적용/미완료 barrier 대기 한도 |
| cleanupNanos | 작업 한도 뒤에도 허용하는 별도 종료 grace |
| pollNanos | admission/barrier/종료 polling 간격 |

이 budget은 같은 System.nanoTime domain의 협력적인 비차단 client/probe/callback을 전제로 한다. 임의로 block하는 backend의 JVM을 강제 종료하는 watchdog이나 realtime 보장이 아니다. 기존 O00 adapter는 같은 TimeSource.system()을 사용한다.

- admission 전 failed stage의 RejectedExecutionException만 bounded 재제출한다. request ID/command를 그대로 유지하고 logical 수를 늘리지 않는다.
- admission 대기는 adapter의 **수락 후 총deadline**과 별도 budget이다. 수락된 요청의 retry/deadline은 기존 O00 adapter 책임이다.
- ERROR_RESPONSE/NO_RESPONSE outcome이나 완료 여부가 불명확한 timeout을 coordinator가 새 요청으로 재시도하지 않는다.
- peakDriverSlots는 admission 대기를 포함하는 driver logical slot 수다. admitted-only pending, RPC/socket 수, 서버 worker 수와 같은 지표가 아니다.
- phase 시작은 worker 준비 이후 release 직전, 종료는 worker 결과 관찰 종료다. 적재/워밍업/drain/cleanup 시간을 측정 phase에 섞지 않는다. 시간값은 제어·기능검증용 관찰이며 아직 histogram/goodput 결과를 export하지 않는다.

각 phase·GET/SET/DELETE별로 scheduled, success, errorResponse, noResponse, notAdmitted, unresolved, client attempts, retriedLogical을 보관한다. 제출 호출 수·admission 거절·잘못된 outcome·최대 slot도 별도다.

**scheduled = success + errorResponse + noResponse + notAdmitted + unresolved**

정상 phase는 backend lifetime snapshot의 전후 delta와도 대조한다. 적재·워밍업·측정 모두 합친 client lifetime 값을 측정 전용 처리량으로 사용하지 않는다. 정리 중 늦게 도착한 outcome도 원래 Work/phase에만 연결한다. 종료 grace 뒤에도 확인되지 않으면 unresolved와 backend pending을 남기며 무응답 값을 발명해 보존식을 맞추지 않는다.

## O00 적용 완료 관찰의 정확한 범위

LocalO00DrainProbe는 같은 JVM에 명시적으로 연결한 기존 IoniaEngine의 공개 progress/viewInfo/failureReason과 O00 client diagnostics만 읽는다. owner 내부 log를 직접 읽거나 기존 wire·서버·엔진을 수정하지 않는다.

- 같은 membership 전체가 정상이고 고정 View/leader를 유지해야 한다.
- 초기 target=0이다. client는 미사용이어야 하고, caller가 새 독립 data directory의 빈 격리 그룹을 제공해야 한다. progress0 자체가 물리 파일 전체의 초기화를 증명하는 것은 아니다.
- target은 이 run의 성공한 unique SET+DELETE 누계다. DELETE MISS도 순서·실행된 쓰기로 센다. GET은 쓰기 log에 넣지 않는다.
- **모든 replica의 recordedCount=lastLogIndex=commitIndex=appliedIndex=target**과 client logical pending/active/publication slot/raw outstanding=0을 요구한다.
- 기록 ACK만 있고 순서/commit/apply가 부족하면 WAIT다. 예상보다 많은 쓰기는 외부 오염/불일치로 FAILED이며 target을 조용히 올리지 않는다.
- replica 누락·View 변경·RECOVERING·failureReason은 정상 부하 시험의 FAILED다. 장애 시나리오의 가용성 판정으로 일반화하지 않는다.
- 적용은 기존 KV API 완료까지다. 전체 peer heartbeat 정지·물리 socket 전부 종료·선택적 fsync 완료·전원 손실 내구성의 증명이 아니다. 독립 replica RAM log와 stable View force/KV fsync 구분은 유지한다.

기존 원격 probe는 View만 반환하므로 **EC2 원격 apply frontier 관찰은 미구현**이다. 이 로컬 관찰기를 실제 다중 host 완료 증거로 쓰지 않는다.

## 종료와 실패 보존

실패/중단 시 새 작업을 멈추고 알려진 미완료 ID를 cancel한 뒤 client close와 worker 중단을 수행한다. bounded grace 동안 outcome과 raw/apply 상태를 확인한다. cancel/close가 원격 abort를 보장하지 않는 기존 계약을 유지한다.

이미 관찰한 성공을 삭제하거나 실패 요청을 다른 seed로 바꿔 재실행하지 않는다. 잘못된 outcome과 미완료 cohort는 INVALID/FAILED 증거다. caller interrupt는 정리 후 복원한다. worker 종료나 raw/apply 정리를 확인하지 못하면 PASSED로 만들지 않는다.

## 로컬 검증 명령

프로젝트 루트에서 JAVA_HOME을 Java21로 설정한다.

```powershell
# 네트워크 없는 coordinator 모델 검사
.\gradlew.bat :experiment-runner:benchmarkTest --tests "*ClosedLoopRunnerTest" --offline --console=plain
# 로컬 포트 예산 gate를 포함한 전체 기능 회귀
.\experiments\ionia-validation\Run-Validation.ps1 -Mode Full -Repeat 1
```

실제 호출/조립 예제는 ClosedLoopO00TcpTest다. fresh 임시 파일 저장소의 로컬3/5노드, streams4/concurrency2, LOAD8/WARMUP7/MEASURE19를 읽기100%와 GET80/SET15/DELETE5로 각각 검증한다. 읽기 전용은 종료 후 독립 audit client로 초기 값도 대조한다. 혼합 경쟁 이력 전체의 최신성/선형성 checker를 구현한 것은 아니다.

이번 단계 추가23검사(coordinator14/관찰기5/TCP4), 최종 전체585개(core392/server90/기존runner16/runner benchmark47/server benchmark40), 실패·오류·skip0. 기존 src238개 SHA256 동일·새 Java8파일, 기존 application JAR에 benchmark class0. 명령·증거·한계는 B3_VALIDATION.json/B3_SOURCE_PRESERVATION.json을 따른다.

## 다음 작업

open-loop와 예정 발행/거절 계수는 후속 단계에서 구현했다(OPEN_LOOP.md 참조). 후속 단계에서 선택적 outcome Observer와 phase/op/outcome별 전수 지연 histogram·로컬 설정/표준 결과 export를 연결했다(LATENCY_EXPORT.md 참조). 이후 변형 adapter·분산 관찰/검사·EC2 pilot을 준비한다. 이번 실행은 작은 로컬 기능 fixture이며 실제 공통 성능 비교·최종 데이터셋 확정·commit/push가 아니다.

