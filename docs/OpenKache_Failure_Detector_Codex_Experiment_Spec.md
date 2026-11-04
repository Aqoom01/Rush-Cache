# OpenKache Failure Detector 추가 실험 구현 명세서 — Codex 실행용

> 기준: 2026-10-27 / 목적: 현재 `failure-detector-lab` 1차 discrete-time 시뮬레이션의 측정 타당성을 확보하고, **Fixed Deadline**, **SWIM 단독**, **SWIM+Lifeguard**, **Phi Accrual+Gossip**의 비교를 재현 가능하게 수행한다. 연구 후보는 후자 두 계열이며 앞의 둘은 Baseline/Ablation이다.
>
> **Codex는 이 파일을 구현 지시서로 사용하되, 작업 시작 시 저장소를 직접 탐색해 실제 모듈명·패키지·클래스·Java/Gradle 버전을 확인해야 한다. 아래 경로·인터페이스 예시는 제안이며 저장소의 기존 구조가 우선한다.**

## 0. Codex에 바로 전달할 지시문

```text
현재 OpenKache 저장소를 읽고 본 명세서(OpenKache_Failure_Detector_Codex_Experiment_Spec.md)에 맞춰 Failure Detector 비교 실험을 단계적으로 구현해줘. 먼저 git 상태, 실제 failure-detector-lab 구조, existing SWIM/Lifeguard/Phi 구현, simulation scheduler, event collector, CSV reporter, 테스트 명령을 조사하고 발견한 구조와 수정 계획을 짧게 정리해. 기존 구현과 실험 결과는 삭제하거나 덮어쓰지 말고, 원래 코드가 가정한 계약을 최대한 재사용해.

구현 순서: (P0) 측정 시점/스케줄링 오류와 메트릭 정의 수정 → (P1) Fixed Deadline Heartbeat 기준선 및 SWIM without Lifeguard ablation 추가 → (P2) 동일 scenario/seed/traffic budget으로 4개 프로필 비교 및 통계·CSV/JSONL 산출 → (P3) 가능하면 실제 UDP + Docker/netem fault runner 구축. P0~P2가 끝나기 전에는 새로운 Hybrid, Geode/JGroups 복제, 자동 replica 승격을 구현하지 마. 외부 라이브러리·설정·경로는 근거 없이 가정하지 말고 기존 프로젝트 우선.

각 단계별 JUnit 테스트와 실행 명령을 제공하고 실제 실행한 결과만 '통과'라고 표시해. 결과 미검증·권한/환경 제약·남은 과제를 명시해. CI와 기존 테스트를 깨뜨리지 말고, 결과 CSV에서 어떤 측정치를 실측했고 어떤 것은 model-estimated인지 구분해. 커밋은 요청받은 경우에만 수행해.
```

## 1. 범위와 우선순위

| 우선순위 | 항목 | 구현 여부 | 배경 |
|---|---|---|---|
| P0 | 공통 이벤트 시간축·정확도 측정기 수정 | 필수 | 기존 SWIM Crash SUSPECT 0ms는 시간 모델 아티팩트라는 보고가 있음 |
| P1 | Fixed Deadline Heartbeat FD | 필수 | 가장 단순한 대조군이 있어야 알고리즘 비용/효과를 설명할 수 있음 |
| P1 | SWIM-only (Lifeguard OFF) | 필수 | Lifeguard의 LHA 기여를 정량화하는 Ablation |
| P2 | 네 프로필 공정한 비교·파라미터 스윕·리포트 | 필수 | 조건별 탐지 지연·오탐·통신 비용 분포 필요 |
| P3 | 실제 UDP 다중 프로세스/컨테이너 + tc/netem | 우선 권장 | discrete-time 결과의 외적 타당성 검증 |
| P3 | Ping OK / GET·SET 실패를 별도 Service Health로 관측 | 조건부 | SSD stall 및 gray failure 분리, FD 상태와 별도 출력 |
| P4 | JGroups 유사 조합, SWIM+Phi Hybrid, 실제 Failover Gate | 보류 | 새 비교군 추가보다 메트릭·실험 정확성 확보가 우선 |

**명시적 비범위:** Redis/Valkey 실제 백업 승격·epoch 투표·fencing 재구현, 전체 Aerospike/Paxos/Geode coordinator 재구현, 사용자 트래픽이 있는 호스트에서 netem 적용, Fault Injection에 의한 데이터 삭제. FD 출력은 안전한 Failover 승격의 증명이 아니다.

## 2. 작업 전 점검과 보호조치

1. 프로젝트 루트에서 `git status --short`, `find`/`rg`로 소스·테스트·빌드 파일 조사. 현재 Java 17 기반 `failure-detector-lab`이라는 문서 기록은 **현행 저장소와 대조**할 것.
2. 현재 실행 명령, 기존 11,160회 시뮬레이션의 run manifest/seed/CSV를 파악하고 `results/legacy/` 등 읽기 전용 위치에 보존. 해당 값들을 수정된 모델 결과와 같은 그래프에 무표시로 섞지 말 것.
3. 기존 detector 상태(`ALIVE`, `SUSPECT`, `DEAD`)·gossip·tick·onMessage·TimeSource·Network 추상화가 있는지 확인. 공유 추상화만 확장하고 필요 없는 전체 리팩터링 금지.
4. 외부 패키지 버전·모듈 경로·빌드 명령을 정확히 확인한 뒤 계획 작성. Docker/`tc`/`stress-ng` 등 없는 도구는 실행 가능한 항목으로 꾸미지 말 것.
5. 정상 테스트 baseline을 먼저 한 번 실행해 기록하고, **구현 후 기존 테스트가 계속 통과**하도록 보장.

## 3. 실험의 핵심 불변식 및 타임라인 정의 (P0)

### 3.1 공통 사실과 event trace

- 실험 시간은 단조 증가하는 simulation/monotonic clock의 **밀리초 또는 나노초**. 정수 오버플로·`wall clock` 변경에 의존하지 말 것.
- `faultStartAt`, `faultEndAt`, `actualProbeSentAt`, `directTimeoutAt`, `indirectTimeoutAt`, `suspectAt`, `deadAt`, `recoveredAt`을 각각 기록. 이벤트별 `observer`, `target`, `incarnation/epoch`, `reason`, `seed`, `scenario`, `profile`, `runId`를 기록.
- **감지 지연:** crash(프로세스 종료) 실험에서는 `firstSuspectAt - faultStartAt` 및 `firstDeadAt - faultStartAt`. 실제 이벤트가 관측되지 않으면 `null`(미검출)로 표시하고 평균에 0ms로 대입하지 말 것.
- **False SUSPECT/DEAD:** 실제 프로세스가 살아 있고 fail-stop crash가 아닌 구간에서 해당 상태를 오판한 이벤트를 별도 집계. `partition` 중 원격 서비스는 접근 불능일 수 있으므로 `nodeAlive`와 `reachableFrom(observer,target)` 및 `serviceHealthy`를 ground truth로 구분해 `falseDeadFailStopPerspective`, `unreachableButAlive` 등으로 보고. **접근 불가를 무조건 잘못된 판정이라고 주장하지 말 것.**
- `duration`은 실제 오판 상태 시작부터 해제 시각까지, 실험 종료 시 우측 검열(censored)된 이벤트는 검열 표시. 실험 종료 후 자동 해제를 기록해서 duration을 부풀리거나 축소하지 말 것.
- 환경 주입 스케줄과 probe 스케줄을 독립적으로 생성. **fault onset offset**을 probe tick과 일치시키지 말고 일정한 분포 또는 고정 offset 배열(예: +0/+137/+491/+809 ms)을 사용. Timer가 실제 예정 시각에 expire되도록 scheduler를 수정.
- 이벤트 큐에서는 동일 timestamp의 처리 우선순위(예: fault injection → packet delivery → scheduled timeout)를 문서화하고, tie-breaking을 deterministic seed와 이벤트 ID로 안정화.

### 3.2 SUSPECT 0ms 문제에 대한 검증 테스트

- 기존 코드에서 `crashStartAt == probeTick`이거나 direct/indirect timeout 미경과인데 `SUSPECT`로 전환되는지 확인.
- 인위적 crash를 직후에 주입한 경우 `SUSPECT`가 `probe+ACK timeout(+indirect stage)` 전에 발생하지 않는지 테스트.
- 초기 crash를 이미 의심 중인 대상으로 설정하는 예외 상황은 별도 fixture로, 일반 crash 결과와 합산 금지.
- **검증 규칙:** 네트워크/스케줄러의 빠른 관찰로 즉시 도달 가능한 신호가 없다면 probe timeout을 무시한 0ms 새 의심 이벤트가 없어야 함. 단순히 nonzero assertion만 추가하는 방식은 금지.

### 3.3 동일 관측 조건과 통신 비용

- 동일한 `scenario`, `seed`, `nodeCount`, `faultStartOffset`, **공통 네트워크/워크로드 trace**를 네 프로필에 재사용해 paired comparison. 프로토콜 별 메시지 발생 시점은 달라질 수 있으므로 traffic trace와 protocol randomness는 각각 별도 시드(stream)로 분리.
- 프로토콜 특성상 모든 프로필을 **동일 초당 메시지 예산**으로 정확히 고정할 수는 없음. 두 종류 결과를 모두 기록: (a) 현실적인 독립 기본 설정 비교, (b) target probe/heartbeat 빈도 또는 예산을 맞춘 sensitivity run. 결과에서 어떤 비교인지 표시.
-  `messageCountPerNodeSec`, `byteCountPerNodeSec`은 실제 UDP에서는 인코딩된 바이트와 송수신 프레임 수로 집계. discrete-time은 `estimatedWireBytes`로 명시하고 gossip piggyback/ACK/indirect/HELLO payload까지 선언된 직렬화 모델에 포함. 프레임 생성 횟수와 gossip 정보 항목 수를 혼동 금지.

## 4. Fixed Deadline Heartbeat 기준선 (P1)

### 목표

Phi와 SWIM 모두가 단순한 fixed heartbeat timeout보다 어떤 상황에서 얼마나 나은지 측정한다. 이 baseline은 독립 membership gossip 알고리즘인 척하지 않는다.

### 상태/로직

- 관측자 A가 target B의 heartbeat 또는 `PING/ACK` 신호를 마지막으로 본 시각 `lastSeenAt[B]` 저장.
- `now - lastSeenAt[B] >= deadlineMs`면 관측자 기준 `SUSPECT`; 별도 확인 기간 `confirmMs` 경과 후 확인 신호가 계속 없으면 `DEAD`(이 상태는 실험용 공통 상태 매핑이며 기존 시스템의 실제 승격 조건 아님).
- 수신한 유효 heartbeat가 설정과 정책상 허용되는 동안, 동일 incarnation의 `SUSPECT`는 `ALIVE` 복귀. DEAD 복귀 정책은 기존 이벤트 계약과 맞추되 `REJOIN/incarnation increase`를 명시.
- configurable `heartbeatIntervalMs`, `deadlineMs`, `confirmMs`, `warmupMs`, `peerSelectionMode`(full peer 또는 deterministic ring) 및 payload size. 기본 비교에서는 heartbeat interval을 공개하고 **all-to-all 통신량을 정직하게 기록**.
- 초기 상태에서 첫 샘플 미수신 상태를 DEAD로 오판하지 않도록 초기 grace; `deadlineMs > heartbeatIntervalMs` 등 검증.

### 수용 테스트

정상 heartbeat는 오탐 0, crash는 deadline/confirm 경과에 따른 전이, 짧은 지연은 회복, 재시작 incarnation 보존, 5/25/100노드에서 full mesh 전송 비용 증가가 기대와 합치하는지 확인.

## 5. SWIM-only Ablation (P1)

- 기존 `SWIM + Lifeguard`에서 `Local Health Awareness`(LHA-Probe, LHA-Suspicion), Buddy System 등 Lifeguard 전용 보정을 각 기능 단위로 비활성화할 수 있도록 **설정 플래그로 분기**. 알고리즘 로직을 복제해서 baseline 버전이 갈라지지 않게 할 것.
- `SWIM_BASE`는 direct/indirect probe, PING-REQ, SUSPECT→DEAD, gossip/incarnation을 그대로 사용하지만 **health 보정 비활성화**.
- `SWIM_LHA`는 기존 Lifeguard 설정 유지. 필요 시 중간 Ablation `SWIM_LHA_PROBE_ONLY`, `SWIM_LHA_SUSPICION_ONLY`, `SWIM_BUDDY_ONLY`를 선택 구현하되 문서에 어떤 부분이 구현됐는지 명시.
- CPU pause 중 **observer 자신이 느린데 target을 DEAD로 잘못 판정하는 경우**, target pause, packet jitter/loss, burst loss에서 두 프로필의 falseSuspect·falseDead·recovery와 probe bytes를 paired run으로 비교.
- 설정 재사용 시 기존 test snapshots, 모든 주기 timer와 event handler가 동일해지는지 검증. LHA 플래그가 우연히 gossip fanout/packet budget을 변경하지 않도록 테스트.

## 6. 기존 Phi Accrual + Gossip 검증 (P2)

- Phi suspicion score  `phi = -log10(P(nextHeartbeatDelay >= elapsed))`에 해당하는 분포 계산이 올바른지 확인. 시간 단위, minimum stddev, sample size, cold-start, acceptable pause, threshold를 공개.
- threshold 4/6/8/10 같은 스윕은 본 실험 수치와 혼동하지 않고 `phi_t4`, ... profile 명칭으로 기록.
- Heartbeat/Phi 계산과 Gossip 전파를 논리적으로 분리: **Phi는 FD 판단 모델이지 독립 전송 프로토콜이 아니며** gossip 전파량은 fanout/payload/interval에 의존.
- 불충분한 sample과 분산 0, NaN/Inf 등 예외 조건에서 검증 가능한 warm-up 정책을 적용. 실제 제품별 Phi 구현 차이를 '공식 Cassandra 그대로 복제'라고 주장하지 말 것.

## 7. 시나리오 매트릭스 및 반복 정책 (P2)

### 최소 시나리오

| 종류 | 필수 경계 조건 | 주요 메트릭 |
|---|---|---|
| normal | 안정 RTT, 노드 변화 없음 | false suspect/dead, msgs/bytes |
| crash | 일시 offset 4종 이상, 1 노드 강제 종료 | suspect/dead latency, miss |
| crash-under-jitter | crash + RTT jitter | latency/p95/false dead |
| packet-loss | 패킷 손실률 0/5/15/30% (예시, YAML 설정) | false events, detect delay |
| burst loss | short 45% burst 등, duration 명시 | false SUSPECT vs DEAD |
| latency | RTT 평균/분산 스윕 | false positive |
| gradual/regime/bursty | 정상→지연 급증 또는 서서히 증가 | adaptation, recovery |
| observer pause | 관측자 CPU/프로세스 정지 1/3/10초 | Lifeguard 기여 |
| target pause | 대상 지연/정지 | false DEAD; recover |
| partition | 5/15/30초 one-way/two-way (구별) | unreachable vs dead 오인 |
| restart | crash 후 incarnation 변화 | stale gossip/refutation |
| service stall | PING 정상, GET/SET 불능·SSD stall | service health flag (별도) |

- 기존 12종 scenario 이름과 동작을 보존하며 필요한 세부 시나리오를 확장. 구버전 실험과 비교 시 scenario 정의 변경을 반드시 표기.
- 기본 비교: 5/25/100 노드, 가능한 환경에서 조건당 30~50회 이상, seed list 고정. Docker 실험은 먼저 3/5/8 노드로 시작하고 자원이 되는 경우 확장. **로컬 Docker의 100 컨테이너를 의무적으로 생성하지 말 것.**
- 매 실험마다 startup stabilization, warmup, fault interval, observation window를 명시. 결측 run·중단된 run은 실패 원인과 함께 기록하고 자동으로 성공만 집계하지 말 것.
- CI 단위 테스트와 긴 벤치마크 분리; 큰 규모 반복 실험은 명시적 CLI로만 실행.

## 8. 데이터 산출물과 평가 방식 (P2)

- `run-manifest.json`: git commit hash, environment/JDK/OS, profile, scenario, seed, node count, YAML config, normalized parameters, run start/end, fault truth/offset, execution success flag.
- `events.jsonl`: timestamp/observer/target/incarnation, packet send/drop/receive, probe state transitions, health status, fault injection, ground truth, reasons.
- `runs.csv`: 1 run × 1 observer/target 또는 명시된 aggregation granularity; elapsed latency nullable, `detected` boolean, false suspect/dead count + durations, per-node/cluster messages and bytes, estimated vs measured source.
- `summary.csv`: profile×scenario×nodeCount×parameter×metric별 n·missing n·mean·median(p50)·p95·p99·CI 또는 bootstrap CI, 별도 false-dead risk. 95% CI를 계산하기 어려운 이산·중도절단 자료는 분위수/개별 결과를 보여주고 허위 정밀도 금지.
- `report.md`: 장단점, 비교 공정성, 모델 한계, 통계 분포, 이전 0ms 수정 영향, 실패한 실험과 미해결 리스크.
- `plots/`는 표준 CSV로부터 생성. 그래프에 반드시 nodeCount, latency definition, profile, whether modeled/observed, sample count 표기.
- `--help`, `--scenario`, `--profiles`, `--nodes`, `--seeds`, `--output-dir`, `--config`, `--dry-run` 같은 재현 CLI를 제공하고 **정확한 실제 명령은 저장소 빌드 구조 확인 후 README에 기록**.

## 9. 실제 UDP / Fault Injection (P3)

1. 기존 UDP SWIM listener/Netty Wire v1 및 테스트 transport가 있다면 재사용하되 클라이언트 TCP listener와 섞지 말 것. 실제 물리/컨테이너 네트워크 경로로 송신할 것.
2. Docker Compose 또는 간단 shell runner로 3/5/8 노드 구동. 호스트 OS 변경을 피하려면 **실험 전용 네임스페이스·컨테이너에만** `tc netem delay/loss/reorder/duplicate` 적용. `CAP_NET_ADMIN`은 해당 실험 컨테이너에 필요한 최소 범위로만 부여.
3. crash는 해당 프로세스 kill, pause는 `docker pause` 또는 SIGSTOP/CONT (시스템 스케줄러/GC pause와 동일하다고 주장 금지), CPU contention은 별도 제한된 stress workload, SSD stall은 I/O 실험으로 명시. **Sleep 기반 지연은 SSD 장치 장애 실측이 아닌 서비스 수준 지연 주입**으로 분리.
4. partition은 방향/대상을 명시하며 target에 대한 특정 포트 차단인지, 전체 네트워크 단절인지 구별. inject/cleanup을 idempotent script로 만들고 종료 시 반드시 netem 제거·컨테이너 정리.
5. 공통 monotonic timestamp와 timeout을 기록하고, 노드 간 clock offset을 별도 측정/보정하지 않은 상태에서는 다른 프로세스의 `nanoTime`끼리 단순 뺄셈하지 말 것. fault injector 기준의 기준시각과 detector 이벤트는 동일 호스트 수집기에서 수집하거나 시간 동기화 방법을 문서화.
6. 실제 네트워크에서 인코딩된 frame bytes, ACK failure, retransmission, CPU/GC pause telemetry, GET/SET p95, zero/partial availability를 별도 수집. Model 결과와 실제 측정 결과를 섞지 말 것.
7. Docker, `tc`, 권한이 없으면 P3는 명시적으로 `SKIPPED_ENVIRONMENT` 처리하고 P0~P2만 완료 보고. 필요한 명령과 권한을 README에 정확히 제시.

## 10. 선택 조건부 Service Health Observer

- 프로세스 membership과 별도로 `HealthSample(target, time, pingHealthy, getHealthy, setHealthy, diskLatencyMs, source)` 기록. GET/SET probe 대상은 실험용 key namespace를 사용하고 실서비스 데이터에 쓰지 말 것.
- `PING OK + GET/SET 실패` 환경을 인위적 service fault와 실제 제한된 I/O stress로 구분. Gray failure는 UDP alive만으로 판단 불가능함을 기록.
- Detector를 자동으로 DEAD로 바꾸거나 **실제 failover/replica promotion을 호출하지 않는다**. 상위 모듈에 health status와 근거 이벤트만 노출.

## 11. 추가 실험 판정 기준 (연구용, 사전 합의 필요)

**절대 수치의 합격 기준은 사용자가/멘토가 정할 운영 SLO 없이 임의로 만들지 말 것.** 먼저 P0~P2를 완료한 뒤 다음 중 무엇을 만족하는지 보고:

- 같은 false-DEAD 허용 수준에서 어느 방식의 crash DEAD latency 분포가 낮은가?
- 같은 crash detection p95 요구 수준에서 어느 방식의 정상 traffic bytes/s가 낮은가?
- observer pause 동안 Lifeguard 적용이 baseline 대비 false events/duration을 낮추는가?
- Phi t6/t8이 설정/노드 규모 변경에서도 partition 오탐 억제를 유지하는가?
- 실측 UDP 환경에서 discrete-time의 순위·추세가 재현되는가?
- 데이터 서비스 장애를 FD의 alive 판단과 구분해서 표시했는가?

위 항목을 **모두 증명할 수 없다면 부족한 데이터를 정확히 표기하고 최종 채택 보류**. 11,160회 기존 실험 횟수를 '검증 완료'의 증명처럼 사용하지 말 것.

## 12. 제안 파일 구성 (저장소 구조 확인 뒤 조정)

```text
failure-detector-lab/
  src/main/java/.../
    detector/FixedDeadlineFailureDetector.java
    detector/DetectorProfile.java
    detector/SwimConfig.java                 # 기존 파일 있다면 플래그 확장
    sim/DeterministicEventScheduler.java     # 기존 큐 보완
    sim/FaultTruthTimeline.java
    sim/SimulationMetricsCollector.java
    sim/ScenarioMatrixRunner.java
    transport/RealUdp...                     # P3, 기존 네트워크 경로 재사용
  src/test/java/.../
    FixedDeadlineFailureDetectorTest.java
    SwimLifeguardAblationTest.java
    FailureTimelineContractTest.java
    DetectionMetricsTest.java
    PhiColdStartTest.java
    ScenarioReproducibilityTest.java
  scripts/experiments/
    run-matrix.sh
    inject-netem.sh
    cleanup-netem.sh
  docs/
    experiment-methodology.md
    experiment-results.md
  results/                                  # 큰 결과 .gitignore 여부 확인
```

**위 파일명은 가이드일 뿐이다. 기존 `tick()`/`onMessage()`/Runner 구조를 우선 활용한다.**

## 13. 단계별 완료 조건(Definition of Done)

- [ ] 저장소 구조·코드 경로·기존 명령 파악 후 P0 계획 문서화
- [ ] SUSPECT 0ms 시간 모델 원인을 특정하고 timeout 반영으로 수정, 회귀 테스트 추가
- [ ] Deadline baseline 및 SWIM-only 구현 후 논리·전이 테스트 통과
- [ ] 기존 Lifeguard·Phi와 동일 시나리오 seed 재현 테스트 통과
- [ ] 적어도 normal/crash/observer pause/partition/loss의 `runs.csv`, `events.jsonl`, `summary.csv` 산출
- [ ] false DEAD 정의와 partition/Service Health의 별도 truth 명시
- [ ] 평균 외 p50/p95/p99/표본 수·누락·측정 기원(model/real) 표기
- [ ] README에 실행 명령·환경·복원 방법 제공
- [ ] 가능한 환경에서 UDP/netem smoke test; 불가 시 이유와 실행 준비방법 명시
- [ ] 기존 테스트에 회귀 없음, 구현 완료/미완료/제약 정확히 구분

## 14. 근거 자료 — 주요 공식 출처

- [SWIM, 2002](https://research.google/pubs/swim-scalable-weakly-consistent-infection-style-process-group-membership-protocol/)
- [Lifeguard, 2018](https://ieeexplore.ieee.org/document/8416202/)
- [Phi Accrual, 2004](https://doi.org/10.1109/RELDIS.2004.1353004)
- [HashiCorp Consul Gossip/Lifeguard](https://developer.hashicorp.com/consul/docs/concept/gossip)
- [Apache Ignite 3 ScaleCube settings](https://ignite.apache.org/docs/ignite3/latest/administrators-guide/config/node-config)
- [Hazelcast Deadline FD](https://docs.hazelcast.com/hazelcast/5.8-snapshot/clusters/deadline-detector)
- [Hazelcast Phi FD](https://docs.hazelcast.com/hazelcast/5.2/clusters/phi-accrual-detector)
- [Infinispan JGroups failure detection](https://infinispan.org/docs/stable/titles/configuring/configuring.html)
- [Redis Cluster PFAIL/FAIL](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)
- [Couchbase automatic failover and disk signals](https://docs.couchbase.com/server/current/learn/clusters-and-availability/automatic-failover.html)
- [2025 NSDI fail-slow tolerance](https://www.usenix.org/conference/nsdi25/presentation/lu)
- [2026 NSDI recovery pilot execution](https://www.usenix.org/conference/nsdi26/presentation/li-zhenyu)

---

*이 파일은 실험 구현 명세서이며, 실제 코드를 읽거나 추가 실험을 실행해 결과를 확정했다는 의미가 아니다.*
