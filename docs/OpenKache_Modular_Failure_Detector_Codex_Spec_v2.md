# OpenKache 모듈형 Failure Detector 연구 · 구현 · 실험 명세서 (Codex v2)

> 작성 기준: 2026-10-27 / 대상: OpenKache의 현행 Java 기반 `failure-detector-lab` 및 필요 시 실제 UDP 실험 환경
> 연구 목표: 논문/상용 구현을 통해 얻은 설계 선택지를 **A. 장애 판단(Decision) / B. 생존 정보 관측·전파(Observation & Dissemination) / C. 오탐 억제·판단 보정(Correction)**으로 분해하고, **호환되는 조합만** 구성하여 각 모듈의 기여도와 상호작용을 정량 검증한다.
> 중요: A/B/C는 연구를 위한 **논리적 설계 축**이지, 세 계층이 모든 Failure Detector의 공식 표준이라는 뜻이 아니다. 새로운 알고리즘의 우수성은 구현 전부터 가정하지 않는다.

## 0. Codex에 바로 붙여넣는 지시문

```text
OpenKache 저장소 루트에 있는 OpenKache_Modular_Failure_Detector_Codex_Spec_v2.md를 읽고, 기존 failure-detector-lab의 SWIM, Lifeguard, Phi, Fixed Deadline, simulation scheduler, metrics, Docker/UDP runner를 먼저 조사해줘.

목표는 A(장애 판단), B(생존 신호 관측·정보 전파), C(의심 판단 보정)를 논리적으로 모듈화해, 호환 가능한 DetectorProfile F0~F7의 성능을 비교할 수 있는 실험 프레임워크를 만드는 거야. 새 프레임워크를 처음부터 재구현하지 말고 기존 상태 전이·clock·gossip·run manifest·CSV/JSONL 생성·JUnit 테스트를 최대한 재사용해. 기존 git 변경 사항과 데이터는 보존해.

반드시 아래 순서로 진행해.
1) 현재 git status, 실제 패키지 구조, 각 구현·계약·실험 명령, 기존 실험 결과 파일을 확인하고 '이미 완료 / 일부 구현 / 미구현' 매핑표와 최소 변경 계획을 보고한다.
2) A/B/C의 분리 가능한 지점과 분리하면 의미가 깨지는 프로토콜 결합점을 문서화한다. 기존 F0~F3 동작을 golden test/trace로 보존하며 필요 최소한의 공통 인터페이스를 도입한다.
3) 보정된 타임라인, fault ground truth 및 측정 라벨을 검증하고, F0~F3에 대해 충분히 반복 가능한 정식 실험을 구현한다.
4) 이후 한 번에 한 설계 요소만 변경하여 F4(Heartbeat inter-arrival 기반 Adaptive Timeout), F5(Fixed Deadline + Ring/Neighbor heartbeat), F6(F5 + Verify Suspect), F7(SWIM의 Lifeguard 세부 기능 ON/OFF)을 순차 추가한다. F4~F7은 '제안'이며 코드에 이미 있다고 가정하지 않는다.
5) 가능하면 기존 Docker/UDP/netem을 재사용해 실제 5/8노드 재현성을 검증한다. 환경에 없으면 실행하지 않은 실험은 명시적으로 미검증 처리한다.

각 단계마다 변경 파일, 실제로 실행한 명령, JUnit 테스트 통과/실패, 시나리오별 산출물, 구현 한계, 다음 단계로 넘어가도 되는지를 출력해. 소스 미확인 가정을 근거로 외부 라이브러리를 추가하지 말 것. 충분히 검증하기 전 Phi+SWIM Hybrid나 Geode/JGroups 전체 복제, 실제 replica 자동 승격, 합의/epoch/fencing을 구현하지 말 것. 결과 파일은 timestamp/runId를 분리해 이전 11,160회 실험·80-run smoke·3-node UDP smoke를 절대 덮어쓰지 말 것. 실제 실행하지 않은 테스트를 '통과'라고 표기하지 말고, 커밋은 별도 요청이 있을 때만 수행해.
```

## 1. 연구 질문과 성공의 의미

**연구 질문:** 장애 판단 정책(A), 생존 관측·상태 전파(B), 오탐 보정(C)을 각각 바꿨을 때 감지 지연·오탐·통신 비용이 어떻게 달라지며, 어떤 호환 조합이 OpenKache의 요구사항에 적합한가?

- **알고리즘의 개수 최대화가 목표가 아니다.** 같은 실험 계약에서 한 요소씩 변경하는 *controlled comparison / ablation*이 목표다.
- **세 축만으로 시스템이 끝나지 않는다.** 공통 멤버십 저장소, 버전/incarnation, monotonic time, scheduling, 전송/직렬화, 계측, fault injection, health probes는 별도 공통 기반이다.
- **모든 조합이 허용되지는 않는다.** Phi는 유효한 heartbeat 간격 분포가 필요하고, SWIM의 suspicion은 probe/indirect-probe 및 membership 프로토콜에 의존한다. Lifeguard를 임의의 Heartbeat FD에 즉시 붙일 수 있다고 가정하지 않는다.
- 최종 결과는 **절대적 세계 최고**가 아니라 OpenKache의 SLO/트래픽·오탐 허용치 하에서 채택할 **Pareto 후보와 선택 근거**다. 운영 SLO가 정해져 있지 않으면 임의의 단일 점수로 순위를 매기지 않는다.

### 연구 축

| 축 | 핵심 질문 | 구현·실험 후보 | 주요 성능 영향 |
|---|---|---|---|
| **A: Decision** | 어느 증거·시점에 SUSPECT/DEAD를 선언하나? | Fixed Deadline, Phi Accrual, SWIM Suspicion, inter-arrival Adaptive Timeout | Detection latency, false SUSPECT/DEAD, 미검출 |
| **B: Observation / Dissemination** | 누가 누구를 어떻게 확인하고 결과를 어떻게 공유하나? | periodic heartbeat, direct/indirect probe, random peer, ring/neighbor, gossip/piggyback, TCP socket 보조 신호 | 관측 커버리지, 네트워크 비용, 전파 시간 |
| **C: Correction** | 관측자 자신이 느리거나 일시적 손실이 있을 때 어떻게 오판을 완화하나? | no correction, Lifeguard LHA-Probe/LHA-Suspicion/Buddy, Verify Suspect | 오탐 억제, 추가 검사 비용, 최종 DEAD 지연 |

**B는 실험상 세 하위 선택지로 구분**: B1 *peer-selection/topology*, B2 *liveness signal*, B3 *membership dissemination*. 겉으로 B 하나라고 해서 서로 독립적인 세 문제를 혼동하지 않는다.

## 2. 현재 기록된 완료 사항과 주의점

다음은 노션 연구 보고서에 **기록되어 있는 사실**이지, Codex가 현재 저장소에서 직접 확인한 상태를 뜻하지 않는다. **반드시 실제 코드와 실행 파일로 다시 확인한다.**

- 1차: SWIM + Lifeguard vs Phi + Gossip, 12개 시나리오 × 5/25/100 노드, 합계 **11,160 runs**의 discrete-time 비교. 단, SWIM SUSPECT **0ms는 scheduler artifact**로 확인되어 탐지 속도 우열에 사용 금지.
- 2026-10-27: deterministic scheduler 보정, **Fixed Deadline**, **SWIM Base**, **SWIM + Lifeguard**, **Phi + Gossip** 네 프로필의 **5-node, 2-seed, 80-run smoke** 및 **3-node 실제 UDP/netem 단회 smoke**를 기록함.
- 과거 기록상의 명령 예: `./gradlew -p failure-detector-lab test selfTest --rerun-tasks`, `./gradlew build --rerun-tasks`. 현재 Gradle 구성에 실제로 존재하는지 확인하고 성공 여부를 새로 기록해야 한다.
- 이 명세는 **기존 결과의 보존, 정확한 메트릭, 전이 회귀 방지**가 신규 후보 추가보다 우선이라는 전제를 가진다.

## 3. 작업 착수 전 Repository Audit (Gate 0)

### 필수 조사

1. `git status --short`, `git branch --show-current`, revision, `git diff --stat`로 사용자의 기존 변경 사항을 확인. 사용자 변경이 있으면 덮어쓰거나 revert하지 않는다.
2. 실제 디렉터리·언어/버전/JDK/Gradle 설정·기존 README·실험 스크립트 파악. 문서의 경로와 클래스명은 **가정**이다.
3. 기존 각 프로필 구현 위치, `tick`/`onMessage`/`schedule`/membership/gossip/UDP transport 같은 계약을 확인.
4. 기존 scenario runner, deterministic scheduler, metric collector, reporter, manifest/JSONL/CSV 구조, 3-node UDP smoke, Docker/`tc netem` 주입 기능을 파악.
5. 기존 테스트 실행 명령을 확인하고 baseline 결과 기록. JUnit이 실패하면 **기존부터 실패했는지**와 **새 변경 때문에 실패했는지** 구분.
6. 결과 보존: legacy, fixed-timeline, 80-run smoke, real UDP 원본 경로를 식별하고 별도 새 `runId` 디렉터리에만 기록. `.gitignore`·대용량 정책 확인.
7. 충돌하거나 불명확한 부분은 파일·라인과 함께 `audit.md`에 기록. 원본 파일을 모른 채 새 클래스를 만들어 병렬 프레임워크를 증식시키지 않는다.

### 이 단계의 산출물

- `audit.md` 또는 동등 문서: **기존 클래스→A/B/C/공통 기반 맵**, 기존 실행 명령, 구현 완료/미완료, 위험한 결합점, 변경 범위, 통과한 baseline 테스트.
- **Gate 0 종료 조건**: 기존 프로필 4개의 실제 동작과 데이터 경로를 추적할 수 있어야 다음 단계로 넘어간다.

## 4. A/B/C 모듈 경계 (Gate 1)

아래 Java 형식은 설계 방향을 설명하는 **의사 코드**다. 패키지·시그니처·타입을 그대로 만들어야 한다는 지시가 아니다. 기존 계약에 어댑터만 추가할 수 있으면 그렇게 할 것.

```java
// 동일 시뮬레이터 시간축 또는 해당 프로세스의 monotonic clock을 사용.
record ObservationEvent(NodeId observer, NodeId target,
                        SignalKind kind, long observedAtNanos,
                        long correlationId, Evidence evidence) {}

interface ObservationStrategy {  // B: peer 선정 + probe/heartbeat 이벤트 공급
    void onTick(long nowNanos);
    void onMessage(TransportEnvelope msg, long nowNanos);
}

interface DisseminationStrategy { // B3: membership 상태 전파 (필요할 때 분리)
    void onMembershipEvent(MembershipEvent event);
}

interface DecisionPolicy {      // A: 필요한 입력 신호를 명시해 compatibility 검사
    void onObservation(ObservationEvent event);
    void onTick(long nowNanos);
}

interface CorrectionPolicy {    // C: 지원하는 Decision/Observation 조합에서만 사용
    void onLocalHealth(LocalHealthSample sample);
    void onSuspicion(SuspicionEvent event);
}

record MembershipEvent(NodeId observer, NodeId target, MemberState state,
                       long observedAtNanos, long incarnation,
                       String reason, Evidence evidence) {}
```

### 필수 불변식

- `state`는 기존 ALIVE/SUSPECT/DEAD 의미를 그대로 유지. Phi처럼 **즉시 DEAD에 매핑**하는 정책이 있으면 모델링된 상태 매핑을 정확히 기록하며, 존재하지 않는 SUSPECT 시간을 0으로 채우지 않는다.
- `correlationId`, incarnation/세대, observer-target, clock domain, 이유를 보존. 늦게 도착한 ACK와 stale gossip이 새 상태를 되돌리지 않도록 기존 규칙을 유지한다.
- 한 프로필이 여러 DecisionPolicy를 동시에 호출해 **중복 DEAD를 발행하지 않도록** 공통 상태/전이 소유권은 단 하나의 계층에 둔다.
- `CorrectionPolicy`는 직접 상태를 무단으로 확정하지 않고 정책이 지원하는 조정점(타임아웃·추가 확인·suspicion 지속)을 통해 동작하도록 설계. 단, 기존 Lifeguard 구현은 기존 절차가 우선한다.
- B가 생성한 관측 이벤트의 종류·샘플 간격·실패의 의미(미수신 vs probe timed-out vs socket closed)를 A가 구별해야 한다.
- Gossip과 다른 노드의 보고는 **독립적인 증거**인지 여부를 구분한다. 동일 원천 보고의 재전달을 다수 증거로 중복 계산하지 않는다.
- 시계: simulator는 단조 simulated time, 실제 실행은 **동일 observer 프로세스 기준 monotonic time**을 사용한다. 서로 다른 프로세스의 `nanoTime`을 직접 빼지 않는다.
- 구현 명령의 공통 `DetectorProfile`은 기능 지원 여부를 기술하며, 호환되지 않는 조합은 **명시적 Validation Error**를 반환해야 한다. 자동으로 비슷한 알고리즘으로 대체 금지.
- A/B/C의 추상화가 기존 SWIM/Lifeguard/Phi를 논리적으로 왜곡한다면 강제 리팩터링하지 말고 `CompositeAdapter`/프로필 설정으로 비교 축만 노출한다.

### Capability 예시

- `HEARTBEAT_INTERARRIVAL`: 반복 heartbeat 도착 시각 시퀀스; **Phi 및 inter-arrival adaptive A**용.
- `REQUEST_RESPONSE_TIMEOUT`: PING/ACK, indirect probe, correlation; **SWIM A**용.
- `MONOTONIC_PROCESS_LOCAL_HEALTH`: **Lifeguard C**용.
- `MULTI_OBSERVER_CONFIRMATION`: 서로 다른 observer의 독립 확인; **Verify Suspect C**용.
- `TCP_SOCKET_CLOSE`: 전송 레벨 연결 종료 증거. 즉시 프로세스 crash라고 단정하지 않음.

## 5. 조합 매트릭스 — 반드시 구현·검증된 것과 제안을 구분 (Gate 2~5)

| ID | A: Decision | B: Observation + Dissemination | C: Correction | 의도 / 수행 상태 |
|---|---|---|---|---|
| **F0** | Fixed Deadline | periodic Heartbeat (+기존 전파 방식) | None | 기존 baseline. **현재 저장소 재검증** |
| **F1** | Phi Accrual | periodic Heartbeat + Gossip | None | 기존 통계 판단. **재검증** |
| **F2** | SWIM Suspicion | Random direct/indirect PING + Gossip | None | 기존 SWIM-only ablation. **재검증** |
| **F3** | SWIM Suspicion | F2와 동일 | Lifeguard 기본 구성 | 기존 SWIM+LHA. **재검증** |
| **F4** | Adaptive inter-arrival Timeout | F0와 동일한 Heartbeat/Topology | None | **신규 후보**: A만 변경, F0/F1과 비교 |
| **F5** | Fixed Deadline | Ring/Neighbor Heartbeat (명시적 membership view) | None | **신규 후보**: B1만 변경, F0과 비교 |
| **F6** | Fixed Deadline | F5와 동일 | Verify Suspect | **신규 후보**: C만 변경, F5와 비교 |
| **F7a~c** | SWIM Suspicion | F2와 동일 | LHA-Probe/LHA-Suspicion/Buddy 옵션별 | **신규 세부 ablation**: F2/F3와 비교 |

**F4~F7은 사전 조사·코드 검증 전에는 미구현으로 취급한다.** F0~F3의 현재 소스가 기능적으로 다를 경우 표에 실제 상태를 수정해 기록한다.

### 추가 후보 백로그 (지금 구현하지 말 것)

- Socket heartbeat 병행 탐지, multiple-observer gate, passive GET/SET health, HyperView/Rapid/Hybrid, SWIM+Phi 무작위 결합 등은 **추가 가설·후속 단계**로만 기록.
- F5의 ring/neighbor 방식은 단일 successor만 감시하면 임의의 노드 장애를 모든 관측자가 즉시 알 수 없다. **누가 누구를 감시하는지**, membership view 변경 및 장애 전파 방식이 정상이어야만 비교 가능하다.
- F6 Verify Suspect는 JGroups 전체 프로토콜 복제가 아니다. **고유 관측자 재확인**과 보류 정책을 최소한으로 구현해 효과만 측정한다. `k=...` 독립 확인의 정의·추가 트래픽을 공개한다.
- F7의 각 옵션이 현재 Lifeguard 구현에서 지원되지 않으면 해당 옵션을 `NOT_IMPLEMENTED`로 남기고 실제 존재하는 LHA 기능의 ON/OFF부터 검증.

## 6. 단계별 구현 상세 — Codex 작업 순서

### Gate 2A: F0~F3 회귀 검증 · 시간축 · 충분한 반복 비교 (가장 먼저)

1. 기존 deterministic scheduler가 **direct ACK timeout + indirect ACK timeout 경과 후** 새 SUSPECT를 낼 수 있게 되었는지 재검증. 같은 시간 tick에서 프로브 시각을 되감지 않는다.
2. `faultInjectedAt`, `probeSentAt`, `directTimeoutAt`, `indirectTimeoutAt`, `suspectAt`, `deadAt`, `recoveredAt` 각각의 기록과 이벤트 우선순위를 검증한다.
3. Fault ground truth를 `processAlive`, `reachable(observer,target)`, `serviceHealthy`로 나누고 partition, paused process, gray failure를 동일한 false DEAD로 취급하지 않는다.
4. 동일 scenario/seed/fault onset offset으로 F0~F3 baseline을 실행. 다만 **전송 메시지 시점 자체가 다르므로 identical network traces가 동일한 메시지 결과를 보장한다고 가정하지 않는다**. 공통 네트워크 무작위장과 전송 프로토콜별 PRNG 스트림을 분리한다.
5. 현실 설정(`realistic-default`)과 비교 제약(`matched-target-activity`, 가능한 경우 `matched-wire-budget`)은 별도 실험으로 구분. 프로토콜 전체 트래픽 동일이라는 근거 없는 문구 금지.
6. smoke 성공 이후 정식 배치 실행: 기존 normal/crash/crash-under-jitter/packet-loss/burst-loss/latency/gradual/bursty/regime/observer-pause/target-pause/partition의 **12종**을 유지. 5/25/100 노드, 조건당 seed 30 이상을 목표로 하되 실제 실행 가능한 범위·시간/비용·환경 제약을 기록한다. 테스트만 통과했다고 대규모 결과를 창작하지 않는다.

### Gate 2B: F4 — Adaptive Timeout 의사결정 정책 (A 축 실험)

- **주의:** F0가 **push Heartbeat** 방식이면 RTT 값이 없다. 이 경우 `EWMA / EW variance of heartbeat inter-arrival`로 다음 heartbeat 지연 허용치를 추정하는 알고리즘을 선택한다. RTT 기반 Jacobson estimator는 별도 **request/response 관측** 실험에서만 사용.
- 예시 통계: heartbeat 수신 간격 `dt_i = t_i - t_(i-1)`에 대해 moving average `mu`, deviation estimate `sigma`, 그리고 `deadline = clamp(mu + k*sigma + grace, minDeadline, maxDeadline)`를 사용하되 실제 공식과 초기값·단위·표본 조건을 문서화한다.
- 사전 설정 변수: `warmupSamples`, `ewmaAlpha`, `sigmaMultiplier`, `graceMs`, `min/maxDeadline`, `suspectConfirmMs` 등. sample starvation, jitter burst, stale heartbeat, restart 처리.
- **같은 B/C를 쓰는 F0/F1/F4를 비교**: normal/crash/gradual/regime/bursty에서 fixed deadline과 통계 기반 적응 판단의 효과 분석.
- 테스트: 일정 heartbeat 주기, 갑작스런 변동, 0 sample/cold start, 부정확한 시간 단위, NaN/Inf, monotonic violation, crash offset 변화, 재시작.

### Gate 3: F5 — Ring/Neighbor 관측 (B 축 실험)

- F0의 Fixed Deadline 및 C=None을 **그대로 유지**하고 B의 peer selection / observation coverage를 변경한다.
- membership view를 바탕으로 각 노드가 successor 또는 configurable `k` neighbors를 감시. deterministic ring order, node join/leave/restart 시 view 업데이트, 장애 노드의 monitoring owner 재선출 정책을 명세한다.
- ring에서 관측되지 않는 peer의 상태를 **단순 미수신으로 DEAD 판정하지 않는다**. 주기적 membership view 및 전파 이벤트로 최종 노드 상태를 공유한다.
- All-to-all(F0)와 Ring(F5)의 **메시지 비용·coverage·전파 지연**을 나눠 비교. 기본값에서 통신 예산이 당연히 다르므로 '동일 topology 효과만'이라고 단정하지 않는다.
- 테스트: 3/5/8 노드, successor crash, 2개 동시 crash, ring wrap-around, restart, 노드 순서 변경, partition에서 분단된 view, membership propagation.

### Gate 4: F6 — Verify Suspect 확인 단계 (C 축 실험)

- F5와 A/B 설정을 동일하게 유지하고 C에 추가 확인 정책만 넣는다.
- 의심한 노드를 DEAD로 확정하기 전 추가 heartbeat 요청 또는 **다른 observer에게 재확인 요청**. 어떤 근거가 '독립 관측'인지, timeout·횟수·grace를 선언.
- 확인 메시지로써 **원본 observer가 다른 경로로 보낸 자기 신호를 독립적인 두 번째 관측자로 세지 않는다**.
- 확인 실패가 곧 프로세스 중단의 확정 증거라는 주장 금지. observer-별 unreachable vs global processAlive 구분.
- 테스트: 정상 jitter, observer pause, 1 observer packet loss, 상대 프로세스 crash, partition 비대칭, duplicate gossip, grace 초과, additional confirm bytes.

### Gate 5: F7 — Lifeguard 세부 Ablation (C 축 실험)

- F2(F3 OFF) ↔ F3(Lifeguard all ON)을 **공통 B와 SWIM 타임아웃 기본값**으로 비교.
- 코드에서 실제 존재하는 LHA-Probe, LHA-Suspicion, Buddy 기능이 있다면 플래그로 별도 ON/OFF; 기존 상태 전이 및 event trace가 유지되는지 검증.
- 변경하지 않은 설정이 gossip fanout, peer selection, probe 주기를 우발적으로 바꾸지 않는지 확인.
- observer CPU/process pause, target pause, burst loss, jitter, partition에서 false SUSPECT/DEAD와 crash DEAD p95 비교.
- Lifeguard는 SWIM용 기법이므로 모든 A/B에 일반적으로 적용하지 않는다.

### Gate 6: 실제 UDP/Docker/netem 연동 (선행 Gate 합격 시)

- 기존 3-node UDP smoke runner를 우선 재사용하고, **5/8-node** 반복, loss/jitter/partition, process crash/restart, observer pause, network disruption, clean-up test를 확장.
- 기존 `cache-server`의 Netty TCP/peer listener와 SWIM UDP listener를 구분하고 단일 Wire 규격 여부를 실제 코드에서 확인.
- `tc netem`은 실험 전용 컨테이너 network namespace에서만 사용. `CAP_NET_ADMIN`은 최소한; host qdisc·실서비스 네트워크 수정 금지.
- **SIGSTOP / docker pause는 GC pause나 SSD stall 자체가 아니다.** CPU, GC, SSD/GET·SET hang은 개별 fault type·label로 구분.
- 프로세스별 mono clock 비교시 cross-process `nanoTime` 직접 subtraction 금지. 같은 observer에서 감지하는 이벤트 혹은 별도 동기화 가능한 source-of-truth를 사용.
- Docker/권한이 없으면 `SKIPPED_ENVIRONMENT`와 누락된 검증 조건을 기록하고 P0~P2까지 보고.

## 7. 호환성 매트릭스와 구성 검증

**다음 표는 profile validation의 최소 규칙이다.**

| A Policy | 필수 B Capability | 허용 C | 금지/주의 |
|---|---|---|---|
| Fixed Deadline | PERIODIC_HEARTBEAT | NONE, 별도 Verify(관측 경로 필요) | 한 번도 감시하지 않은 peer를 만료시켜 DEAD 금지 |
| Phi Accrual | MONOTONIC_INTERARRIVAL_SERIES | NONE | 불규칙한 SWIM random peer PING을 분포 검증 없이 Phi sample로 입력 금지 |
| Adaptive Inter-arrival Timeout | MONOTONIC_INTERARRIVAL_SERIES | NONE | push heartbeat인데 RTT 신호가 있다고 가정하지 않음 |
| SWIM Suspicion | DIRECT_INDIRECT_PROBE + MEMBERSHIP_INCARNATION | NONE, LIFEGUARD | PING-REQ 없이 indirect probe 성공처럼 보고 금지 |
| Ring + Deadline | RING_MONITOR_ASSIGNMENT + HEARTBEAT + VIEW_UPDATE | NONE, VERIFY_SUSPECT | ring에서 관측자별 감시 대상 외 peer를 무조건 DEAD 판정 금지 |

- Configuration manifest에 `modules.decision`, `modules.observation/topology/dissemination`, `modules.correction` 및 세부 설정을 기록.
- 호환성 위반 시 시작 전에 explicit validation error와 이유 출력. **의도하지 않은 silent fallback 금지.**
- 잠재 Hybrid는 별도 experiment flag와 별도 연구 가설·테스트·성능 비교 계획이 승인될 때만 구현한다.

## 8. 실험 설계 및 통계 (Gate 2~6 공통)

### 대조 실험 세트

1. **A-only:** F0 vs F1 vs F4. 동일 Heartbeat·전파·C=None 및 비교 가능한 입력 샘플 계약 유지. Phi의 직접 DEAD 매핑처럼 의미가 다른 state는 별도 표기.
2. **B-only:** F0 vs F5. A=Fixed/C=None 유지. 감시할 peer 수와 coverage 차이를 함께 보고; 필요 시 k ring sweep.
3. **C-only:** F5 vs F6, F2 vs F3, 그리고 F7 세부 옵션. 타이머·메시지·추가 확인 비용을 함께 보고.
4. **Interaction:** (A×B)/(B×C) 효과가 단순히 합해진다고 가정하지 말고, 필요성이 확인된 호환 조합만 추가.

### 공통 Fault와 메트릭

- 기존 12개 시나리오: normal, crash, crash-under-jitter, packet-loss, packet-loss-burst, latency, gradual, bursty, regime, observer-pause, target-pause, partition.
- 신규 추가가 필요한 경우: restart/incarnation, multi-crash, ring partition, asymmetric loss, long GC pause, service stall 등을 **별도 scenarioId로 저장**하고 기존 12개 정의를 바꾸지 않는다.
- **감지 지연:** `firstSuspectAt - faultStartAt`, `firstDeadAt - faultStartAt`. 이벤트가 존재하지 않으면 `null`, `detected=false`, `censored=true`를 유지.
- **오탐:** `falseSuspectCount`, `falseSuspectDurationMs`, `falseDeadCount`, `falseDeadDurationMs`, `recoveryFlaps`. fail-stop 관점(processAlive)과 observer 관점(reachable) 및 서비스 관점(serviceHealthy)을 별도 집계.
- **비용:** protocol frames/s, encoded payload bytes/s, model-estimated frame bytes/s, per-observer/cluster traffic, TCP connection count, extra confirmation messages, CPU time 가능 시.
- **수치 출처:** `simulation-observed`(이벤트 시간), `model-estimated`(모델 비용), `real-udp-observed`(실제 인코딩 패킷 및 동일 observer의 시간)로 분류. 서로 섞어 평균 금지.
- **분포:** seed별 raw data, N/미검출 N, p50/p95/p99, 평균, 변동성(CI 또는 부트스트랩; 어려우면 결측·표본 작음 명시). censored latency를 0 또는 실험 duration으로 대입해 평균 내지 않는다.
- **실험 설정:** `runId`, profileId, A/B/C 구현·버전·설정, scenarioId, seed, topology, nodeCount, fault onset/offset, traffic comparison mode, JDK/OS/revision, trial status.
- **sample size:** Gate 2 smoke와 정식 성능 실험을 분리. 정식 결과는 5/25/100-node *가능한* 최소 30 seeds/condition을 기본 목표로 하되 자원·실행 시간 제약 시 수행 범위와 부족한 근거를 공개. 실제 UDP는 3-node smoke와 5/8-node 반복 검증을 별도 배치.

### 필수 결과 파일 (기존 구현 경로 우선)

```text
results/<new-run-id>/
  run-manifest.json         # 시작 시각, 버전, profile 구성, scenario, seed, 환경
  events.jsonl              # fault/probe/heartbeat/timeout/state transition/health
  runs.csv                  # run-level + observer-target granularity 선언
  summary.csv               # profile × scenario × topology × metric 별 통계
  module-contributions.csv  # 비교쌍(F0/F1/F4, F0/F5, F5/F6, F2/F3/F7)의 차이
  compatibility.csv         # 허용/거절 조합과 구체적 이유
  report.md                 # 해석, 불확실성, 유효하지 않은 비교, 남은 검증
  plots/                    # 출처·N·조건·비교 모드 라벨
```

- 파일 schema는 기존 도구와 호환되도록 가능한 한 확장하고 임의 재명명 금지.
- 출력 디렉터리가 이미 존재하면 **오류 반환/새 runId 생성**하고 덮어쓰기 금지. 원본 11,160-run과 80-run smoke 결과는 유지.
- 기존 `model-estimated`와 실제 `real-udp-observed`를 chart에 함께 표시할 때 서로 다른 단위/시리즈로 식별한다.

## 9. JUnit 및 재현 테스트 (반드시 실제 실행)

| 테스트 영역 | 최소 검증 항목 |
|---|---|
| Audit regression | F0~F3 기존 상태 전이/gossip/CSV schema 회귀, 기존 전체 테스트 |
| Clock/scheduler | direct/indirect timeout 경과, fault onset offset, 동일 타임스탬프 우선순위, 0ms artifact 재발 방지 |
| A Policy | deadline warmup/recovery, Phi cold start, adaptive sample 부족/NaN/Inf/monotonic, restart |
| B Observation | ring wrap-around/coverage, multi-crash, indirect ACK correlation, duplicate/out-of-order gossip, view update |
| C Correction | observer pause 보정, recheck timeout, 독립 증거 중복 방지, LHA feature toggle |
| Compatibility | 유효 프로필 로드, 잘못된 Phi+random PING 거절, 미지원 Lifeguard 조합 거절, silent fallback 없음 |
| Reproducibility | 같은 seed/config 결과 동일, fault/network/protocol PRNG 분리, concurrent run isolation |
| Metrics | false vs unreachable 구분, censored latency, `source` label, bytes/message 집계 |
| Docker/netem | 실행/cleanup, capability 제한, 시나리오별 실측 trace; 권한 없으면 SKIPPED 명시 |

**매 Gate에서 실제 실행한 명령**을 `verification.md` 등에 그대로 기록. 실패한 기존 테스트는 기존 실패인지 회귀인지 확인. CI에는 빠른 단위 테스트만 넣고 긴 배치/네트워크 실험은 명시적 명령으로 분리한다.

## 10. Gate별 Definition of Done 및 중단 조건

| Gate | 완료 조건 | 다음 단계 진행 조건 |
|---|---|---|
| **0 Audit** | 실제 repository 구조, 기존 코드/실험 파일/명령/테스트 현황 정리 | 이전 결과 보존 확인 |
| **1 Modularization** | 계약·Capabilities·Profile validation·F0~F3 회귀 테스트 | 기존 4개 프로필 동작/이벤트 의미 유지 |
| **2 Baseline & A** | 보정 모델 F0~F3 반복, F4 인터벌 적응형 판단 + A-only 비교 | 의미·통계·메시지 비용 라벨 검증 |
| **3 B** | F5 ring 관측 및 coverage/비용 분석, multi-crash 테스트 | B만 달라졌음을 입증하고 coverage 표시 |
| **4 C** | F6 Verify Suspect 및 F7 LHA ablation, 효과와 추가 비용 비교 | C 이득과 탐지 지연 증가 모두 측정 |
| **5 Experiment** | 공통 scenario/seed 재실험·module-contributions.csv·report.md | 미검출/오탐/제약 명확히 기술 |
| **6 Real UDP** | 기존 runner 사용 5/8-node 반복 smoke/단계별 실험 | Docker 권한/자원 확보 시에만, 아니면 SKIPPED |

### Codex의 각 Gate 종료 응답 형식

```text
[Gate 번호/작업명]
- 실제 확인한 코드·파일 경로:
- 수정/추가 파일:
- 구현 내용(A/B/C 중 해당 위치와 입력·출력 계약):
- 실제 실행한 명령과 결과(PASS/FAIL/SKIPPED):
- 생성된 결과 파일 경로와 표본 수:
- 기존 결과/CI 보존 여부:
- 유효하지 않거나 거절된 조합:
- 남은 위험과 다음 단계 준비 상태:
```

**절대 금지:** 원본 결과 삭제/수정, 기존 사용자 변경 덮어쓰기, 커밋·push 무단 실행, 사용자 승인 없이 대규모 EC2/클라우드 자원 생성, 호스트 네트워크 장애 주입, 데이터 파괴, 없는 테스트를 성공 처리, 미실행 성능표 작성, 서비스 health와 process alive 혼동.

## 11. 보고서에 넣을 최종 해석 형태

- **A에 대한 결론:** 어떤 판단 정책이 같은 생존 관측으로 더 낮은 false DEAD를 제공하며 crash detection p95에 어떤 비용을 내는가?
- **B에 대한 결론:** 같은 판단 조건에서 Ring·Random·All-to-all의 통신 비용과 장애 커버리지/멤버십 전파 지연은 어떤가?
- **C에 대한 결론:** Lifeguard·Verify Suspect가 어떤 observer pause/loss 시나리오에서 오탐을 낮추며 감지 시간을 얼마나 지연시키는가?
- **조합에 대한 결론:** 각 A/B/C 축의 개선이 결합 후에도 유지되는가? 유의미한 interaction은 무엇인가?
- **선정 기준:** OpenKache에 필요한 최대 crash detection p95, 허용 false DEAD, 노드당 트래픽·CPU, 복구 정책과의 책임 경계를 멘토/팀과 확정한 뒤 요구사항 충족 조합을 선정. **절대적 최고라고 주장하지 않음.**

## 12. 연구 근거 (실제 문헌·운영 구현)

- [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol](https://research.google/pubs/swim-scalable-weakly-consistent-infection-style-process-group-membership-protocol/)
- [Lifeguard: Local Health Awareness for More Accurate Failure Detection](https://ieeexplore.ieee.org/document/8416202/)
- [The Phi Accrual Failure Detector](https://doi.org/10.1109/RELDIS.2004.1353004)
- [JGroups documentation: FD, FD_ALL, FD_SOCK, VERIFY_SUSPECT](https://javagroups.sourceforge.net/manual4/index.html)
- [Apache Geode failure detection](https://geode.apache.org/docs/guide/114/managing/network_partitioning/failure_detection.html)
- [Apache Ignite 3 configuration](https://ignite.apache.org/docs/ignite3/latest/administrators-guide/config/node-config)
- [Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)
- [Couchbase Automatic Failover](https://docs.couchbase.com/server/current/learn/clusters-and-availability/automatic-failover.html)
- [On the Quality of Service of Failure Detectors](https://citeseerx.ist.psu.edu/document?doi=2b7f05e348c1133d7b62693ee1f674a3c6151b6c)

---

**최종 주의:** 이 문서는 Codex가 수행할 일의 명세서이며 **F4~F7이 이미 구현되었다는 보고서가 아니다.** 실제 구현 상태·테스트 결과·측정 지표는 저장소를 조사하고 코드를 실행한 뒤에만 갱신한다.
