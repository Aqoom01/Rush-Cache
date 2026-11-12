# OpenKache Failure Detector — SWIM 미검출 근본 원인과 재실험

## 1. 결론

기존 SWIM Base, LHA-Probe, Buddy의 25·100노드 DEAD 미검출은 SWIM이
Crash를 찾지 못해서가 아니었다. Lifeguard confirmation을 끈 ablation이
Lifeguard의 `0 confirmations = maximum timeout` 규칙은 그대로 사용한 구현 의미
오류가 1차 원인이었고, 그 결과 만들어진 DEAD deadline이 90초 simulation
horizon 뒤에 위치한 것이 직접적인 미관측 원인이었다.

- 구현 로직 오류: **있음**. confirmation 비활성 profile이 Lifeguard 최대
  suspicion timeout을 영구적으로 사용했다.
- 파라미터 설정 문제: **있음**. Base에 `suspicionMaxTimeoutMultiplier=6`의
  max-to-min 구간을 유지하면서 deadline을 단축할 confirmation 경로는 껐다.
- 관측 시간 부족: **2차 원인**. 잘못 길어진 25·100노드 deadline이 90초를
  넘었기 때문에 결과가 right-censored 됐다.
- timer·상태 전이 오류: **없음**. 5노드에서 같은 timer가 정확히 DEAD를
  발생시켰고, 25·100노드도 horizon을 늘리면 예정 시각에 실행되는 구조였다.
- 반복 SUSPECT에 의한 무한 연장: **없음**. 동일 reporter는 confirmation set에
  중복 추가되지 않고 기존 deadline도 증가하지 않았다.
- 이벤트 기록·집계 오류: **없음**. `events.jsonl`의 상태 전이와 `runs.csv`의
  `detected_dead`, `false_negative_events`가 일치했다.

수정 후 같은 90초 horizon에서 5개 SWIM 조합이 5·25·100노드 Crash 30 seeds
전부 DEAD로 전이했다. horizon을 늘리거나 timeout 상수를 임의로 낮추지 않았다.

## 2. 코드 경로와 기존 계산

### Probe와 상태 전이

- `ExperimentRunner.scheduleSwimProbe`: 1초마다 Direct PING을 시작한다.
- Direct ACK가 500ms 안에 없으면 `DIRECT_TIMEOUT`을 기록한다.
- `scheduleIndirectProbe`: PING-REQ 3개를 시작하고 추가 500ms를 기다린다.
- Direct와 Indirect가 모두 실패하면 `recordProbe(..., false, ...)`가
  `ALIVE -> SUSPECT`를 발생시킨다.
- `SwimLifeguardDetector.tick`: 현재 시각이 suspicion deadline 이상이면
  `SUSPECT -> DEAD`를 발생시킨다.
- Crash 기본 horizon은 90초이고 fault는 20초에 시작한다. seed별
  0/137/491/809ms offset을 적용한다.

따라서 seed 1의 첫 상태 시각은 다음과 같다.

| 이벤트 | 시각 |
|---|---:|
| Crash | 20.000s |
| Direct Timeout | 20.500s |
| Indirect Timeout / 최초 SUSPECT | 21.000s |
| Simulation 종료 | 90.000s |

### Suspicion 공식

기존 공식은 다음과 같다.

```text
minimum = suspicionMultiplier(4)
        × ln(clusterSize + 1)
        × probeInterval(1s)
        × LHA suspicion awareness multiplier

maximum = minimum × suspicionMaxTimeoutMultiplier(6)
```

Lifeguard confirmation이 0개이면 `maximum`, 3개이면 `minimum`을 사용한다.
문제는 SWIM Base, LHA-Probe, Buddy가 confirmation을 비활성화했지만 초기
timeout을 `suspicionTimeoutNanos(0)`으로 계산해 영원히 maximum에 머문 것이다.

| 노드 | Base minimum | 기존 Base timeout(maximum) | SUSPECT 시각 | 기존 DEAD 예정 | 90초 내 실행 |
|---:|---:|---:|---:|---:|---:|
| 5 | 7.167s | 43.002s | 21.000s | 64.002s | 예 |
| 25 | 13.032s | 78.194s | 21.000s | 99.194s | 아니오 |
| 100 | 18.460s | 110.763s | 21.000s | 131.763s | 아니오 |

이 계산은 수정 전 재현 결과
`results-swim-miss-repro-20261028/events.jsonl`의
`suspicionDeadlineNanos`와 일치한다.

### LHA-Suspicion만 DEAD에 도달한 이유

LHA-Suspicion과 전체 Lifeguard는 confirmation을 활성화한다. 시뮬레이터는
Crash 또는 partition에서 첫 Indirect Timeout 직후 서로 다른 reporter 3개의
confirmation을 제공한다. 이 때문에 초기 maximum deadline이 minimum으로
단축된다.

첫 실패는 awareness score를 1로 올리므로 LHA-Suspicion minimum은 Base의
2배가 된다.

```text
LHA-Suspicion timeout = 4 × ln(N + 1) × 1s × (awarenessScore 1 + 1)
```

따라서 기존 결과에서 LHA-Suspicion이 Base보다 본질적으로 우수해서
DEAD에 도달한 것이 아니다. Base는 잘못 maximum에 고정됐고,
LHA-Suspicion은 confirmation으로 maximum을 줄일 수 있었기 때문이다.
또한 confirmation 3개를 같은 simulation 시각에 제공하는 것은 실제 gossip
전파 지연을 생략한 모델 단순화이므로 별도의 실제 네트워크 검증이 필요하다.

## 3. 수정 내용

### Detector 의미 수정

`SwimLifeguardDetector.suspicionTimeoutNanos`에서
`suspicionConfirmationsEnabled == false`이면 fixed `minimum`을 반환한다.

```text
변경 전: confirmation 비활성 + confirmations=0 -> maximum
변경 후: confirmation 비활성 -> fixed minimum
```

이는 수치를 좋게 만들기 위한 timeout 축소가 아니다. Vanilla SWIM의 fixed
suspicion timeout과 Lifeguard의 confirmation 기반 max-to-min 구간을 분리한
것이다. confirmation이 활성화된 전체 Lifeguard와 LHA-Suspicion의 계산식은
변경하지 않았다.

### 추적·집계 보강

- `SUSPICION_DEADLINE_SCHEDULED` event 추가
  - initial/final deadline
  - SUSPECT 이후 timeout
  - simulation 종료 시각
  - horizon 내 실행 여부
  - confirmation/LHA-Suspicion 활성 여부
- `SIMULATION_ENDED` event 추가
- `summary.csv`에 `dead_detection_rate` ratio 추가
- 기존 미검출은 계속 null과 `missing_n`으로 기록한다.

### 자동화 테스트

- 성공 probe가 계속되는 정상 peer는 ALIVE를 유지
- 5/25/100노드 Base fixed minimum 공식
- deadline 직전 SUSPECT, deadline 시각 DEAD
- 반복 SUSPECT가 deadline을 늘리지 않음
- 새 incarnation refutation 뒤 과거 deadline이 DEAD를 만들지 않음
- 5개 SWIM 조합 × 5/25/100노드 Crash에서 DEAD 도달

## 4. 재실험

### 실행 명령

수정 전 재현:

```bash
./gradlew -p failure-detector-lab run --args='--seeds 1 --nodes 5,25,100 --profiles swim-base,swim-lifeguard,swim-lha-probe,swim-lha-suspicion,swim-buddy --scenario crash --output-dir results-swim-miss-repro-20261028'
```

수정 후 smoke:

```bash
./gradlew -p failure-detector-lab run --args='--seeds 1 --nodes 5,25,100 --profiles swim-base,swim-lifeguard,swim-lha-probe,swim-lha-suspicion,swim-buddy --scenario crash --output-dir results-swim-fix-smoke-20261028'
```

전체 공정 재실험:

```bash
./gradlew -p failure-detector-lab run --args='--seeds 30 --nodes 5,25,100 --output-dir results-swim-root-cause-formal-20261028'
```

전체 재실험은 10 profiles × 12 scenarios × 30 seeds × 3 node sizes =
10,800 runs다. 기존과 같은 90초 Crash horizon을 포함한 scenario 설정을
사용했다.

## 5. 변경 전후 Crash 결과

표의 값은 DEAD p95이고 괄호는 30회 중 미검출 수다.

| 방법론 | 5노드 변경 전 → 후 | 25노드 변경 전 → 후 | 100노드 변경 전 → 후 |
|---|---:|---:|---:|
| SWIM Base | 44.865s (0) → **9.030s (0)** | 미검출 (30) → **14.895s (0)** | 미검출 (30) → **20.323s (0)** |
| SWIM + Lifeguard | 16.197s (0) → 16.197s (0) | 27.928s (0) → 27.928s (0) | 38.784s (0) → 38.784s (0) |
| SWIM + LHA-Probe | 44.865s (0) → **9.030s (0)** | 미검출 (30) → **14.895s (0)** | 미검출 (30) → **20.323s (0)** |
| SWIM + LHA-Suspicion | 16.197s (0) → 16.197s (0) | 27.928s (0) → 27.928s (0) | 38.784s (0) → 38.784s (0) |
| SWIM + Buddy | 44.865s (0) → **9.030s (0)** | 미검출 (30) → **14.895s (0)** | 미검출 (30) → **20.323s (0)** |

수정 후 다섯 방법론 모두 5·25·100노드 DEAD detection rate가 1.0이다.
정상 상태 100노드 모델 통신 비용은 모두 130.844 B/node/s로 변경 전과 같다.

### 필수 탐지 지표

아래 값은 30 seeds에서 `events.jsonl`의 상태 전이 시각으로 산출한
**simulation-observed** 값이다. 다섯 조합의 최초 SUSPECT p50/p95/p99는 모든
노드 크기에서 각각 1.350s / 1.863s / 1.863s였다.

| 조합 | 노드 | DEAD p50 | DEAD p95 | DEAD p99 | DEAD Detection Rate | 미관측 |
|---|---:|---:|---:|---:|---:|---:|
| SWIM Base / LHA-Probe / Buddy | 5 | 8.517s | 9.030s | 9.030s | 1.000 | 0/30 |
| SWIM Base / LHA-Probe / Buddy | 25 | 14.382s | 14.895s | 14.895s | 1.000 | 0/30 |
| SWIM Base / LHA-Probe / Buddy | 100 | 19.810s | 20.323s | 20.323s | 1.000 | 0/30 |
| Lifeguard / LHA-Suspicion | 5 | 15.684s | 16.197s | 16.197s | 1.000 | 0/30 |
| Lifeguard / LHA-Suspicion | 25 | 27.415s | 27.928s | 27.928s | 1.000 | 0/30 |
| Lifeguard / LHA-Suspicion | 100 | 38.271s | 38.784s | 38.784s | 1.000 | 0/30 |

Crash에서는 모든 조합의 false SUSPECT와 false DEAD가 0이었다. p50/p95/p99는
DEAD가 관측된 표본만 대상으로 계산하며, `summary.csv`의 `n`과 `missing_n`을
반드시 함께 읽는다. 이번 수정 후에는 30/30이 관측되어 이 표에 생존분석상의
검열 표본은 없다. 수정 전 25·100노드 Base/LHA-Probe/Buddy는 `n=0`,
`missing_n=30`이므로 성공 표본의 p95가 아니라 **미관측**으로 표시했다.

### 통신 비용의 해석

다음은 Crash 실행의 평균값이다. 이벤트 시각, 탐지율, false event는
**simulation-observed**이고, 메시지·바이트 비용은 시뮬레이터가 protocol
frame 수와 설정된 frame 크기로 계산한 **model-estimated** 값이다. 따라서
아래 비용은 실제 UDP packet capture나 OS/JVM 오버헤드의 실측값이 아니다.

| 조합 | 5노드 msg/B per node·s | 25노드 msg/B per node·s | 100노드 msg/B per node·s |
|---|---:|---:|---:|
| SWIM Base | 3.713 / 237.644 | 2.360 / 151.067 | 2.107 / 134.833 |
| SWIM + Lifeguard | 2.057 / 131.622 | 2.029 / 129.862 | 2.024 / 129.532 |
| SWIM + LHA-Probe | 2.057 / 131.622 | 2.029 / 129.862 | 2.024 / 129.532 |
| SWIM + LHA-Suspicion | 3.713 / 237.644 | 2.360 / 151.067 | 2.107 / 134.833 |
| SWIM + Buddy | 3.865 / 247.353 | 2.391 / 153.008 | 2.114 / 135.319 |

전체 원시·집계 행은
`results-swim-root-cause-formal-20261028/runs.csv`, `runs.jsonl`,
`events.jsonl`, `summary.csv`에 보존했다.

## 6. 다른 장애 시나리오

수정 후 SWIM Base의 짧아진 fixed suspicion은 Crash를 정상적으로 감지하지만,
일시적인 지연과 partition에서는 더 공격적으로 DEAD를 선언한다. 다음 값은
30 seeds의 false DEAD event 평균이다.

| 시나리오 | 노드 | SWIM Base | SWIM + Lifeguard | LHA-Suspicion |
|---|---:|---:|---:|---:|
| 고정 지연 | 5 | 1.000 | **0.000** | **0.000** |
| 고정 지연 | 25 | 1.000 | **0.000** | **0.000** |
| 점진 지연 | 5/25/100 | 1.000 | **0.000** | **0.000** |
| Partition | 5 | 1.000 | **0.000** | **0.000** |
| Partition | 25 | 1.000 | **0.000** | **0.000** |
| Partition | 100 | 0.000 | 0.000 | 0.000 |

- Normal: 모든 SWIM 조합 false SUSPECT/DEAD 0.
- Packet loss: n=100 false SUSPECT 평균 0.4, false DEAD 0.
- Burst loss: false SUSPECT 평균 3.233, false DEAD 0.
- Observer pause: 모든 SWIM 조합 false SUSPECT/DEAD 0.
- Target pause: false SUSPECT 1.0, false DEAD 0.

따라서 현재 simulation에서 Lifeguard의 확인된 이점은 Crash 가속이 아니라
LHA-Suspicion이 일시 장애를 DEAD로 확정하지 않도록 시간을 벌어주는 것이다.
false SUSPECT 발생 횟수 자체는 줄이지 못했다.

## 7. 기존 방법론과 재비교

| 방법론 | 5노드 Crash p95 | 25노드 | 100노드 |
|---|---:|---:|---:|
| Fixed Deadline + All-to-all | 4.963s | 4.963s | 4.963s |
| Adaptive Deadline + All-to-all | 4.963s | 4.963s | 4.963s |
| Fixed Deadline + Ring | 4.963s | 4.963s | 4.963s |
| Fixed Deadline + Ring + Verify | 4.363s | 4.363s | 4.363s |
| Phi Accrual + Gossip | 19.263s | 19.263s | 19.263s |
| SWIM Base | 9.030s | 14.895s | 20.323s |
| SWIM + Lifeguard | 16.197s | 27.928s | 38.784s |

Ring 결론은 유지된다. 수정은 SWIM fixed suspicion 의미만 바꿨고 전체 재실험에서
100노드 normal 모델 비용은 All-to-all 6,476.800 B/node/s, Ring
65.422 B/node/s이며 단일 Crash p95는 둘 다 4.963s였다.

다만 Ring 결과도 아직 단일 target, 결정적 successor 경로 중심이고 다중 Crash,
membership churn, 반복 UDP 통계는 검증하지 않았다.

## 8. 최종 판단

1. **SWIM Base는 정상적으로 작동하는가?**
   수정 후 모든 Crash 조건에서 SUSPECT와 DEAD로 전이한다. 반복 SUSPECT,
   incarnation refutation, deadline 경계 테스트도 통과했다.
2. **Lifeguard의 개선 효과가 확인되는가?**
   Crash 속도 개선은 확인되지 않았다. 오히려 awareness 보정 때문에 Base보다
   느리다. 하지만 고정·점진 지연과 5·25노드 partition에서 false DEAD를
   1.0에서 0으로 줄였다. 이 효과는 LHA-Suspicion에 집중되어 있다.
3. **Ring Heartbeat 우세 결론은 유지되는가?**
   현재 단일 Crash simulation에서는 유지된다. 동일 4.963s p95에 모델 비용이
   약 99배 낮다. 실제 운영 우세는 아직 주장하지 않는다.
4. **어떤 설정에서 무엇이 유리한가?**
   빠른 Crash 판정은 Fixed/Ring 계열, 낮은 SWIM 오판은 LHA-Suspicion,
   통계적으로 보수적인 판단은 Phi가 유리했다. 최종 선택은 OpenKache SLO가
   필요하다.
5. **추가 검증은?**
   confirmation 전파 지연을 포함한 실제 SWIM UDP 반복, CPU/GC pause,
   다중 장애, Ring 재구성, 실제 GET/SET 복구 시간이 남아 있다.

## 9. 검증 결과와 산출물

- `./gradlew -p failure-detector-lab test selfTest --rerun-tasks`: JUnit 52 tests,
  failures 0, errors 0; self-test 통과.
- `./gradlew build --rerun-tasks`: 루트 OpenKache 20 tasks 통과.
- `results-swim-miss-repro-20261028/`: 수정 전 15-run 재현.
- `results-swim-fix-smoke-20261028/`: 수정 후 15-run smoke.
- `results-swim-root-cause-formal-20261028/`: 수정 후 전체 10,800 runs.
- 전체 결과 `run-manifest.json`, `runs.jsonl`, `events.jsonl` parse 통과.
- 전체 결과 행 수:
  - `runs.csv`: 10,801
  - `summary.csv`: 3,961
  - `runs.jsonl`: 10,800
  - `events.jsonl`: 1,433,197
  - `module-contributions.csv`: 51,841
  - `compatibility.csv`: 97

## 10. 아직 검증하지 못한 부분

- Lifeguard confirmation 3개를 실제 gossip 지연과 loss를 거쳐 받는 UDP 실험
- SWIM Base/Lifeguard의 5·8노드 반복 UDP 통계
- 실제 CPU saturation, JVM GC pause, event-loop backlog
- OpenKache `cache-server` 통합과 GET/SET 복구 시간
- 실제 wire encoding byte 및 CPU/heap 비용
- 다중 Crash, node join/leave, membership view 수렴
- 자동 failover·replica 승격은 이번 작업 범위 밖이다.
