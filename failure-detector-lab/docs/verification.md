# v2 검증 기록 (2026-10-27)

아래에서 “통과”는 이 작업에서 실제 명령을 실행해 종료 코드와 산출물을 확인한 경우만 뜻한다.

## 운영 제품 기반 5종 검증 (2026-10-30)

- Java runtime: OpenJDK 21.0.12.1 확인. Lab bytecode target은 기존 호환성을 위해 17 유지.
- `./gradlew -p failure-detector-lab test selfTest`: 통과.
- Redis fresh master majority, Ignite coordinator/reconnection/stale view, Aerospike
  heartbeat/agreement 분리 JUnit: 통과.
- 5개 프로필의 8노드 single crash 및 consecutive-3 공통 JUnit: 통과.
- 형식 simulation: 5 profiles × 10 scenarios × 30 seeds = 1,500 runs, 실행 실패 0.
- 최종 Mode A 결과: `results-fd5-native-30seed-20261030-v5/`.
- raw JSONL 1,500개, `runs.csv` 1,500 data rows, summary CSV 5개, PNG 5개 생성 확인.
- Python analyzer는 외부 패키지 없이 실행 및 PNG signature 확인.

single crash의 성공 run p95는 SWIM+Lifeguard 1,000ms, Redis 15,000ms, Phi 21,150ms,
Ignite 4,050ms, Aerospike 1,500ms다. 모두 30/30 감지했다. Redis는 공식 예제
`cluster-node-timeout=15000`, Ignite는 기본 `networkTimeout=5000`, Aerospike는 권장
150ms × 10 intervals를 반영했다. 이 값은
`simulation-observed`이며 network byte는 `model-estimated`이므로 native 제품 성능 순위가
아니다. 자세한 표와 그래프는 결과 디렉터리의 `report/benchmark_report.md`에 있다.

## 코드와 빌드

- `./gradlew -p failure-detector-lab test selfTest --rerun-tasks`: 통과.
  JUnit XML 합계 47 tests, failures 0, errors 0; self-test 성공.
- `./gradlew -p failure-detector-lab test selfTest jar --rerun-tasks`: 통과.
- `sh -n failure-detector-lab/scripts/experiments/run-udp-netem.sh`: 통과.
- `./gradlew build --rerun-tasks`: 통과. 루트 20 tasks 실행.
- `./gradlew -p failure-detector-lab run --args='--help'`: 통과. F0~F7c 목록 확인.

## 형식 simulation

실행 조건은 12 scenarios × 30 seeds × 5/25/100 nodes × 10 profiles × default mode다.

- 실행: 통과, 10,800 runs.
- 위치: `results-modular-v2-formal-20261027-v2/`.
- `runs.csv`: header 포함 10,801행.
- `summary.csv`: header 포함 3,601행.
- `module-contributions.csv`: header 포함 51,841행.
- `compatibility.csv`: header 포함 97행(96 combinations).
- `runs.jsonl`: 10,800행.
- `events.jsonl`: 1,415,734행.
- manifest JSON 및 두 JSONL 전체 parse: 통과.

대표 결과는 다음과 같다. crash는 DEAD p95 ms이며 괄호는 `missing_n`이다.

| 프로필 | n=5 | n=25 | n=100 | normal 비용 n=100 (model B/node/s) |
|---|---:|---:|---:|---:|
| F0 fixed | 4,963 (0) | 4,963 (0) | 4,963 (0) | 6,476.800 |
| F1 phi | 19,263 (0) | 19,263 (0) | 19,263 (0) | 294.400 |
| F2 SWIM base | 44,865.227 (0) | 미검출 (30) | 미검출 (30) | 130.844 |
| F3 Lifeguard | 16,197.076 (0) | 27,927.772 (0) | 38,783.964 (0) | 130.844 |
| F4 adaptive | 4,963 (0) | 4,963 (0) | 4,963 (0) | 6,476.800 |
| F5 ring | 4,963 (0) | 4,963 (0) | 4,963 (0) | 65.422 |
| F6 ring+verify | 4,363 (0) | 4,363 (0) | 4,363 (0) | 65.422 |
| F7a LHA-Probe | 44,865.227 (0) | 미검출 (30) | 미검출 (30) | 130.844 |
| F7b LHA-Suspicion | 16,197.076 (0) | 27,927.772 (0) | 38,783.964 (0) | 130.844 |
| F7c Buddy | 44,865.227 (0) | 미검출 (30) | 미검출 (30) | 130.844 |

이 값은 현재 timeout·suspicion 모델의 결과다. 특히 F2/F7a/F7c의 대규모 미검출은 실험
종료 90초와 suspicion scaling의 상호작용이므로 “알고리즘은 장애를 못 찾는다”로 일반화할
수 없다. OpenKache SLO가 없으므로 최종 우승 프로필도 아직 정하지 않는다.

## 실제 UDP + Docker

ring heartbeat 방향 수정 후 다음 네 명령을 실제 실행했다.

- F5, 5노드 crash: 통과. SUSPECT 708.363ms, DEAD 1,158.377ms.
- F6, 5노드 crash: 통과. SUSPECT 721.654ms, DEAD 971.956ms.
- F5, 8노드 crash: 통과. SUSPECT 655.460ms, DEAD 1,055.769ms.
- F6, 8노드 crash: 통과. SUSPECT 768.647ms, DEAD 1,019.305ms.

모두 `timeSource=observer-monotonic`, `wireSource=real-udp-observed`다. 각 1회 smoke라 평균,
분위수, 우열을 주장하지 않는다. 최초 방향 오류 결과는 `INVALID_RUN.md`와 함께 보존했다.

### Pre-EC2 자원 장애 스모크 (2026-10-30)

8개 독립 JVM/container, 1 CPU·512MiB/container, 128MiB heap에서 F3 SWIM+Lifeguard를
각 1회 실행했다. 통계적 결론이 아니라 주입·수집 경로 검증이다.

| Fault | 실제 주입 증거 | detector 관찰 |
|---|---|---|
| target pause 3초 | pause wall window 3.211초 | SUSPECT 382.976ms, DEAD 없음, release marker 후 ALIVE 43.005ms |
| observer pause 3초 | observer container pause 3.177초 | SUSPECT/DEAD 없음 |
| target CPU contention 3초 | peak CPU 100.52%, cgroup throttled 9,775.271ms | SUSPECT/DEAD 없음 |
| target I/O contention 3초 | cgroup write 4,440,129,536B, peak CPU 100.96% | SUSPECT/DEAD 없음 |

- `./gradlew -p failure-detector-lab test selfTest`: 통과.
- CPU pause 측정 순서를 수정한 뒤 두 번째 8노드 run에서 recovery 계측 확인.
- CPU/HDD `stress-ng` exit 0과 cgroup delta 확인.
- 원시 결과: `results-pre-ec2-smoke-*-20261030*`.
- 집계/그래프: `results-pre-ec2-smoke-report-20261030/`.
- 최초 Docker Desktop 비활성 상태의 실패 시도는 결과로 집계하지 않았다.

startup grace를 3초로 통일한 동일 build에서 5개 networked mechanism profile × target
pause 3초 파일럿도 실행했다. 5/5 run이 완료됐고 모든 run의 `preFaultDeadEvents=0`을
확인했다. 아래는 각 1회 값이라 p95나 우열로 해석하지 않는다.

| Profile | SUSPECT | DEAD | ALIVE recovery |
|---|---:|---:|---:|
| adaptive deadline | 539.549ms | 939.554ms | N/A (same incarnation DEAD policy) |
| fixed deadline | 571.380ms | 971.473ms | N/A (same incarnation DEAD policy) |
| Phi gossip | 미판정 | 미판정 | N/A |
| Ring verify H3/Q3/R3 | 606.063ms | 1,206.574ms | N/A (same incarnation DEAD policy) |
| SWIM+Lifeguard | 253.053ms | 미판정 | fault-end 후 47.970ms |

최종 파일럿과 SVG는 `results-pre-ec2-pilot-5profiles-20261030-v2/report/`에 있다. 최초
파일럿에서 adaptive startup false DEAD를 발견한 결과도 삭제하지 않고 v1 디렉터리에
보존했다.

### 운영 제품 inspired 5종 실제 socket crash 파일럿 (2026-10-30)

동일한 8 container/JVM 환경에서 node-1을 실제 stop하고 각 adapter의 실제 socket 경로를
검증했다. 아래는 각 1회 파일럿이며 p95·순위의 근거가 아니다.

| Profile | 실제 경로 | Crash → DEAD | 사전 DEAD | 관찰창 유효 |
|---|---|---:|---:|---|
| Aerospike mesh inspired | TCP heartbeat + UDP agreement | 1,736.341ms | 0 | 예 |
| SWIM+Lifeguard | UDP probe/gossip | 4,119.302ms | 0 | 예 |
| Ignite TCP ring inspired | persistent TCP heartbeat + UDP control | 4,893.036ms | 0 | 예 |
| Redis Cluster inspired | UDP PING/PONG + PFAIL report | 15,708.250ms | 0 | 예 |
| Cassandra Phi inspired | UDP versioned heartbeat | 20,103.372ms | 0 | 예 |

- 5/5 실행에서 DEAD를 관찰했고 `preFaultDeadEvents=0`이었다.
- Ignite는 모든 생존 노드가 TCP heartbeat를 유지하도록 수정한 v3에서 node-1만 제거되고
  후속 node-2 연쇄 오탐이 없음을 로그로 확인했다.
- 최초 Ignite 16초 실행과 Redis 28초 실행은 detector별 최소 사후 관찰창 미달 사례로
  보존했으며 최종 집계에서 제외했다.
- 집계/CSV/SVG: `results-product-socket-pilot-5detectors-20261030/report/`.
- 수치는 inspired adapter 결과이며 Redis/Cassandra/Ignite/Aerospike native 제품 성능이 아니다.

### 운영 제품 inspired 5종 × 10 faults × 3 seeds 검증 행렬 (2026-10-31)

- 150/150 sequence 실행 완료, 미해결 runner 실패 0, `COMPLETED` marker 생성 확인.
- `execution-status.csv`의 passed run directory만 집계해 재시도 전 불완전 결과가 섞이지 않게 했다.
- 실제 socket 경로: UDP 90회, TCP heartbeat+UDP control 60회.
- fault 이전 DEAD 이벤트는 전체 150회에서 0건이었다.
- detector별 최소 사후 관찰창 기준으로 119회가 통계 유효, 31회가 관찰창 미달이었다.
- 유효/시도: Aerospike 30/30, Ignite 30/30, SWIM 30/30, Redis 22/30,
  Phi 7/30. 미달 결과는 성공·실패 통계에서 제외했다.
- Crash: Aerospike 3/3, Ignite 3/3, SWIM 3/3, Phi 2/2 유효 run 탐지;
  Redis는 2/3 탐지했다. Phi 1회는 관찰창 미달이다.
- Crash DEAD p95(성공 run): Aerospike 1,728.958ms, SWIM 4,140.516ms,
  Ignite 4,868.534ms, Redis 15,997.287ms, Phi 19,022.065ms.
- 45% loss에서 살아 있는 process에 대한 DEAD: Aerospike 3/3, Ignite 2/3,
  SWIM 2/3, Redis 0/3, Phi 0/3.
- CPU contention은 유효 run에서 target peak 약 99.05~105.61%, cgroup throttled p95
  약 32.54~33.66초였고 DEAD 판단은 없었다.
- I/O contention은 유효 run에서 target write p95 약 132.38~136.19MB였고 DEAD 판단은 없었다.
- 자원 장애의 Docker 준비 시간이 recovery 관찰창을 잠식한 원인을 확인해 이후 runner 기본
  observer duration에 15초 setup budget을 추가했다. 기존 31회는 소급해 유효 처리하지 않았다.
- 결과/CSV/SVG: `results-product-pre-ec2-validation-20261030-v2/report/`.

#### 관찰창 미달 재실행 및 최종 병합 (2026-11-01)

- 기존 미달 31회만 동일 profile/fault/seed로 재실행했고 31/31 runner 통과를 확인했다.
- 자원 장애 observer duration에 15초 setup budget, 제품 network fault observer duration에
  60초를 적용했다.
- 최초 결과와 재실행을 `(profile, fault, seed, node_count)`로 중복 제거하고 유효 재실행을
  우선 선택했다.
- 최종 logical run은 150회이며 150/150 모두 detector별 최소 사후 관찰창을 충족했다.
- profile별 유효/시도는 5종 모두 30/30, fault 이전 DEAD는 0건이다.
- Crash 탐지: Aerospike 3/3, Ignite 3/3, Phi 3/3, SWIM 3/3, Redis 3/3.
- Crash DEAD p95(성공 run): Aerospike 1,728.958ms, SWIM 4,140.516ms,
  Ignite 4,868.534ms, Redis 16,091.255ms, Phi 19,356.929ms.
- 45% loss에서 살아 있는 process에 대한 DEAD는 Aerospike 3/3, Ignite 2/3,
  SWIM 2/3, Phi 0/3, Redis 0/3이다.
- 최종 결과/CSV/SVG: `results-product-pre-ec2-final-20261101/report/`.

#### Redis crash 미검출 수정 및 재실험 (2026-11-01)

- 원격 master 보고가 observer의 로컬 PFAIL보다 먼저 도착하면 보고가 저장된 뒤 정족수를
  재평가하지 않는 상태 전이 경쟁 조건을 확인했다.
- `recordFailureReport`와 로컬 PFAIL 전환이 동일한 정족수 판정 함수를 사용하도록 수정하고,
  원격 보고 선도착 순서를 회귀 테스트로 추가했다.
- 동일 Docker 8노드 조건에서 Redis crash seed 1~3을 재실행해 3/3 runner 통과,
  3/3 FAIL 탐지, fault 이전 DEAD 0건을 확인했다.
- DEAD latency는 seed 1 15,966.211ms, seed 2 15,480.387ms,
  seed 3 16,035.502ms이며 p95는 16,028.573ms다.
- 수정 전 Redis crash 2/3은 제품 특성이 아니라 inspired adapter 결함이 포함된 결과이므로
  최종 병합에서 수정 후 결과로 대체했다.
- 재실험 원본: `results-product-redis-crash-racefix-20261101/`.

#### Redis 전체 10 faults 재실행 및 최종 교체 (2026-11-01)

- 수정 전 코드와 수정 후 코드가 최종 150회에 섞이지 않도록 Redis의 10 faults × 3 seeds,
  총 30회를 수정 코드로 전부 재실행했다.
- 30/30 runner 통과, 30/30 최소 사후 관찰창 충족, fault 이전 DEAD 0건이다.
- Crash는 3/3 FAIL 탐지했고 DEAD latency는 seed 1 15,987.643ms,
  seed 2 16,097.985ms, seed 3 16,030.687ms이며 p95는 16,091.255ms다.
- loss15, loss45, delay, target/observer pause, target/observer throttle,
  CPU contention, I/O contention에서는 살아 있는 process에 대한 DEAD가 모두 0/3이다.
- 최종 병합의 Redis 30개 logical run이 모두
  `results-product-redis-all-racefix-20261101/`의 수정 후 실행을 가리키는지 확인했다.
- 전체 재실행 원본/보고서: `results-product-redis-all-racefix-20261101/`.

#### 무장애 정상 상태 baseline: 3-seed smoke 및 30-seed 확장 (2026-11-01)

- `normal` 시나리오는 fault marker와 장애 주입 없이 8노드 실제 socket 구성을 60초간
  관찰하며, 이때 발생한 SUSPECT/DEAD를 false positive로 집계한다.
- 5 profiles × 3 seeds = 15회 모두 runner 통과, 15/15 최소 60초 관찰창을 충족했다.
- 5종 모두 false SUSPECT run 0/3, false DEAD run 0/3이며 전체 이벤트 수도 각각 0건이다.
- 최초 실행 중 Docker Desktop Linux Engine 종료로 9회가 즉시 실패했으나 detector 실행
  결과로 집계하지 않았다. Engine 재시작 후 checkpoint resume로 해당 9회만 재실행했고,
  분석기는 `execution-status.csv`의 passed directory만 사용했다.
- 정상 baseline 병합 후 최종 logical run은 165회이며 165/165 유효,
  fault 이전 SUSPECT 0건, fault 이전 DEAD 0건이다.
- 정상 baseline 원본/보고서: `results-product-normal-baseline-3seed-20261101/`.
- 갱신된 최종 결과/CSV/SVG: `results-product-pre-ec2-final-20261101/report/`.
- 이어서 5 profiles × 30 seeds = 150회를 실행했고 150/150 runner 통과,
  150/150 최소 60초 관찰창 충족을 확인했다.
- profile별 false SUSPECT와 false DEAD는 모두 0/30이다. Wilson 95% 신뢰구간은
  각각 0.0~11.35%이므로 관측 0건을 실제 확률 0%로 단정하지 않는다.
- 정상 observer CPU peak p95는 Redis 1.085%, Ignite 1.116%, Phi 1.281%,
  SWIM 1.813%, Aerospike 2.934% 순이다.
- 정상 observer application wire bytes p50은 Phi 1,560B, Redis 3,120B,
  Ignite 6,600B, SWIM 29,718B, Aerospike 44,768B 순이다. 이는 cluster 전체가 아닌
  observer process의 60초 송수신 합계다.
- 장애 행렬 150회와 정상 baseline 150회를 병합한 최종 logical run은 300회이며
  300/300 유효, fault 이전 SUSPECT 0건, fault 이전 DEAD 0건이다.
- 30-seed 원본/보고서: `results-product-normal-baseline-30seed-20261101/`.

#### Crash 30-seed 확장 및 최종 병합 (2026-11-02)

- 동일 8노드 Docker 실제 socket 구성에서 5 profiles × crash 30 seeds = 150회를
  실행했고 150/150 runner 통과, 150/150 최소 사후 관찰창 충족을 확인했다.
- 5종 모두 crash 탐지 30/30, 미검출 0, fault 이전 SUSPECT/DEAD 0건이다. 탐지율
  100%의 양측 Wilson 95% 신뢰구간은 profile별 88.65~100%다.
- 중단 시점에는 59회가 통과했고 Docker 종료로 남은 91회에 failed checkpoint가 남았다.
  `FD_MATRIX_RESUME=1` 재개 후 150개 logical directory가 모두 passed가 되었으며,
  분석기는 `execution-status.csv`에서 passed directory만 집계한다. 실패 기록 91개는
  실행 이력 보존용이며 최종 통계 표본에 포함되지 않는다.
- Crash DEAD latency와 10,000회 고정-seed 비모수 bootstrap 95% 신뢰구간은 다음과 같다.

| Profile | 탐지 | p50 (bootstrap 95% CI) | p95 (bootstrap 95% CI) | p99 |
|---|---:|---:|---:|---:|
| Aerospike mesh inspired | 30/30 | 1,667.73ms (1,650.58~1,681.60) | 1,739.38ms (1,711.65~1,742.97) | 1,742.09ms |
| SWIM+Lifeguard | 30/30 | 4,047.58ms (4,018.04~4,081.18) | 4,160.03ms (4,133.67~4,173.23) | 4,170.13ms |
| Ignite TCP ring inspired | 30/30 | 4,628.89ms (4,553.71~4,789.16) | 5,039.89ms (4,926.63~5,139.19) | 5,121.11ms |
| Redis Cluster inspired | 30/30 | 15,877.55ms (15,778.27~15,961.35) | 16,217.78ms (16,045.18~16,249.95) | 16,246.95ms |
| Cassandra Phi inspired | 30/30 | 19,033.25ms (18,854.40~19,255.56) | 19,425.28ms (19,324.25~19,461.27) | 19,457.82ms |

- p95 bootstrap 구간이 인접 profile끼리 겹치지 않아 이 통제 환경의 crash 속도 순위는
  `Aerospike > SWIM > Ignite > Redis > Phi`로 안정적이다. 이 구간은 표본 추출 불확실성만
  나타내며 단일 Docker host의 체계적 편향이나 EC2 외적 타당성을 포함하지 않는다.
- 이전 fault 9종 × 3 seeds 135회, crash 150회, normal 150회를 논리 조건별 병합한
  최종 데이터셋은 435/435 유효하다. profile별 87회이고 전체 fault 이전 SUSPECT/DEAD는
  각각 0건이다.
- Crash 원본/보고서: `results-product-crash-30seed-20261101/`.
- 갱신된 최종 CSV/SVG/보고서: `results-product-pre-ec2-final-20261101/report/`.

## 미검증·남은 과제

- 제품 inspired 실제 socket 5종의 crash 30회 반복은 완료했다. loss/delay/pause/resource는
  아직 조건별 3회이므로 30회 확장이 필요하다.
- F0~F4/F7a-b의 5·8노드 반복 UDP 통계와 loss/delay matrix.
- F7c 실제 UDP adapter.
- 실제 OpenKache `cache-server` 통합과 GET/SET workload 복구 시간.
- 실제 GC 유발 pause, native Linux `dm-delay`, 실제 제품 frame byte.
- OpenKache SLO/가중치 확정 후 Pareto 및 채택 판단.
- 자동 failover와 replica 승격은 의도적으로 미구현.
