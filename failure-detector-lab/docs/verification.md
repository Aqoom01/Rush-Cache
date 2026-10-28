# v2 검증 기록 (2026-10-27)

아래에서 “통과”는 이 작업에서 실제 명령을 실행해 종료 코드와 산출물을 확인한 경우만 뜻한다.

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

## 미검증·남은 과제

- F0~F4/F7a-b의 5·8노드 반복 UDP 통계와 loss/delay matrix.
- F7c 실제 UDP adapter.
- 실제 OpenKache `cache-server` 통합과 GET/SET workload 복구 시간.
- CPU, GC pause, heap, 실제 제품 frame byte.
- OpenKache SLO/가중치 확정 후 Pareto 및 채택 판단.
- 자동 failover와 replica 승격은 의도적으로 미구현.
