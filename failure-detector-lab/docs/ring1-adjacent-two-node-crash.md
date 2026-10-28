# Ring-1 인접 2노드 동시 Crash 엣지케이스

## 질문

단일 장애에서 탐지 시간과 통신 비용이 유리했던 Ring-1이 인접한 두 노드의
동시 Crash도 모두 탐지할 수 있는지 확인했다.

## 조건

- 정적 membership view, 재구성 없음
- 각 노드는 Ring의 다음 노드 하나만 감시
- 5노드와 8노드에서 모든 인접 쌍을 한 번씩 회전
- 두 인접 노드는 20초에 동시에 Crash
- Fixed Deadline: Heartbeat 1초, SUSPECT deadline 3초, DEAD confirm 2초
- 전체 관측 시간 40초

예를 들어 다음 Ring에서 `node-1`과 `node-2`를 동시에 Crash시켰다.

```text
node-0 -> node-1 -> node-2 -> node-3 -> node-4 -> node-0
```

`node-0`은 `node-1`을 계속 감시하므로 장애를 탐지할 수 있다. 반면
`node-2`의 유일한 관측자인 `node-1`도 함께 죽기 때문에 `node-2`에는
SUSPECT 이벤트가 발생하지 않는다.

## 결과

| 노드 수 | 회전한 인접 쌍 | 두 장애 모두 DEAD | 탐지한 장애 노드 |
|---:|---:|---:|---:|
| 5 | 5 | 0/5 | 5/10 |
| 8 | 8 | 0/8 | 8/16 |
| 합계 | 13 | **0/13** | **13/26** |

- 살아 있는 관측자가 있는 첫 번째 Crash 노드: SUSPECT 2초, DEAD 4초
- 관측자와 함께 죽은 두 번째 Crash 노드: SUSPECT 미발생, DEAD 미발생
- 비인접 두 노드의 관측자가 모두 살아 있는 대조 조건에서는 2/2 DEAD 탐지

따라서 결과는 Timeout이나 관측 시간 부족이 아니라 Ring-1의 단일 관측자
구조에서 발생하는 **구조적 미검출**이다. Verify Suspect 역시 먼저 SUSPECT가
발생해야 실행되므로 이 경우를 단독으로 보완할 수 없다.

## 다음 판단

다음 실험은 각 노드를 두 선행 노드가 감시하도록 하는 Ring-2에서 같은 인접
2노드 Crash를 실행하는 것이다. Ring-2가 2/2를 안정적으로 탐지하면서 Ring-1
대비 통신 비용을 약 2배 이내로 유지하는지 측정한다. 이후 재구성, 다중 장애,
실제 UDP 복잡도가 커지면 SWIM + Lifeguard와 비교한다.

## 실행과 산출물

```bash
./gradlew -p failure-detector-lab ringAdjacentCrashExperiment \
  --args='--nodes 5,8 --neighbors 1 --output-dir results-ring1-adjacent-crash-20261028'
```

- `results-ring1-adjacent-crash-20261028/runs.csv`
- `results-ring1-adjacent-crash-20261028/targets.csv`
- `results-ring1-adjacent-crash-20261028/events.jsonl`
- `results-ring1-adjacent-crash-20261028/manifest.json`
- `results-ring1-adjacent-crash-20261028/summary.md`

탐지 시각은 `simulation-observed`, 메시지·byte 비용은 `model-estimated`다.
이번 결과는 결정적 시뮬레이션이며 실제 UDP 다중 Crash 반복 실험은 아직
수행하지 않았다.

## 검증

- `./gradlew -p failure-detector-lab test selfTest --rerun-tasks`:
  JUnit 55개, failures 0, errors 0, self-test 통과
- `./gradlew build --rerun-tasks`: 루트 OpenKache 20 tasks 통과
