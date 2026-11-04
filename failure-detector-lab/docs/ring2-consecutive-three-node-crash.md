# Ring-2 연속 3노드 동시 Crash 실험

## 질문

Ring-2가 인접 2노드 동시 Crash를 모두 탐지한 뒤에도, 연속된 장애가 감시
중복도보다 하나 더 많아질 때 안전한지 확인했다.

## 조건

- 5노드와 8노드에서 장애 시작 위치를 한 번씩 회전한 총 13개 실행
- 정적 membership view, 재구성 없음
- 각 노드는 Ring의 다음 두 노드를 감시
- 연속된 세 노드는 20초에 동시에 Crash
- Heartbeat 1초, SUSPECT deadline 3초, DEAD confirm 2초
- 전체 관측 시간 40초
- Verify Suspect 비활성: Ring-2 관측 구조 자체의 한계만 측정

예를 들어 `node-0`, `node-1`, `node-2`가 함께 Crash하면 `node-0`과
`node-1`에는 살아 있는 관측자가 남지만, `node-2`의 관측자
`node-0|node-1`은 모두 Crash한다.

## 결과

| 노드 수 | 회전한 장애 위치 | 세 장애 모두 DEAD | 탐지한 장애 노드 | 미검출 |
|---:|---:|---:|---:|---:|
| 5 | 5 | 0/5 | 10/15 | 5 |
| 8 | 8 | 0/8 | 16/24 | 8 |
| 합계 | 13 | **0/13** | **26/39** | **13** |

- 탐지된 26개 대상: SUSPECT 2초, DEAD 4초
- 미검출된 13개 대상: 살아 있는 관측자 0개, SUSPECT·DEAD 이벤트 없음
- 모든 장애 시작 위치에서 연속된 세 번째 대상 하나가 동일하게 미검출

따라서 Ring-2는 연속 2노드 Crash까지는 보호하지만 정적 membership
view에서 연속 3노드 Crash를 완전히 탐지하지 못한다. 이는 timeout이나
관측 시간 문제가 아니라 감시 중복도 `k`보다 긴 연속 장애 `k+1`에서 생기는
구조적 한계다. Verify Suspect는 SUSPECT 이후의 확인 단계이므로 관측자가
전혀 없어 SUSPECT가 발생하지 않는 이 공백을 단독으로 해결할 수 없다.

## 비용 해석

| 노드 수 | 실행당 messages | 실행당 bytes |
|---:|---:|---:|
| 5 | 284 | 18,176 |
| 8 | 530 | 33,920 |

메시지·byte는 프로토콜 모델 추정치다. 인접 2노드 Crash 실험보다 값이 작은
이유는 장애 노드가 하나 더 많아 장애 이후 Heartbeat를 보내는 노드가 줄었기
때문이다. 따라서 이 수치를 Ring-1/Ring-2 비용 배수 비교로 사용해서는 안 된다.

탐지 시간과 상태 전이는 `simulation-observed`, 메시지·byte는
`model-estimated`다. 실제 UDP 손실, 스케줄링 지터, membership 전파 비용은
이번 결과에 포함되지 않았다.

## 결론과 다음 실험

Ring-2는 낮은 고정 비용으로 인접 2노드 장애를 보완하지만 임의 개수의 연속
장애에 안전한 최종 해법은 아니다. Ring-3으로 중복도를 늘리면 같은 방식으로
3노드 장애를 보완할 수 있으나, `k+1` 장애에서 한계가 반복되고 정상 시 통신
비용도 다시 증가한다.

다음 우선순위는 단순히 Ring-k를 늘리는 것보다 **장애 판정 후 membership
view 재구성**을 구현해 새 관측자가 미검출 대상을 이어서 감시할 수 있는지,
그 과정에서 중복·누락 관측과 수렴 비용이 얼마나 생기는지 확인하는 것이다.
그 뒤 실제 UDP 반복 실험에서 재구성 가능한 Ring-2와 SWIM + Lifeguard를
동일 fault·seed·traffic budget으로 비교한다.

## 실행과 산출물

```bash
./gradlew -p failure-detector-lab ringAdjacentCrashExperiment \
  --args='--nodes 5,8 --neighbors 2 --crash-count 3 --output-dir results-ring2-consecutive3-crash-20261028'
```

- `results-ring2-consecutive3-crash-20261028/runs.csv`
- `results-ring2-consecutive3-crash-20261028/targets.csv`
- `results-ring2-consecutive3-crash-20261028/events.jsonl`
- `results-ring2-consecutive3-crash-20261028/manifest.json`
- `results-ring2-consecutive3-crash-20261028/summary.md`
