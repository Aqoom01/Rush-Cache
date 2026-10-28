# Ring-2 인접 2노드 동시 Crash 실험

## 질문

Ring-1에서 발견한 인접 2노드 동시 Crash의 구조적 미검출을 관측 이웃 수를
두 개로 늘리는 것만으로 해결할 수 있는지 확인했다.

## 조건

- Ring-1 실험과 동일한 5·8노드, Crash 위치, 시간 설정
- 정적 membership view, 재구성 없음
- 각 노드는 Ring의 다음 두 노드를 감시
- 5노드 5개, 8노드 8개의 모든 인접 Crash 쌍
- 두 인접 노드는 20초에 동시에 Crash
- Heartbeat 1초, SUSPECT deadline 3초, DEAD confirm 2초
- 전체 관측 시간 40초
- Verify Suspect 비활성: Ring-2 관측 구조 자체의 효과만 측정

## 결과

| 방법 | 인접 쌍 | 두 장애 모두 DEAD | 탐지 노드 | SUSPECT | DEAD | 모델 비용 |
|---|---:|---:|---:|---:|---:|---:|
| Ring-1 | 13 | 0/13 | 13/26 | 탐지 대상 2초 | 탐지 대상 4초 | 1.00x |
| Ring-2 | 13 | **13/13** | **26/26** | **2초** | **4초** | **2.00x** |

Ring-2에서는 인접한 두 노드가 함께 Crash해도 각 대상에 살아 있는 관측자가
최소 하나 남았다. 모든 위치에서 두 노드가 SUSPECT와 DEAD로 전이했으며
Ring-1의 탐지 지연을 그대로 유지했다.

모델 비용은 정확히 두 배였다.

| 노드 수 | Ring-1 messages / bytes | Ring-2 messages / bytes |
|---:|---:|---:|
| 5 | 163 / 10,432 | 326 / 20,864 |
| 8 | 286 / 18,304 | 572 / 36,608 |

탐지 시간은 `simulation-observed`, 메시지·byte는 `model-estimated`다. 따라서
현재 결과는 Ring-2가 **정적 Ring의 인접 2노드 Crash**를 낮은 추가 모델
비용으로 해결한다는 근거이며, 실제 UDP와 membership 재구성 비용까지
검증했다는 의미는 아니다.

## 다음 실험

1. Ring-2의 구조적 한계를 확인하기 위한 연속 3노드 동시 Crash
2. 장애 판정 후 membership view 재구성과 중복·누락 관측
3. Ring-2 + Verify Suspect의 partition false DEAD
4. 통과한 구성과 SWIM + Lifeguard의 실제 UDP 30회 반복 비교

## 실행과 산출물

```bash
./gradlew -p failure-detector-lab ringAdjacentCrashExperiment \
  --args='--nodes 5,8 --neighbors 2 --output-dir results-ring2-adjacent-crash-20261028-v2'
```

- `results-ring2-adjacent-crash-20261028-v2/runs.csv`
- `results-ring2-adjacent-crash-20261028-v2/targets.csv`
- `results-ring2-adjacent-crash-20261028-v2/events.jsonl`
- `results-ring2-adjacent-crash-20261028-v2/manifest.json`
- `results-ring2-adjacent-crash-20261028-v2/summary.md`
