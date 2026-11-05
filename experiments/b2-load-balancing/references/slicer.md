# Google Slicer — 부하 균형과 이동량(churn) 상한

## 출처

[Slicer: Auto-Sharding for Datacenter Applications](https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf)

- OSDI 2016, Adya et al.
- 핵심 절: **§4.4 Load Balancing**, **§4.4.1 Sharding Algorithm: Weighted-move**, §4.4.4(거절한 대안: 부하 인식 consistent hashing)
- B1이 해시 범위 Tablet 구조로 같은 논문을 정리했다: [../../b1-tablet-distribution/references/slicer-scylla-tablets.md](../../b1-tablet-distribution/references/slicer-scylla-tablets.md). 아래 인용은 그 문서에서 가져왔다.

## 논문에서 가져온 것

1. **부하 균형은 이동으로 맞추되, 한 번에 움직이는 양에 상한을 둔다.** weighted-move는 조정 한 번에 key 공간 이동을 제한한다: "The constants in the algorithm (50–150 slices per task, 1% and 9% key movement per adjustment) were chosen by observing existing applications." (§4.4.1)
2. **이득이 큰 이동부터 고른다.** 가장 바쁜 서버의 부하를 줄이는 이동을 우선한다.
3. 부하 인식 consistent hashing은 "displaced traffic ends up randomly distributed, not directed at a cool task"라 교체했다(§4.4.4). 이 프로젝트의 `weighted-rendezvous`가 그 계열이라 비교 대상으로 넣었다.

## `slicer-inspired`에서 자체 결정한 것

| 항목 | 논문 | 이 구현 |
|---|---|---|
| 이동 예산 단위 | key 공간 비율 | 데이터 **byte** 비율. Tablet 크기가 이동 비용이라는 B2 실험 가정에 맞춤 |
| 예산 값 | 9%(조정당) | 9%(결정 한 번당) |
| 이동 선택 | weighted-move의 단계별 규칙(merge·move·split) | 가장 바쁜 노드 → 가장 한가한 노드 이동 중 "최대 이용률 감소 / byte"가 가장 큰 것을 반복 |
| split·merge | 같은 알고리즘 안에서 함께 | 하지 않음. split은 `SplitPolicy`가 따로 맡는다(Benchmark C에서 조합) |

## 결과에서 본 것 ([../policy-benchmark.md](../policy-benchmark.md))

- 정적 부하에서는 greedy보다 이동량이 96% 이상 적고 균형은 0-5% 나쁘다.
- 분할로 Tablet이 늘거나(Benchmark C) hotspot이 움직이면 예산 때문에 따라가지 못한다. 예산 값이 결과를 크게 좌우하므로, Slicer의 값을 그대로 쓴 것이 이 실험 규모(노드 4대, Tablet 8-256개)에 맞는지는 확인하지 않았다.
