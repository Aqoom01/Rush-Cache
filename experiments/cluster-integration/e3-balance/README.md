# B1+B2+B3 통합: 쏠림을 B2 정책으로 나누고 B3 pre-copy로 옮기기 (실서버)

[통합 실험 설계](../design.md) §4 E3를 B3 이동까지 붙여 실행한다. 설계서의 고정 기본 조합(B1 해시범위 Tablet + B2 분할 + B3 pre-copy)으로, B2 Split 정책만 바꿔 비교한다.

## 세 연구가 맡는 일

| 단계 | 담당 | 실제로 하는 것 |
|---|---|---|
| 관찰 | Runner | 측정 중 보낸 요청으로 window(2초)마다 key별 읽기·쓰기 수를 센다 |
| 분할 결정 | **B2** `SplitPolicy` | 분할 시작 조건을 넘은 Tablet의 key 부하(`TabletLoad`)를 보고 분할 지점을 정하거나 보류한다 |
| 분할 반영 | **B1** Controller | Runner가 CONTROL SPLIT(expectedVersion CAS)을 보내면 표를 나누고 노드에 push한다 |
| 배치 결정 | **B2** `PlacementPolicy` | 나눈 뒤의 표·Tablet 부하로 옮길 Tablet과 목적지를 정한다 |
| 이동 | **B3** `PreCopyProtocol` | Runner가 source에 MIGRATE를 보내면 실제 노드 간 TCP로 복사하고 Controller MOVE CAS로 commit한다 |
| 라우팅 | **B1** | 클라이언트는 표로 직행하고 MOVED를 따라간다 |

정책 코드는 `core.balance`(B2)·`core.migration`(B3)·`core.tablet`(B1)의 구현을 그대로 호출한다. Runner `BalanceController`는 관찰·호출·지시만 한다.

## 조건

| 항목 | 값 |
|---|---|
| 서버 | B1 2대, 처음 Tablet 4개(노드당 2) |
| key·value | 100만 × 1 KiB(측정 전 전부 SET) |
| 부하 | open-loop 5,000 req/s, GET/SET/DELETE 40/30/30, HOT_RANGE: 해시가 작은 key 10%에 요청 90% → Tablet 0(node 0)에 쏠림 |
| 구간 | warm-up 4초, 측정 120초, 측정 5초 뒤부터 window 2초 |
| 분할 시작 조건 | B2 design.md §4.1·Benchmark C 규칙: Tablet window 요청 > 노드 용량/4가 3 window 연속, 분할 후 3 window 유예, Tablet 상한 256. 노드 용량 = window 요청 ÷ (2 × 0.7) |
| Placement | `hysteresis-rebalancing`(최대/평균 > 1.25에서 시작, 1.10에서 멈춤, 최근 3 window 이동 Tablet 제외) |
| 이동 | pre-copy, 대역 상한 없음 |

| config | Split 정책 |
|---|---|
| `1-no-balance` | 균형 조정 없음(대조군) |
| `2-split-for-heat` | split-for-heat θ0.8 (DynamoDB 착안) |
| `3-midpoint-split` | midpoint (Baseline) |
| `4-estore-split` | estore-inspired (E-Store 착안) |

설계서 E3의 비교 축(midpoint / heat θ0.6 / heat θ0.8)과 다르게 θ 대신 정책 3종을 비교한다. 이 부하는 hot key가 구간에 고르게 퍼져 있어 key 하나가 60%를 넘는 일이 없으므로 θ0.6과 θ0.8의 결정이 같다.

## 실행

```
NODES=2, CONFIG=experiments/cluster-integration/e3-balance   (GitLab CI 웹 실행)
```

## 결과 읽는 법

`cluster-<id>.json`의 `balance.windows[]`가 window마다 한 줄이다.

- `nodeLoad`·`imbalance`: 그 window 요청을 window 끝 표로 노드에 나눈 부하와 최대/평균(결정 전)
- `splits[]`: 분할 시작 조건을 넘은 Tablet마다 `split`(지점·생긴 Tablet) 또는 `no-split`(B2 보류 사유)
- `moves[]`: Placement 이동 계획과 B3 이동 결과(`server.writeBlockedMs`·`sentBytes`·라운드)
- `nodeLoadAfterPlan`·`imbalanceAfterPlan`: 같은 window 부하를 결정 반영 뒤 표로 다시 나눈 값(계획상 효과)

그 밖에 `timeline-<id>.csv`(100 ms 구간별 지연), `tabletLoad`, `consistency`(정합성), `gc-node-*.log`는 [E4](../e4-migration/README.md)와 같다.

## 해석할 때 주의

- window 길이(2초)와 시작 조건(3 window 연속)이 복구 시간을 정한다. 정책 간 상대 비교로만 읽는다(설계 §4 E3 주의 1).
- 이동 중에는 결정 단계가 끝나지 않으므로 그 window는 길어지고, 그동안 들어온 요청은 다음 window 관찰에 넣지 않는다.
- Tablet 크기는 "해시 구간 폭 비율 × 전체 데이터"로 어림해 Placement에 넘긴다(key가 해시 공간에 고르게 퍼진다고 가정).
- 분할 시작 조건은 B2 Benchmark C 규칙을 Runner에 옮긴 것이다. B2 공통 `SplitTrigger`는 아직 core에 없다.
