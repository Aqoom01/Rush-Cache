# B2 Load-aware Balancing + Split/Merge

부하(QPS, 이후 CPU·IOPS)를 기준으로 Tablet을 나누고(Hot Split), 합치고(Cold Merge), 재배치하는 정책을 구현하고 비교한다.

## 문서

| 파일 | 내용 |
|---|---|
| [design.md](design.md) | B2 전체 설계: 범위, 경계, 공통 입력·출력 모델, 불변식, 지표, 실험 계획 |
| [references/](references/) | 참고 논문·시스템별 정리. 읽은 내용과 프로젝트 적용 방식을 분리해 적는다 |
| [policy-benchmark.md](policy-benchmark.md) | Split 3종·Placement 7종 벤치마크(A-C) 결과, 순위, trade-off, 약점 |

## 실험

| 폴더 | 내용 |
|---|---|
| [split-comparison/](split-comparison/) | in-process. 정책별 분할 지점과 다음 window의 요청 분배(Benchmark A 포함) |
| [placement-comparison/](placement-comparison/) | in-process. Tablet 64개 고정, 노드별 부하·이동량(Benchmark B) |
| [end-to-end/](end-to-end/) | in-process. 분할 + 배치 조합(Benchmark C) |
| [cluster-split/](cluster-split/) | EC2 cache-server 2대. 정책별 분할 지점으로 라우팅했을 때 노드별 부하·p99·처리량(memory·nvme) |
| [cluster-placement/](cluster-placement/) | EC2 cache-server 4대. 배치 정책의 window별 라우팅 계획을 재생해 노드별 부하·p99·CPU·이동 후 miss를 잰다. 결과: [results.md](cluster-placement/results.md) |

## 정책(variant) 목록

정책 하나는 참고 문헌 하나에 대응한다. 실험 결과의 `variant` 값으로 이 이름을 쓴다.

| variant | 기능 | 근거 | 상태 |
|---|---|---|---|
| `midpoint-split` | Split | Baseline (hash 구간의 가운데에서 분할. 논문 §4.4의 비교 대상) | 구현 (`MidpointSplitPolicy`) |
| `split-for-heat` | Split | [DynamoDB Split-for-Heat](references/dynamodb-split-for-heat.md) | 구현 (`HeatSplitPolicy`) |
| `estore-inspired` | Split | [E-Store](references/estore.md): hot key 묶음 경계에서 hot/cold 분리 | 구현 (`EStoreSplitPolicy`) |
| `greedy-load-placement` | 재배치 | Baseline (LPT greedy, 현재 배치 무시) | 구현 (`GreedyLoadPlacement`) |
| `slicer-inspired` | 재배치 | [Slicer](references/slicer.md): 이동 예산 안에서 균형 | 구현 (`SlicerInspiredPlacement`) |
| `estore-inspired` | 재배치 | [E-Store](references/estore.md): hot Tablet 먼저 이동 | 구현 (`EStoreInspiredPlacement`) |
| `power-of-two-choices` | 재배치 | Mitzenmacher 2001: 후보 2개 중 나은 곳 | 구현 (`PowerOfTwoChoicesPlacement`) |
| `weighted-rendezvous` | 재배치 | 용량 가중 HRW: 부하를 보지 않는 결정적 배치 | 구현 (`WeightedRendezvousPlacement`) |
| `cost-aware-rebalancing` | 재배치 | 자체 설계: 이득 > 이동 비용일 때만 | 구현 (`CostAwareRebalancing`) |
| `hysteresis-rebalancing` | 재배치 | 자체 설계: 임계값 두 개 + cooldown | 구현 (`HysteresisRebalancing`) |
| 미정 | Merge | 미정 | - |

## 코드 위치

알고리즘 코드는 `cache-core`의 `io.openkache.core.balance`에 둔다. 이 디렉터리에는 설계, 참고 문헌 정리, 실험 설정과 결과 요약만 둔다. 상세는 [design.md](design.md) §7을 따른다.
