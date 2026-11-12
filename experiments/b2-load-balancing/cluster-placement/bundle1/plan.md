# bundle1 계획 요약 (ClusterPlacementPlanner 자동 생성)

계획 단계의 예상값이다(Runner와 같은 요청열로 센 Tablet 부하 기준). 실측은 EC2 결과로 본다.

| scenario | policy | seed | 이동 Tablet 합 | 예상 평균 최대/평균 |
|---|---|---|---|---|
| HOT_RANGE | greedy-load-placement | 1 | 897 | 1.128 |
| HOT_RANGE | slicer-inspired | 1 | 34 | 1.181 |
| HOT_RANGE | estore-inspired | 1 | 9 | 1.126 |
| HOT_RANGE | cost-aware-rebalancing | 1 | 0 | 3.249 |
| HOT_RANGE | hysteresis-rebalancing | 1 | 9 | 1.128 |
| HOT_RANGE | no-rebalance | 1 | 0 | 3.249 |
| HOT_RANGE | greedy-load-placement | 2 | 928 | 1.129 |
| HOT_RANGE | slicer-inspired | 2 | 39 | 1.180 |
| HOT_RANGE | estore-inspired | 2 | 9 | 1.129 |
| HOT_RANGE | cost-aware-rebalancing | 2 | 0 | 3.249 |
| HOT_RANGE | hysteresis-rebalancing | 2 | 9 | 1.127 |
| HOT_RANGE | no-rebalance | 2 | 0 | 3.249 |
| ZIPF | greedy-load-placement | 1 | 870 | 1.036 |
| ZIPF | slicer-inspired | 1 | 12 | 1.028 |
| ZIPF | estore-inspired | 1 | 7 | 1.051 |
| ZIPF | cost-aware-rebalancing | 1 | 0 | 1.248 |
| ZIPF | hysteresis-rebalancing | 1 | 2 | 1.049 |
| ZIPF | no-rebalance | 1 | 0 | 1.248 |
| ZIPF | greedy-load-placement | 2 | 836 | 1.039 |
| ZIPF | slicer-inspired | 2 | 26 | 1.037 |
| ZIPF | estore-inspired | 2 | 6 | 1.039 |
| ZIPF | cost-aware-rebalancing | 2 | 0 | 1.291 |
| ZIPF | hysteresis-rebalancing | 2 | 1 | 1.103 |
| ZIPF | no-rebalance | 2 | 0 | 1.291 |
