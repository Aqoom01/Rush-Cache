# bundle3 계획 요약 (ClusterPlacementPlanner 자동 생성)

계획 단계의 예상값이다(Runner와 같은 요청열로 센 Tablet 부하 기준). 실측은 EC2 결과로 본다.

| scenario | policy | seed | 이동 Tablet 합 | 예상 평균 최대/평균 |
|---|---|---|---|---|
| SCALE_OUT | greedy-load-placement | 1 | 603 | 1.027 |
| SCALE_OUT | slicer-inspired | 1 | 20 | 1.045 |
| SCALE_OUT | weighted-rendezvous | 1 | 50 | 1.286 |
| SCALE_OUT | cost-aware-rebalancing | 1 | 0 | 1.295 |
| SCALE_OUT | greedy-load-placement | 2 | 551 | 1.030 |
| SCALE_OUT | slicer-inspired | 2 | 23 | 1.048 |
| SCALE_OUT | weighted-rendezvous | 2 | 50 | 1.275 |
| SCALE_OUT | cost-aware-rebalancing | 2 | 0 | 1.226 |
