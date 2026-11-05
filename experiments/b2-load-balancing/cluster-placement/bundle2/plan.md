# bundle2 계획 요약 (ClusterPlacementPlanner 자동 생성)

계획 단계의 예상값이다(Runner와 같은 요청열로 센 Tablet 부하 기준). 실측은 EC2 결과로 본다.

| scenario | policy | seed | 이동 Tablet 합 | 예상 평균 최대/평균 |
|---|---|---|---|---|
| MOVING_HOTSPOT | greedy-load-placement | 1 | 909 | 1.464 |
| MOVING_HOTSPOT | slicer-inspired | 1 | 93 | 1.722 |
| MOVING_HOTSPOT | weighted-rendezvous | 1 | 50 | 1.571 |
| MOVING_HOTSPOT | hysteresis-rebalancing | 1 | 62 | 1.675 |
| MOVING_HOTSPOT | greedy-load-placement | 2 | 885 | 1.336 |
| MOVING_HOTSPOT | slicer-inspired | 2 | 95 | 1.687 |
| MOVING_HOTSPOT | weighted-rendezvous | 2 | 50 | 1.572 |
| MOVING_HOTSPOT | hysteresis-rebalancing | 2 | 59 | 1.687 |
| BURSTY | greedy-load-placement | 1 | 907 | 1.422 |
| BURSTY | slicer-inspired | 1 | 24 | 1.387 |
| BURSTY | weighted-rendezvous | 1 | 50 | 1.281 |
| BURSTY | hysteresis-rebalancing | 1 | 90 | 1.532 |
| BURSTY | greedy-load-placement | 2 | 919 | 1.360 |
| BURSTY | slicer-inspired | 2 | 19 | 1.440 |
| BURSTY | weighted-rendezvous | 2 | 50 | 1.314 |
| BURSTY | hysteresis-rebalancing | 2 | 65 | 1.362 |
