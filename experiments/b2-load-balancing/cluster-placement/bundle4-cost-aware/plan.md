# bundle4-cost-aware 계획 요약 (ClusterPlacementPlanner 자동 생성)

계획 단계의 예상값이다(Runner와 같은 요청열로 센 Tablet 부하 기준). 실측은 EC2 결과로 본다.

| scenario | policy | seed | 이동 Tablet 합 | 예상 평균 최대/평균 |
|---|---|---|---|---|
| HOT_RANGE | cost-aware-20perMB | 1 | 9 | 1.126 |
| HOT_RANGE | cost-aware-200perMB | 1 | 9 | 1.126 |
| HOT_RANGE | cost-aware-20perMB | 2 | 9 | 1.128 |
| HOT_RANGE | cost-aware-200perMB | 2 | 9 | 1.128 |
| ZIPF | cost-aware-20perMB | 1 | 2 | 1.024 |
| ZIPF | cost-aware-200perMB | 1 | 1 | 1.104 |
| ZIPF | cost-aware-20perMB | 2 | 3 | 1.046 |
| ZIPF | cost-aware-200perMB | 2 | 2 | 1.080 |
| SCALE_OUT | cost-aware-20perMB | 1 | 6 | 1.036 |
| SCALE_OUT | cost-aware-200perMB | 1 | 6 | 1.053 |
| SCALE_OUT | cost-aware-20perMB | 2 | 7 | 1.033 |
| SCALE_OUT | cost-aware-200perMB | 2 | 4 | 1.049 |
