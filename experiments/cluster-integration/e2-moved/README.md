# E2 MOVED 비용과 표 전파

B1 8대, 노드당 Tablet 4개에서 open-loop 5,000 req/s로 60초간 측정한다. seed 42, Zipf 0.99, key 100만 × value 1 KiB, GET/SET/DELETE 40/30/30을 사용한다. B1+B3 이동 wire가 구현되어 있으므로 `01-precopy-20.properties`에서 E4·E5와 같은 pre-copy로 데이터를 복사한 뒤 소유권을 전환한다. job별 대역 상한은 0(무제한), batch는 64 KiB, 종료 임계값은 256 KiB, 라운드 상한은 10이다. 서로 다른 Tablet 20개를 순차 이동하며 목적지는 8개 노드에 나눈다. 첫 이동은 3초, 마지막은 41초에 예정돼 측정 종료까지 19초가 남는다. 앞 이동이 늦으면 다음 이동도 늦어지므로, 마지막 이동이 측정 안에 끝났는지 `migrations[]`의 `phase`와 `finishedMs`로 확인한다.

| 순서 | 예정 시각(s) | Tablet | 소스 | 목적지 |
|---:|---:|---:|---:|---:|
| 1 | 3 | 0 | 0 | 1 |
| 2 | 5 | 4 | 1 | 2 |
| 3 | 7 | 8 | 2 | 3 |
| 4 | 9 | 12 | 3 | 4 |
| 5 | 11 | 16 | 4 | 5 |
| 6 | 13 | 20 | 5 | 6 |
| 7 | 15 | 24 | 6 | 7 |
| 8 | 17 | 28 | 7 | 0 |
| 9 | 19 | 1 | 0 | 3 |
| 10 | 21 | 5 | 1 | 4 |
| 11 | 23 | 9 | 2 | 5 |
| 12 | 25 | 13 | 3 | 6 |
| 13 | 27 | 17 | 4 | 7 |
| 14 | 29 | 21 | 5 | 0 |
| 15 | 31 | 25 | 6 | 1 |
| 16 | 33 | 29 | 7 | 2 |
| 17 | 35 | 2 | 0 | 5 |
| 18 | 37 | 6 | 1 | 6 |
| 19 | 39 | 10 | 2 | 7 |
| 20 | 41 | 14 | 3 | 0 |

## 결과 읽기

- `cluster-<id>.json`의 `latencyByHops["0"]`, `latencyByHops["1"]`에서 같은 런의 `count`, `p50Us`, `p99Us`를 비교한다. hop 1 표본이 없거나 적으면 지연 차이를 해석하지 않는다. `latencyByHops["2+"].count`와 3회 리다이렉트 후 네 번째 MOVED를 받은 `redirectExhausted`는 별도로 기록한다. `2+` 집계에는 redirect 초과 실패도 포함될 수 있다. pre-copy freeze 중 발생한 BUSY 재시도 대기가 hop별 지연에 섞일 수 있다. `requestsWithBusyRetry`와 `busyRetries`를 함께 확인하고 hop 지연 차이 전체를 MOVED 추가 왕복 비용으로 해석하지 않는다.
- `migrations[]`의 `phase: 4`와 `finishedMs`로 측정 구간 내 완료 건수를 확인한다. **이동당 MOVED 관측 요청 수**는 `redirectedRequests / 완료 이동 수`로 정규화한다. `redirectedRequests`는 적어도 한 번 MOVED를 받은 요청 수이고, 한 요청의 여러 MOVED 응답을 각각 세는 카운터는 현재 없다.
- `consistency.lostSet`, `consistency.wrongValue`, `consistency.staleHit`가 모두 0이어야 정상이다. `checked`, `unverified`, `uncertainExcluded`도 함께 기록해 실제 검사 범위를 확인한다. fencing은 결과의 정합성 위반과 함께 해석하며, 옛 소스가 받은 쓰기만 따로 세는 카운터는 현재 없다.
- `sendLag`와 `achievedRatePerSec`로 단일 부하 생성기가 예정 부하를 따라갔는지 확인한다. 표 전파는 hop 분포와 `tableRefreshes`를 보며, 밀리초 단위 전파 완료 시간은 주 지표로 삼지 않는다.
