# E1 라우팅 오버헤드

기존 단일 서버와 B1의 직행 요청 비용을 비교한다. B1 노드 수를 늘리는 조건에서는 노드별 부하도 달라지므로 결과를 노드 수에 따른 포화 처리량으로 해석하지 않는다.

| 서버 | 멤버 | 부하 단계 |
|---|---:|---|
| kache | 1 | 동시 요청 연결 8 / 64 |
| B1 | 1 | 8 / 64 |
| B1 | 4 | 8 / 64 |
| B1 | 8 | 8 / 64 |

네 토폴로지 모두 Runner 기본 closed-loop 경로를 사용한다. 각 연결은 응답을 받은 뒤 다음 요청을 보내므로 `concurrency`는 부하 단계이고 RPS는 측정값이다. `cluster.mode=b1`의 open-loop는 TABLE_DUMP가 필요해서 kache와 공유할 수 없다. 각 단계는 별도 설정으로 실행해 서버를 재시작한다. seed 42, Zipf 0.99, key 100만 × value 1 KiB, GET/SET/DELETE 40/30/30, warm-up 10만 요청, 측정 20만 요청을 공통으로 사용한다. 기본 경로는 warm-up 요청으로 상태를 만든다.

## 초기 표와 직행 경계

`HashRangeScheme.initial`(`cache-core/src/main/java/io/openkache/core/tablet/range/HashRangeScheme.java`)은 `N × T`개 Tablet을 만든 뒤 노드 `j`에 Tablet `[j·T, (j+1)·T)`를 배정한다. 여기서 `N`은 멤버 수, `T=4`는 `deploy.b1.tablets.per.node`다. `TabletRange.uniform`의 step은 `floor((2^64 − 1) / (N·T))`이므로 Runner 경계 `j`는 `j·T·step`의 unsigned 십진수다(`j=1…N−1`). 예를 들어 B1 4대의 첫 경계는 `4611686018427387900`이다. 1대 조건은 경계가 없다. CI가 `target.hosts`를 NodeId 순으로 채우고 `RunnerConfig.targetIndex`가 이 경계로 목적지를 고른다.

## 결과 읽기

- 같은 `concurrency`에서 `throughputPerSecond`, `p50Nanos`, `p99Nanos`를 함께 비교한다. 전체 비교와 별도로 `targets[]`에서 노드별 요청 수와 지연을 확인한다.
- closed-loop에는 `sendLag`가 없다. 높은 단계에서 연결 수를 늘려도 달성 RPS가 거의 오르지 않고 p99가 커지면, 측정 구간의 부하 생성기 CPU(예: `sar -u 1`)와 서버 CPU를 대조한다. 부하 생성기 CPU가 포화에 가깝고 서버에 여유가 있으면 생성기 병목 가능성을 표시한다. 설계의 유효 구간 예시는 부하 생성기 CPU 70% 이하이다.
- 현재 기본 경로는 서버·부하 생성기 CPU 시계열을 결과에 넣지 않으며 B1 서버는 `/metrics`가 없다. CPU 비교가 필요하면 측정 구간의 노드별 CPU를 별도 수집한다. CPU 없이 RPS 정체의 원인을 확정할 수 없다.
