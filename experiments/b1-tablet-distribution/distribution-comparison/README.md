# distribution-comparison

분배 방식 variant를 같은 입력으로 돌려 균형·이동량·표 크기·조회 비용을 비교한다. 서버·네트워크·실제 시계를 쓰지 않는 in-process 실험이다. 설계는 [../design.md](../design.md) §6, 결과 해석은 [results.md](results.md).

## 실행

```bash
./gradlew :cache-core:testClasses
java -cp cache-core/build/classes/java/main:cache-core/build/classes/java/test \
    io.openkache.core.tablet.experiment.DistributionComparison \
    experiments/b1-tablet-distribution/distribution-comparison/summary.csv
```

러너는 `cache-core/src/test/java/io/openkache/core/tablet/experiment/`에 있다. JUnit 테스트가 아니라 `main`이라 `./gradlew test`에서는 돌지 않는다(돌 때마다 결과 파일을 쓰지 않기 위해). 틀 자체의 검증은 같은 패키지의 `MetricsTest`가 한다. 전체 실행은 1분 안쪽이다.

## 동일 조건

variant마다 잘게 자르는 손잡이가 다르다. 같은 조건으로 비교하려고 **전체 단위 수 G**(노드 8대 기준 표의 줄 수)를 공통 변수로 두고 variant별로 환산한다.

| variant | 손잡이 | 환산 (기준 노드 8대) |
|---|---|---|
| `fixed-slot` | 전체 슬롯 수 S | S = G |
| `hash-range-tablet` | 노드당 Tablet 수 T | T = G / 8 |
| `ring-vnode` | 노드당 토큰 수 V | V = G / 8 |

그 밖의 입력은 variant와 무관하게 같다: key(seed별 16바이트 난수 → `KeyHash`), 노드 ID 1..n, 노드 추가·제거 순서, Zipf 요청열(seed별). 지표는 전부 `RoutingTable`의 공개 연산(`lookup`, `ownedBy`, `tablets`)으로만 계산하므로 variant 코드가 지표에 끼어들지 않는다.

## 변수

| 변수 | 값 |
|---|---|
| G | 256, 1024, 4096, 16384 |
| K (key 수) | 1만, 10만, 100만 (S1만). 나머지는 100만 |
| θ (Zipf) | 0.9, 0.99, 1.2 (S5만). 요청 100만 건 |
| seed | 1~5 |
| 노드 | 8대 기준. S2는 +1, S3는 −1, S4는 1 → 16 → 8 |

## 시나리오와 지표

| 시나리오 | 내용 | 지표 (`metric` 열) |
|---|---|---|
| S1 | 초기 표에 key 배치 | `unit_max_mean` 노드별 단위 수 max/mean (결정적), `key_max_mean` 노드별 key 수 max/mean |
| S2 | 노드 1대 추가 | `moved_key_ratio`, `moved_unit_ratio`, `ideal_ratio` = 1/(n+1), `old_to_old_moves` 새 노드가 아닌 곳으로 간 이동 수, `*_after` 추가 후 균형 |
| S3 | 노드 1대 제거 | S2와 같음. `ideal_ratio` = 1/n |
| S4 | 1대씩 16대까지 늘리고 8대로 줄임 | `cumulative_moved_*_ratio` 누적 이동, `ideal_cumulative_ratio` Σ 1/n, `worst_unit_max_mean` 전 단계 중 최악, `final_key_max_mean` |
| S5 | Zipf 요청 쏠림 | `request_max_mean` 그대로 뒀을 때, `greedy_bound` 단위를 통째로 옮겨 도달 가능한 최선(LPT), `hottest_unit_share`, `hottest_key_share` |
| AUX | 표 크기·조회 | `metadata_bytes_initial`, `metadata_bytes_after_add`, `lookup_ns_min_of_3` |

- `greedy_bound`: 단위별 요청 수를 큰 것부터 가장 한가한 노드에 넣었을 때의 max/mean. 표는 바꾸지 않는다. "어떤 재배치 정책도 이 아래로는 못 간다"는 한계다. `supportsMove()`가 false인 variant는 `NA`.
- `hottest_key_share` × 노드 수가 `greedy_bound`의 하한이다. key 하나는 어떤 분배로도 나눌 수 없다.
- `lookup_ns`: 워밍업 1회 후 100만 회 평균, 3회 중 최소. JMH가 아니므로 자릿수만 믿는다.

## 결과 파일

`summary.csv`는 long 형식이다: `variant, scenario, G, nodes, keys, theta, seed, metric, value`. `theta`가 없는 시나리오는 `-`, 적용 불가 지표는 `NA`. seed별 원본 행을 그대로 두고 평균·범위는 `results.md`에서 계산한다.
