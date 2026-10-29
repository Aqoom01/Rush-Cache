# distribution-comparison

분배 방식 variant를 같은 입력으로 돌려 균형·이동량·표 크기·조회 비용을 비교한다. 서버·네트워크·실제 시계를 쓰지 않는 in-process 실험이다. 설계는 [../design.md](../design.md) §6, 결과 해석은 [results.md](results.md).

## 실행

```bash
./gradlew :cache-core:testClasses
java -cp cache-core/build/classes/java/main:cache-core/build/classes/java/test \
    io.openkache.core.tablet.experiment.DistributionComparison \
    experiments/b1-tablet-distribution/distribution-comparison/summary.csv
```

러너는 `cache-core/src/test/java/io/openkache/core/tablet/experiment/`에 있다. JUnit 테스트가 아니라 `main`이라 `./gradlew test`에서는 돌지 않는다(돌 때마다 결과 파일을 쓰지 않기 위해). 틀 자체의 검증은 같은 패키지의 `MetricsTest`가 한다. 전체 실행은 variant 5개로 약 10분이다(`ring-vnode-alloc`의 S4가 5분).

## 동일 조건

variant마다 잘게 자르는 손잡이가 다르다. 같은 조건으로 비교하려고 **전체 단위 수 G**(노드 8대 기준 표의 줄 수)를 공통 변수로 두고 variant별로 환산한다.

| variant | 손잡이 | 환산 (기준 노드 8대) |
|---|---|---|
| `fixed-slot` | 전체 슬롯 수 S | S = G |
| `hash-range-tablet` | 노드당 Tablet 수 T | T = G / 8 |
| `ring-vnode` | 노드당 토큰 수 V (무작위 토큰) | V = G / 8 |
| `ring-vnode-alloc` | 노드당 토큰 수 V (노드별 소유량이 고르도록 계산한 토큰) | V = G / 8 |
| `hash-tag-tablet` | 노드당 Tablet 수 T (표는 hash-range-tablet과 같고 key → 해시만 hash tag) | T = G / 8 |

그 밖의 입력은 variant와 무관하게 같다: key(seed별 16바이트 난수 → `KeyHash`), 노드 ID 1..n, 노드 추가·제거 순서, Zipf 요청열(seed별). 지표는 전부 `RoutingTable`의 공개 연산(`lookup`, `ownedBy`, `tablets`)으로만 계산하므로 variant 코드가 지표에 끼어들지 않는다. S7만 실제 key 바이트열(`{g<묶음>}:<번호>`)을 만들어 variant의 `keyHash`로 해시한다. tag를 보는 variant와 key 전체를 보는 variant가 여기서 갈린다.

## 변수

| 변수 | 값 |
|---|---|
| G | 256, 1024, 4096, 16384 |
| K (key 수) | 1만, 10만, 100만 (S1만). 나머지는 100만 |
| θ (Zipf) | 0.9, 0.99, 1.2 (S5만). 요청 100만 건 |
| seed | 1~5 |
| 노드 | 8대 기준. S2는 +1, S3는 −1, S4는 1 → 16 → 8 |
| P (S7 묶음 수) | 100, 1천, 1만, 10만. key 100만 개를 묶음에 나눠 담는다 |
| θ_g (S7 묶음 크기 쏠림) | 0 (거의 균등), 0.99 (큰 묶음 몇 개가 key의 상당 부분). `theta` 열에 기록 |

## 시나리오와 지표

| 시나리오 | 내용 | 지표 (`metric` 열) |
|---|---|---|
| S1 | 초기 표에 key 배치 | `unit_max_mean` 노드별 단위 수 max/mean (결정적), `key_max_mean` 노드별 key 수 max/mean |
| S2 | 노드 1대 추가 | `moved_key_ratio`, `moved_unit_ratio`, `ideal_ratio` = 1/(n+1), `old_to_old_moves` 새 노드가 아닌 곳으로 간 이동 수, `*_after` 추가 후 균형 |
| S3 | 노드 1대 제거 | S2와 같음. `ideal_ratio` = 1/n |
| S4 | 1대씩 16대까지 늘리고 8대로 줄임 | `cumulative_moved_*_ratio` 누적 이동, `ideal_cumulative_ratio` Σ 1/n, `worst_unit_max_mean` 전 단계 중 최악, `final_key_max_mean` |
| S5 | Zipf 요청 쏠림 | `request_max_mean` 그대로 뒀을 때, `greedy_bound` 단위를 통째로 옮겨 도달 가능한 최선(LPT), `hottest_unit_share`, `hottest_key_share` |
| S7 | 태그 묶음. key 100만 개를 tag 묶음 P개에 담고 Zipf θ=0.99 요청 | `key_max_mean`, `largest_group_share` 가장 큰 묶음의 key 비율, `groups_split_ratio` key가 두 노드 이상에 흩어진 묶음 비율, `same_node_pair_ratio` 같은 묶음 두 key가 같은 노드에 있을 확률, `request_max_mean`, `greedy_bound`, `hottest_unit_share`, `moved_key_ratio_add` 노드 1대 추가 시 이동 비율 |
| AUX | 표 크기·조회 | `metadata_bytes_initial`, `metadata_bytes_after_add`, `lookup_ns_min_of_3` |

- `greedy_bound`: 단위별 요청 수를 큰 것부터 가장 한가한 노드에 넣었을 때(LPT)의 max/mean. 표는 바꾸지 않는다. **단위를 통째로 옮기는 재배치로 내릴 수 있는 값**이지 하한이 아니다(LPT는 최적의 4/3 이내이고 보통 최적과 같다). 진짜 하한은 `hottest_unit_share` × 노드 수(단위 하나는 못 나눔)이며, 두 값이 같으면 LPT가 최적에 닿은 것이다. `supportsMove()`가 false인 variant는 `NA`.
- `hottest_key_share` × 노드 수는 split까지 해도 못 내려가는 하한이다. key 하나는 어떤 분배로도 나눌 수 없다.
- `lookup_ns`: 워밍업 1회 후 100만 회 평균, 3회 중 최소. JMH가 아니므로 자릿수만 믿는다.
- `groups_split_ratio`: key가 2개 이상인 묶음만 센다. `same_node_pair_ratio`: 묶음 크기로 가중한 순서쌍 기준. multi-key 요청이 한 노드에서 끝날 확률이다. key 전체를 해시하면 노드 8대에서 약 0.125, tag를 해시하면 1.0.

## 결과 파일

`summary.csv`는 long 형식이다: `variant, scenario, G, nodes, keys, theta, groups, seed, metric, value`. `theta`·`groups`가 없는 시나리오는 `-`, 적용 불가 지표는 `NA`. S7의 `theta`는 묶음 크기 쏠림 θ_g다(요청 쏠림은 0.99 고정). seed별 원본 행을 그대로 두고 평균·범위는 `results.md`에서 계산한다.
