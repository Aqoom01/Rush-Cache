# split-comparison

Split 정책 variant를 같은 입력으로 돌려, 분할 후 다음 window의 요청이 얼마나 고르게 나뉘는지와 효과 없는 분할을 피하는지 비교한다. 서버·네트워크·실제 시계를 쓰지 않는 in-process 실험이다. 설계는 [../design.md](../design.md) §6, 결과 해석은 [results.md](results.md).

## 실행

```bash
./gradlew :cache-core:testClasses
java -cp cache-core/build/classes/java/main:cache-core/build/classes/java/test \
    io.openkache.core.balance.experiment.SplitComparison \
    experiments/b2-load-balancing/split-comparison/summary.csv
```

러너는 `cache-core/src/test/java/io/openkache/core/balance/experiment/`에 있다. JUnit 테스트가 아니라 `main`이라 `./gradlew test`에서는 돌지 않는다(돌 때마다 결과 파일을 쓰지 않기 위해). 틀 자체의 검증은 같은 패키지의 `SplitComparisonTest`가 한다. 실행은 1초 안쪽이고, 요약 표를 표준 출력으로 낸다.

## 동일 조건

두 정책은 같은 seed에서 같은 key와 같은 요청열을 받는다. 정책과 θ는 결정에만 영향을 준다.

| 항목 | 값 |
|---|---|
| Tablet | 해시 공간 전체 하나 (`TabletRange.FULL`) |
| key | seed별 16바이트 난수 1,000개 → `KeyHash`. 해시값 오름차순 |
| window당 요청 | 4,000건 (모두 읽기로 센다. 정책은 읽기+쓰기 합만 본다) |
| 결정 / 측정 | window 0의 관찰로 결정하고, 같은 분포에서 새로 뽑은 window 1로 효과를 잰다 |
| 분할 시작 조건 | 생략. 요청 4,000건이 T(1,000)의 4배라 항상 통과하므로 두 정책에 차이를 만들지 않는다 |

## 변수

| 변수 | 값 |
|---|---|
| variant | `midpoint-split`, `split-for-heat` |
| 시나리오 | S1 균등, S2 앞쪽(해시값이 작은 쪽) 20% key에 요청 80%, S3 가운데 key 하나에 요청 90% |
| θ | 0.6, 0.7, 0.8, 0.9. `split-for-heat`의 보류 기준이자 "효과 없는 분할" 판정 기준. `midpoint-split`의 결정에는 영향이 없다 |
| seed | 1-10 |

## 지표 (`metric` 열)

| 지표 | 정의 |
|---|---|
| `split` | 분할했으면 1 |
| `single_hot_key` | `SINGLE_HOT_KEY`로 보류했으면 1 |
| `decided_larger_share` | 결정에 쓴 window 0에서 큰 쪽 요청 / 전체. 분할하지 않았으면 `NA` |
| `next_larger_share` | 다음 window 1에서 큰 쪽 요청 / 전체. **효과는 이 값으로 판단한다.** 이상값 0.5 |
| `ineffective_split` | 분할했는데 `next_larger_share` > θ면 1. 분할하지 않았으면 `NA` |
| `hottest_key_share` | window 1에서 가장 많이 받은 key 하나의 비율. 어떤 분할로도 큰 쪽이 이 값 아래로 갈 수 없다 |

## 결과 파일

`summary.csv`는 long 형식이다: `variant, scenario, theta, seed, metric, value`. seed별 원본 행을 그대로 두고 평균·범위는 `results.md`에 적는다. Metrics 결과 Schema가 합의되면 그 형식으로 옮긴다.
