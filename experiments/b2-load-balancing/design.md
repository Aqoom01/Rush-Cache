# B2 설계

상태: B1의 hash 범위 Tablet(`core.tablet.TabletRange`) 위에서 Baseline(`midpoint-split`)과 `split-for-heat`, 공통 불변식 테스트, 시나리오 S1-S3 비교 실험을 구현했다. 첫 결과는 [split-comparison/results.md](split-comparison/results.md). 분할 시작 단계(`SplitTrigger`)는 아직 구현하지 않았다. 구현 현황은 §7을 따른다.

## 1. 목표와 범위

B2는 Tablet의 부하를 관찰해 세 가지를 결정한다.

| 기능 | 결정 내용 | 단계 |
|---|---|---|
| Hot Split | 부하가 몰린 Tablet을 어디서 나눌지, 나눠도 효과가 있는지 | 1단계 (지금) |
| Cold Merge | 부하가 낮은 인접 Tablet을 합칠지 | 이후 |
| 재배치 | 어떤 Tablet을 어느 노드로 옮길지 | 이후 |

1단계는 Split 하나만 다룬다. 참고 기법은 [DynamoDB Split-for-Heat](references/dynamodb-split-for-heat.md)이고, 구간의 가운데에서 나누는 분할(Baseline)과 비교한다. 논문 §4.4가 비교 대상으로 든 방식이 이것이다.

B2는 **계획만 만든다**. 실제 데이터 이동은 B3(`core.migration`)가, Tablet의 소유와 라우팅은 B1(`core.tablet`)이 맡는다.

## 2. 다른 연구와의 경계

| 대상 | B2가 쓰는 것 | 현재 상태 | 대응 |
|---|---|---|---|
| B1 Tablet 구조 | `TabletRange`(부호 없는 64비트 hash 구간), `KeyHash`, `RoutingTable.withSplit(id, at)` | 구현·병합됨 | B2 입력은 `TabletRange`와 key 해시값을 쓴다. `Split(at)`의 `at`을 `withSplit`에 그대로 넘긴다 |
| B1 분배 방식 | split을 지원하는 variant | `fixed-slot`은 `supportsSplit() = false`. `hash-range-tablet`은 설계 단계 | 정책 단위 테스트는 B1 variant 없이 진행. 실제 표 위 실험은 `hash-range-tablet` 구현 후 |
| B3 Migration | Split·이동 계획 실행 | 미구현 | 계획 생성까지만 하고 실행하지 않는다 |
| A2 Workload | 부하 패턴 생성 | Uniform, 고정 Hot-Key만 있음 | Hot Range 패턴은 B2 테스트 안에서 생성한다. 필요성이 확인되면 A2에 공통화를 요청한다 |
| Metrics (본인) | 실험 결과 기록 | 결과 Schema 합의 전 | 1단계 결과는 테스트 출력으로 확인하고, Schema 확정 후 JSON 기록으로 옮긴다 |

**B1의 hash 범위 결정 반영 (2026-10-22).** Tablet은 key 바이트 순서가 아니라 key hash(64비트) 구간이다(B1 design.md §2). 그래서 다음을 바꿨다.

- B2 임시 타입 `KeyRange`(바이트 key 범위)를 지우고 B1 `TabletRange`를 쓴다.
- Baseline을 "key 순서상 가운데 key"에서 "hash 구간의 산술적 가운데"로 바꿨다. hash 구간은 양 끝이 정해져 있어 가운데가 항상 정의된다.
- 순차 증가 key의 쓰기는 hash로 여러 Tablet에 흩어지므로, 순차 쓰기 처리(tail 예측, `SEQUENTIAL_WRITES`)와 시나리오 S4·S5를 뺐다.

## 3. 입력: 부하 관찰

정책은 Tablet 하나의 관찰 결과(`TabletLoad`)를 입력으로 받는다.

| 항목 | 의미 |
|---|---|
| 구간 | B1 `TabletRange` `[start, end)`. 부호 없는 64비트, `end = 0`은 2^64 |
| 관찰 구간 | 논리 시간 window 번호. 실제 시계를 쓰지 않는다 |
| key별 요청 수 | window 안에서 key마다 읽기 수, 쓰기 수. key는 해시값(`KeyHash.of`)으로만 다루며 해시값 오름차순(부호 없음) |

- "앞쪽 key"는 해시값이 작은 key를 뜻한다. key 이름의 순서와는 무관하다.
- 1단계는 key별로 정확히 센다. 실제 시스템은 샘플링하지만, 정확한 값으로 먼저 정책의 차이를 확인한다.
- CPU·IOPS는 1단계 입력에 넣지 않는다. 요청 수만으로 분할 지점 선택의 차이를 먼저 확인한다.

## 4. 출력: 분할 결정

```
SplitPolicy.decide(TabletLoad) → Split(at) | NoSplit(reason)
```

`at`은 오른쪽 조각의 시작 해시값이다. 왼쪽은 `[start, at)`, 오른쪽은 `[at, end)`가 된다.

| NoSplit 사유 | 의미 | 판단 위치 | 상태 |
|---|---|---|---|
| `BELOW_THRESHOLD` | 분할 시작 조건(§4.1)을 만족하지 않는다 | 공통 단계 | 미구현 |
| `COOLDOWN` | 직전 분할로 생긴 Tablet이 유예 기간 안에 있다 | 공통 단계 | 미구현 |
| `NO_VALID_POINT` | 나눌 자리가 없다. 구간 폭이 1이거나, `split-for-heat`에서 key가 둘 미만 | 정책 | 구현 |
| `SINGLE_HOT_KEY` | 부하가 key 하나에 몰려, 어디서 나눠도 그 key가 있는 쪽이 θ를 넘는다 | `split-for-heat` | 구현 |

아래 값과 규칙은 참고 자료에 공개되어 있지 않아 **프로젝트에서 직접 정했다**. 근거는 각 항목에 적는다.

### 4.1 분할 시작 조건 (두 정책 공통)

두 정책은 분할을 시작하는 조건이 같다. 그래야 결과 차이가 "어디서 자르는가"와 "언제 보류하는가"에서만 나온다.

| 파라미터 | 기본값 | 근거 |
|---|---|---|
| window | 가상 시간 1단위 | in-process 실험이라 실제 시계를 쓰지 않는다 |
| T (Tablet 처리 한도) | 1,000 요청/window | DynamoDB의 partition 처리량 한도에 해당하는 추상값. 실험 부하를 window당 2,000건 이상으로 주면 분할 대상이 된다 |
| K (지속 기간) | 3 window 연속 | 블로그가 높은 부하가 지속될(sustained) 때 분할한다고 설명한다. 일시적으로 튀는 부하로는 자르지 않는다 |
| 분할 후 유예 | 3 window | 분할로 생긴 두 Tablet은 이 기간 동안 다시 분할하지 않는다. 효과를 다음 window로 측정하므로(§6) 그 사이에 또 잘리면 측정이 섞이고, 연쇄 분할도 막는다 |

T와 K는 이 실험의 비교 대상이 아니므로 두 정책에 똑같이 적용하고 바꾸지 않는다.

### 4.2 분할 지점

| variant | 분할 지점 | 효과 없는 경우 판단 |
|---|---|---|
| `midpoint-split` (Baseline) | hash 구간의 산술적 가운데. 전체 공간이면 2^63. key 분포와 요청 부하는 보지 않는다 | 하지 않음. 구간 폭이 1일 때만 `NO_VALID_POINT` |
| `split-for-heat` | 왼쪽 요청 합이 전체의 절반에 가장 가까워지는 key (§4.3) | 큰 쪽 비율이 θ를 넘으면 `SINGLE_HOT_KEY` |

Baseline은 key가 구간 한쪽에 몰려 있으면 빈 조각을 만들 수 있다. 이것도 비교 대상인 "가운데 분할"의 특성이라 그대로 둔다.

### 4.3 `split-for-heat`의 분할 지점과 보류 판단

**효과 판단 기준 θ = 0.8.** 분할 후 큰 쪽의 요청 비율이 0.8 이하여야, 즉 가장 뜨거운 쪽에서 최소 20%를 덜어 내야 분할한다.

**절차**

1. key가 둘 미만이면 `NO_VALID_POINT`.
2. key를 해시값 순서로 놓고, k번째 key가 오른쪽의 첫 key일 때의 큰 쪽 요청 수를 모든 k(1 이상 n-1 이하)에 대해 계산한다. 가장 작은 k를 고른다. 같은 값이 여럿이면 key 개수의 가운데(n/2)에 가까운 k를 고른다.
3. 그 k에서 큰 쪽 비율이 θ 이하면 `Split(k번째 key의 해시값)`, 넘으면 `SINGLE_HOT_KEY`.

분할 지점이 실제 key의 해시값이므로 두 조각 모두에 key가 하나 이상 있다.

분할은 한 번만 자르므로 뜨거운 key 하나를 따로 떼어 낼 수 없다. 가운데 key 하나가 비율 s를 받고 나머지가 고르게 받으면, 가장 잘 잘라도 큰 쪽은 (1 + s) / 2다. 따라서 θ = 0.8은 "가운데 key 하나가 60%를 넘게 받으면 보류"와 같다.

**예시** (key 1,000개를 hash 공간에 고르게 둠. `HeatSplitPolicyTest`와 같은 값)

| 상황 | split-for-heat 계산 | 결정 |
|---|---|---|
| 앞쪽 20% key에 요청 80% (16건 × 200개, 1건 × 800개, 전체 4,000건) | 125번 key에서 2,000 : 2,000 → 0.50 | `Split` (Baseline은 2^63에서 잘라 3,500 : 500 → 0.875) |
| 가운데 key 하나에 50% (999건, 나머지 1건씩, 전체 1,998건) | 500 : 1,498 → 0.75 | `Split` |
| 가운데 key 하나에 90% (8,991건, 나머지 1건씩, 전체 9,990건) | 가장 잘 잘라도 500 : 9,490 → 0.95 | `SINGLE_HOT_KEY` |

## 5. 불변식

정책 구현은 모두 아래를 지켜야 하며, `SplitInvariantTest`가 모든 정책에 똑같이 검사한다.

1. `start < at < end`다. 두 조각은 모두 비어 있지 않다(B1 `TabletRange.splitAt`이 받아들이는 지점).
2. 두 조각 `[start, at)`, `[at, end)`는 원래 구간을 빈틈과 겹침 없이 덮고, 모든 key는 정확히 한쪽에 들어간다.
3. 같은 입력에는 같은 결정을 낸다. 난수와 실제 시각을 쓰지 않는다.
4. 입력을 변경하지 않는다.
5. `NoSplit`에는 반드시 사유가 있다. 나눌 자리가 없으면(폭 1 구간) `NO_VALID_POINT`다.
6. 같은 window의 요청을 두 조각으로 다시 나누면, 양쪽 요청 수의 합은 원래 Tablet의 요청 수와 같다.

정책별 성질은 정책 테스트에서 본다. 예: `split-for-heat`는 두 조각 모두에 key가 있다(Baseline은 보장하지 않음).

## 6. 첫 실험: Baseline과 Split-for-Heat 비교

### 질문

요청 부하를 고려해 분할 지점을 고르면, 구간의 가운데에서 나누는 것보다 부하가 실제로 더 고르게 나뉘는가? 효과 없는 분할을 피할 수 있는가?

### 시나리오

Tablet 하나에 key 1,000개를 hash 공간에 두고, window당 요청을 분할 시작 조건 T의 2배 이상으로 아래 패턴에 따라 보낸다.

| 시나리오 | 요청 패턴 | 예상 |
|---|---|---|
| S1 균등 | 모든 key에 고르게 | 두 정책의 결과가 비슷하다 |
| S2 치우친 Hot Range | 요청의 80%가 해시값이 작은 쪽 20% key에 몰림 | Baseline은 0.875 정도로 한쪽에 몰리고, Split-for-Heat는 0.5에 가깝게 나뉜다 |
| S3 단일 Hot Key | 가운데 key 하나가 요청의 90%를 받음 | Baseline은 분할하지만 효과가 없다. Split-for-Heat는 `SINGLE_HOT_KEY`로 보류한다 |

"예상"은 가설이다. 결과가 다르면 그대로 기록한다. 이전 설계의 S4·S5(순차 쓰기)는 hash 범위에서 의미가 없어 뺐다(§2).

### 측정

효과는 **분할 결정에 쓴 window가 아니라 그다음 window**의 실제 요청으로 판단한다. 같은 window로 판단하면 정책이 자기 입력을 맞춘 것에 불과하기 때문이다.

| 지표 | 정의 | 이상적인 값 |
|---|---|---|
| 큰 쪽 부하 비율 | 다음 window에서 두 Tablet 중 요청이 많은 쪽 / 합계 | 0.5 |
| 분할 횟수 | 정책이 `Split`을 낸 횟수 | 시나리오별로 다름 |
| 보류 사유별 횟수 | `NoSplit` 사유별 개수 | S3에서 `SINGLE_HOT_KEY` |
| 효과 없는 분할 수 | 분할했지만 다음 window의 큰 쪽 비율이 θ(0.8)를 넘은 횟수 | 0에 가까울수록 좋음 |

**θ 민감도:** θ를 0.6, 0.7, 0.8, 0.9로 바꿔 S1-S3를 다시 실행한다. θ가 너무 낮으면 쓸 만한 분할도 보류하고, 너무 높으면 효과 없는 분할을 허용한다. 기본값 0.8을 직접 정했으므로, 그 선택이 결과를 얼마나 좌우하는지를 함께 보고한다. T와 K는 바꾸지 않는다.

1차 실행에서 S1-S3로는 θ의 영향이 드러나지 않았다. S2는 어떤 θ보다 낮게, S3는 어떤 θ보다 높게 잘리기 때문이다. θ는 뜨거운 key 하나의 비율 s가 2θ - 1 근처(θ = 0.6-0.9이면 s = 20-80%)일 때 결정을 가르므로, 민감도를 보려면 s를 그 범위에서 바꾸는 시나리오를 추가한다.

### 실행 환경

- in-process로 실행한다. 서버, 네트워크, 실제 시계를 쓰지 않는다.
- 요청 생성은 seed를 고정한다. 시나리오마다 seed 여러 개로 반복한다.
- 시간은 논리 window 번호다. 결과의 time_domain은 virtual이다.
- 서버 실측(분할 전후 p99 latency)은 B1 `hash-range-tablet`과 B3 이동이 구현된 뒤에 한다.

## 7. 코드 구조

```
cache-core/src/main/java/io/openkache/core/balance/
├── package-info.java              (있음)
├── KeyLoad.java                   [구현] key 해시값 하나의 window 내 읽기·쓰기 수
├── TabletLoad.java                [구현] Tablet 하나의 window 관찰 결과 (B1 TabletRange + key별 요청 수)
└── split/
    ├── SplitPolicy.java           [구현] variant(), decide(TabletLoad) → SplitDecision
    ├── SplitDecision.java         [구현] Split(at) | NoSplit(reason)
    ├── NoSplitReason.java         [구현] NO_VALID_POINT, SINGLE_HOT_KEY. 공통 단계 사유는 SplitTrigger 구현 때 추가
    ├── MidpointSplitPolicy.java   [구현] Baseline
    ├── HeatSplitPolicy.java       [구현] Split-for-Heat (θ 기본 0.8, 생성자로 변경 가능)
    └── SplitTrigger.java          두 정책 공통: 분할 시작 조건(T, K)과 분할 후 유예 (§4.1)
(이후) merge/, placement/

cache-core/src/test/java/io/openkache/core/balance/
├── TabletLoadTest.java            [구현] 입력 검증(부호 없는 정렬, 중복, 구간 밖, 음수)
└── split/
    ├── Loads.java                 [구현] 테스트 입력 생성 도우미 (key를 hash 구간에 고르게 배치)
    ├── SplitInvariantTest.java    [구현] §5 불변식을 모든 정책에 적용. 새 정책은 POLICIES에 추가
    ├── MidpointSplitPolicyTest.java [구현]
    └── HeatSplitPolicyTest.java   [구현] §4.3 예시 표, θ 경계, 두 조각 모두 key
└── experiment/                    [구현] §6 비교 실험 (JUnit이 아닌 main. B1 tablet/experiment와 같은 방식)
    ├── SplitComparison.java       실험 러너. 결과를 long 형식 CSV로 쓰고 요약 표를 출력
    ├── Scenarios.java             seed별 key 해시값, 시나리오 S1-S3 요청 생성
    └── SplitComparisonTest.java   실험 틀(입력 생성, 지표 계산, 결정성) 검증

experiments/b2-load-balancing/
├── README.md                      variant 목록
├── design.md                      이 문서
├── references/                    논문·시스템별 정리
└── split-comparison/              [구현] 실행 방법(README.md), 원본(summary.csv), 해석(results.md)
```

규칙:

- **패키지는 기능별로 나눈다** (`split`, `merge`, `placement`). 논문별로 나누지 않는다. 같은 기능의 정책들을 같은 인터페이스로 비교해야 하기 때문이다.
- **논문 하나는 정책 클래스 하나**가 된다. 클래스 Javadoc에 참고 문헌을 적고, `references/`의 정리 문서와 이름을 맞춘다.
- **`SplitPolicy` 인터페이스는 지금 둔다.** 구현이 처음부터 둘(Baseline, Split-for-Heat)이라 근거 없는 추상화가 아니다. `merge`, `placement`의 인터페이스는 두 번째 정책이 생길 때 만든다.
- **실험 폴더는 논문이 아니라 비교하는 질문 기준으로 만든다.** `split-comparison/`은 모든 Split 정책을 같은 시나리오로 비교한다. Split 논문을 추가하면 같은 폴더의 실험에 variant가 하나 늘어난다. Merge·재배치 실험은 `merge-comparison/`, `placement-comparison/`처럼 따로 만든다.
- **Tablet 관련 공통 타입은 B1 것을 쓴다.** B2가 따로 범위 타입을 만들지 않는다. `core.model`, `core.api`에도 올리지 않는다.

## 8. 미결 사항

| 항목 | 결정할 사람 | 기한 |
|---|---|---|
| 실험 결과 JSON 형식 | Metrics(본인), 팀 합의 | 결과 Schema 합의 후 |
| B1 `hash-range-tablet` 구현 시점 (실제 표 위 split 실험 조건) | B1 | B1 일정 |
| Hot Range 패턴의 Workload 공통화 | A2 | 필요성 확인 후 |
| Cold Merge, 재배치의 참고 기법 | B2 | Split 1단계 후 |

**결정된 사항 (프로젝트 자체 결정)**

| 항목 | 결정 | 위치 | 날짜 |
|---|---|---|---|
| 분할 시작 조건 | 부하 > 1,000 요청/window가 3 window 연속. 두 정책 공통 | §4.1 | 2026-10-19 |
| 분할 후 유예 | 3 window. 두 정책 공통 | §4.1 | 2026-10-19 |
| 효과 판단 기준 θ | 0.8. 민감도는 0.6/0.7/0.8/0.9로 확인 | §4.3, §6 | 2026-10-19 |
| Tablet 표현 | B1 hash 구간(`TabletRange`)을 쓴다. B2 임시 `KeyRange`와 바이트 key 비교 규칙은 삭제 | §2, §3 | 2026-10-22 |
| Baseline 정의 | hash 구간의 산술적 가운데 | §4.2 | 2026-10-22 |
| 순차 쓰기 처리 | hash 범위에서 의미가 없어 제외 (이전 결정: tail 요청을 오른쪽 몫으로 예상) | §2 | 2026-10-22 |
