# B2 설계

상태: 초안. 아래 클래스와 실험은 아직 구현하지 않았다.

## 1. 목표와 범위

B2는 Tablet의 부하를 관찰해 세 가지를 결정한다.

| 기능 | 결정 내용 | 단계 |
|---|---|---|
| Hot Split | 부하가 몰린 Tablet을 어디서 나눌지, 나눠도 효과가 있는지 | 1단계 (지금) |
| Cold Merge | 부하가 낮은 인접 Tablet을 합칠지 | 이후 |
| 재배치 | 어떤 Tablet을 어느 노드로 옮길지 | 이후 |

1단계는 Split 하나만 다룬다. 참고 기법은 [DynamoDB Split-for-Heat](references/dynamodb-split-for-heat.md)이고, key 범위의 가운데에서 나누는 분할(Baseline)과 비교한다. 논문 §4.4가 비교 대상으로 든 방식이 이것이다.

B2는 **계획만 만든다**. 실제 데이터 이동은 B3(`core.migration`)가, Tablet의 소유와 라우팅은 B1(`core.tablet`)이 맡는다.

## 2. 다른 연구와의 경계

| 대상 | B2가 기대하는 것 | 현재 상태 | 1단계 대응 |
|---|---|---|---|
| B1 Tablet 구조 | Tablet의 Key Range, owner, version | 미정 | B2 내부 최소 타입(`KeyRange`, `TabletLoad`)을 `core.balance`에 두고, B1 확정 후 교체한다. `core.model`에는 올리지 않는다 |
| B3 Migration | Split·이동 계획 실행 | 미구현 | 계획 생성까지만 하고 실행하지 않는다 |
| A2 Workload | 부하 패턴 생성 | Uniform, 고정 Hot-Key만 있음 | Hot Range, 순차 증가 패턴은 B2 테스트 안에서 생성한다. 필요성이 확인되면 A2에 공통화를 요청한다 |
| Metrics (본인) | 실험 결과 기록 | 결과 Schema 합의 전 | 1단계 결과는 테스트 출력으로 확인하고, Schema 확정 후 JSON 기록으로 옮긴다 |

**확인 필요 (B1):** Split 지점을 고르려면 key가 정렬된 공간에 있어야 한다. Tablet이 원래 key의 바이트 순서 범위인지, key hash의 범위인지에 따라 "순차 증가 key" 시나리오의 의미가 달라진다. hash 범위라면 순차 key의 쓰기는 여러 Tablet에 흩어진다.

## 3. 입력: 부하 관찰

정책은 Tablet 하나의 관찰 결과를 입력으로 받는다.

| 항목 | 의미 |
|---|---|
| Key Range | `[start, end)`. key는 바이트열, 바이트 사전순으로 비교한다. 양 끝은 무한대를 허용한다 |
| 관찰 구간 | 논리 시간 window 번호. 실제 시계를 쓰지 않는다 |
| key별 요청 수 | window 안에서 key마다 읽기 수, 쓰기 수 |
| 범위 안의 key 목록 | Baseline이 가운데 key를 고를 때 쓴다 |
| 이전 최대 key | 이번 window 시작 전까지 이 Tablet에서 본 가장 큰 key. 순차 쓰기 예측(§4.3)에 쓴다. 기록이 없으면 없음 |

- 1단계는 key별로 정확히 센다. 실제 시스템은 샘플링하지만, 정확한 값으로 먼저 정책의 차이를 확인한다.
- CPU·IOPS는 1단계 입력에 넣지 않는다. 요청 수만으로 분할 지점 선택의 차이를 먼저 확인한다.

## 4. 출력: 분할 결정

```
SplitPolicy.decide(TabletLoad) → Split(splitKey) | NoSplit(reason)
```

| NoSplit 사유 | 의미 | 판단 위치 |
|---|---|---|
| `BELOW_THRESHOLD` | 분할 시작 조건(§4.1)을 만족하지 않는다 | 공통 단계 |
| `COOLDOWN` | 직전 분할로 생긴 Tablet이 유예 기간 안에 있다 | 공통 단계 |
| `NO_VALID_POINT` | 범위 안에 유효한 분할 지점이 없다 (key가 하나뿐 등) | 정책 |
| `SINGLE_HOT_KEY` | 부하가 key 하나에 몰려, 어디서 나눠도 그 key가 있는 쪽이 기준을 넘는다 | `split-for-heat` |
| `SEQUENTIAL_WRITES` | 쓰기가 계속 증가하는 key 끝에 몰려, 나눠도 오른쪽 Tablet만 뜨거워진다 | `split-for-heat` |

아래 값과 규칙은 참고 자료에 공개되어 있지 않아 **프로젝트에서 직접 정했다**. 근거는 각 항목에 적는다.

### 4.1 분할 시작 조건 (두 정책 공통)

두 정책은 분할을 시작하는 조건이 같다. 그래야 결과 차이가 "어디서 자르는가"와 "언제 보류하는가"에서만 나온다.

| 파라미터 | 기본값 | 근거 |
|---|---|---|
| window | 가상 시간 1단위 | in-process 실험이라 실제 시계를 쓰지 않는다 |
| T (Tablet 처리 한도) | 1,000 요청/window | DynamoDB의 partition 처리량 한도에 해당하는 추상값. 실험 부하를 window당 2,000건 정도로 주면 분할 대상이 된다 |
| K (지속 기간) | 3 window 연속 | 블로그가 높은 부하가 지속될(sustained) 때 분할한다고 설명한다. 일시적으로 튀는 부하로는 자르지 않는다 |
| 분할 후 유예 | 3 window | 분할로 생긴 두 Tablet은 이 기간 동안 다시 분할하지 않는다. 효과를 다음 window로 측정하므로(§6) 그 사이에 또 잘리면 측정이 섞이고, 연쇄 분할도 막는다 |

T와 K는 이 실험의 비교 대상이 아니므로 두 정책에 똑같이 적용하고 바꾸지 않는다.

### 4.2 분할 지점

| variant | 분할 지점 | 효과 없는 경우 판단 |
|---|---|---|
| `midpoint-split` (Baseline) | 범위 안 key를 순서대로 놓았을 때 가운데 key. 요청 부하는 보지 않는다 | 하지 않음. 분할 시작 조건을 만족하면 항상 분할 |
| `split-for-heat` | 왼쪽 부하가 전체의 절반에 가장 가까워지는 key (§4.3) | 큰 쪽 예상 부하 비율이 θ를 넘으면 보류 (§4.3) |

Baseline의 "가운데"는 바이트열 key 공간의 산술적 중간이 아니라 존재하는 key의 순서상 가운데로 정한다. 실험의 key가 범위 안에 고르게 퍼져 있으면 둘은 거의 같다. 산술적 중간은 범위 끝이 무한대일 때 정의하기 어렵기 때문이다.

### 4.3 `split-for-heat`의 예상 부하 계산과 보류 판단

**효과 판단 기준 θ = 0.8.** 분할 후 큰 쪽의 예상 부하가 원래의 80% 이하여야, 즉 가장 뜨거운 쪽에서 최소 20%를 덜어 내야 분할한다.

**순차 쓰기는 별도 규칙이 아니라 예상 부하 계산에 반영한다.** 지난 window의 분포만 보면 순차 쓰기에 속는다. 새 key가 계속 끝에 붙으면, 지난 window의 가운데에서 잘라도 다음 window의 새 key는 모두 오른쪽으로 간다.

```text
지난 window: 새 key 4001-4400에 쓰기 → 4200에서 자르면 예상 1,000 : 1,000 (0.5)
다음 window: 새 key 4401-4800에 쓰기 → 실제 0 : 2,000 (1.0)
```

그래서 요청을 두 종류로 나눠 예상한다.

| 구분 | 정의 | 예상할 때 |
|---|---|---|
| 기존 key 요청 | 이전 최대 key 이하인 key로 간 요청 | 분할 지점 기준으로 왼쪽·오른쪽에 나눠 센다 |
| tail 요청 | 이전 최대 key보다 큰 key로 간 요청 | 다음 window에도 끝에 붙는다고 보고 **항상 오른쪽**으로 센다 |

이전 최대 key 기록이 없으면 tail 요청은 없는 것으로 본다. 분할 시작 조건이 3 window 연속이므로 실제 판단 시점에는 기록이 있다.

**절차**

1. 범위 안에 기존 key가 둘 미만이면 `NO_VALID_POINT`.
2. **분할 지점:** 기존 key를 순서대로 누적해 왼쪽 부하가 전체 요청의 절반에 가장 가까운 지점을 고른다. 누적이 절반을 넘기는 key는 왼쪽에 넣는 경우와 오른쪽에 넣는 경우를 모두 계산해 큰 쪽 비율이 작은 쪽을 택한다. 분할된 두 범위에는 기존 key가 하나 이상씩 있어야 한다.
3. **예상 비율:** 왼쪽 = 분할 지점 왼쪽의 기존 key 요청. 오른쪽 = 나머지 기존 key 요청 + tail 요청 전부. 큰 쪽 / 전체를 계산한다.
4. 예상 비율 ≤ θ이면 `Split`.
5. θ를 넘으면 보류하고 사유를 정한다.
   - tail을 오른쪽에 고정하지 않고(일반 key처럼 나눠) 다시 계산했을 때 θ 이하라면, 실패 원인은 tail이므로 `SEQUENTIAL_WRITES`.
   - 그래도 θ를 넘으면, 누적이 절반을 넘기는 key 하나가 어느 쪽에 있어도 기준을 넘는 경우이므로 `SINGLE_HOT_KEY`.

모든 보류가 "분할해도 큰 쪽이 θ를 넘는다"는 한 가지 기준으로 설명된다. 순차 판단용 임계값을 따로 두지 않는다.

분할은 한 번만 자르므로 뜨거운 key 하나를 따로 떼어 낼 수 없다. 범위 가운데의 key 하나가 비율 s를 받고 나머지가 고르게 받으면, 가장 잘 잘라도 큰 쪽은 (1 + s) / 2다. 따라서 θ = 0.8은 "가운데 key 하나가 60%를 넘게 받으면 보류"와 같다.

**예시** (window당 2,000건)

| 상황 | 계산 | 결정 |
|---|---|---|
| 앞쪽 20% key에 요청 80% | 기존 key 누적 절반 지점에서 1,000 : 1,000 → 0.50 | `Split` (Baseline은 가운데에서 잘라 1,750 : 250 → 0.875) |
| 범위 가운데 key 하나에 50%, 나머지 고르게 | 그 key 쪽에 반대편 이웃 key 부하가 같이 남아 1,500 : 500 → 0.75 | `Split` |
| 범위 가운데 key 하나에 90%, 나머지 고르게 | 어느 쪽에 넣어도 1,900 : 100 → 0.95 | `SINGLE_HOT_KEY` |
| tail 1,800건, 기존 key 200건 | 오른쪽 ≥ 1,800 → 0.90 | `SEQUENTIAL_WRITES` |
| tail 600건, 기존 key에 고르게 1,400건 | 왼쪽 기존 1,000 : 오른쪽 기존 400 + tail 600 → 0.50 | `Split` (기존 key 부하는 실제로 나뉜다) |

## 5. 불변식

정책 구현은 모두 아래를 지켜야 하며, 단위 테스트로 검증한다.

1. `start < splitKey < end`이다. 분할된 두 범위는 모두 비어 있지 않다.
2. 두 범위 `[start, splitKey)`, `[splitKey, end)`는 원래 범위를 빈틈과 겹침 없이 덮는다.
3. 같은 입력에는 같은 결정을 낸다. 난수와 실제 시각을 쓰지 않는다.
4. 입력을 변경하지 않는다.
5. `NoSplit`에는 반드시 사유가 있다.
6. 같은 window의 요청을 두 범위로 다시 나누면, 양쪽 요청 수의 합은 원래 Tablet의 요청 수와 같다.

## 6. 첫 실험: Baseline과 Split-for-Heat 비교

### 질문

요청 부하를 고려해 분할 지점을 고르면, key 범위의 가운데에서 나누는 것보다 부하가 실제로 더 고르게 나뉘는가? 효과 없는 분할을 피할 수 있는가?

### 시나리오

Tablet 하나에 key 1,000개를 두고, window당 요청 2,000건(분할 시작 조건 T의 2배)을 아래 패턴으로 보낸다.

| 시나리오 | 요청 패턴 | 예상 |
|---|---|---|
| S1 균등 | 모든 key에 고르게 | 두 정책의 분할 지점과 결과가 비슷하다 |
| S2 치우친 Hot Range | 요청의 80%가 key 범위 앞쪽 20%에 몰림 | Baseline은 0.875 정도로 한쪽에 몰리고, Split-for-Heat는 0.5에 가깝게 나뉜다 |
| S3 단일 Hot Key | 범위 가운데 key 하나가 요청의 90%를 받음 | Baseline은 분할하지만 효과가 없다. Split-for-Heat는 `SINGLE_HOT_KEY`로 보류한다 |
| S4 순차 증가 쓰기 | window마다 새 key 400개를 계속 증가하는 값으로 씀 (요청의 90%) | Baseline은 분할 후 오른쪽만 뜨겁다. Split-for-Heat는 `SEQUENTIAL_WRITES`로 보류한다 |
| S5 일부 순차 쓰기 | 요청의 30%는 S4처럼 새 key에, 70%는 기존 key에 고르게 | Split-for-Heat는 분할하고, 다음 window에도 0.5에 가깝게 나뉜다 |

"예상"은 가설이다. 결과가 다르면 그대로 기록한다.

### 측정

효과는 **분할 결정에 쓴 window가 아니라 그다음 window**의 실제 요청으로 판단한다. 같은 window로 판단하면 정책이 자기 입력을 맞춘 것에 불과하기 때문이다.

| 지표 | 정의 | 이상적인 값 |
|---|---|---|
| 큰 쪽 부하 비율 | 다음 window에서 두 Tablet 중 요청이 많은 쪽 / 합계 | 0.5 |
| 분할 횟수 | 정책이 `Split`을 낸 횟수 | 시나리오별로 다름 |
| 보류 사유별 횟수 | `NoSplit` 사유별 개수 | S3, S4에서 해당 사유 |
| 효과 없는 분할 수 | 분할했지만 다음 window의 큰 쪽 비율이 θ(0.8)를 넘은 횟수 | 0에 가까울수록 좋음 |

**θ 민감도:** θ를 0.6, 0.7, 0.8, 0.9로 바꿔 S1-S5를 다시 실행한다. θ가 너무 낮으면 쓸 만한 분할도 보류하고, 너무 높으면 효과 없는 분할을 허용한다. 기본값 0.8을 직접 정했으므로, 그 선택이 결과를 얼마나 좌우하는지를 함께 보고한다. T와 K는 바꾸지 않는다.

### 실행 환경

- in-process로 실행한다. 서버, 네트워크, 실제 시계를 쓰지 않는다.
- 요청 생성은 seed를 고정한다. 시나리오마다 seed 여러 개로 반복한다.
- 시간은 논리 window 번호다. 결과의 time_domain은 virtual이다.
- 서버 실측(분할 전후 p99 latency)은 Tablet 라우팅(B1)과 이동(B3)이 구현된 뒤에 한다.

## 7. 코드 구조

```
cache-core/src/main/java/io/openkache/core/balance/
├── package-info.java              (있음)
├── KeyRange.java                  B2 내부 최소 타입. B1 Tablet 구조 확정 후 교체
├── TabletLoad.java                관찰 window의 key별 요청 수, 이전 최대 key
└── split/
    ├── SplitTrigger.java          두 정책 공통: 분할 시작 조건(T, K)과 분할 후 유예 (§4.1)
    ├── SplitPolicy.java           decide(TabletLoad) → SplitDecision
    ├── SplitDecision.java         Split(splitKey) | NoSplit(reason)
    ├── MidpointSplitPolicy.java   Baseline
    └── HeatSplitPolicy.java       Split-for-Heat
(이후) merge/, placement/

cache-core/src/test/java/io/openkache/core/balance/split/
├── SplitInvariantTest.java        §5 불변식을 모든 정책에 적용
├── HeatSplitPolicyTest.java
└── SplitScenarioTest.java         §6 시나리오 S1-S5 (요청 패턴 생성 포함)

experiments/b2-load-balancing/
├── README.md                      variant 목록
├── design.md                      이 문서
├── references/                    논문·시스템별 정리
└── split-comparison/              Split 정책 비교 실험의 설정과 결과 요약 (실험 실행 방식 확정 후)
```

규칙:

- **패키지는 기능별로 나눈다** (`split`, `merge`, `placement`). 논문별로 나누지 않는다. 같은 기능의 정책들을 같은 인터페이스로 비교해야 하기 때문이다.
- **논문 하나는 정책 클래스 하나**가 된다. 클래스 Javadoc에 참고 문헌을 적고, `references/`의 정리 문서와 이름을 맞춘다.
- **`SplitPolicy` 인터페이스는 지금 둔다.** 구현이 처음부터 둘(Baseline, Split-for-Heat)이라 근거 없는 추상화가 아니다. `merge`, `placement`의 인터페이스는 두 번째 정책이 생길 때 만든다.
- **실험 폴더는 논문이 아니라 비교하는 질문 기준으로 만든다.** `split-comparison/`은 모든 Split 정책을 같은 시나리오로 비교한다. Split 논문을 추가하면 같은 폴더의 실험에 variant가 하나 늘어난다. Merge·재배치 실험은 `merge-comparison/`, `placement-comparison/`처럼 따로 만든다.
- **B2 내부 타입은 공통 계약이 아니다.** `core.model`, `core.api`에 올리지 않는다. B1과 합의한 공통 타입이 생기면 그것으로 바꾼다.

## 8. 미결 사항

| 항목 | 결정할 사람 | 기한 |
|---|---|---|
| Tablet이 key 범위인지 hash 범위인지 | B1 (B2 의견 전달) | 2주 차 초 |
| 실험 결과 JSON 형식 | Metrics(본인), 팀 합의 | 결과 Schema 합의 후 |
| Hot Range, 순차 패턴의 Workload 공통화 | A2 | 필요성 확인 후 |
| Cold Merge, 재배치의 참고 기법 | B2 | Split 1단계 후 |

**결정된 사항 (2026-10-19, 프로젝트 자체 결정)**

| 항목 | 결정 | 위치 |
|---|---|---|
| 분할 시작 조건 | 부하 > 1,000 요청/window가 3 window 연속. 두 정책 공통 | §4.1 |
| 분할 후 유예 | 3 window. 두 정책 공통 | §4.1 |
| 효과 판단 기준 θ | 0.8. 민감도는 0.6/0.7/0.8/0.9로 확인 | §4.3, §6 |
| 순차 쓰기 처리 | 별도 규칙 없이, tail 요청을 오른쪽 몫으로 예상해 θ로 판단 | §4.3 |
