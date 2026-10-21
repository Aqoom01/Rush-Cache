# Google Slicer와 ScyllaDB Tablets — 해시 범위 Tablet

## 출처

### 논문

[Slicer: Auto-Sharding for Datacenter Applications](https://www.usenix.org/system/files/conference/osdi16/osdi16-adya.pdf)

- Adya et al., Google. USENIX OSDI 2016
- 읽을 부분: **§2.1 Sharding Model**, **§4.1 Assignment Generation**, **§4.3 Fault Tolerance**, **§4.4 Load Balancing**(특히 4.4.1 Weighted-move, 4.4.4 rejected alternative), **§4.5 Strong Consistency**, **§5.1 Production Measurements**

### 문서

[ScyllaDB — Data Distribution with Tablets](https://docs.scylladb.com/manual/master/architecture/tablets.html)

- ScyllaDB 공식 문서. 명세가 직접 언급한 시스템이다
- 읽을 부분: **Load Balancing**, **Scaling Out**, **Absolute number of tablets**

### 관련

- DynamoDB "split for consumption"(ATC 2022 §4.4)은 B2 참고 자료 [dynamodb-split-for-heat.md](../../b2-load-balancing/references/dynamodb-split-for-heat.md)에 정리되어 있다. split을 **언제·어디서** 하느냐는 B2의 결정이고, 이 문서는 split이 가능한 **표의 구조**를 다룬다.

## 핵심 아이디어

key를 해시한 숫자선을 **가변 길이 구간**(Slicer는 slice, ScyllaDB는 tablet)으로 자르고, 중앙에서 "구간 → 담당 서버" 표를 만들어 모든 서버·클라이언트에 뿌린다. 부하가 쏠리면 표를 고친다: 구간을 다른 서버로 **옮기고**, 뜨거운 구간을 **쪼개고**, 한가한 이웃 구간을 **합친다**.

```text
key → 해시 → 숫자선의 어느 구간인가 → 그 구간의 담당 서버
                [  T0  ][ T1 ][   T2   ][T3]      ← 구간 길이가 제각각 (split·merge 결과)
                 서버A   서버B   서버A    서버C
```

고정 슬롯(B1-A)과의 차이는 **구간 수와 길이가 바뀐다**는 것 하나다. 그래서 뜨거운 구간을 반으로 잘라 반쪽만 옮길 수 있다.

## 효과와 한계

### 기대 효과

- 뜨거운 구간을 쪼개서 일부만 옮길 수 있어, 고정 단위보다 세밀하게 균형을 잡는다
- "어느 구간을 어느 서버로"를 직접 정할 수 있다. consistent hashing처럼 밀려난 부하가 랜덤한 곳으로 가지 않는다
- Slicer 운영 결과: 정적 분할 대비 가장 바쁜 서버의 부하 **중앙값 63% 감소**(최대 99.3%), 시간당 key churn 4%, 표 전파 95%가 1.7초 이내 (§5.1)

### 한계

- **표가 필요하다**: 중앙이 표를 만들고 퍼뜨려야 한다. 중앙이 죽으면 표 변경이 멈춘다(요청은 각자 들고 있는 표로 계속 흐름, §4.3)
- **split으로도 key 하나는 못 나눈다**: 구간을 아무리 잘라도 hot key 하나는 한 서버에 남는다 (우리 비교 실험에서도 θ=1.2 바닥 1.52로 확인)
- **표가 계속 커질 수 있다**: split만 하면 구간 수가 늘어난다. 그래서 merge가 함께 필요하다 (Slicer는 서버당 50~150개 사이로 유지)
- **범위 조회 불가**: 해시라서 비슷한 key가 흩어진다 (§2.1)
- **데이터 이동은 Slicer가 하지 않는다**: Slicer는 표만 바꾸고 "이 구간 이제 네 것"을 알려줄 뿐이다. 캐시 앱은 버리고 원본에서 다시 읽는다. ScyllaDB는 데이터를 파일 단위로 직접 스트리밍한다

## 읽으면서 답할 질문

답변에는 근거가 되는 자료와 절을 함께 적는다. 자료에 없는 내용은 **"자료에 없음"**으로 표시한다.

### 1. 왜 key 순서가 아니라 해시 구간인가?

**답**

key를 해시하면 원래 key 공간에서 몰려 있던 뜨거운 key들이 해시 공간에서는 고르게 흩어져 균형 잡기가 쉽다. 또 key가 10개든 10억 개든 표의 크기와 Slicer의 일은 같고, 새 key가 생겨도 Slicer를 거칠 필요가 없다. 대가는 비슷한 key가 흩어져 범위 조회(scan)를 못 하는 것인데, 대부분의 앱이 단일 key 연산이라 문제가 되지 않았다.

캐시인 우리 프로젝트도 범위 조회를 쓰지 않으므로 같은 판단을 한다 (design.md §3, B2 질문에 대한 답).

**근거**

Slicer **§2.1**: "Slicer hashes each application key into a 63-bit slice key; each slice in an assignment is a range in this hashed keyspace." "Hashing keys simplifies the load balancing algorithm because clusters of hot keys in the application's keyspace are likely uniformly distributed in the hashed keyspace. The cost is lost locality."

### 2. 구간은 몇 개로, 언제 쪼개고 합치나?

**답**

| | Slicer | ScyllaDB |
|---|---|---|
| 처음 | 해시 공간을 서버 수로 **균등** 분할 | 설정값(`initial`, 샤드당 최소 개수 등) 중 가장 큰 값 |
| 개수 범위 | 서버당 **50~150개** | `tablets_per_shard_goal`(soft limit), 2의 거듭제곱으로 맞춤 |
| split 조건 | 구간이 평균 구간보다 **2배 이상 뜨겁고**, 서버당 150개 미만일 때. 쪼개기만 하고 옮기지는 않음(다음 라운드에서 옮길 수 있게) | 평균 tablet **크기**가 목표의 2배를 넘으면 전체 개수를 2배로 |
| merge 조건 | 서버당 50개 초과, 합친 구간이 평균보다 차갑고, 받는 서버가 최대 부하를 넘지 않을 때. 키 공간 1%까지 | 평균 크기가 목표의 절반 미만이면 개수를 절반으로 |
| 이동 | "균형 개선 ÷ 이동량"이 큰 것부터, 한 번에 키 공간 **9%**까지 | 디스크 사용률 높은 노드 → 낮은 노드 |

Slicer는 **부하(요청 수·CPU)** 기준, ScyllaDB는 **데이터 크기** 기준이다. Slicer는 50~150, 1%, 9%를 "기존 앱을 보고 정했고 민감도는 측정하지 않았다"고 밝힌다.

이 표의 "언제·무엇을" 부분은 **B2의 결정**이고, B1-B는 그 결정을 반영할 수 있는 표 연산(`withSplit`, `withMerge`, `apply`)을 제공한다.

**근거**

Slicer **§4.4 Load Balancing**, **§4.4.1 Sharding Algorithm: Weighted-move**(phase 3 merge 조건 (a)~(d), phase 4 move와 9% 예산, phase 5 split 조건 (a)(b)), "The constants in the algorithm (50–150 slices per task, 1% and 9% key movement per adjustment) were chosen by observing existing applications." ScyllaDB **Absolute number of tablets**, **Load Balancing**.

### 3. 왜 consistent hashing이 아니라 표인가?

**답**

Slicer는 부하 인식 consistent hashing을 18개월 운영하다가 지금의 표 + weighted-move로 바꿨다. 이유는 세 가지다.

1. key가 적은 앱(서버당 수십~수백)에서는 통계적으로 균형이 나빴다
2. 균형을 맞추려면 서버당 vnode가 1000개 가까이 필요했고, 그만큼 표(디코딩한 할당)가 커졌다
3. **뜨거운 곳을 원하는 데로 못 보낸다**: 바쁜 서버의 vnode를 줄이면 밀려난 부하가 한가한 서버가 아니라 **랜덤한 서버**로 간다

표 방식은 같은 문제를 "이 구간을 저 서버로"로 직접 해결한다. 우리 B1-C(ring-vnode)는 이 비교를 수치로 재현하기 위한 기준선이다.

**근거**

Slicer **§4.4.4 A rejected design alternative**: "consistent hashing gives us less control over hot spots. We can cool off a task by reducing its virtual node count, but the displaced traffic ends up randomly distributed, not directed at a cool task." "After 18 months in service, we replaced it with the weighted-move algorithm, which balances better with less key churn."

### 추가 확인 항목

| 항목 | 현재 확인한 내용 | 추가로 확인할 내용 |
|---|---|---|
| 표 버전 | Assigner가 새 표에 **단조 증가 generation 번호**를 붙이고, 읽은 값 위에 덮어쓸 때만 성공(CAS) (§4.1) | 우리 `RoutingVersion` + expectedVersion과 같은 구조. 전파 스토리에서 구현 |
| 두 주인 방지 | lease 3개(job/guard/bridge)로 "한 key는 한 서버만". 바뀌지 않은 구간은 표 교체 중에도 서비스(bridge lease). 없으면 교체마다 2.6초 전체 정지 (§4.5) | 우리는 lease 대신 **Tablet별 epoch**. B3 cutover 설계에서 bridge lease 아이디어 참고 |
| 표 전파 | Assigner → Distributor → 구독자, 2단계 트리, pull 방식. 95% 1.7초 (§4.2, §5.1.4) | 우리는 Controller push + 응답 피기백. 전파 스토리에서 측정 |
| 데이터 이동 방식 | Slicer: 하지 않음(앱 몫). ScyllaDB: SSTable 파일 단위 스트리밍 | **B3의 결정**. 원본에서 재적재 / 서버 간 복사 / lazy pull 중 선택 |
| 재조정 중지 | 가장 바쁜 서버 CPU 25% 미만이면 재조정 안 함 (§4.4.2) | B2에 참고값으로 전달 |

## 프로젝트 적용

공개된 설계를 참고한 실험용 variant를 `hash-range-tablet`으로 구현한다.

### 적용할 내용

- 해시 공간의 가변 구간 표, 구간 단위 이동·split·merge 연산
- 처음 표는 균등 분할 + 연속 블록 배치(B1-A와 같은 규칙)
- 중앙(Controller)이 표를 만들고 버전 번호로 관리

### 적용하지 않는 내용

- weighted-move 균형 알고리즘, split·merge **결정 규칙**: B2(`core.balance`)의 몫. B1-B는 연산만 제공
- key 복제(asymmetric key redundancy): A2·B2 의제
- lease 기반 일관성: Tablet별 epoch로 대체
- Distributor 2단계 전파 트리: 노드 수가 적어 Controller 직접 push로 충분

### 직접 설계하고 기록할 내용

- 초기 Tablet 수: 노드당 T개(기본 32~64, 사전 실험 3). 비교 실험에서는 전체 G 기준으로 T = G/8
- 노드 추가·제거 시 기본 규칙: B2 계획이 없을 때 쓰는 개수 기준 donor 선택
- split·merge 후에도 지켜야 할 불변식(구간 수 상한·하한은 B2와 합의)

비교 실험과 불변식은 프로젝트의 `design.md` §5·§6을 따른다.
