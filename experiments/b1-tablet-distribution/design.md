# B1 설계

상태: 공통 타입·인터페이스·`fixed-slot`까지 구현(§7). 소유권 판정, 전파, 나머지 variant, §6 실험은 미구현.

## 1. 목표와 범위

B1은 "이 key는 어느 노드에 있는가"를 모든 참여자(클라이언트, 노드, Controller)가 같은 답을 내도록 만들고, 그 답(라우팅 표)이 바뀔 때 퍼지게 한다.

| 기능 | 결정 내용 | 단계 |
|---|---|---|
| 분배 방식 | key → Tablet → 노드를 어떻게 정하는가. variant 3개를 같은 인터페이스로 | 1단계 (지금): 인터페이스 + `fixed-slot` |
| 라우팅 표 | 표의 모양, 버전(`RoutingVersion`), 변경 연산 | 1단계 |
| 소유권 판정 | 노드가 요청을 받았을 때 내 것인지 판정. `MOVED`/`STALE_EPOCH` | 2단계 |
| 표 전파 | Controller push + 응답 피기백 + `MOVED` 리다이렉트 | 2단계 |
| 노드 간 통신 (common) | `NodeId`↔주소 매핑, `Transport` 구현, 송수신 예제 | 2단계 |

1단계는 분배 방식 인터페이스와 `fixed-slot` 하나만 다룬다. 참고 기법은 [Redis Cluster 고정 해시 슬롯](references/redis-cluster-hash-slots.md)이다. `hash-range-tablet`, `ring-vnode`는 같은 인터페이스로 뒤에 추가한다.

B1은 **표를 만들고 바꾸는 연산을 제공**한다. 무엇을 옮기고 쪼갤지 결정하는 것은 B2(`core.balance`), 데이터를 실제로 옮기는 것은 B3(`core.migration`)가 맡는다.

## 2. 다른 연구와의 경계

| 대상 | 상대가 B1에 기대하는 것 | 현재 상태 | 1단계 대응 |
|---|---|---|---|
| B2 Balancing | Tablet의 범위·owner·version, 부하를 셀 단위(`TabletId`), split/merge 연산 | B2가 임시 `KeyRange`, `TabletLoad`를 `core.balance`에 둠 | `Tablet`, `TabletRange`, `RoutingTable.withSplit/withMerge`를 1단계에 넣는다. B2 임시 타입은 이것으로 교체 |
| B3 Migration | "무엇을 어디로 옮기나" 목록, cutover 시 owner·epoch 갱신 연산 | 미구현 | `Change(unit, from, to)` 목록과 `RoutingTable.withOwner`를 1단계에 넣는다. 이동 자체는 하지 않는다 |
| A1 KV Store | 영향: 한 Tablet의 key는 전부 한 worker에 있어야 B3가 Tablet 단위로 내보낼 수 있다 | `KvEngine`은 `key.hashCode() % workers`로 worker 선택 | 1단계는 건드리지 않는다. 2단계에서 `KvEngine`에 worker 선택 함수 주입점을 A1에 요청한다 |
| experiment-runner (B3) | 클라이언트 라우팅(표 캐시, `MOVED` 재시도) | 단일 노드 전용 | 1단계는 in-process 실험만. 2단계에서 runner 안에 B1 소유 client 패키지를 두는 것을 B3와 합의한다 |
| Metrics | 실험 결과 기록 | 결과 Schema 합의 전 | 1단계 결과는 테스트 출력과 CSV. Schema 확정 후 JSON으로 옮긴다 |

**B2의 질문에 대한 답 (B2 design.md §2):** Tablet은 **key hash(64비트)의 범위**다. key 바이트 순서 범위가 아니다. 이유는 §3. 따라서 순차 증가 key의 쓰기는 여러 Tablet에 흩어지며, B2의 "순차 쓰기" 시나리오(S4)와 `SEQUENTIAL_WRITES` 보류 규칙은 hash 범위에서는 발생하지 않는다. key 순서 범위 variant(Bigtable 계열)는 범위 조회가 선택 사항인 캐시에 실용 가치가 낮아 구현 대상에서 제외했다. B2가 그 시나리오를 꼭 보려면 variant 추가를 다시 논의한다.

## 3. 공통 모델

세 variant가 같이 쓰는 타입이다. `core.model`의 `TabletId`, `NodeId`, `RoutingVersion`, `OwnershipEpoch`는 그대로 쓴다.

| 타입 | 내용 | 비고 |
|---|---|---|
| `KeyHash` | `Bytes` key → 64비트 해시. MurmurHash3 x64_128의 앞 64비트, seed 고정 | 클라이언트와 서버가 같은 값을 내야 하므로 `cache-core`에 둔다. JDK만 쓰므로 직접 구현 |
| `TabletRange` | 해시 구간 `[start, end)`. 부호 없는 64비트. `end == 0`은 2^64 | `fixed-slot`은 균등 구간, `hash-range-tablet`은 가변 구간, `ring-vnode`는 토큰 사이 구간 |
| `Tablet` | `id`, `range`, `owner`(NodeId), `epoch`(OwnershipEpoch) | 필드 4개. 이동 상태와 부하 통계는 표에 넣지 않는다(각각 B3 노드 로컬, B2 별도 구조) |
| `RoutingTable` | 공간을 빈틈·겹침 없이 덮는 `Tablet` 목록 + `RoutingVersion` | 불변 객체. 변경 연산은 새 표를 돌려주고 version+1 |
| `Change` | `(TabletId unit, NodeId from, NodeId to)` | B3가 실행할 이동 계획 한 건. `RoutingTable.apply(Change)`는 현재 주인이 `from`인지 확인한 뒤 반영한다 |
| `Plan` | `(RoutingTable prepared, List<Change> moves)` | 노드 증감 계획. `prepared`는 split·merge처럼 주인이 안 바뀌는 변경만 반영한 표(데이터 이동 없음), `moves`는 그 위에서 실행할 이동. 링(vnode) 방식은 새 토큰 위치에서 구간을 나눠야 하므로 `prepared`가 입력과 다르다 |

**key hash 범위를 쓰는 이유.** 캐시 KV는 범위 조회가 선택 사항이고, hash 범위는 key 분포와 무관하게 균등하다(사전 실험: Tablet 128개로 key 100만 개를 나눴을 때 max/mean 1.006). key 순서 범위는 접두사가 같은 key와 순차 key가 한 Tablet에 몰린다.

**`OwnershipEpoch`는 Tablet마다 하나.** Redis는 노드마다 하나(configEpoch)지만, Tablet 단위로 두면 "같은 Tablet을 두 노드가 주장"하는 충돌을 Tablet마다 독립적으로 풀 수 있다. 규칙은 Redis와 같다: 더 큰 epoch가 이긴다.

## 4. 공통 인터페이스: `DistributionScheme`

variant가 구현하는 계약이다. B2·B3는 이 인터페이스만 호출하고, 어떤 variant가 꽂혀 있는지 모른다.

```
name()                                    실험 결과의 variant 값
initial(nodes)                → RoutingTable   첫 표. 버전 1, epoch 1
onNodeAdded(table, node)      → Plan      새 노드가 받을 Tablet. 이동 실행은 B3
onNodeRemoved(table, node)    → Plan      떠나는 노드의 Tablet을 받을 노드
supportsSplit()               → boolean   false면 B2는 이 variant를 split 실험에서 제외
supportsMove()                → boolean   false면 노드 증감 외의 임의 재배치를 하지 않는다 (ring-vnode)
metadataBytes(table)          → int       표를 이 variant의 형식으로 직렬화했을 때 크기
```

- **조회와 split/merge는 인터페이스에 없다.** 세 variant 모두 "해시 공간을 구간으로 덮는 표"로 표현되므로 `RoutingTable.lookup(hash)`,
  `withSplit(id, at)`, `withMerge(a, b)`, `withOwner`, `apply(Change)`를 variant와 무관하게 공통으로 쓴다. variant는 표를 처음 만드는 규칙과
  노드 증감 시 계획만 정한다. 그 결과 B2·B3가 만지는 타입은 `RoutingTable`, `Tablet`, `Change`, `Plan`뿐이고 `DistributionScheme`은
  Controller와 실험 코드만 쓴다.
- `lookup`의 입력은 key가 아니라 **64비트 해시값**이다. 세 variant 모두 해시 공간 위에서 동작하므로 key 바이트열을 받을 이유가 없다.
- Tablet 식별자는 variant마다 다른 타입을 두지 않고 **`TabletId` 하나로 통일**한다(슬롯 번호, 토큰 번호도 `TabletId`). B2·B3 코드가 variant마다 갈라지지 않게 하기 위해서다.
- `TabletId`는 Controller만 발급한다. split 자식의 id가 노드마다 다르게 매겨지는 일을 막는다.
- 인터페이스를 지금 두는 이유: 구현이 처음부터 셋이라 근거 없는 추상화가 아니다. 사전 실험의 `Partitioner`(5종 구현)를 정리한 것이다.

### 4.1 `fixed-slot`의 결정 (1단계)

아래 값은 Redis 값을 그대로 쓸 수 없어 **프로젝트에서 직접 정했다**. 근거는 각 항목에 적는다.

| 파라미터 | 기본값 | 근거 |
|---|---|---|
| 슬롯 수 S | 4096 | Redis 16384는 노드 1000대 가정(하트비트 비트맵 2KB). 노드 ≤ 10대면 4096으로 노드당 400개 이상이라 정수 나눗셈 편차가 0.3% 이하. 1024·16384와 비교 실험(§6) |
| 슬롯 번호 | 공통 `KeyHash`의 균등 구간 번호 (`hash / (2^64 / S)`) | Redis는 `CRC16(key) mod 16384`. 상위 비트를 쓰면 분포는 같고 `RoutingTable.lookup`을 그대로 쓸 수 있다. 해시 자체는 variant 간 공정 비교를 위해 공통으로 고정 |
| 초기 배치 | 슬롯 i → 노드 (i mod n) | 최대·최소 차이 1 |
| 노드 추가 | 가장 많이 가진 노드에서 하나씩, 새 노드가 S/(n+1)에 닿을 때까지 | 이동 수가 이론 최소 S/(n+1). 기존 노드끼리는 이동 없음 |
| 노드 제거 | 떠나는 노드의 슬롯을 가장 적게 가진 노드에 하나씩 | 이동 수 = 그 노드의 슬롯 수 |
| hash tag | 없음 | 단일 key 연산만 |
| 이동 중 라우팅 | `ASK` 없음. 표상 주인이 끝까지 처리하고 핸드오프 순간에만 `MOVED` | Valkey 9.0 atomic slot migration과 같은 모양. 세 variant의 전파 방식을 하나로 고정하기 위해 |

## 5. 불변식

variant 구현은 모두 아래를 지켜야 하며, 단위 테스트로 검증한다.

1. 표의 Tablet들은 해시 공간 전체 `[0, 2^64)`를 빈틈과 겹침 없이 덮는다. 해시 0, 2^63, 2^64−1 모두 어떤 Tablet에 속한다.
2. 같은 표, 같은 해시에는 같은 `locate` 결과를 낸다. 난수와 실제 시각을 쓰지 않는다.
3. 변경 연산(`withOwner`, `split`, `merge`, `onNodeAdded`의 결과 적용)은 입력 표를 바꾸지 않고 새 표를 돌려주며, `RoutingVersion`은 정확히 1 증가한다.
4. `withOwner`는 그 Tablet의 `OwnershipEpoch`만 1 올리고 다른 Tablet의 epoch는 바꾸지 않는다.
5. `onNodeAdded`가 돌려준 `Change`를 모두 적용하면 새 노드를 포함한 모든 노드의 Tablet 수 차이가 1 이하다(`fixed-slot`, `hash-range-tablet`). `ring-vnode`는 토큰 수만 보장한다.
6. `onNodeAdded`의 `Change`는 `to`가 모두 새 노드다(기존 노드끼리 이동 없음). `onNodeRemoved`의 `Change`는 `from`이 모두 떠나는 노드다.
7. `split`의 두 자식 범위는 부모 범위를 빈틈·겹침 없이 덮고, `merge`는 인접하고 owner가 같은 두 Tablet만 합친다.

## 6. 첫 실험: variant 간 분배 특성 비교

### 질문

고정 슬롯 분배는 노드 증감 시 얼마나 적게 옮기고, 균형은 얼마나 유지하며, 그 대가로 표가 얼마나 커지는가? 표가 없는 `ring-vnode`와 비교해 표를 두는 비용과 이득은 얼마인가?

### 시나리오

key 100만 개(길이 16바이트, seed 고정)를 노드 8대에 분배한다.

| 시나리오 | 내용 | 예상 |
|---|---|---|
| S1 초기 균형 | 분배 직후 노드별 key 수 | `fixed-slot` max/mean ≈ 1.00. `ring-vnode`는 vnode 수에 따라 1.1~1.3 |
| S2 노드 추가 | 8 → 9대. 이동한 key 비율, 추가 후 균형 | 이동 ≈ 1/9. `fixed-slot`은 기존 노드끼리 이동 0 |
| S3 노드 제거 | 8 → 7대 | 이동 ≈ 1/8 |
| S4 쏠림 | Zipf θ=0.99 요청 100만 건. 노드별 요청 수 max/mean | 초기 균형이 좋아도 요청은 쏠린다. 이것이 B2의 재배치가 필요한 근거 |
| S5 S 민감도 | S ∈ {1024, 4096, 16384}로 S1~S3 반복 | S/노드 수가 클수록 균형이 좋고 표가 커진다. 4096이 충분한지 확인 |

"예상"은 가설이다. 사전 시뮬레이션(연구 노트)의 수치와 다르면 그대로 기록한다.

### 측정

| 지표 | 정의 | 이상적인 값 |
|---|---|---|
| 균형 | 노드별 key 수(또는 요청 수)의 max / mean | 1.0 |
| 이동 비율 | 노드 증감 전후 owner가 바뀐 key 수 / 전체 key 수 | 추가 1/(n+1), 제거 1/n |
| 메타데이터 | `metadataBytes(table)` | 작을수록 좋음. `ring-vnode`는 토큰 목록 크기 |
| 조회 비용 | `locate` 1회의 나노초(JDK `System.nanoTime`, 100만 회 평균) | 작을수록 좋음 |

### 실행 환경

- in-process로 실행한다. 서버, 네트워크, 실제 시계를 쓰지 않는다.
- key 생성과 Zipf 요청은 seed를 고정한다. 시나리오마다 seed 여러 개로 반복한다.
- 결과는 `variant`, `scenario`, `seed`, 지표를 열로 하는 `summary.csv`에 기록한다. 결과 Schema가 확정되면 JSON으로 옮긴다.
- 표 전파 시간, `MOVED` 재시도율, Controller 장애 시 실패 수는 2단계(실제 노드·클라이언트)에서 측정한다.

## 7. 코드 구조

```
cache-core/src/main/java/io/openkache/core/tablet/
├── package-info.java              (있음)
├── KeyHash.java                   Bytes → 64비트 해시 (MurmurHash3, seed 고정)
├── TabletRange.java               [start, end) 해시 구간
├── Tablet.java                    id, range, owner, epoch
├── RoutingTable.java              불변 표. lookup / withOwner / apply / withSplit / withMerge
├── Change.java                    (unit, from, to)
├── Plan.java                      (prepared, moves)
├── DistributionScheme.java        §4 인터페이스
└── slot/
    └── FixedSlotScheme.java       fixed-slot
(이후) range/HashRangeScheme.java, ring/RingScheme.java, OwnershipCheck.java, Controller

cache-core/src/test/java/io/openkache/core/tablet/
├── KeyHashTest.java, TabletRangeTest.java
├── RoutingTableTest.java               §5 불변식 1~4·7 (표 자체)
├── DistributionSchemeContract.java     §5 불변식 5·6 + 인터페이스 계약. 모든 variant 테스트가 상속
├── slot/FixedSlotSchemeTest.java       계약 상속 + fixed-slot 고유 규칙
└── (이후) DistributionScenarioTest.java   §6 시나리오 S1~S5 (key 생성, Zipf 포함)

experiments/b1-tablet-distribution/
├── README.md                      variant 목록
├── design.md                      이 문서
├── references/                    논문·시스템별 정리
└── distribution-comparison/       variant 비교 실험의 설정과 결과 요약 (실험 실행 방식 확정 후)
```

규칙:

- **패키지는 variant별로 나눈다** (`slot`, `range`, `ring`). B2가 기능별로 나누는 것과 다른데, B1은 "같은 기능의 다른 구현"이 아니라 "분배 방식 자체"가 비교 대상이기 때문이다. 공통 타입은 `tablet` 바로 아래에 둔다.
- **variant 하나는 클래스 하나**가 된다. 클래스 Javadoc에 참고 문헌을 적고, `references/`의 정리 문서와 이름을 맞춘다.
- **B2·B3가 쓰는 공통 타입(`Tablet`, `TabletRange`, `RoutingTable`, `Change`)은 영향받는 담당자 리뷰를 받는다.** `core.model`에는 올리지 않고 `core.tablet`에 둔다. `model`에 올릴 필요가 생기면 그때 팀 확인.
- **실험 폴더는 비교하는 질문 기준으로 만든다.** `distribution-comparison/`은 모든 variant를 같은 시나리오로 비교한다.

## 8. 미결 사항

| 항목 | 결정할 사람 | 기한 |
|---|---|---|
| `DistributionScheme`·`Change`·`Tablet` 모양 확정 | B1 (B2·B3 리뷰) | 2주 차 초 |
| B2의 순차 쓰기 시나리오를 hash 범위에서 어떻게 할지 | B2 (B1 의견 전달: §2) | 2주 차 초 |
| Controller를 별도 프로세스로 둘지, `cache-server`의 역할 플래그로 둘지 | 저장소 담당, B1 | 2단계 전 |
| `KvEngine` worker 선택 주입점 | A1 (B1 요청) | 2단계 전 |
| 클라이언트 라우팅 코드를 `experiment-runner` 안 어디에 둘지 | B3, B1 | 2단계 전 |
| cutover 시 `withOwner`를 호출하는 쪽 (Controller가 B3 요청을 받아 호출) | B3, B1 | B3 설계 시 |
| 실험 결과 JSON 형식 | Metrics, 팀 합의 | 결과 Schema 합의 후 |

**결정된 사항 (2026-10-19, 프로젝트 자체 결정)**

| 항목 | 결정 | 위치 |
|---|---|---|
| Tablet 경계 | key hash(64비트) 범위. key 순서 범위 아님 | §3 |
| 구현 variant | `fixed-slot`, `hash-range-tablet`, `ring-vnode` 3개. key 순서 범위, jump/rendezvous 같은 계산식 해시는 제외 | README |
| 해시 함수 | MurmurHash3 x64 앞 64비트, seed 고정. 세 variant 공통 | §3 |
| epoch 단위 | Tablet마다 `OwnershipEpoch` 하나. 충돌은 큰 epoch가 이김 | §3 |
| 표 전파 | Controller push + 응답 피기백 + `MOVED`. variant마다 다르게 하지 않음 | §1, §4.1 |
| 이동 중 라우팅 | `ASK` 없음 | §4.1 |
| `fixed-slot` S 기본값 | 4096. 1024·16384와 비교 | §4.1, §6 |
| 인터페이스 모양 | 조회·split/merge·이동 반영은 `RoutingTable` 공통. variant는 `initial`, `onNodeAdded/Removed → Plan`, `supportsSplit/Move`, `metadataBytes`만 | §4 |
