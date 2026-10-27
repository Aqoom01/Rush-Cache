# B1 설계

상태: 공통 타입·인터페이스·`fixed-slot`·`hash-range-tablet`·표 전파·소유권 판정(in-process)까지 구현(§7). `ring-vnode`, wire 인코딩, §6 실험의 split 시나리오는 미구현.

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
| 초기 배치 | 연속 블록: 노드 j → 슬롯 [j·S/n, (j+1)·S/n), 나머지는 앞 노드부터 1개씩 | 최대·최소 차이 1. 연속 슬롯의 주인이 같아 표를 구간으로 압축하면 크기가 노드 수에 비례(176 B). 라운드로빈이면 G × 20 B (비교 실험 AUX) |
| 노드 추가 | 가장 많이 가진 노드에서 하나씩, 새 노드가 S/(n+1)에 닿을 때까지 | 이동 수가 이론 최소 S/(n+1). 기존 노드끼리는 이동 없음 |
| 노드 제거 | 떠나는 노드의 슬롯을 가장 적게 가진 노드에 하나씩 | 이동 수 = 그 노드의 슬롯 수 |
| hash tag | 없음 | 단일 key 연산만 |
| 이동 중 라우팅 | `ASK` 없음. 표상 주인이 끝까지 처리하고 핸드오프 순간에만 `MOVED` | Valkey 9.0 atomic slot migration과 같은 모양. 세 variant의 전파 방식을 하나로 고정하기 위해 |

### 4.2 `hash-range-tablet`의 결정 (2단계)

참고 자료: [Google Slicer와 ScyllaDB Tablets](references/slicer-scylla-tablets.md). fixed-slot과 표·조회·이동 연산은 같고, **구간 수와 길이가 split·merge로 바뀐다**는 점만 다르다. split·merge를 **언제·어디서** 할지는 B2가 정하고, 이 variant는 그 결정을 받아 적용할 수 있게 `supportsSplit() = true`를 돌려준다.

| 파라미터 | 기본값 | 근거 |
|---|---|---|
| 초기 Tablet 수 | 노드당 T개, 기본 T = 64 (비교 실험은 T = G / 8) | Slicer는 서버당 50~150개 유지(§4.4.1). 사전 실험 3에서 노드당 32~64개면 노드 추가 직후 균형 1.03 이내, 표 5~10 KB |
| 초기 배치 | 해시 공간을 n·T개로 균등 분할, 연속 블록 배치 | Slicer 처음 할당이 균등 분할(§4.4). 배치는 fixed-slot과 같은 연속 블록(표 압축) |
| 노드 추가 | 가장 긴 노드의 마지막 Tablet부터, 받고 나서 그 donor의 받기 전 길이 이상이 되면 멈춘다. Tablet 하나뿐인 노드는 donor 제외 | 새 노드 길이는 [평균 − 가장 긴 Tablet, 추가 전 최대] 안. 큰 Tablet은 B2가 먼저 split해야 균형이 잡힌다. B2의 재배치 계획이 있으면 그것을 쓴다 |
| 노드 제거 | 떠나는 노드의 Tablet을 가장 적게 가진 노드에 하나씩 | 위와 같음 |
| split | `RoutingTable.withSplit(id, at)`. `at`은 해시값. 자식 둘 다 부모의 주인·epoch 승계, 뒤쪽 자식은 새 id | 쪼개기만 하고 주인은 안 바꾼다(Slicer phase 5 "Split hot slices without changing their task assignments"). 옮기는 건 별도 `Change` |
| merge | `RoutingTable.withMerge(left, right)`. 인접하고 주인이 같을 때만. epoch는 둘 중 큰 값 | Slicer phase 3도 먼저 같은 서버로 옮긴 뒤 합친다 |
| 노드 균형 기준 | Tablet **개수**가 아니라 해시 공간 **길이** 기준으로 donor를 고른다 | split 후에는 Tablet마다 길이가 달라 개수로 세면 key 균형이 틀어진다. fixed-slot과 다른 유일한 규칙 |

### 4.3 표 전파·소유권 판정의 결정 (2단계)

variant와 무관하게 하나다. 세 참여자(Controller, 노드, 클라이언트)가 각자 표 사본을 들고, 원본은 Controller 하나다. 사본이 낡아도 서비스는 멈추지 않고, 낡은 사본으로 온 요청은 `MOVED`로 바로잡는다. 코드는 `core.tablet.routing`.

| 규칙 | 결정 | 근거 |
|---|---|---|
| 전파 | Controller가 변경 직후 모든 노드에 push 1회(실패해도 재시도 없음) + 모든 응답에 응답자의 표 버전(피기백) + 낡은 요청에 `MOVED` | 사전 실험(표 전파 전략 비교)에서 push+피기백+리다이렉트가 수렴 2 ms, Controller 분할 중 실패 0. push를 놓친 노드는 피기백으로 알고 `GetTable`로 받아 간다 |
| 버전 | `RoutingVersion`은 Controller만 올린다. 노드·클라이언트는 더 큰 버전만 받아들인다 | 늦게 도착한 옛 push로 표가 뒤로 가지 않는다 |
| 변경 | `Controller.apply(expectedVersion, 변경)`. 요청자가 본 버전이 현재와 다르면 거절(CAS) | B2·B3가 서로 모르고 같은 Tablet을 건드리는 사고 방지. B3 `MigrationCommitter.commit(job, expectedTable)` = `move(expectedTable.version(), job.change())` |
| 노드 판정 | 내 표에서 내가 주인이면 OK. 아니면 `MOVED` + 새 주인. 요청 epoch < 내 epoch면 `STALE_EPOCH`. 요청 epoch > 내 epoch면 내 표가 낡은 것이므로 redirect 없는 `MOVED`로 거절하고 표를 받아 온다. epoch 0은 검사 안 함 | Redis Cluster MOVED. `ASK`는 쓰지 않는다(§4.1) |
| 두 주인 방지 | 표가 아니라 노드 로컬에 거절 플래그(`LocalRouting.stopServing`). cutover 순서: (1) 옛 주인 거절 시작 → (2) 마지막 복사 → (3) `Controller.move` → (4) 새 주인 서비스. 플래그는 그때 epoch에 묶이고, 주인이 바뀐 표를 받으면 자동으로 풀린다 | 사전 실험의 stale serve(0.3~1.1건)는 (1) 없이 라우팅만으로 할 때의 수치. 순서는 B3 설계(§3)와 같다 |
| 클라이언트 | `MOVED`면 표를 받아 오고, 못 받으면 redirect 힌트로 바로 재시도. 최대 3회, 넘으면 `UNAVAILABLE`. 피기백으로 더 새 버전을 보면 다음 호출 전에 갱신 | redis-py 등 Redis 클라이언트의 관례(3회). 응답을 기다리는 경로에서 표를 동기로 받지 않기 위해 |
| Controller 장애 | 서비스는 계속(노드가 표를 들고 있음), 표 변경만 멈춤. 복제·버전 영속화는 범위 밖 | 보고서에 한계로 명시 |
| push 수신 확인 | 없음. 못 받은 노드는 `check` 뒤 `needsRefresh()`를 보고 `requestTable`을 부르는 것이 엔진(서버 연동)의 책임 | 피기백이 메우므로 Controller가 확인·재시도하지 않는다 |

### 4.4 `ring-vnode`의 결정 (3단계, 설계만)

참고 자료: [Dynamo와 Cassandra vnode](references/dynamo-cassandra-vnode.md). 표 없이 토큰 목록만으로 분배하는 **Dynamo Strategy 1**(순수 링)을 재현한다. Strategy 3(균등 구간 Q개 고정)는 fixed-slot과 같으므로 따로 두지 않는다. 구간의 주인이 토큰 위치로 정해지므로 임의 재배치가 없고(`supportsMove() = false`), "표를 두면 무엇을 얻는가"의 기준선이다.

| 파라미터 | 기본값 | 근거 |
|---|---|---|
| 토큰 수 V | 노드당 V개 (비교 실험은 V = G / 8). 기본값은 실험 후 | ketama 100~200, Cassandra 무작위 256 / 알고리즘 배치 16. Karger는 노드당 Θ(log n)개면 균형 |
| 토큰 위치 | `hash(nodeId, i)`, i = 0..V−1. 결정적 | 불변식 2(난수 없음). 무작위 배치의 균형 문제를 재현하면서 실험은 재현 가능 |
| 배치 규칙 옵션 | `RANDOM`(위) / `SPLIT_LONGEST`(새 토큰을 현재 가장 긴 구간의 중점에) | Cassandra 2.x 무작위 vs 4.0 allocator의 차이를 재는 실험 변수. allocator의 복제 인식 부분은 복제가 없어 생략 |
| 초기 표 | 모든 노드의 토큰을 정렬, 토큰 사이가 Tablet, 주인은 시계 방향 다음 토큰의 노드 | Dynamo §4.2. 첫 토큰 앞 구간은 마지막 토큰에서 wrap |
| 노드 추가 | `prepared` = 새 토큰 V개 자리에서 `withSplit`(주인 그대로). `moves` = 그 앞쪽 조각 V개를 새 노드로 | `prepared`가 입력과 다른 유일한 variant(§3 `Plan`). 기존 노드끼리 이동 없음(monotone) |
| 노드 제거 | 떠나는 노드의 Tablet 각각을 시계 방향 다음 토큰의 주인에게 | 받는 노드를 고를 수 없다. 남는 경계는 이동 완료 후 Controller가 `withMerge`로 정리(선택) |
| split / move | 없음 | B2 실험에서는 그리디 한계를 참고값으로만 |
| `metadataBytes` | 16 + 토큰 수 × 12 (8 B 토큰 + 4 B NodeId) | 표가 아니라 토큰 목록 크기 |

## 5. 불변식

variant 구현은 모두 아래를 지켜야 하며, 단위 테스트로 검증한다.

1. 표의 Tablet들은 해시 공간 전체 `[0, 2^64)`를 빈틈과 겹침 없이 덮는다. 해시 0, 2^63, 2^64−1 모두 어떤 Tablet에 속한다.
2. 같은 표, 같은 해시에는 같은 `locate` 결과를 낸다. 난수와 실제 시각을 쓰지 않는다.
3. 변경 연산(`withOwner`, `split`, `merge`, `onNodeAdded`의 결과 적용)은 입력 표를 바꾸지 않고 새 표를 돌려주며, `RoutingVersion`은 정확히 1 증가한다.
4. `withOwner`는 그 Tablet의 `OwnershipEpoch`만 1 올리고 다른 Tablet의 epoch는 바꾸지 않는다.
5. `onNodeAdded`가 돌려준 `Change`를 모두 적용하면: `fixed-slot`은 새 노드를 포함한 모든 노드의 Tablet 수 차이가 1 이하다. `hash-range-tablet`은 길이 기준이다. 추가 시 새 노드 길이가 [평균 − 가장 긴 Tablet, 추가 전 최대] 안이고, 제거 시 노드 간 길이 차이가 max(이전 차이, 가장 긴 Tablet) 이하다(길이가 모두 같으면 fixed-slot과 같은 결과). `ring-vnode`는 토큰 수만 보장한다.
6. `onNodeAdded`의 `Change`는 `to`가 모두 새 노드다(기존 노드끼리 이동 없음). `onNodeRemoved`의 `Change`는 `from`이 모두 떠나는 노드다.
7. `split`의 두 자식 범위는 부모 범위를 빈틈·겹침 없이 덮고, `merge`는 인접하고 owner가 같은 두 Tablet만 합친다.

## 6. 분배 방식 비교 실험 (in-process)

실행 방법·지표 정의·결과는 `distribution-comparison/`의 [README.md](distribution-comparison/README.md)와 [results.md](distribution-comparison/results.md). 1차(fixed-slot)는 2026-10-20에 실행했다.

### 질문

1. 단위 수 G(fixed-slot의 S)는 균형에 얼마나 영향을 주고, 어디서부터 효과가 사라지는가?
2. 노드 증감 시 이동량은 이론 최소 1/(n+1)에 붙는가? 반복 증감에도 누적 손실이 없는가?
3. 요청이 쏠릴 때, 단위를 통째로 옮기는 것만으로 도달할 수 있는 균형의 한계는 G에 따라 어떻게 달라지는가? (split이 필요한 영역과 분배로는 못 푸는 영역을 가른다)
4. (variant가 늘면) 표를 두는 방식(fixed-slot, hash-range)과 두지 않는 방식(ring-vnode)의 균형·이동량·표 크기 차이는?

### 동일 조건

variant마다 잘게 자르는 손잡이가 다르므로(fixed-slot은 전체 S, hash-range는 노드당 T, ring은 노드당 V) **전체 단위 수 G**를 공통 변수로 두고 환산한다: S = G, T = V = G / 8 (기준 노드 8대). key·노드 ID·증감 순서·Zipf 요청열은 seed로 고정해 variant와 무관하게 같고, 지표는 `RoutingTable`의 공개 연산으로만 계산한다.

### 시나리오

| | 내용 | 변수 | 측정 | 가설 (사전 시뮬 근거) |
|---|---|---|---|---|
| S1 초기 균형 | 노드 8대, key K개 배치 | K ∈ {1만, 10만, 100만} × G ∈ {256, 1024, 4096, 16384} | 단위 수 max/mean(결정적), key 수 max/mean | 단위 수는 G/n 정수 나눗셈만큼. key 수는 K가 작을수록 나쁨 |
| S2 노드 추가 | 8 → 9대 | G 4종 | 이동 key·단위 비율, 추가 후 균형, 기존 노드 간 이동 수 | ≈ 1/9, 기존 노드 간 0 |
| S3 노드 제거 | 8 → 7대 | G 4종 | 이동 비율, 제거 후 균형 | ≈ 1/8 |
| S4 연속 증감 | 1 → 16대 한 대씩, 다시 16 → 8 | G = 4096 | 누적 이동 비율 vs Σ 1/n, 매 단계 균형 | 누적 = 이론(여분 churn 0), 매 단계 max−min ≤ 1 |
| S5 쏠림 한계 | 노드 8대, Zipf θ ∈ {0.9, 0.99, 1.2} 요청 100만 | G 4종 | 그대로 max/mean, 단위 이동 그리디 한계, 가장 뜨거운 단위·key 비중 | 그대로 ~1.7(θ=0.99, 실험 2: 1.73). 한계는 G 클수록 1.0에 접근, θ=1.2는 hot key 하나 때문에 ~1.5 바닥(실험 2: 1.516) |
| 보조 | 표 크기(초기 / 추가 후), `lookup` ns | G 4종 | | 표 크기는 구간 수 × 20B, ns는 log G |

"가설"은 실행 전에 적은 것이고 결과가 다르면 그대로 기록한다.

### 측정 정의

- 단위 수 max/mean: `ownedBy(n).size()` 노드별 최대 / (G/n)
- key 수 max/mean: key마다 `lookup(KeyHash.of(key)).owner()` 집계, 최대 / (K/n)
- 이동 비율: 전·후 표에서 owner가 바뀐 key 수 / K (단위 기준은 `Plan.moves().size()` / 단위 수)
- 그리디 한계: 단위별 요청 수를 LPT(큰 것부터 가장 한가한 노드에)로 재배치했을 때의 max/mean. 표는 안 바꾼다. "어떤 재배치도 이 아래로는 못 간다". `supportsMove()`가 false인 variant는 참고값
- Zipf: 순위 r의 확률 ∝ 1/r^θ, 누적분포 이진 탐색, seed 고정

### 실행 환경

- in-process. 서버·네트워크·실제 시계 없음. seed 5개 × 시나리오. 전체 1분 안쪽
- 코드: `cache-core/src/test/java/io/openkache/core/tablet/experiment/` (`main`이 있는 러너 + 틀 검증 `MetricsTest`). `./gradlew test`에서는 돌지 않는다
- 결과: `distribution-comparison/summary.csv` (long 형식: `variant, scenario, G, nodes, keys, theta, seed, metric, value`)
- 표 전파 시간, `MOVED` 재시도율, Controller 장애 시 실패 수는 2단계(실제 노드·클라이언트)에서 측정한다

### 1차 결과 요약 (fixed-slot, 2026-10-20)

G는 균형에 거의 영향이 없고(key 균형은 노드당 key 수가 정함, 이동 비율은 G ≥ 1024면 이론 최소), 증감 직후 정수 나눗셈 불균형·표 크기·조회 비용만 바꾼다. θ ≤ 0.99의 쏠림은 단위 이동만으로 1.01 이하까지 내려가고, θ = 1.2는 hot key 하나(19%) 때문에 G와 무관하게 1.52가 바닥이다. S 기본값 4096 유지, 초기 배치는 라운드로빈 → 연속 블록으로 변경 반영(표 크기 82 KB → 176 B). 상세는 results.md.

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
├── slot/
│   └── FixedSlotScheme.java       fixed-slot
├── range/
│   └── HashRangeScheme.java       hash-range-tablet (길이 기준 균형)
└── routing/                       표 전파·소유권 판정 (variant 무관, §4.1)
    ├── TableMessage.java          TablePush / GetTable / TableReply (PeerMessage)
    ├── OwnershipCheck.java        순수 판정: OK / MOVED(redirect) / STALE_EPOCH, 피기백 버전
    ├── LocalRouting.java          노드 로컬: 표 사본(버전 더 클 때만 교체) + 거절 플래그(B3 cutover 1단계)
    ├── Controller.java            표 원본. apply(expectedVersion, 변경) = CAS, 성공 시 push
    └── RoutingClient.java         클라이언트: 표 캐시, MOVED 따라가기(최대 3회), 피기백으로 갱신
(이후) ring/RingScheme.java, wire 인코딩(MOVED redirect 필드, 표 메시지 opcode)

cache-core/src/test/java/io/openkache/core/tablet/
├── KeyHashTest.java, TabletRangeTest.java
├── RoutingTableTest.java               §5 불변식 1~4·7 (표 자체)
├── DistributionSchemeContract.java     §5 불변식 5·6 + 인터페이스 계약. 모든 variant 테스트가 상속
├── slot/FixedSlotSchemeTest.java       계약 상속 + fixed-slot 고유 규칙
├── range/                              계약 상속 + 길이 기준 균형, 무작위 split/merge/증감
├── routing/                            판정·클라이언트·Controller 단위 + PropagationScenarioTest(Controller 1 + 노드 3 + 클라이언트, FakeTransport)
└── experiment/                         §6 실험 러너(DistributionComparison, main), Workload, Metrics, Variants, MetricsTest

experiments/b1-tablet-distribution/
├── README.md                      variant 목록
├── design.md                      이 문서
├── references/                    논문·시스템별 정리
└── distribution-comparison/       variant 비교 실험: README(실행·지표), summary.csv, results.md
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
| `fixed-slot` S 기본값 | 4096 유지. 1차 실험으로 확인(1024도 충분, 16384는 이득 없음) | §4.1, §6 |
| 인터페이스 모양 | 조회·split/merge·이동 반영은 `RoutingTable` 공통. variant는 `initial`, `onNodeAdded/Removed → Plan`, `supportsSplit/Move`, `metadataBytes`만 | §4 |
| `ring-vnode` 재현 대상 | Dynamo Strategy 1(토큰이 구간을 정하는 순수 링). Strategy 3는 fixed-slot과 같아 제외. 토큰 배치는 결정적 해시, 배치 규칙 2종을 실험 변수로 (2026-10-27) | §4.4 |
