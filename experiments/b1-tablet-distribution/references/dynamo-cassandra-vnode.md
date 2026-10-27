# Dynamo와 Cassandra vnode — consistent hashing 링

## 출처

### 논문

[Consistent Hashing and Random Trees: Distributed Caching Protocols for Relieving Hot Spots on the World Wide Web](https://www.cs.princeton.edu/courses/archive/fall09/cos518/papers/chash.pdf)

- Karger et al., MIT. ACM STOC 1997. consistent hashing을 처음 정의한 논문
- 읽을 부분: **§4 Consistent Hashing**(4.1 정의: balance·monotonicity·spread·load, 4.2 구성과 Theorem 4.1, 4.3 구현, Theorem 4.4). §3·§5의 random tree는 웹 캐시 프로토콜이라 우리와 무관

[Dynamo: Amazon's Highly Available Key-value Store](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)

- DeCandia et al., Amazon. ACM SOSP 2007
- 읽을 부분: **§4.2 Partitioning Algorithm**(링, 토큰, virtual node의 세 가지 장점), **§6.2 Ensuring Uniform Load distribution**(Strategy 1·2·3 정의, Figure 8 비교, Strategy 3로 옮긴 이유)

### 문서

[Apache Cassandra — Dynamo: Dataset Partitioning, Multiple tokens per physical node](https://cassandra.apache.org/doc/latest/cassandra/architecture/dynamo.html)

- Cassandra 공식 문서. vnode의 장점과 "토큰이 많을 때의 비용" 네 가지
- [CASSANDRA-13701 Lower default num_tokens](https://issues.apache.org/jira/browse/CASSANDRA-13701): 4.0에서 기본 토큰 수 256 → 16. 근거로 [CASSANDRA-7032](https://issues.apache.org/jira/browse/CASSANDRA-7032)(균형 잡힌 토큰 배치 알고리즘)를 든다
- [Instaclustr — Cassandra vnodes: how many should I use?](https://www.instaclustr.com/blog/cassandra-vnodes-how-many-should-i-use/): 토큰 수별 소유 구간 편차 실측(8개 22%, 16개 15%, 32개 11%)
- [The Last Pickle — The Impacts of Changing the Number of VNodes](https://thelastpickle.com/blog/2021/01/29/impacts-of-changing-the-number-of-vnodes.html): 256 → 16으로 내린 이유(가용성·repair·streaming)

### 같은 계열의 시스템

- [libketama](https://www.metabrew.com/article/libketama-consistent-hashing-algo-memcached-clients) (Last.fm, 2007): memcached 클라이언트용 링. 서버당 100~200개 점, 표 없이 클라이언트가 계산. 캐시에서 consistent hashing을 쓰는 가장 흔한 형태
- Riak: 링을 처음부터 고정 개수(기본 64)의 균등 구간으로 자르고 노드가 번갈아 가진다. Dynamo Strategy 3와 같은 모양이라 사실상 고정 슬롯(B1-A)이다
- Google Slicer: 부하 인식 consistent hashing을 18개월 쓰다가 표 방식으로 바꿨다. [slicer-scylla-tablets.md](slicer-scylla-tablets.md) 질문 3

## 핵심 아이디어

해시 공간을 **원(링)** 으로 보고, 노드마다 **토큰**(링 위의 점)을 여러 개 찍는다. key는 해시한 뒤 시계 방향으로 처음 만나는 토큰의 노드가 맡는다. "구간 → 노드" 표를 따로 만들지 않고, **토큰 목록만 있으면** 누구나 같은 답을 계산한다.

```text
key → 해시 → 링 위의 위치 → 시계 방향으로 가장 가까운 토큰 → 그 토큰의 노드

        ·A1
   ·C2        ·B1       노드 A·B·C, 노드당 토큰 2개 (vnode)
 ·B2            ·A2     key k 가 B1 과 A2 사이에 있으면 → A2 → 노드 A
   ·C1        ·
        ·
```

노드 하나에 토큰을 여러 개 두는 것이 **virtual node(vnode)** 다. 토큰이 하나뿐이면 노드 위치가 운에 따라 쏠리고, 노드가 빠질 때 그 구간 전체가 시계 방향 이웃 하나에게 몰린다. 토큰을 V개로 나누면 구간이 잘게 흩어져 균형이 좋아지고, 빠진 노드의 부하가 여러 이웃에게 나뉜다 (Dynamo §4.2).

```text
[고정 슬롯]   구간 S개 고정, "슬롯 → 노드" 표를 중앙이 관리
[해시 범위]   구간 가변, "구간 → 노드" 표를 중앙이 관리
[링 vnode]    구간 = 토큰 사이. 표 없음. 토큰 목록(노드당 V개)만 공유
```

세 방식 모두 "해시 공간을 구간으로 덮는다"는 점은 같다. 링은 **구간의 주인이 토큰 위치로 정해져 있어 마음대로 바꿀 수 없다**는 점이 다르다.

## 효과와 한계

### 기대 효과

- **표가 없다**: 노드당 토큰 V개 × 노드 수만 있으면 된다. 중앙 없이도 모두가 같은 답을 낸다 (ketama가 이렇게 쓴다)
- **노드 증감 시 최소 이동**: 노드가 들어오면 새 토큰 바로 앞 구간만 새 노드로 간다. 기존 노드끼리는 안 움직인다(monotonicity, Karger Theorem 4.1). 기대 이동 비율은 1/(n+1) (Theorem 4.4)
- **빠진 노드의 부하가 분산**: 토큰 V개가 링 여기저기 흩어져 있어, 한 노드가 빠지면 그 부하를 V개 이웃이 나눠 받는다 (Dynamo §4.2)
- **이기종 노드**: 성능 좋은 노드에 토큰을 더 준다 (Dynamo §4.2 세 번째 장점)

### 한계

- **균형이 운에 달렸다**: 토큰을 무작위로 찍으면 구간 길이가 제각각이다. 균형을 맞추려면 토큰이 많이 필요하다(Karger: 노드당 Θ(log n)개, Cassandra: 무작위 배치면 256개). 토큰 16개에 노드 60대면 가장 큰 노드와 작은 노드의 소유 구간이 22%까지 벌어진다 (Instaclustr, 토큰 8개 기준)
- **원하는 곳으로 못 옮긴다**: 바쁜 노드의 토큰을 빼면 그 구간은 **시계 방향 이웃**에게 간다. 한가한 노드를 골라 보낼 수 없다. Slicer가 표로 바꾼 이유 (§4.4.4)
- **split이 없다**: 뜨거운 구간을 쪼개려면 토큰을 추가해야 하는데, 토큰은 노드에 묶여 있어 "구간만 쪼개고 주인은 유지"가 안 된다
- **토큰이 많으면 운영 비용**: 구간 수 = 토큰 수라서 repair·streaming·범위 조회 작업이 토큰 수에 비례한다. Cassandra가 256을 16으로 내린 이유 (CASSANDRA-13701, The Last Pickle)
- **"표가 없다"는 말은 절반만 맞다**: 토큰 목록은 결국 "경계 → 노드" 표다. 노드 n대 × V개 = 항목 n·V개로, hash-range-tablet에서 노드당 T = V개일 때와 크기가 같다. 다른 점은 이 표를 **중앙이 고칠 수 없다**는 것뿐이다

## 읽으면서 답할 질문

답변에는 근거가 되는 자료와 절을 함께 적는다. 자료에 없는 내용은 **"자료에 없음"**으로 표시한다.

### 1. vnode는 왜 필요하고, 노드당 몇 개여야 하는가?

**답**

토큰이 노드당 하나면 노드 위치가 무작위라 구간 길이가 크게 다르다. Karger는 노드(bucket)마다 점을 **k·log(C)개**(C = 노드 수 상한) 찍으면 높은 확률로 각 노드가 전체의 O(1/|V|)만 맡는다고 증명했다(balance). 실제 시스템은 상수를 경험으로 정했다.

| 시스템 | 노드당 토큰 | 배치 | 비고 |
|---|---|---|---|
| ketama | 100~200 | 무작위(해시) | 클라이언트 라이브러리. 중앙 없음 |
| Dynamo Strategy 1 | T (값 미공개) | 무작위 | 구간 길이 제각각. 운영 문제로 Strategy 3로 교체 |
| Cassandra ≤ 2.x | 256 | 무작위 | 균형 잡히지만 운영 비용 큼 |
| Cassandra 4.0 | **16** | 알고리즘(replica-aware, 가장 큰 구간을 쪼개는 위치에 배치) | 무작위면 16개로는 "hopeless"(The Last Pickle). 알고리즘 배치로 편차 ~15% |

Instaclustr 실측(알고리즘 배치, 노드 20~500대): 토큰 8개 편차 22%, 16개 15%, 32개 11%. 노드 6~12대에서는 편차가 더 작다. 우리 규모(노드 ≤ 10대)라면 **무작위 배치는 노드당 수백 개, 알고리즘 배치는 16~32개**가 출발점이다.

**근거**

Karger **§4.2**: "we will need k·log(C) points for each bucket for some constant k", **Theorem 4.1** (2) Balance. Dynamo **§4.2**: "the random position assignment of each node on the ring leads to non-uniform data and load distribution … each node gets assigned to multiple points in the ring." Cassandra 문서 **Multiple tokens per physical node**: "the default number of tokens per node had to be quite high, at 256", "a new deterministic token allocator was added which intelligently picks tokens". Instaclustr: "a variance of roughly 15% between the nodes with the smallest and largest token space allocation"(16개), "22% variance"(8개), "11%"(32개). Dynamo의 T 값은 **자료에 없음**.

### 2. 노드가 들어오고 나갈 때 무엇이 얼마나 움직이는가?

**답**

**들어올 때**: 새 노드의 토큰 V개가 각각 어떤 구간 안에 떨어진다. 그 구간의 앞부분(이전 토큰 ~ 새 토큰)만 새 노드로 간다. 기존 노드끼리는 안 움직인다(monotone). 기대 이동 비율은 1/(n+1)이고, 떼어 오는 상대는 V개 토큰이 떨어진 자리의 주인들이라 여러 노드에서 조금씩 받는다.

**나갈 때**: 떠나는 노드의 토큰 V개가 사라지고, 각 구간은 **시계 방향 다음 토큰의 노드**로 흡수된다. 어느 노드로 갈지 고를 수 없다. 받는 쪽은 V개 토큰의 다음 주인들이라 역시 여러 노드로 흩어진다.

이동 **비율**은 표 방식(1/(n+1))과 같다. 다른 것은 (a) 상대를 고를 수 없다는 것, (b) 이동 **단위**가 "토큰 사이 구간"이라 길이가 제각각이라는 것이다. Dynamo는 (b) 때문에 애를 먹었다. 새 노드가 구간을 가져가려면 주는 쪽이 로컬 저장소를 **스캔**해서 그 구간의 key를 골라내야 했고, 성수기에는 bootstrap에 하루가 걸렸다. 구간을 고정하면(Strategy 3) 구간 = 파일이라 파일만 옮기면 된다.

**근거**

Karger **Theorem 4.1** (1) monotone: "When a new bucket is added, the only items that move are those that are now closest to one of the new bucket's associated points. No items move between old buckets." **Theorem 4.4**: 두 view 사이에 같은 bucket에 남는 item의 기대 비율은 |V₁∩V₂| / |V₁∪V₂|. Dynamo **§6.2 Strategy 1**: "when a new node joins the system, it needs to 'steal' its key ranges from other nodes. However, the nodes handing the key ranges off to the new node have to scan their local persistence store … the bootstrapping has taken almost a day to complete." Dynamo **§4.2**: "departure or arrival of a node only affects its immediate neighbors and other nodes remain unaffected."

### 3. 링을 쓰던 시스템들은 왜 표 쪽으로 갔는가?

**답**

세 시스템이 각자 다른 이유로 순수 링에서 멀어졌다.

| 시스템 | 처음 | 나중 | 이유 |
|---|---|---|---|
| Dynamo | Strategy 1: 무작위 토큰 T개, 토큰이 구간을 정함 | Strategy 3: 공간을 **Q개 균등 구간**으로 고정, 노드당 Q/S개 토큰 | 분할(partitioning)과 배치(placement)가 얽혀 있어 노드를 늘리면 구간이 바뀜. Strategy 3가 균형 최고, 멤버십 정보 1000분의 1 (Figure 8), bootstrap·아카이브 쉬움 |
| Cassandra | 토큰 1개 → 무작위 256개 | 알고리즘 배치 16개 | 256개는 균형은 좋지만 repair·streaming·가용성 비용. 적은 토큰은 무작위로는 균형이 안 잡혀 **배치 알고리즘**이 필요 |
| Slicer | 부하 인식 consistent hashing | 구간 → 서버 표 + weighted-move | 밀려난 부하가 랜덤한 곳으로 감. key 적으면 균형 나쁨. vnode 1000개 필요 |

공통점은 **"어느 구간을 어디에 둘지"를 직접 정하고 싶어졌다**는 것이다. Dynamo Strategy 3는 사실상 고정 슬롯(B1-A)이고, Cassandra의 배치 알고리즘은 "가장 큰 구간을 쪼개는 자리에 토큰을 둔다"는 중앙 계산이다. 표를 아예 없애는 순수 링(Strategy 1, ketama)은 **중앙이 없어도 되는 클라이언트 라이브러리**에서 살아남았다.

우리는 Controller가 있으므로 표를 두는 비용(전파, 버전)을 이미 치르고 있다. B1-C는 "그 비용을 치르고 얻는 것이 수치로 얼마인가"를 재는 기준선이다. Dynamo Figure 8과 같은 그림(같은 메타데이터 크기에서 균형 비교)을 우리 세 variant로 그린다.

**근거**

Dynamo **§6.2**: "The fundamental issue with this strategy is that the schemes for data partitioning and data placement are intertwined." "strategy 3 achieves the best load balancing efficiency … reduces the size of membership information maintained at each node by three orders of magnitude." "The disadvantage of strategy 3 is that changing the node membership requires coordination in order to preserve the properties required of the assignment." The Last Pickle: "Cassandra's random token allocation is hopeless when it comes to small vnode values." Slicer **§4.4.4** (slicer-scylla-tablets.md 질문 3).

### 추가 확인 항목

| 항목 | 현재 확인한 내용 | 추가로 확인할 내용 |
|---|---|---|
| 토큰 배치 알고리즘의 정확한 규칙 | Cassandra 3.0+ `allocate_tokens_for_local_replication_factor`. "replica-aware", 새 노드 토큰을 가장 큰 구간에 배치 (CASSANDRA-7032) | 알고리즘 세부(복제 고려 부분)는 **자료에 없음**(코드 `TokenAllocation`). 우리는 복제 없이 "가장 긴 구간의 중점" 규칙으로 단순화 |
| 조회 비용 | 정렬된 토큰 배열 이진 탐색 O(log nV). Karger §4.3은 구간을 나눠 기대 O(1) | 우리 `RoutingTable.lookup`도 이진 탐색이라 variant 간 차이 없음. 보조 지표 ns로 확인 |
| 복제(preference list) | Dynamo는 시계 방향 N−1개 후속 노드에 복제, vnode라 같은 물리 노드가 겹치면 건너뜀 (§4.3) | A1·A2 영역. 분배 variant와 무관하게 설계 |
| 이기종 노드 | 토큰 수로 가중치 (Dynamo §4.2) | 멘토 답(노드 성능이 다를 수 있는지)에 따라. 다르다면 ring은 토큰 수, 표 방식은 B2 계획으로 반영 |
| 노드 제거 후 경계 | 토큰이 빠지면 구간이 이웃에 흡수된다. 경계 자체는 남아도 되고(Tablet 2개가 같은 주인), 합쳐도 된다 | 우리는 이동 후 Controller가 `withMerge`로 정리할지 결정. §4.4 |

## 프로젝트 적용

공개된 설계를 참고한 실험용 variant를 `ring-vnode`로 구현한다. **Dynamo Strategy 1**(토큰이 구간을 정하는 순수 링)을 재현한다. Strategy 3는 fixed-slot과 같으므로 따로 두지 않는다.

### 적용할 내용

- 노드당 토큰 V개, 토큰 사이 구간이 Tablet. 주인은 시계 방향 다음 토큰의 노드
- 토큰 위치는 `hash(nodeId, i)`로 **결정적**으로 찍는다(불변식 2: 난수 없음). 무작위 배치의 균형 문제를 그대로 재현하면서 실험은 재현 가능
- 노드 추가 = 새 토큰 V개 자리에서 구간을 쪼개고(`prepared`, 주인 그대로) 앞쪽 조각을 새 노드로(`moves`). 노드 제거 = 떠나는 노드의 Tablet을 시계 방향 다음 주인에게
- `supportsSplit() = false`, `supportsMove() = false`. 임의 재배치가 없으므로 B2 실험에서는 **참고값**(그리디 한계)만 낸다
- `metadataBytes` = 토큰 수 × (8 B 토큰 + 4 B NodeId) + 16 B 헤더. 표가 아니라 토큰 목록 크기

### 적용하지 않는 내용

- 복제·preference list·sloppy quorum·vector clock: A1·A2 영역
- gossip 멤버십: Controller push로 대체 (design.md §4.3)
- Dynamo Strategy 2·3: Strategy 3 = fixed-slot. Strategy 2(토큰은 배치만, 구간은 균등 고정)는 fixed-slot에 "노드 → 슬롯" 규칙만 바꾼 것이라 별도 variant로 두지 않음
- Cassandra의 복제 인식 배치: 복제가 없으므로 "가장 긴 구간 쪼개기"로 단순화

### 직접 설계하고 기록할 내용

- 노드당 토큰 수 V (비교 실험은 V = G/8, 기본값은 실험 후)
- 토큰 배치 규칙 두 가지를 같은 클래스의 옵션으로: **무작위**(`hash(nodeId, i)`, Strategy 1·Cassandra ≤ 2.x) / **균등 쪼개기**(새 토큰을 현재 가장 긴 구간의 중점에, Cassandra 4.0 allocator 단순화). 둘의 균형·이동 차이가 실험 질문
- 노드 제거 후 남는 경계를 합칠지(Controller가 `withMerge`), 그대로 둘지
- `prepared`가 입력과 다른 유일한 variant이므로 Plan 계약(`prepared`는 주인 불변) 테스트를 이 variant에서 확인

비교 실험과 불변식은 프로젝트의 `design.md` §5·§6을 따른다.
