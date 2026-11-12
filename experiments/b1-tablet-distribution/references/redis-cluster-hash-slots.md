# Redis Cluster 고정 해시 슬롯

## 출처

### 스펙 문서

[Redis Cluster specification](https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec/)

- Redis 공식 문서. 논문이 아니라 구현과 동기화되는 설계 문서다.
- 읽을 부분: **Key distribution model**, **MOVED Redirection**, **Live reconfiguration**, **ASK redirection**, **Configuration epoch**, **Hash slots configuration propagation**

### 구현

[redis/redis `src/cluster.c`](https://github.com/redis/redis/blob/unstable/src/cluster.c)

- `getNodeByQuery()`: 요청을 받은 노드가 직접 처리 / `MOVED` / `ASK` / `TRYAGAIN` 중 무엇을 돌려줄지 정하는 함수.
- `clusterCommandSlots()`: 클라이언트에게 주는 "슬롯 구간 → 노드 주소" 표.

### 같은 계열의 시스템

- [Valkey 9.0 Atomic Slot Migration](https://valkey.io/topics/atomic-slot-migration/) (2025-10): Redis 포크. `ASK` 없이 슬롯 단위로 복제해 한 번에 소유권을 넘긴다.
- [Hazelcast Data Partitioning](https://docs.hazelcast.com/hazelcast/latest/architecture/data-partitioning): Java. 271 파티션, 가장 오래된 멤버가 표를 만들어 15초마다 전파. 소스 `com.hazelcast.internal.partition.impl.PartitionStateGeneratorImpl`(배치), `MigrationPlanner`(이동 순서).
- [Aerospike Smart Client](https://www.aerospike.com/features/smart-client/): 4096 파티션, 클라이언트가 2초마다 표 세대 번호를 폴링.
- [Apache Ignite Data Partitioning](https://ignite.apache.org/docs/latest/data-modeling/data-partitioning): 1024 파티션, 파티션 → 노드는 rendezvous hashing.

## 핵심 아이디어

key 공간을 처음부터 고정 개수 S의 슬롯으로 잘라 두고, "슬롯 → 노드" 표 하나로 위치를 관리한다. 노드가 늘거나 줄어도 S는 바뀌지 않고 표만 바뀐다.

```text
[mod-N]      key ──hash mod N──▶ 노드                N이 바뀌면 거의 모든 key가 이동
[고정 슬롯]   key ──hash mod S──▶ 슬롯 ──표──▶ 노드    S는 고정. 표의 몇 칸만 고치면 그 슬롯의 key만 이동
```

노드 추가, 제거, 균형 조정이 전부 "슬롯을 다른 노드로 옮긴다" 한 가지 연산으로 환원된다.

## 효과와 한계

### 기대 효과

- 표가 길이 S의 배열 하나라 조회가 O(1)이고, 메타데이터가 S × 몇 바이트로 끝난다.
- 슬롯 하나의 크기가 대략 전체/S로 균등·고정이라 이동 비용을 미리 계산할 수 있다.
- 노드 3대 → 4대일 때 이동하는 key가 전체의 1/4(mod-N은 약 3/4).

### 한계

- **슬롯보다 작은 쏠림:** hot key 하나가 슬롯 하나를 뜨겁게 만들면, 그 슬롯을 통째로 옮기는 것밖에 할 수 없다. 슬롯을 더 쪼갤 수 없다.
- **split 없음:** 바쁜 슬롯을 반으로 나누는 연산이 없다. 가변 구간 Tablet(Slicer, ScyllaDB)과의 결정적 차이.
- **S는 되돌릴 수 없다:** 너무 작으면 균형이 거칠고(노드 9대에 슬롯 16개면 2:1 편차), 너무 크면 표와 하트비트가 커진다.
- **이동 중 라우팅이 복잡:** 레거시 방식은 `ASK`/`ASKING`/`MIGRATING`/`IMPORTING` 네 상태를 클라이언트와 서버가 다 이해해야 한다. Valkey 9가 이것을 없앤 이유다.

## 읽으면서 답할 질문

답변에는 근거가 되는 자료와 절을 함께 적는다. 자료에 없는 내용은 **"자료에 없음"**으로 표시한다.

### 1. 슬롯 수 16384는 어디서 나왔고, 우리는 몇으로 해야 하는가?

**답**

노드가 하트비트마다 "내가 가진 슬롯" 비트맵을 실어 보내는데, 16384비트 = 2KB다. 노드 1000대 상한에서 하트비트 크기와 균형 사이의 타협이다. 표는 슬롯 수의 상한이자 노드 수의 상한(슬롯 수 ≥ 노드 수)이기도 하다.

우리는 노드 10대 안팎이고 하트비트에 비트맵을 싣지 않으므로(Controller push), 16384가 필요하지 않다. 4096이면 노드당 400개 이상이라 정수 나눗셈 편차가 0.3% 이하다. 1024·4096·16384를 실험으로 비교한다(design.md §6 S5).

**근거**

스펙 **Key distribution model**: "The cluster's key space is split into 16384 slots, effectively setting an upper limit for the cluster size of 16384 master nodes (however, the suggested max size of nodes is on the order of ~1000 nodes)." **Heartbeat packet content**: 하트비트 헤더에 "A bitmap of the hash slots served by the sending node"가 들어간다. 2KB라는 계산은 16384/8이며, "1000대 vs 2KB 타협"이라는 설명은 스펙 본문이 아니라 Redis 저자의 공개 답변(GitHub issue)에서 나온 것이다.

### 2. 표가 낡은 클라이언트와 이동 중인 슬롯은 어떻게 처리하는가?

**답**

두 가지 리다이렉트로 처리한다.

| 응답 | 뜻 | 클라이언트 동작 |
|---|---|---|
| `MOVED slot host:port` | 이 슬롯은 영구히 다른 노드 것 | 표를 고치고 재시도. 스펙은 슬롯 하나가 아니라 표 전체를 다시 받기를 권한다 |
| `ASK slot host:port` | 이동 중. 이번 요청만 다른 노드로 | 표를 고치지 않고, `ASKING`을 먼저 보낸 뒤 그 요청만 대상 노드로 |

이동 중에는 원본 A가 `MIGRATING`, 대상 B가 `IMPORTING` 상태다. A에 온 요청은 key가 A에 있으면 처리하고 없으면 `ASK`→B. B에 온 요청은 `ASKING`이 있으면 처리하고 없으면 `MOVED`→A(표상 주인은 아직 A). 이동이 끝나면 A가 `MOVED`를 보내고 그때 표가 바뀐다.

Valkey 9.0은 `ASK`를 쓰지 않는다. 슬롯을 스냅샷 + 변경분 스트리밍으로 복제한 뒤 쓰기를 잠깐 멈추고 한 번에 소유권을 넘긴다. 이동 중에도 주인은 끝까지 원본이고, 넘기는 순간에만 `MOVED`가 나간다.

**근거**

스펙 **MOVED Redirection**, **ASK redirection**, **Live reconfiguration**(`CLUSTER SETSLOT <slot> MIGRATING|IMPORTING`, `GETKEYSINSLOT`, `MIGRATE`). `cluster.c` `getNodeByQuery()`: `migrating_slot`이고 key가 없으면 `CLUSTER_REDIR_ASK`, `importing_slot`이고 `CLIENT_ASKING` 플래그가 있으면 직접 처리. Valkey는 [Atomic Slot Migration 문서](https://valkey.io/topics/atomic-slot-migration/)의 단계 설명(snapshot → streaming → pause → handoff).

### 3. 두 노드가 같은 슬롯을 자기 것이라고 하면 누가 이기는가?

**답**

노드마다 `configEpoch`(버전 번호)를 두고 두 규칙으로 푼다.

1. 슬롯의 주인이 비어 있는데 누가 주장하면 그 노드로.
2. 슬롯에 주인이 있는데 더 큰 `configEpoch`를 가진 노드가 주장하면 그 노드로 갈아탄다.

장애 조치로 승격된 노드나 슬롯 import를 마친 노드는 `configEpoch`를 올린다. 그래서 가장 최근 변경이 항상 이긴다("last failover wins"). 낡은 노드가 복귀해 옛 주장을 하면 다른 노드가 더 큰 epoch의 정보를 `UPDATE` 메시지로 보내 바로잡는다. `configEpoch`는 바뀔 때마다 디스크에 fsync해 재시작 후 역행을 막는다.

우리 `OwnershipEpoch`는 이 규칙을 Tablet 단위로 쪼갠 것이다. Redis는 노드마다 하나, 우리는 Tablet마다 하나다.

**근거**

스펙 **Configuration epoch**, **Hash slots configuration propagation**(Rule 1, Rule 2), **UPDATE messages, a closer look**.

### 추가 확인 항목

| 항목 | 현재 확인한 내용 | 추가로 확인할 내용 |
|---|---|---|
| 노드 증감 시 어느 노드에서 슬롯을 뗄지 | 스펙은 `redis-cli --cluster rebalance`에 맡기고 규칙을 적지 않음 | **자료에 없음**. Hazelcast `PartitionStateGeneratorImpl`(기존 주인 유지 → 미배정 분배 → 과잉→부족 이전)을 참고해 직접 정함 |
| 표 전파 | gossip + `UPDATE`, full mesh, 노드 100대·타임아웃 60초면 초당 330 ping | 우리는 gossip을 쓰지 않으므로 해당 없음 |
| `MOVED` 응답의 주소 인코딩 | `-MOVED <slot> <host>:<port>` 텍스트 | 우리 wire(protocol-v1)에서는 `NodeId`를 싣는다. 주소는 클라이언트가 `NodeId`→주소 매핑으로 푼다 |

## 프로젝트 적용

공개된 설계를 참고한 실험용 variant를 `fixed-slot`으로 구현한다.

### 적용할 내용

- 고정 개수 슬롯과 "슬롯 → 노드" 배열 표
- 노드 증감을 "슬롯 이동" 한 연산으로 환원
- `MOVED` 리다이렉트와 "큰 epoch가 이긴다" 충돌 규칙

### 적용하지 않는 내용

- CRC16: variant 간 공정 비교를 위해 공통 `KeyHash`(MurmurHash3)를 쓴다.
- hash tag: 단일 key 연산만 다룬다.
- `ASK`/`ASKING`/`MIGRATING`/`IMPORTING`: 이동 중 라우팅은 Valkey 9 방식(표상 주인이 끝까지 처리, 핸드오프에 `MOVED`)으로 세 variant 공통.
- gossip 전파: Controller push + 응답 피기백 + `MOVED`로 고정.

### 직접 설계하고 기록할 내용

- 슬롯 수 S (기본 4096, 1024·16384와 비교)
- 노드 추가·제거 시 슬롯을 떼어 올 노드 선택 규칙
- 표 직렬화 형식(연속 슬롯이 같은 주인이면 구간으로 압축)과 `metadataBytes` 계산

비교 실험과 불변식은 프로젝트의 `design.md` §5·§6을 따른다.
