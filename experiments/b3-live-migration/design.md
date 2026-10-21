# B3 설계

상태: 설계 초안(2026-10-21). 코드는 `core.migration`의 `package-info.java`만 있다. 공통 모델, 프로토콜, §6 실험은 모두 미구현. 구현 현황은 §7을 따른다.

## 1. 목표와 범위

### 단계 정의

이 문서의 단계 이름은 아래 표를 따른다. "1단계"만 지금 구현하고, 나머지는 모두 "후속"이다. B1·B2 문서의 "2단계"는 각 연구의 단계를 가리키므로 B3 단계와 섞어 쓰지 않는다.

| 단계 | 구현 내용 | 실행 환경 | 선행 조건 |
|---|---|---|---|
| **1단계: Stop-and-Copy Baseline** | job 상태 기계, freeze, snapshot batch 전송·ACK, 조건부 commit, cleanup, 되돌리기 | in-process, 논리 tick | 없음 (지금) |
| 후속: Pre-Copy 비교 | dirty generation, delta·`seq`, `CATCHING_UP`, 수렴 종료 조건 | in-process | 1단계의 commit·fencing 계약 고정 |
| 후속: 노드 증감 판단 | `ElasticScalingPolicy`, fake provisioner·membership | in-process | 1단계 |
| 후속: 이동 Scaling | `MigrationScheduler`, 여러 job의 동시성·전송 예산 | in-process | 1단계 |
| 후속: 실제 노드 연계 | 노드 간 migration 메시지, multi-node Runner, `MOVED` 재시도 client, lease, 실제 p99 측정, source 지연 삭제 | 실제 노드 | B1 소유권 판정·표 전파, A1 scan·hook API |
| 후속: Post-Copy 확장 | destination-owned `post-copy`, pull 경로, `PostCopyState` | in-process → 실제 노드 | Pre-Copy 비교 |

### 기능과 단계

B3는 cluster metric으로 노드 증감을 결정하고, 그 결과 생긴 "이 Tablet을 A에서 B로 옮긴다"는 계획을 **서비스를 유지한 채** 실행한다. 옮긴 뒤에는 예전 주인이 더 이상 그 Tablet을 바꾸지 못하게 한다.

| 기능 | 결정 내용 | 단계 |
|---|---|---|
| Elastic Scaling Policy | metric에서 desired node count와 `KEEP`·`SCALE_OUT`·`SCALE_IN`을 결정. tolerance·stabilization·warm-up 적용 | 후속: 노드 증감 판단 |
| Migration Job | Tablet 하나를 옮기는 상태 기계, commit 시점, 실패 시 되돌리기 규칙 | 1단계 (지금) |
| 데이터 복사 프로토콜 | snapshot·변경분(delta)·cutover를 어떤 순서로 할지. 공통 실행기 아래 source-owned와 destination-owned 계열을 분리 | 1단계: `stop-and-copy` / 후속: Pre-Copy 비교, Post-Copy 확장 |
| 소유권 전환 | cutover에서 owner·epoch를 바꾸고 낡은 owner를 차단(fencing) | 1단계: in-process 모델 / 후속: 실제 노드 연계 |
| 이동 실행 Scaling | 노드 증감으로 생긴 여러 `Change`를 받아 전체 이동 시간·동시성·대역폭·요청 영향을 측정 | 후속: 이동 Scaling |
| 속도 조절 | 동시 job 수, 전송률 제한. p99를 보며 조절하는 동적 제한은 범위 밖 | 후속: 이동 Scaling (고정 예산까지) |

**1단계는 `stop-and-copy` Baseline 하나만 구현한다.** source-owned job의 최소 상태 기계, freeze, 전체 snapshot 전송, 검증, commit, cleanup과 실패 시 되돌리기를 먼저 고정한다. 근거는 [VM Live Migration](references/vm-live-migration.md) §3.1의 pure stop-and-copy다.

`pre-copy`는 Baseline의 commit·fencing 계약이 고정된 뒤 비교 구현한다. snapshot 중 변경 추적, dirty generation, delta 순서와 수렴 종료 조건이 추가되기 때문이다. destination-owned `post-copy`([Rocksteady](references/rocksteady.md))는 그보다 뒤의 확장 후보다. 시작할 때 소유권을 넘기므로 destination → source pull, 진행 상태, 아직 안 온 key의 read 처리, 늦게 도착한 pull이 새 write를 덮지 않게 하는 규칙이 별도로 필요하다.

B3는 **노드 수를 판단하고 이동을 실행한다**. `ElasticScalingPolicy`가 desired node count와 증감 방향을 결정하지만 실제 노드 생성·종료는 외부 provisioner·membership이 수행한다. scale-out의 `READY`, scale-in의 `DRAINING` 상태에서 어떤 Tablet을 어디로 옮길지는 B1의 노드 증감 계획(`DistributionScheme.onNodeAdded/Removed`)이 만들고, load-aware placement·rebalance는 B2(`core.balance`)가 보완한다. 라우팅 표를 바꾸는 연산은 B1(`core.tablet`)이 제공한다. scale-in 노드는 모든 migration이 끝난 뒤에만 종료한다.

Scale-out/in은 두 단계로 나눈다. 먼저 B3가 metric에서 `KEEP`, `SCALE_OUT(desiredNodes)`, `SCALE_IN(desiredNodes, candidateNode)`를 결정한다. 그다음 B1·B2가 만든 `Change` 목록은 바꾸지 않고 B3가 job 수·동시성·전송 예산을 조절해 실행한다. Tablet split/merge, 일반 balancing·placement 정책, 실제 인프라 provision은 범위 밖이다.

**참고자료의 역할은 다음으로 한정한다.**

| 자료 | 채택하는 근거 | 프로젝트에서 직접 정할 부분 |
|---|---|---|
| [Elastic Scaling Policy](references/elastic-scaling-policy.md) | desired node count 비율식, tolerance, scale-in stabilization, 새 노드 warm-up, scale-out/in 비대칭 | metric 선택, threshold, window·cooldown 값, scale-in candidate |
| [VM Live Migration](references/vm-live-migration.md) | stop/pre-copy 구조, dirty 반복 전송, downtime·총 이동 시간, 256 KiB 종료 기준 | KV tombstone·적용 순서·고정 전송률에서의 추가 종료 조건 |
| [MongoDB Chunk Migration](references/mongodb-chunk-migration.md) | source-owned 요청 처리, copy → catch-up → metadata commit → cleanup, 참여 shard당 동시 이동 제한 | freeze 중 요청 응답, coordinator의 원자적 commit 방법 |
| [Redis Cluster Resharding](references/redis-cluster-resharding.md) | 데이터 이동과 소유권 확정의 분리, 완료 후 `MOVED` | `ASK`를 사용하지 않는 B1 계약과 cutover 전파 |
| [Rocksteady](references/rocksteady.md) | destination-owned post-copy, background/priority pull, aggregate throughput·tail latency 측정 | cache MISS 허용 범위, 늦은 pull과 새 write의 순서, source 장애 결말 |

각 논문의 수치는 그 시스템의 실측 결과일 뿐 OpenKache의 목표값이나 예상 결과로 사용하지 않는다.

## 2. 다른 연구와의 경계

| 대상 | B3가 기대하는 것 | 현재 상태 | 1단계 대응 |
|---|---|---|---|
| Metrics | 노드별 사용률·처리량의 같은 시간 창 집계 | 결과 Schema 합의 전 | 1단계 범위 밖. scaling 구현 전에 metric 이름·단위·window를 합의한다 |
| Provisioner·Membership | 노드 생성·종료, `JOINING → READY`, `DRAINING → REMOVED` 상태와 action 결과 | 외부 계층 계약 미정 | 1단계 범위 밖. 후속 scaling 단계에서 in-process fake로 먼저 검증한다 |
| B1 Tablet·라우팅 | 이동 계획 `Change(unit, from, to)`, cutover용 `RoutingTable.apply(Change)`, Tablet별 `OwnershipEpoch`, `MOVED` 판정 | `Tablet`, `RoutingTable`, `Change`, `Plan` 구현. 소유권 판정·표 전파는 B1 design.md의 2단계 | `Change`를 job 입력으로 그대로 쓴다. commit은 `apply(Change)`로 새 표를 만드는 것으로 표현한다. 노드의 `MOVED` 판정은 1단계 실험 안의 최소 판정으로 대신한다 |
| B2 Balancing | 재배치 계획 | Split만 설계. 재배치 정책 미정 | 1단계 입력은 테스트가 만든 `Change`나 B1 `Plan.moves()` |
| A1 KV Store | Tablet 하나의 key 전체 읽기(scan), 쓰기 관찰 hook, 받은 데이터 일괄 적재, Tablet 범위 삭제 | `KvEngine`은 `key.hashCode() % workers`로 worker를 고른다. SCAN 없음. `KvStore`의 key별 version은 DELETE 후 다시 1부터 시작 | 1단계는 실험 안의 최소 저장소로 in-process 실행. 후속 실제 노드 연계 전에 A1에 위 4개와 `KeyHash` 기준 worker 선택(B1이 요청한 것과 같음)을 요청한다 |
| A2 Workload | 이동 중 쓰기 부하 | Uniform, 고정 Hot-Key. Zipfian 미구현 | 쓰기율·쏠림(Zipf) 요청열은 B3 실험 안에서 seed 고정으로 만든다. 필요성이 확인되면 A2에 공통화를 요청한다 |
| Experiment Runner (본인) | 후속 실제 노드 연계의 실측: 3-node, `MOVED` 재시도 client, 장애 주입 | 단일 노드 전용 | 1단계는 in-process. 후속 실제 노드 연계에서 multi-node 실행과 B1 소유 client 라우팅 패키지 위치를 B1과 합의한다 |
| A1·A3 장애 처리 | 이동 중 source·destination 장애 감지 | 미구현 | 1단계는 각 단계에서 job을 일부러 중단시켜 되돌리기만 확인한다 |
| Experiment Metrics | 실험 결과 기록 | 결과 Schema 합의 전 | 1단계 결과는 테스트 출력과 CSV. Schema 확정 후 JSON으로 옮긴다 |

**캐시라서 달라지는 점.** 원본 데이터(DB)가 있어도 이동 중 성공 응답한 `SET`을 누락하면 destination에 이전 값이 남아 **stale HIT**가 될 수 있다. `DELETE`를 누락해도 지워야 할 값이 되살아나므로 마찬가지다. 따라서 1단계 정합성 기준은 "성공 응답한 모든 SET·DELETE가 cutover 결과에 반영"이다. `post-copy`에서 아직 한 번도 도착하지 않은 key를 MISS로 허용하는 정책은 이 기준과 분리해 후속 Post-Copy 확장에서 정한다.

## 3. 공통 모델

`core.model`의 `TabletId`, `NodeId`, `RoutingVersion`, `OwnershipEpoch`와 `core.tablet`의 `Change`, `RoutingTable`을 그대로 쓴다.

### 1단계 모델

1단계 `stop-and-copy` 구현에 필요한 타입만 둔다.

| 타입 | 내용 | 비고 |
|---|---|---|
| `MigrationJob` | `Change` 하나 + variant + 현재 상태 + 진행도(보낸 바이트, 마지막 ACK batch id) | job 하나 = Tablet 하나 |
| `JobState` | `PENDING → COPYING → SWITCHING → CLEANING_UP → DONE`, 실패하면 `ABORTED` | 아래 상태 표 |
| `TransferBatch` | batch id + record 수 + 직렬화 바이트 수 + checksum | destination이 검증·적용한 뒤 ACK |
| `TransferProgress` | snapshot 완료 여부 + 마지막 전송/ACK batch id + 전송 바이트 | commit 가능 여부와 지표 계산 |

**1단계 상태**

| 상태 | `stop-and-copy`의 동작 | 되돌리기 |
|---|---|---|
| `COPYING` | write를 멈추고(freeze) snapshot batch를 전체 전송 | destination 복사본 폐기, freeze 해제 |
| `SWITCHING` | 마지막 snapshot batch ACK 확인 → coordinator commit | commit 전이면 위와 같음 |
| `CLEANING_UP` | source의 Tablet 데이터 삭제 | commit 후라 되돌리지 않는다. 삭제 실패는 fenced 상태에서 재시도 |

### 후속 모델

1단계에서는 만들지 않는다. 해당 단계에서 추가한다.

| 단계 | 추가 타입·필드 | 내용 |
|---|---|---|
| Pre-Copy 비교 | `JobState.CATCHING_UP` | 직전 라운드에 바뀐 key만 재전송 (라운드 1, 2, …). 상태 순서는 §4.2 |
| Pre-Copy 비교 | `MigrationJob`의 라운드 수, 마지막 ACK `seq` | 수렴 판정과 지표 |
| Pre-Copy 비교 | `Delta` `(seq, key, PUT value \| DELETE)` | 변경분 한 건 |
| Pre-Copy 비교 | `TransferBatch`·`TransferProgress`의 마지막 `seq` | delta batch와 final `seq` 검증 |
| 노드 증감 판단 | `ClusterMetrics` | 관측 시점, 현재 노드 수, 노드별 사용률·처리량, 진행 중 action·migration |
| 노드 증감 판단 | `ScalingDecision` | `KEEP`, `SCALE_OUT(desiredNodes)`, `SCALE_IN(desiredNodes, candidateNode)`. `minNodes`·`maxNodes` 범위로 제한 |
| 노드 증감 판단 | `ScalingState` | 연속 상·하한 관측 수, stabilization 후보, warm-up·cooldown, 진행 중 action |
| Post-Copy 확장 | `PostCopyState` | commit이 먼저인 별도 상태 순서와 실패 계약 |

`post-copy`(후속 Post-Copy 확장)는 순서가 다르다. 시작 시 commit한 뒤 destination이 pull로 채우므로 위의 선형 `JobState`와 source-owned abort 계약을 그대로 적용하지 않는다. `MigrationProtocol`의 실행·측정 틀만 공유하고, `PostCopyState`와 commit 이후 실패 계약을 별도로 둔다(§8).

**commit point는 coordinator가 현재 라우팅 표를 원자적으로 한 번 교체하는 순간이다.** `RoutingTable.apply(change)`는 전달받은 불변 스냅샷에서 현재 주인이 `from`인지 확인해 새 테이블을 만들지만, 그 자체는 compare-and-swap이 아니다. 따라서 실행기는 job 시작 시의 예상 table/version을 들고 coordinator에 commit을 요청하고, coordinator가 직렬화하거나 `compareAndSet(expectedTable, newTable)`로 하나만 성공시켜야 한다. 1단계 단일 thread runner에서는 이 commit adapter를 직렬 실행으로 모사한다. 성공 시 `RoutingVersion`과 해당 Tablet의 `OwnershipEpoch`가 1씩 오른다. MongoDB의 완전 동기화 후 metadata 갱신은 commit 순서의 근거이고, OpenKache의 원자화 방법은 프로젝트 결정이다.

**전송 완료 검증은 cutover에서 Tablet 전체를 다시 스캔하지 않는다.** 1단계 snapshot batch는 record 수·직렬화 바이트 수·checksum을 포함하고 destination은 적용 후 ACK한다. 모든 snapshot batch가 ACK됐을 때만 commit한다. 후속 `pre-copy`는 delta batch와 final `seq` 검증을 추가한다. 단위·계약 테스트는 이 전송 검증과 별도로 source의 최종 상태와 destination 전체 map을 exact compare한다.

**commit 전에 source의 freeze를 확인한다.** source가 그 Tablet의 write를 더 받지 않는다는 확인(freeze 응답)을 받은 뒤에만 commit한다. 그래야 어느 순간에도 write를 받는 주인이 둘이 되지 않는다.

**freeze 중 요청 처리.**

| 요청 | 처리 | 근거 |
|---|---|---|
| read (GET) | source가 계속 처리 | write가 멈춰 있으므로 source 값이 정답이다 |
| write (SET, DELETE) | 즉시 거절. 재시도 가능한 상태(`BUSY`)로 응답 | 대기 큐에 붙잡아 두면 지연이 숨고 큐 상한·순서 규칙이 필요하다. 캐시 client는 재시도하거나 DB로 우회할 수 있다 |
| commit 후 source로 온 요청 | `MOVED`(새 owner, 새 `RoutingVersion`) | B1 §4.1. `ASK`는 쓰지 않는다([Redis Cluster Resharding](references/redis-cluster-resharding.md)) |

**fencing.** 노드 간 migration 메시지(snapshot, delta)에는 job 시작 시점의 그 Tablet epoch를 싣는다. 받는 쪽은 자기가 아는 epoch보다 낮은 메시지를 적용하지 않는다. commit 뒤에 늦게 도착한 이전 job의 delta가 새 주인의 데이터를 덮지 못하게 하기 위해서다. lease(기한부 소유권)는 1단계에 넣지 않는다. 노드 정지·네트워크 단절이 생기는 후속 실제 노드 연계에서 A1의 failover 규칙과 함께 정한다.

## 4. 공통 인터페이스: `MigrationProtocol`

variant가 구현하는 계약이다. job 실행기와 실험 코드는 이 인터페이스만 호출하고, 어떤 variant가 꽂혀 있는지 모른다.

```
name()                           실험 결과의 variant 값
begin(job)                       → JobState   PENDING에서 첫 상태로. stop-and-copy는 여기서 freeze
step(budgetBytes)                → JobState   이번 시간 단위에 보낼 수 있는 만큼 보내고 다음 상태를 돌려준다
acceptsWrite()                   → boolean    지금 source가 이 Tablet의 write를 받아도 되는가 (freeze 여부)
abort()                          → JobState   commit 전이면 되돌리고 ABORTED. commit 후면 거부
```

- **시간은 호출자가 준다.** `step`은 "이번 단위에 보낼 수 있는 바이트"를 받는다. 1단계 실험은 논리 시간 단위(tick)마다 고정 예산을 주고, 후속 실제 노드 연계에서는 실제 전송 완료로 바꾼다. 프로토콜 코드는 실제 시계를 읽지 않는다.
- 후속 `pre-copy`는 `onSourceWrite(key)` 관찰 hook을 추가한다. source의 write 반영과 dirty generation 기록을 같은 순서로 연결해야 하며, 1단계 Baseline 인터페이스에는 넣지 않는다.
- **(후속 Pre-Copy) snapshot과 delta 사이에는 barrier가 있다.** snapshot batch가 모두 ACK되기 전에는 delta를 destination에 적용하지 않는다. 늦게 도착한 옛 snapshot 값이 최신 delta를 덮지 않게 하기 위해서다.
- **commit은 프로토콜 밖에서 한다.** 프로토콜이 마지막 batch ACK(후속 Pre-Copy에서는 final `seq` ACK)까지 확인하면 실행기가 예상 table/version과 함께 coordinator에 commit을 요청한다. coordinator의 직렬화 또는 CAS가 성공해야 `CLEANING_UP`으로 간다.
- 이 인터페이스의 source-owned 계약은 `stop-and-copy`, `pre-copy`가 공유한다. `post-copy`는 실행·측정 API는 공유하지만 상태 순서와 실패 계약은 별도 contract로 테스트한다.

아래 값과 규칙은 참고 자료의 값을 그대로 쓸 수 없어 **프로젝트에서 직접 정했다**(초안). 근거는 각 항목에 적는다.

### 4.1 `stop-and-copy` (Baseline)

| 항목 | 기본값 | 근거 |
|---|---|---|
| freeze 시점 | `begin`에서 즉시 | 복사하는 동안 아무도 데이터를 바꾸지 않으므로 복사본 = 원본이 보장된다 |
| 복사 | Tablet의 key 전체를 한 번씩 | 전송량의 최솟값(Tablet 크기 × 1.0) 기준이 된다 |
| 검증 | 모든 snapshot batch의 checksum·record 수·ACK 확인. 계약 테스트는 source/destination exact map 비교 | 별도 전체 digest scan을 cutover에 추가하지 않는다 |
| cleanup | commit 직후 source 범위 삭제 | 1단계는 in-process라 삭제를 늦출 이득이 없다. MongoDB는 metadata 갱신 뒤 열린 cursor가 없어지면 비동기로 지운다(Range Migration Procedure 7단계). 후속 실제 노드 연계에서 이 방식을 검토한다 |

### 4.2 `pre-copy` (후속: Pre-Copy 비교)

상태 순서: `PENDING → COPYING(라운드 0 snapshot, 서비스 유지) → CATCHING_UP(라운드 1, 2, …) → SWITCHING(freeze → 남은 dirty 전송 → final seq ACK → commit) → CLEANING_UP → DONE`. 되돌리기 규칙은 1단계와 같다(commit 전이면 destination 폐기, freeze 해제).

| 항목 | 기본값 | 근거 |
|---|---|---|
| 변경 추적 | 두 generation의 dirty key 집합. 값은 보낼 때 source에서 읽는다 | 라운드 시작 시 `collecting`을 `sending`으로 swap하고 새 write는 새 `collecting`에 기록한다 |
| 추적 시작 | snapshot 첫 key를 읽기 **전**에 켠다 | 그 틈에 바뀐 key가 빠지는 것이 가장 흔한 버그다 |
| 삭제 | 보낼 때 source에 값이 없으면 DELETE delta(tombstone) | 안 보내면 snapshot에 들어간 옛 값이 destination에 남아 stale HIT |
| 적용 순서 | snapshot 완료 barrier 뒤 delta 적용. destination은 key별 마지막 `seq`보다 큰 delta만 적용 | 늦은 snapshot·재전송·중복이 새 값을 덮지 않게 한다. `KvStore` version 대신 job `seq`를 쓴다 |
| 종료 조건 1 | 남은 dirty의 직렬화 예상량 ≤ 256 KiB | key 바이트 + 현재 value 또는 tombstone + framing을 합한다. key 개수가 아니라 전송 바이트다 |
| 종료 조건 2 | 라운드 상한 10 | 수렴하지 않을 때의 안전장치. 상한값 자체는 근거가 없어 민감도로 확인한다(§6.2) |
| 종료 조건 3 | 이번 라운드 dirty 직렬화 예상량 ≥ 직전 라운드 예상량 | 논문 §5.3은 "계산된 전송률이 최대치를 넘으면" 종료한다. 이 실험은 전송률이 고정이라, 같은 의미로 "더 돌아도 전송할 바이트가 줄지 않으면" 멈춘다 |
| stop phase | freeze → 남은 dirty 전송 → final `seq` ACK → coordinator commit | 마지막은 짧은 stop-and-copy다(논문 §3.3 Stage 3) |
| 전송률 | 고정(실험 변수) | 논문 §5.3의 동적 전송률 제한은 범위 밖이다. 후속 이동 Scaling도 고정 예산만 비교한다 |
| 다시 바뀐 key | 건너뛰지 않음 | 논문 §5.4는 보내기 직전 이번 라운드에 또 바뀐 page를 건너뛴다. 후속 pre-copy S4(쓰기 쏠림) 결과를 보고 개선 후보로 둔다 |

**쓰기율과 수렴.** 라운드 i의 길이는 직전 dirty 양 ÷ 전송률이고, 그동안 새로 생기는 dirty는 대략 (쓰기 유입 바이트 ÷ 전송률) = ρ배다. 그래서 dirty는 라운드마다 약 ρ배가 되고, ρ < 1이면 줄어들고 ρ ≥ 1이면 줄지 않는다. 같은 key에 쓰기가 몰리면 중복이 합쳐져 실제 dirty는 이보다 작다. 이 예측을 §6.2 S3·S4로 확인한다.

**dirty generation 규칙.** 라운드가 시작되면 `collecting` 참조를 새 빈 집합으로 교체하고 이전 집합을 `sending`으로 고정한다. 전송 중 같은 key에 write가 오면 새 `collecting`에 다시 들어가므로, `sending`에서 key를 제거하거나 라운드를 끝내도 변경을 잃지 않는다. `sending`의 key는 읽는 순간의 최신 value 또는 tombstone으로 직렬화하고 그때 `seq`를 부여한다.

### 4.3 `ElasticScalingPolicy` (후속: 노드 증감 판단)

정책은 [Elastic Scaling Policy](references/elastic-scaling-policy.md)의 target tracking을 baseline으로 사용한다.

`desiredNodes = ceil(currentNodes × currentMetric / targetMetric)`

metric은 노드 수가 늘면 노드당 값이 감소하는 평균 처리량 또는 사용률이어야 한다. 결과는 `[minNodes, maxNodes]`로 제한한다. 여러 metric을 사용하면 각 metric의 desired node 수를 계산하고, scale-out은 최댓값을 채택한다. scale-in은 모든 metric이 scale-in을 허용할 때만 가능하다.

| 항목 | 후속 구현 규칙(초안) | 이유 |
|---|---|---|
| tolerance | `abs(currentMetric / targetMetric - 1)`이 tolerance 이하면 `KEEP` | target 주변의 작은 흔들림으로 인한 flapping 방지 |
| scale-out | 상한 초과가 `scaleOutConsecutiveWindows`번 이어지면 비율식으로 `desiredNodes` 계산 | 짧은 noise는 거르고 지속 부하에는 빠르게 반응 |
| scale-in | 하한 미만이 `scaleInStabilizationWindows` 동안 유지되면 한 번에 1개 감소 | availability를 위해 축소를 보수적으로 수행 |
| warm-up | 새 노드가 READY이고 초기 migration이 끝날 때까지 scale-in 금지 | 불완전한 metric으로 방금 추가한 노드를 다시 제거하지 않음 |
| cooldown | scale action 또는 migration 진행 중이면 새 결정 보류 | 같은 membership 상태에서 중복 계획 생성 방지 |
| candidate | 가장 낮은 부하의 eligible node. 동률은 `NodeId` 순 | 결정론적 baseline. 실제 drain 가능 여부는 B1 계획 결과로 확인 |

tolerance, 연속 관측 수, stabilization window의 숫자는 아직 확정하지 않는다. 공식 제품 기본값은 환경 의존적이므로 §6.3 민감도 실험의 변수로 둔다. 정책은 실제 노드를 만들거나 제거하지 않고 `ScalingDecision`만 낸다. `ScalingController`는 scale-out 노드의 `READY` 또는 scale-in 후보의 `DRAINING`을 확인한 뒤 B1 계획을 요청하고, 생성된 `Change`를 `MigrationScheduler`에 넘긴다. scale-in 종료 요청은 모든 job이 `DONE`인 뒤에만 보낸다.

## 5. 불변식

1단계 `stop-and-copy`는 아래를 지켜야 하며 계약 테스트로 검증한다. 후속 `pre-copy`도 같은 source-owned 계약을 재사용한다. commit이 먼저인 `post-copy`에는 1·4의 abort 규칙을 그대로 적용하지 않고 별도 계약을 만든다.

1. commit 전 어느 단계에서 중단해도 라우팅 표와 source 데이터는 job 시작 전과 같다.
2. 어느 순간에도 한 Tablet의 write를 받아들이는 노드는 최대 하나다.
3. 응답이 성공한 SET·DELETE는 cutover 후 destination에 모두 반영되어 있다. 이전 SET 값이나 DELETE된 key가 destination에서 되살아나지 않는다.
4. coordinator의 조건부 commit은 job마다 최대 한 번만 성공하고, `RoutingVersion`은 정확히 1, 그 Tablet의 `OwnershipEpoch`만 1 오른다.
5. commit 후 source는 그 Tablet의 요청을 처리하지 않고 `MOVED`로 응답한다. 낮은 epoch를 단 migration 메시지는 받는 쪽이 적용하지 않는다.
6. 같은 snapshot batch나 delta를 두 번 받아도 결과가 같고, ACK되지 않은 batch는 commit 조건을 만족시키지 않는다.
7. 같은 입력과 seed에는 같은 결과를 낸다. 난수와 실제 시각을 쓰지 않는다.
8. freeze는 이동 중인 Tablet 하나에만 적용된다. 다른 Tablet의 요청은 job 상태와 무관하게 처리된다.
9. 테스트 종료 시 source의 논리적 최종 상태와 destination 전체 map의 exact compare 결과가 같다.
10. 같은 `ClusterMetrics`와 `ScalingState`에는 같은 `ScalingDecision`을 낸다.
11. `desiredNodes`는 항상 `minNodes`와 `maxNodes` 사이이며 한 action이 진행 중일 때 두 번째 action을 만들지 않는다.
12. tolerance 안의 metric 변동은 node count를 바꾸지 않는다.
13. scale-in은 stabilization과 warm-up 조건을 모두 만족하고 진행 중 migration이 없을 때만 결정한다.
14. scale-out/in 결정만으로 routing table을 바꾸지 않는다. membership 변화와 B1 계획 뒤 각 migration의 commit에서만 바뀐다.

## 6. 실험 계획

### 6.1 1단계: Stop-and-Copy Baseline 검증

1단계에서 실제로 돌리는 실험이다. `stop-and-copy` 하나로 정합성과 Baseline 수치를 만든다. variant 비교는 하지 않는다.

#### 질문

1. 성공 응답한 SET·DELETE가 cutover 결과에 모두 반영되는가? 이전 값이나 지운 key가 되살아나지 않는가?
2. downtime이 Tablet 크기와 전송률에 비례하는가? (예상: 크기 ÷ B + 마지막 ACK·commit 시간)
3. 각 상태에서 중단하면 되돌리기가 불변식을 지키는가?
4. 손상·중복·누락된 batch를 commit 전에 잡아내는가?
5. freeze가 이동 중인 Tablet에만 적용되고, commit 뒤 source는 `MOVED`만 내는가?

#### 동일 조건

Tablet 하나, key N개(key 16바이트, value 128바이트 고정), 전송률 B(바이트/tick), 쓰기 요청열(쓰기율, SET·DELETE 비율)을 seed로 고정한다. 쓰기율은 **ρ = 쓰기로 바뀌는 바이트/tick ÷ B**로 나타낸다. 지표는 실행기가 관찰한 이벤트(거절된 write, 보낸 바이트, commit 시점)로만 계산한다.

#### 시나리오

| | 내용 | 변수 | 측정 | 가설 |
|---|---|---|---|---|
| B1 Tablet 크기 | 쓰기 없음 | N ∈ {1만, 10만, 100만} | downtime, 총 이동 시간, 전송량 비율 | downtime ≈ 크기 ÷ B + ACK·commit. 전송량 비율 1.0 + batch 헤더 |
| B2 이동 중 쓰기 | N = 10만 | ρ ∈ {0, 0.1, 0.5} | 거절된 write 수, 불변식 3 위반 수 | 거절 수 ≈ downtime × 쓰기율. 위반 0 |
| B3 DELETE 포함 | N = 10만, ρ = 0.1 | freeze 전 DELETE 비율 ∈ {0, 30%} | 불변식 3·9 위반 수 | 0. freeze 전에 지운 key가 destination에 없다 |
| B4 중단 | N = 1만 | 중단 시점 ∈ {COPYING 중, SWITCHING ACK 전, commit 직후, CLEANING_UP 중} | 불변식 1·3·4 위반 수 | commit 전 중단은 원상태, commit 후 중단은 destination이 정답 |
| B5 전송 이상 | N = 1만 | 주입 ∈ {checksum 손상, batch 중복, batch 누락, ACK 지연} | 전송 검증 실패 감지 수, 잘못된 commit 수 | 손상·누락은 commit 전에 감지, 중복은 결과 동일(불변식 6). 잘못된 commit 0 |
| B6 격리 | 이동 Tablet 1개 + 다른 Tablet 요청 | 다른 Tablet 요청 비율 | 다른 Tablet의 거절 수, commit 뒤 source 응답 | 다른 Tablet 거절 0. commit 뒤 이동 Tablet 요청은 모두 `MOVED` |

"가설"은 실행 전에 적은 것이고 결과가 다르면 그대로 기록한다.

#### 측정 정의

| 지표 | 정의 | 이상적인 값 |
|---|---|---|
| downtime | 그 Tablet의 write가 처음 거절된 tick부터, commit 뒤 destination에서 write가 처음 처리될 수 있는 tick까지 | 1단계에서는 Baseline 수치로 기록 |
| 총 이동 시간 | `begin`부터 `DONE`까지 | |
| 전송량 비율 | 보낸 바이트 ÷ Tablet 크기 | 1.0 + batch 헤더 |
| 거절된 write 수 | freeze 중 `BUSY`로 거절한 write 수 | downtime × 쓰기율에 비례 |
| 불변식 위반 수 | §5의 1~9를 실험 끝에 검사 | 0 |
| 전송 검증 실패 수 | batch checksum·record 수·ACK 불일치를 감지한 수 | B5에서 주입 수와 같음 |

- downtime 정의는 [VM Live Migration](references/vm-live-migration.md) §3.1의 "실행 중인 인스턴스가 없어 서비스를 못 하는 시간"을 write 기준으로 옮긴 것이다. read는 freeze 중에도 source가 처리하므로 넣지 않는다.

### 6.2 후속: Pre-Copy 비교

`pre-copy`를 추가한 뒤 6.1과 같은 입력으로 두 variant를 비교한다.

#### 질문

1. `pre-copy`는 `stop-and-copy`보다 downtime을 얼마나 줄이고, 그 대가로 전송량과 총 이동 시간이 얼마나 느는가?
2. 쓰기율이 전송률에 가까워지면 `pre-copy`는 어디서부터 수렴하지 않는가? 그때 downtime은 `stop-and-copy`에 얼마나 가까워지는가?
3. 쓰기가 일부 key에 몰리면 dirty 집합이 얼마나 빨리 줄어드는가?
4. `CATCHING_UP`을 포함한 각 상태에서 중단했을 때 되돌리기가 불변식을 지키는가?

#### 동일 조건

6.1과 같다. 여기에 key 선택 분포(균등, Zipf)를 더해 두 variant에 똑같이 준다.

#### 시나리오

| | 내용 | 변수 | 측정 | 가설 |
|---|---|---|---|---|
| S1 쓰기 없음 | ρ = 0 | N ∈ {1만, 10만, 100만} | downtime, 전송량 | `stop-and-copy` downtime = 전체 복사와 ACK 시간. `pre-copy`는 final ACK·commit 시간만 남는다 |
| S2 Tablet 크기 | ρ = 0.1 | N ∈ {1만, 10만, 100만} | downtime, 총 시간 | `stop-and-copy` downtime은 N에 비례. `pre-copy` downtime은 마지막 dirty 양에 주로 좌우된다 |
| S3 쓰기율 | N = 10만, 균등 쓰기 | ρ ∈ {0.01, 0.1, 0.5, 0.9, 1.2} | 라운드별 dirty, 종료 사유, downtime, 전송량 ÷ 크기 | dirty는 라운드마다 약 ρ배. ρ ≥ 1이면 종료 조건 3으로 멈추고 downtime이 크게 는다 |
| S4 쓰기 쏠림 | N = 10만, ρ = 0.5 | Zipf θ ∈ {0, 0.99, 1.2} | S3와 같음 | 쏠릴수록 중복이 합쳐져 빨리 수렴하고 전송량이 준다 |
| S5 DELETE 포함 | N = 10만, ρ = 0.5 | 쓰기 중 DELETE 비율 ∈ {0, 30%} | 불변식 3 위반 수 | 0. tombstone이 빠지면 여기서 드러난다 |
| S6 중단 | N = 1만 | 중단 시점 ∈ {COPYING, CATCHING_UP, SWITCHING 검증 전, commit 직후} | 불변식 1·3·4 위반 수 | commit 전 중단은 원상태, commit 후 중단은 destination이 정답 |
| 민감도 | S3를 다시 | 라운드 상한 ∈ {3, 10, 30}, 종료 임계 ∈ {64 KiB, 256 KiB, 1 MiB} | downtime, 전송량 | 상한·임계를 직접 정했으므로 그 선택이 결과를 얼마나 좌우하는지 함께 보고한다 |

#### 측정 정의

6.1의 지표에 아래를 더한다.

| 지표 | 정의 |
|---|---|
| 라운드 수, 종료 사유 | `pre-copy`가 stop phase로 넘어간 이유(종료 조건 1·2·3) |
| 전송 검증 실패 수 | 6.1 + delta batch와 final `seq` 불일치 |

### 6.3 후속: 노드 증감 판단

1단계 범위 밖이다. 1단계 Baseline과 소유권 전환 계약이 고정된 뒤 구현한다.

#### 질문

1. 지속 부하가 증가할 때 필요한 노드 수를 얼마나 빠르고 정확하게 결정하는가?
2. 짧은 spike와 target 주변 진동에서 불필요한 scale action을 얼마나 억제하는가?
3. 부하가 줄었을 때 가용성을 해치지 않고 언제 scale-in하는가?
4. 새 노드 warm-up이나 migration 중 중복·반대 방향 결정을 막는가?

#### 동일 조건

시간은 logical tick이고, 각 observation window마다 `ClusterMetrics`를 하나 공급한다. metric stream과 node별 부하는 seed로 고정한다. provisioner와 membership은 fake로 두어 action 요청, READY, migration 완료 시점을 제어한다. policy는 실제 시계를 읽지 않는다.

#### 시나리오

| | metric stream | 변수 | 측정 | 기대 |
|---|---|---|---|---|
| E1 지속 증가 | target 아래에서 시작해 상한 위로 step 증가 | 현재 노드 수 ∈ {2, 4, 8}, 증가 폭 | desired node 오차, 결정 지연, 부족 용량 면적 | 비율식에 맞는 scale-out을 연속 관측 뒤 결정 |
| E2 짧은 spike | 1~2 window만 상한 초과 후 복귀 | 연속 관측 수 ∈ {1, 2, 3} | 불필요한 action 수 | 설정된 연속 관측 수보다 짧은 spike는 `KEEP` |
| E3 target 진동 | target ± 작은 폭을 반복 | tolerance ∈ {0, 5%, 10%, 20%} | action 수, 방향 전환 수 | tolerance 안에서는 flapping 없음 |
| E4 지속 저부하 | 하한 아래로 내려가 유지 | stabilization window ∈ {1, 3, 5, 10} | scale-in 지연, 과축소 수 | window 통과 뒤 한 노드씩 감소 |
| E5 warm-up·migration | scale-out 뒤 READY와 migration 완료를 지연 | warm-up·migration tick | 중복·반대 action 수 | 완료 전 scale-in과 새 action 없음 |
| E6 경계 | 극단적으로 높거나 낮은 metric | minNodes, maxNodes | 범위 위반 수 | desired node가 항상 범위 안 |
| E7 candidate | node별 부하가 서로 다름 | 동률 포함 | 선택 node, 결정 재현성 | 가장 낮은 eligible node, 동률은 `NodeId` 순 |

#### 측정 정의

| 지표 | 정의 |
|---|---|
| decision delay | 조건이 처음 충족된 window부터 action 결정까지 |
| desired node 오차 | 정책 출력과 비율식·범위 제한으로 계산한 기대값의 차이 |
| 불필요한 action | spike 종료 후 곧바로 반대 action이 필요해진 결정 |
| direction reversal | stabilization 기간 안에 scale-out과 scale-in 방향이 뒤집힌 횟수 |
| 부족 용량 면적 | target 초과 비율을 action 완료까지 window별 합산한 값 |
| 과축소 수 | scale-in 직후 metric이 상한을 넘어 scale-out이 필요해진 횟수 |
| 중복 action | 이전 action 또는 migration 완료 전에 새 action을 낸 횟수. 이상값 0 |

### 6.4 후속: 이동 Scaling

1단계 범위 밖이다. 단일 job Baseline이 안정된 뒤 여러 `Change`를 동시에 실행하는 scheduler를 붙인다.

B1·B2가 만든 `Change` 목록은 고정하고, B3 scheduler가 동시에 몇 개를 실행하고 전송 예산을 어떻게 나누는지만 바꾼다. 목적은 재배치 결과의 품질이 아니라 **이동 workload가 커질 때 migration 실행 자체가 어떻게 확장되는지** 확인하는 것이다.

#### 질문

1. job 수가 늘어날 때 전체 완료 시간(makespan)과 aggregate throughput은 어떻게 변하는가?
2. 동시 실행 수를 늘리면 어느 지점부터 공유 대역폭과 endpoint 경합 때문에 이득이 사라지는가?
3. 같은 source 또는 destination을 공유하는 job이 많을 때 대기 시간과 starvation이 생기는가?
4. 이동 중 write가 많을 때 완료율·downtime·전송량 증폭이 어떻게 달라지는가?

#### 고정 입력과 scheduler

- 입력은 `List<Change>`이며 B3는 Tablet·source·destination을 바꾸지 않는다.
- scheduler는 재현 가능한 FIFO를 baseline으로 사용한다. placement 최적화는 하지 않는다.
- `maxConcurrentJobs`와 node별·cluster 전체 `budgetBytesPerTick`만 제어한다.
- MongoDB의 "참여 shard당 동시 migration 1개"를 첫 endpoint 제한 baseline으로 두되, OpenKache 기본값은 실험 결과로 결정한다.

#### 시나리오

| | 고정 조건 | 변수 | 측정 |
|---|---|---|---|
| M1 job 수 | endpoint가 겹치지 않는 동일 크기 Tablet, ρ = 0.1 | jobs ∈ {1, 4, 16, 64} | makespan, aggregate throughput, job 대기 시간 |
| M2 동시성 | jobs = 16, cluster 전송 예산 고정 | maxConcurrentJobs ∈ {1, 2, 4, 8} | M1 + 전송 예산 사용률 |
| M3 endpoint 경합 | jobs = 16, 동시성 = 4 | endpoint 형태 ∈ {서로 다름, source 공유, destination 공유} | node별 throughput, p50/p99 대기 시간, starvation |
| M4 write 부하 | jobs = 16, 동시성 = 4 | ρ ∈ {0, 0.5, 0.9, 1.2} | 완료율, job별 downtime, 거절 write, 전송량 증폭 |
| M5 variant | M2·M4 반복 | `stop-and-copy`, `pre-copy`, Post-Copy 확장 뒤 `post-copy` | makespan, 요청 영향, aggregate throughput |

#### 측정 정의

| 지표 | 정의 |
|---|---|
| makespan | 첫 job `begin`부터 마지막 job `DONE`까지 |
| aggregate migration throughput | 모든 job이 전송한 총 바이트 ÷ makespan |
| useful throughput | cutover 후 destination에 남은 유효 데이터 바이트 ÷ makespan |
| 전송량 증폭 | 모든 전송 바이트 ÷ 최종 유효 데이터 바이트 |
| job 대기 시간 | scheduler 입력부터 `begin`까지. p50·p95·p99와 최대값을 함께 기록 |
| 완료율 | 제한된 실험 시간 안에 `DONE`이 된 job ÷ 전체 job |
| 요청 영향 | job별 downtime, `BUSY` 거절 수. 실제 p99 latency는 후속 실제 노드 연계에서 측정 |
| 공정성 | endpoint가 같은 job들의 최소·최대 전송 바이트와 starvation job 수 |

Rocksteady의 758 MB/s와 tail latency 수치는 측정 항목을 고르는 근거이지 이 실험의 목표값이 아니다. Redis의 `ASK`는 구현하지 않으므로 source-owned variant의 요청 영향에는 `BUSY`와 commit 후 `MOVED`만 포함한다.

### 6.5 실행 환경

- in-process로 실행한다. 서버, 네트워크, 실제 시계를 쓰지 않는다. 시간은 논리 tick이고 결과의 time_domain은 virtual이다.
- 전송은 tick당 바이트 예산으로 모델링한다. 단일 job은 B, scaling 실험은 node별·cluster 전체 예산을 scheduler가 활성 job에 나눈다. 네트워크 지연과 실제 직렬화 CPU 비용은 없다.
- seed 5개 × 시나리오.
- 코드: `cache-core/src/test/java/io/openkache/core/migration/experiment/` (`main`이 있는 러너). 1단계에는 6.1 러너만 둔다. `./gradlew test`에서는 돌지 않는다(B1과 같은 방식).
- 결과: `migration-comparison/summary.csv`(6.1·6.2), `scaling-decision/summary.csv`(6.3), `movement-scaling-comparison/summary.csv`(6.4) (long 형식). node decision 결과에는 `metric`, `target`, `tolerance`, `stabilization_windows`, `current_nodes`, `desired_nodes`, `decision`을 포함한다. movement scaling 결과에는 `jobs`, `max_concurrent_jobs`, `endpoint_pattern`, `cluster_budget_bytes_per_tick`을 포함한다.
- 서버 실측(이동 중 다른 Tablet 요청의 p99, `MOVED` 재시도 수, 실제 downtime)은 B1 소유권 판정·전파와 multi-node Runner가 구현된 뒤 후속 실제 노드 연계에서 한다.

## 7. 코드 구조

```
cache-core/src/main/java/io/openkache/core/migration/
├── package-info.java              (있음)
├── MigrationJob.java              Change + variant + 상태 + 진행도
├── JobState.java                  §3 상태
├── MigrationProtocol.java         §4 인터페이스
├── TransferBatch.java             batch id + record 수 + 바이트 수 + checksum
├── TransferProgress.java          snapshot 완료 + 전송/ACK batch id + 전송 바이트
├── MigrationCommitter.java        expected table/version 기반 조건부 commit
├── stopcopy/
│   └── StopAndCopyProtocol.java   1단계 stop-and-copy Baseline

(후속) Delta.java, precopy/PreCopyProtocol.java,
       precopy/DirtyGenerations.java, postcopy/PostCopyProtocol.java,
       scaling/{ClusterMetrics, ScalingDecision, ScalingState,
       ElasticScalingPolicy, ScalingController, MigrationScheduler}.java

cache-core/src/test/java/io/openkache/core/migration/
├── SourceOwnedMigrationContract.java  §5의 1단계 불변식. 우선 stop-copy만 사용
├── stopcopy/StopAndCopyProtocolTest.java
└── experiment/                        1단계 Baseline 러너, 최소 저장소, tick 실행기, 지표

(후속) precopy/PreCopyProtocolTest.java, scaling policy·scheduler 실험

experiments/b3-live-migration/
├── README.md                      variant 목록
├── design.md                      이 문서
├── references/                    논문·시스템별 정리
├── migration-comparison/          6.1 Baseline 검증과 6.2 Pre-Copy 비교 결과 (실험 실행 후)
├── scaling-decision/              metric 기반 노드 증감 판단 결과 (실험 실행 후)
└── movement-scaling-comparison/   여러 migration job의 scaling 결과 (실험 실행 후)
```

규칙:

- **패키지는 variant별로 나눈다** (`stopcopy`, `precopy`, `postcopy`). B1과 같은 이유다. "같은 기능의 다른 정책"이 아니라 복사 방식 자체가 비교 대상이다. 공통 타입은 `migration` 바로 아래에 둔다.
- **variant 하나는 클래스 하나**가 된다. 클래스 Javadoc에 참고 문헌을 적고, `references/`의 정리 문서와 이름을 맞춘다.
- **B1 타입(`Change`, `RoutingTable`, `Tablet`)은 그대로 쓴다.** 바꿔야 하면 B1 리뷰를 받는다. B3 타입은 공통 계약이 아니므로 `core.model`, `core.api`에 올리지 않는다.
- **공통 계약을 건드리지 않는다.** freeze 중 거절은 기존 `ResultStatus.BUSY`로 표현한다. 전용 상태가 필요해지면 영향 범위를 먼저 설명하고 팀 확인을 받는다.
- **실험 폴더는 비교하는 질문 기준으로 만든다.** `migration-comparison/`은 모든 복사 방식을 같은 시나리오로 비교한다. `post-copy`를 추가하면 같은 폴더의 실험에 variant가 하나 는다. `scaling-decision/`은 metric·target·stabilization을 바꾸며 노드 수 결정을 측정한다. `movement-scaling-comparison/`은 job 수·동시성·총 이동 데이터량을 바꾸며 전체 이동 시간·대역폭·요청 지연을 측정한다.

## 8. 미결 사항

| 항목 | 결정할 사람 | 기한 |
|---|---|---|
| 실제 scaling metric의 이름·단위·집계 window와 source | B3, Metrics | 실제 연계 전 |
| provisioner·membership action API와 노드 상태 전이 | B3, Infra·B1 | 후속 노드 증감 판단 전 |
| scale-in candidate가 drain 불가일 때 다음 후보·취소 규칙 | B3, B1 | 후속 노드 증감 판단 전 |
| target, tolerance, 연속 관측 수, stabilization·warm-up 기본값 | B3 | Scaling 결정 실험 후 |
| coordinator가 제공할 expected table/version 기반 원자적 commit API. `RoutingTable.apply(Change)` 자체는 CAS가 아님 | B3, B1 | 후속 실제 노드 연계 전 |
| job을 실행하는 주체 (Controller / source / destination) | B3, B1 | 후속 실제 노드 연계 전 |
| Tablet scan, 쓰기 관찰 hook, 일괄 적재, 범위 삭제 API와 `KeyHash` 기준 worker 선택 | A1 (B3 요청, B1 요청과 함께) | 후속 실제 노드 연계 전 |
| 노드 간 migration 메시지(snapshot, delta, pull)의 wire 형식과 batch checksum 알고리즘 | B3, Network/Shared Types(B1) | 후속 실제 노드 연계 전 |
| lease 도입 여부와 source 장애 시 job 처리 | B3, A1·A3 | 후속 실제 노드 연계 설계 시 |
| TTL 만료·eviction을 delta로 보내는 규칙 | B3, A1 | A1 TTL·eviction 구현 후 |
| key별 version이 필요한지 (post-copy pull과 재전송) | B3, A1 | `post-copy` 설계 시 |
| `post-copy`에서 아직 안 온 key의 read: pull을 기다릴지(Rocksteady §3.3), MISS로 답할지 | B3 | `post-copy` 설계 시 |
| `post-copy`에서 늦게 도착한 pull이 destination의 새 write·DELETE를 덮지 않게 하는 규칙 | B3, A1 | `post-copy` 설계 시 |
| `post-copy` 중 source 장애 시 결말 (남은 key를 MISS로 두고 `DONE` 처리할지) | B3 | `post-copy` 설계 시 |
| endpoint별 동시 job 수와 전송률 기본값. MongoDB의 참여 shard당 1개는 M2·M3의 baseline일 뿐 OpenKache 결정값은 아님 | B3 | Scaling 실험 후 |
| 실험 결과 JSON 형식 | Metrics, 팀 합의 | 결과 Schema 합의 후 |

**결정된 사항 (B1에서 넘겨받은 전제, 2026-10-19)**

| 항목 | 결정 | 위치 |
|---|---|---|
| Tablet 경계 | key hash(64비트) 범위 | B1 design.md §3 |
| epoch 단위 | Tablet마다 `OwnershipEpoch` 하나. 충돌은 큰 epoch가 이김 | B1 design.md §3 |
| 이동 중 라우팅 | `ASK` 없음. 표상 주인이 끝까지 처리하고 핸드오프 순간에만 `MOVED` | B1 design.md §4.1 |
| 이동 계획 형식 | `Change(unit, from, to)`. `apply`는 한 table snapshot에서 현재 주인이 `from`인지 확인해 새 table을 생성하며 원자적 교체는 호출자가 담당 | B1 design.md §3 + 코드 확인 |

**이 문서의 초안 결정 (2026-10-21, 검토 후 확정)**

| 항목 | 결정 | 위치 |
|---|---|---|
| 노드 증감 책임 | B3가 metric에서 desired node count와 증감 방향을 결정. 실제 provision은 외부 계층, 이동 계획은 B1·B2, 실행은 B3 | §1·§4.3 |
| scaling baseline | target tracking 비율식 + tolerance + 보수적 scale-in + warm-up·진행 중 migration gate | §4.3·§6.3 |
| 1단계 variant | `stop-and-copy` Baseline만 구현. `pre-copy`, `post-copy`, scaling은 후속 단계 | §1 |
| commit point | source freeze와 final ACK 뒤 coordinator가 expected table/version을 조건부로 원자 교체 | §3 |
| freeze 중 요청 | read는 source가 처리, write는 `BUSY`로 즉시 거절 | §3 |
| 변경 추적 | collecting/sending 두 dirty generation + 전송 `seq`. 삭제는 tombstone | §4.2 |
| `pre-copy` 종료 조건 | 남은 dirty 직렬화 예상량 ≤ 256 KiB, 라운드 상한 10, dirty bytes가 줄지 않으면 종료 | §4.2 |
| 전송 검증 | 1단계는 snapshot batch의 checksum·record 수·ACK를 확인한다. 후속 Pre-Copy는 delta batch와 final `seq` 검증을 추가한다. 계약 테스트는 전체 map exact compare | §3·§4.1·§4.2 |
| 이동 scaling 범위 | 노드 증감 판단과 movement 실행을 분리. movement 단계는 고정된 `Change` 목록에서 job 수·동시성·전송 예산·endpoint 경합만 변경 | §1·§6.3·§6.4 |
