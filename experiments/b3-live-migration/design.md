현재 범위는 [고정 배치 B3 명세](scaling-decision/README.md)와 [Java 모듈](java/README.md)이 우선한다. 아래는 초기 설계이며 B2 분할·병합·배치 알고리즘을 현재 필수 기능으로 추가하지 않는다.

# B3 설계

**최신 구현 현황(2026-11-02):** 아래 단계 계획 중 pre-copy와 post-copy 프로토콜·수신기·정합성 테스트를 구현했다. 상세와 확정된 post-copy 실패/조회 계약은 [protocols.md](protocols.md)에 있다. pre-copy의 라운드·seq는 공통 `MigrationJob` 필드를 변경하지 않고 protocol의 `rounds()`·`acknowledgedSequence()`로 제공하며 `DeltaBatch`로 전달한다. source-owned `JobState.CATCHING_UP`을 추가했고 post-copy는 별도 `PostCopyState`를 사용한다. S1~S5 세 방식 비교 backend와 b3-comparison-json-1 원본/요약 생성을 구현했다. S3 민감도와 별도 S6 취소·장애 비교도 구현하고 실험했다. 자체 노드 증감 비교 구현·설정·결과는 제거했다. 선행 시스템의 수 판단 7개를 공통 고정 배치에 적용한 연구 변형으로 수정했다. 원 논문 제품·실제 노드 실행의 재현은 아니다. [변경점과 실행 범위](scaling-decision/README.md), [결과](scaling-decision/results.md)를 따른다. M scheduler·다중 노드 fixture·M1~M5 backend를 구현하고 정식 210회·축소 두 반복 168회를 완료했다. 결과와 실제 실행 범위는 [다중 이동 결과](movement-scaling-comparison/results.md)를 따른다. schema v2 dirty 상세 보고서와 실제 KV/peer 연계는 후속이다. 아래 1단계·후속 구분과 초안 설명은 원래 구현 순서이며 최신 완료 여부는 이 문단과 구현 안내를 기준으로 한다.

Baseline 실행: 공통 Runner 연계와 §6.1 SC1~SC6 결과 보관까지 구현했다. S1~S5 variant 비교 backend도 구현했다. S3 민감도와 별도 S6 취소·장애 비교 backend도 구현했다. 실제 KV/peer 연계·scaling과 v2 dirty 라운드 상세는 후속이다. 실행 방법은 [migration-comparison/README.md](migration-comparison/README.md), 보관된 실행은 [Baseline 보고서](migration-comparison/runs/run-2026-10-22T04-48-53.944252700Z-a4354ce4/repeat-001/results.md)와 [조건별 요약](migration-comparison/runs/run-2026-10-22T04-48-53.944252700Z-a4354ce4/repeat-001/summary.csv)을 따른다. S1~S5는 2026-10-23 generator v2 full 240회를 참조 run으로 선정했고 migration-comparison/results.md와 B3 통합 results.md에 정리했다. SC 수치는 별도 Baseline으로 보존한다.

## 1. 목표와 범위

### 단계 정의

이 문서의 단계 이름은 아래 표를 따른다. pre-copy·post-copy의 in-process 프로토콜은 구현했고 각 비교 실험 backend는 후속이다. B1·B2 문서의 "2단계"는 각 연구의 단계를 가리키므로 B3 단계와 섞어 쓰지 않는다.

| 단계 | 구현 내용 | 실행 환경 | 선행 조건 |
|---|---|---|---|
| **1단계: Stop-and-Copy Baseline** | job 상태 기계, freeze, snapshot batch 전송·ACK, 조건부 commit, cleanup, 되돌리기 | in-process, 논리 tick | 없음 (지금) |
| Pre-Copy 비교 | dirty generation, delta·`seq`, `CATCHING_UP`, 수렴 종료 조건 구현. 비교 backend는 후속 | in-process | 1단계의 commit·fencing 계약 |
| 후속: 노드 증감 판단 | 선행 알고리즘 재현 대상·입출력·평가 환경 선정 | 미구현 | 원문 전제와 B1/B2 실행 계약 확인 |
| 후속: 이동 Scaling | `MigrationScheduler`, 여러 job의 동시성·전송 예산 | in-process | 1단계 |
| 후속: 실제 노드 연계 | 노드 간 migration 메시지, multi-node Runner, `MOVED` 재시도 client, lease, 실제 p99 측정, source 지연 삭제 | 실제 노드 | B1 소유권 판정·표 전파, A1 scan·hook API |
| Post-Copy 확장 | destination-owned `post-copy`, pull 경로, `PostCopyState` 구현. 비교 backend·실제 연계는 후속 | in-process → 실제 노드 | 별도 실패·요청 계약 |

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

**1단계의 기준형은 `stop-and-copy`다.** source-owned job의 최소 상태 기계, freeze, 전체 snapshot 전송, 검증, commit, cleanup과 실패 시 되돌리기를 고정했다. 근거는 [VM Live Migration](references/vm-live-migration.md) §3.1의 pure stop-and-copy다.

`pre-copy`는 Baseline의 commit·fencing 계약을 재사용하고 snapshot 중 변경 추적, dirty generation, delta 순서와 수렴 종료 조건을 추가했다. destination-owned `post-copy`([Rocksteady](references/rocksteady.md))는 시작할 때 소유권을 넘긴다. destination → source pull, 별도 진행 상태, 미도착 key의 read 재시도, 늦게 도착한 pull의 write 덮어쓰기 방지와 장애 후 재개를 [별도 계약](protocols.md)으로 구현했다.

B3는 **노드 수를 판단하고 이동을 실행한다**. `ElasticScalingPolicy`가 desired node count와 증감 방향을 결정하지만 실제 노드 생성·종료는 외부 provisioner·membership이 수행한다. scale-out의 `READY`, scale-in의 `DRAINING` 상태에서 어떤 Tablet을 어디로 옮길지는 B1의 노드 증감 계획(`DistributionScheme.onNodeAdded/Removed`)이 만들고, load-aware placement·rebalance는 B2(`core.balance`)가 보완한다. 라우팅 표를 바꾸는 연산은 B1(`core.tablet`)이 제공한다. scale-in 노드는 모든 migration이 끝난 뒤에만 종료한다.

Scale-out/in은 두 단계로 나눈다. 먼저 B3가 metric에서 `KEEP`, `SCALE_OUT(desiredNodes)`, `SCALE_IN(desiredNodes, candidateNode)`를 결정한다. 그다음 B1·B2가 만든 `Change` 목록은 바꾸지 않고 B3가 job 수·동시성·전송 예산을 조절해 실행한다. Tablet split/merge, 일반 balancing·placement 정책, 실제 인프라 provision은 범위 밖이다.

**참고자료의 역할은 다음으로 한정한다.**

| 자료 | 채택하는 근거 | 프로젝트에서 직접 정할 부분 |
|---|---|---|
| [선행 알고리즘](references/elastic-scaling-algorithms.md) | 실제 시스템별 관측·계산·행동·평가 절차 | OpenKache 재현 대상·생략·변경 범위의 별도 결정 |
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
| Experiment Metrics | 실험 결과 원본·요약과 재현 metadata | 현재 SC Baseline은 공통 Runner의 JSON 원본·CSV 요약·Markdown 보고서 생성과 실행 결과 보관까지 구현 | JSON 원본은 실행한 PC에 남기고 반복별 요약·보고서를 Git에 보관한다. 후속 비교 형식과 공통 Runner의 필드·schema 연결은 §6.6에 정의한다 |

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

variant가 구현하는 source-owned 기본 계약이다. 현재 Baseline 실행기는 `StopAndCopyProtocol`의 추가 lifecycle 연산도 직접 호출한다. 후속 비교 실행기는 §6.6의 test 전용 adapter를 통해 variant를 선택하고, 공통 측정 코드가 구체 protocol에 의존하지 않도록 한다. 현재 다섯 연산만으로 commit·진행도 관측까지 공통화됐다는 뜻은 아니다.

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

### 4.2 `pre-copy` (in-process 프로토콜 구현, 비교 backend는 후속)

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

### 4.3 노드 수 판단 — 고정 배치에서 비교

현재는 7개 수 판단만 NodeCountPolicy에 구현하고 fixed-tablet-lpt-v1 배치를 고정한다. [선행 시스템별 알고리즘](references/elastic-scaling-algorithms.md)과 [실험 안내](scaling-decision/README.md)에 원문과의 변경점·실행 모형·비교군을 명시한다. 현재 전략 클래스·선택 설정·B3Workflow는 구현됐다. 아래 초기 설계보다 [현재 Java 모듈](java/README.md)을 우선하며 실제 노드 I/O adapter·실서버 실행은 후속이다.

| 선행 시스템 | 검토할 알고리즘 | 필요한 전제 |
|---|---|---|
| SCADS 수 판단 변형 | 성능 경계 기반 수 탐색 | 공통 고정 배치·단일 owner |
| ElastMan | feedforward 모델 + p99 feedback | 실측 지연·처리량과 성능 식별 |
| E-Store 수 판단 | CPU 상·하한 한 노드 증감 | 평균 CPU·공통 고정 배치 |
| Accordion 수 판단 변형 | 서버 수 후보 선택·공통 배치 가능성 | MILP 재배치 제외 |
| P-Store | SPAR 예측 + 동적 계획법 | 부하 이력·재구성 시간·유효 용량 |
| TIRAMOLA | MDP 기반 행동 평가 | 상태·행동·보상·관측 이력 |
| PAX 수 판단 변형 | ARIMA·평균 CPU 범위 | partition 선택·replica·hint 제외 |

특정 target tracking 식이나 자체 비용식을 재현 대상 대신 사용하지 않는다. 원문 알고리즘을 단순화하거나 Tablet에 맞게 바꾸면 변경점을 명시한다. 현재 없는 복제·실측 p99·학습 이력을 가상 값으로 채워 원문 재현으로 표시하지 않는다.

실행 연계의 책임은 기존 B1/B2/B3 경계를 따른다. B3가 증감을 판단하고, 외부 계층이 노드 준비·종료를 담당하며, B1/B2 계획을 migration 실행에 전달한다. 새 노드가 실제로 부하를 맡는 시점과 준비 완료를 구분하고 축소는 drain 완료 뒤에만 수행한다. 구체 API와 중복 작업 처리 규칙은 재현 대상 선정 후 합의한다.

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

연구 담당 영역 B1~B3와 구분하기 위해 이 절의 실험 ID는 SC1~SC6를 쓴다.

| | 내용 | 변수 | 측정 | 가설 |
|---|---|---|---|---|
| SC1 Tablet 크기·전송 예산 | 쓰기 없음 | N ∈ {1만, 10만, 100만}; N=10만에서 B ∈ {16, 64, 256 KiB/tick} | downtime, 총 이동 시간, 전송량 비율 | downtime ≈ 크기 ÷ B + ACK·commit. 전송량 비율 1.0 + batch 헤더 |
| SC2 이동 중 쓰기 | N = 10만 | ρ ∈ {0, 0.1, 0.5} | 거절된 write 수, 불변식 3 위반 수 | 거절 수 ≈ downtime × 쓰기율. 위반 0 |
| SC3 DELETE 포함 | N = 10만, ρ = 0.1 | freeze 전 DELETE 비율 ∈ {0, 30%} | 불변식 3·9 위반 수 | 0. freeze 전에 지운 key가 destination에 없다 |
| SC4 중단 | N = 1만 | 중단 시점 ∈ {COPYING 중, SWITCHING ACK 전, commit 직후, CLEANING_UP 중} | 불변식 1·3·4 위반 수 | commit 전 중단은 원상태, commit 후 중단은 destination이 정답 |
| SC5 전송 이상 | N = 1만 | 주입 ∈ {checksum 손상, 항목 수 불일치, 데이터 크기 불일치, batch 중복/누락, 마지막 ACK 지연, 잘못된 ACK, ACK 누락} | 전송 검증 실패 감지 수, 잘못된 commit 수 | 손상·누락은 commit 전에 감지, 중복은 결과 동일(불변식 6). 잘못된 commit 0 |
| SC6 격리 | 이동 Tablet 1개 + 다른 Tablet 요청 | 다른 Tablet 쓰기 ∈ {0, 1, 16회/tick} | 다른 Tablet의 거절 수, commit 뒤 source 응답 | 다른 Tablet 거절 0. commit 뒤 이동 Tablet 요청은 모두 `MOVED` |

"가설"은 실행 전에 적은 것이고 결과가 다르면 그대로 기록한다.

#### 측정 정의

| 지표 | 정의 | 이상적인 값 |
|---|---|---|
| 쓰기 차단 지속 시간 | 최초 write 거절부터 현재 owner의 write 재개를 관측한 tick까지. 정상 이동은 destination, 취소는 source의 재개를 측정. downtime은 이 지표의 정상 경로 값이다 | 취소·실패 복구 비용도 포함 |
| 총 이동 시간 | `begin`부터 `DONE` 또는 `ABORTED`까지. cleanup 재시도 포함 | |
| 전송량 비율 | 보낸 바이트 ÷ Tablet 크기 | 1.0 + batch 헤더 |
| 거절된 write 수 | freeze 중 `BUSY`로 거절한 write 수 | downtime × 쓰기율에 비례 |
| 불변식 위반 수 | §5의 1~9를 실험 끝에 검사 | 0 |
| 전송 검증 실패 수 | batch checksum·record 수·ACK 불일치를 감지한 수 | 손상·잘못된 ACK는 1; 누락은 검증 검출 0이며 timeout으로 복구 |

- downtime 정의는 [VM Live Migration](references/vm-live-migration.md) §3.1의 "실행 중인 인스턴스가 없어 서비스를 못 하는 시간"을 write 기준으로 옮긴 것이다. read는 freeze 중에도 source가 처리하므로 넣지 않는다.

### 6.2 후속: Pre-Copy 비교

`pre-copy`를 추가한 뒤 아래 S1~S6의 각 조건을 `stop-and-copy`와 함께 새로 실행한다. SC1~SC6는 Baseline 검증, S1~S6는 동일 입력의 variant 비교로 유지한다. 현재 SC 결과를 S 비교의 측정값으로 대신 사용하지 않는다. 실행 계약과 산출물은 §6.6을 따른다.

#### 질문

1. `pre-copy`는 `stop-and-copy`보다 downtime을 얼마나 줄이고, 그 대가로 전송량과 총 이동 시간이 얼마나 느는가?
2. 쓰기율이 전송률에 가까워지면 `pre-copy`는 어디서부터 수렴하지 않는가? 그때 downtime은 `stop-and-copy`에 얼마나 가까워지는가?
3. 쓰기가 일부 key에 몰리면 dirty 집합이 얼마나 빨리 줄어드는가?
4. `CATCHING_UP`을 포함한 각 상태에서 중단했을 때 되돌리기가 불변식을 지키는가?

#### 동일 조건

key/value 크기·batch framing·전송 예산 모델은 6.1과 같다. 기본값은 N=10만, B=64 KiB/tick, 균등 분포, SET/DELETE=70/30, seed=1~5다. 각 행에서 명시한 N·rho·DELETE 비율·분포만 바꾼다. S1의 rho=0, S2의 rho=0.1, S3~S6의 기본 rho=0.5를 사용한다. 두 variant에는 동일 초기 데이터와 tick별 요청 **시도** 열을 준다(§6.6). finite Zipf는 S1~S5 비교 backend에 구현했다. 공통 Workload Generator의 Zipf와 v2 전체 비교 suite는 별도다. θ=0은 균등 분포로 정의하고 두 variant가 같은 생성기를 사용한다.

#### 시나리오

| | 내용 | 변수 | 측정 | 가설 |
|---|---|---|---|---|
| S1 쓰기 없음 | ρ = 0 | N ∈ {1만, 10만, 100만} | downtime, 전송량 | `stop-and-copy` downtime = 전체 복사와 ACK 시간. `pre-copy`는 final ACK·commit 시간만 남는다 |
| S2 Tablet 크기 | ρ = 0.1 | N ∈ {1만, 10만, 100만} | downtime, 총 시간 | `stop-and-copy` downtime은 N에 비례. `pre-copy` downtime은 마지막 dirty 양에 주로 좌우된다 |
| S3 쓰기율 | N = 10만, 균등 쓰기 | ρ ∈ {0.01, 0.1, 0.5, 0.9, 1.2} | 라운드별 dirty, 종료 사유, downtime, 전송량 ÷ 크기 | dirty는 라운드마다 약 ρ배. ρ ≥ 1이면 종료 조건 3으로 멈추고 downtime이 크게 는다 |
| S4 쓰기 쏠림 | N = 10만, ρ = 0.5 | Zipf θ ∈ {0, 0.99, 1.2} | S3와 같음 | 쏠릴수록 중복이 합쳐져 빨리 수렴하고 전송량이 준다 |
| S5 DELETE 포함 | N = 10만, ρ = 0.5 | 쓰기 중 DELETE 비율 ∈ {0, 30%} | 불변식 3 위반 수 | 0. tombstone이 빠지면 여기서 드러난다 |
| S6 중단 | N = 1만, ρ = 0.5 | 공통 경계 ∈ {COPYING, SWITCHING 검증 전, commit 직후}; CATCHING_UP은 pre-copy 전용 추가 검증 | 불변식 1·3·4 위반 수 | commit 전 중단은 원상태, commit 후 중단은 destination이 정답 |
| S3 민감도 | S3를 다시 | 라운드 상한 ∈ {3, 10, 30}, 종료 임계 ∈ {64 KiB, 256 KiB, 1 MiB} | downtime, 전송량 | 상한·임계를 직접 정했으므로 그 선택이 결과를 얼마나 좌우하는지 함께 보고한다 |

S6에서 `CATCHING_UP`은 stop-and-copy에 없는 상태이므로 미지원으로 기록하고 해당 행의 차이값을 계산하지 않는다. 비교 전용 한쪽 결과를 다른 상태의 Baseline 결과와 짝짓지 않는다. S3 민감도는 별도 `case_id`로 구분하며 pre-copy 설정은 라운드 상한 10, 종료 임계 256 KiB가 기본이다. 각 설정 조합마다 동일 입력의 stop-and-copy를 함께 실행하되, 이 두 알고리즘 설정은 stop-and-copy에서 적용 대상 아님으로 기록한다.

2026-10-28 S6는 별도 InterruptionExperiment/InterruptionResearchMain으로 구현했다. 공통 readyToCommit 취소·commit 직후 취소 거부·부분 cleanup 실패, source-owned 복사/마지막 ACK 대기 취소, pre-copy CATCHING_UP 취소, post-copy source 접근 불가·pull timeout의 복구 및 미복구를 구별한다. extended profile은 partial live delta 적용·ACK 후 취소까지 10경계 × seed 5개 × 세 방식 = 150행 중 실행 90개·미지원 60개다. 기존 full/quick 135행의 재생성은 유지한다. b3-s6-json-1 원본의 protocolOutcome과 실험 검증 outcome을 분리하고 FAILED를 이동 완료로 간주하지 않는다. 원본·실행·검증 계약은 migration-comparison/README.md의 S6 절을 따른다. source-owned 방식의 마지막 ACK 대기 실제 상태가 COPYING일 수 있으므로 상태 이름 대신 ACK barrier를 공통 경계로 사용한다.

#### 측정 정의

6.1의 정합성 검사를 재사용하고 쓰기 차단은 실제 BUSY 시작부터 재개까지 측정한다. 비교용 전송량 비율의 분모는 두 방식에 공통인 **이동 시작 전 데이터 bytes**로 고정하고, 각 방식의 최종 데이터 bytes도 별도로 기록한다. 이동 중 성공한 DELETE 수가 달라도 분모 변경을 전송 효율 개선으로 혼동하지 않기 위해서다. 아래 지표를 더한다.

| 지표 | 정의 |
|---|---|
| 라운드 수, 종료 사유 | `pre-copy`가 stop phase로 넘어간 이유(종료 조건 1·2·3) |
| 전송 검증 실패 수 | 6.1 + delta batch와 final `seq` 불일치 |

### 6.3 후속: 선행 알고리즘의 노드 증감 검증

현재 [선행 메커니즘 가상 실험](scaling-decision/README.md)은 별도 7조건으로 실행한다. 아래 E1~E7은 더 넓은 후속 평가 질문이며 이 전체 계획이나 실제 노드 연계를 완료했다는 의미가 아니다.

| 시나리오 | 확인할 질문 |
|---|---|
| E1 지속 증가 | 용량 확보까지의 지연과 서비스 성능 저하가 얼마나 큰가? |
| E2 짧은 spike | 준비·이동 비용에 비해 불필요한 증감이 발생하는가? |
| E3 반복 변동 | 증감 방향 전환과 데이터 왕복 이동을 얼마나 유발하는가? |
| E4 지속 저부하 | drain 가능한 축소로 자원을 얼마나 회수하는가? |
| E5 준비·migration 지연 | 이동 중 서비스 간섭과 새 수요를 구분해 대응하는가? |
| E6 자원·배치 경계 | min/max·메모리·Tablet 배치 제약을 만족하는가? |
| E7 노드 선택·쏠림 | 선택한 노드와 재배치가 실제 hotspot을 해소하는가? |

hot range·이동 hotspot·단일 hot key를 고정 Tablet 경계에서 관측한다. split/merge와 Slicer 배치 비교는 B3에서 제외한다.

원문 재현 범위와 관측·모델의 준비 여부를 먼저 확정한다. 정책에는 같은 외부 요청열·초기 데이터·seed를 주되, 정책에 따라 달라지는 사용률·배치를 다시 관측한다. 정책과 무관한 고정 사용률을 서비스 결과처럼 재생하지 않는다.

측정은 실제 서비스의 처리량·지연 목표 위반, 준비부터 부하 분담까지의 시간, 누적 노드 사용, 실제 이동 bytes, 계획 계산 시간, 안전성 위반을 기준으로 설계한다. 가상 시간과 실환경 결과는 분리한다. 필요 노드 수를 특정 비교 정책의 공식으로 정의해 다른 방법의 정답으로 삼지 않는다. 예측·학습 방식은 입력 이력과 평가 구간을 분리한다.

### 6.4 이동 Scaling — M1~M5 구현·실험 완료

현재 [선행 Scheduler](scheduler-comparison/README.md)는 고정 목적지의 9실행 정책만 비교한다. CAMIG·후보 목적지 선택을 삭제했고 full810×2·quick324×2로 재실행했다. M1~M5는 FIFO 조건 변화 실험으로 보존한다.

단일 이동 후속 단계로 여러 `Change`를 동시에 실행하는 scheduler를 구현했다. 실제 세 protocol을 다중 노드 메모리 저장소에서 호출한 정식 210회와 축소 반복 168회를 완료했다. 구현 위치·전체 설정·공식 run·해석 제한은 [다중 이동 안내](movement-scaling-comparison/README.md)와 [결과](movement-scaling-comparison/results.md)를 따른다. 아래 설계에서 M3는 노드 동시 상한 1/4를 함께 비교하고, M4는 Pre-copy 기준, M5는 세 방식의 동일 조건 비교로 실행했다.

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
| M5 variant | M2·M4 반복 | `stop-and-copy`, `pre-copy`, `post-copy` | makespan, 요청 영향, aggregate throughput |

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

- in-process로 실행한다. 서버·네트워크·실제 시계를 쓰지 않는다. `time_domain=virtual`, 단위는 tick이다.
- 단일 이동은 tick당 byte 예산으로 모델링한다. 여러 job은 node별·cluster 전체 예산을 scheduler가 활성 job에 나눈다. ACK와 framing의 비용 포함 범위를 manifest에 명시한다.
- seed 1~5를 사용한다. 설정·입력·생성기 버전이 같아야 같은 요청열로 판단하며, seed만 같은 것을 동일 입력으로 간주하지 않는다.
- 실험의 정식 진입점은 config를 읽는 공통 `experiment-runner`다. B3 main과 최소 adapter는 `cache-core/src/test/java/io/openkache/core/migration/experiment/`에 있으며 연구 backend로 실행한다. 현재 Baseline·S1~S5·S3 민감도·S6·선행 메커니즘 비교 backend의 config 기반 호출이 구현됐다. M1~M5의 MovementResearchMain도 공통 Runner로 실행하며 실제 노드 증감 통합은 후속이다. 연결 계약은 §6.6을 따른다. main 전체 실험은 JUnit에서 자동 실행하지 않고 대표 경로와 계약을 검증한다.
- 모든 실험 결과는 실험 폴더의 `runs/<execution_id>/`에 실행별로 둔다. §6.1·§6.2는 `migration-comparison/`, §6.3은 `scaling-decision/`, §6.4는 `movement-scaling-comparison/`이다. 현재 §6.1 Baseline의 [실행 보고서](migration-comparison/runs/run-2026-10-22T04-48-53.944252700Z-a4354ce4/repeat-001/results.md)와 조건별 요약을 보관한다. Migration의 공식 run은 실험별 입구 문서에서 지정한다. 선행 메커니즘 결과는 scaling-decision 입구에서 정식 run을 지정하며 M의 공식 run과 결과는 movement-scaling-comparison/results.md에서 지정한다. 저장 계약과 결과 문서 계층은 §6.6을 따른다.
- 서버 실측(p99, MOVED 재시도 수, 실제 downtime)은 B1 소유권 전파와 multi-node Runner가 구현된 뒤 별도로 수행한다. virtual 결과와 같은 비교군으로 합산하지 않는다.

### 6.6 비교·확장 실험의 실행 및 저장 계약

**상태: 공통 Runner의 config 기반 B3 Baseline 호출·반복·제한 시간·JSON 원본·CSV/보고서 생성은 구현됐다(`b3-baseline-json-1`). 2026-10-22 전체 설정 실행의 반복별 요약·보고서를 Git에 보관한다. S1~S5·S3 민감도·S6 비교도 각 전용 backend와 schema로 구현됐다. 선행 메커니즘은 b3-literature-algorithms-json-1로 별도 실행한다. M1~M5는 b3-movement-json-1 backend로 구현·실행했다. 아래 통합 b3-research-v2와 실제 노드 증감 통합은 후속 설계다.**

#### 실험 단위와 동일 입력

| 구분 | 시나리오 | 실행 단위 | 결과 위치 |
|---|---|---|---|
| Baseline 검증 | SC1~SC6 | stop-and-copy 단독 검증 | migration-comparison/runs/<execution_id>/ (재실행 시 생성) |
| 단일 이동 비교 | S1~S6, S3 민감도 | 조건·seed마다 stop-and-copy와 pre-copy | migration-comparison/runs/<execution_id>/ |
| 노드 증감 판단(후속) | E1~E7 | 선행 알고리즘 선정 후 같은 외부 workload에서 비교 | scaling-decision/runs/<execution_id>/ |
| 여러 job 이동 | M1~M5 | 고정 Change 목록·workload에 scheduler·variant 적용 | movement-scaling-comparison/runs/<execution_id>/ |

`execution_id`는 한 suite 실행 전체의 고유 ID, `case_id`는 seed·variant를 제외한 조건 정의의 ID, `run_id`는 조건·seed·candidate 실행의 고유 ID다. `candidate_id`는 같은 case 안에서 알고리즘과 설정 묶음을 구별한다. 예를 들어 같은 target-tracking 정책의 tolerance 5%와 10%는 서로 다른 candidate다. `case_id`는 공통 조건과 variant별 설정 묶음을 참조한다. S3 민감도의 라운드 상한·종료 임계 조합도 서로 다른 case가 된다.

- S 비교 쌍의 키는 `(execution_id, case_id, seed)`이며 stop-and-copy/pre-copy candidate를 각각 하나씩 지정한다. E 실험은 같은 metric stream·초기 노드 상태의 candidate들을, M 실험은 같은 Change 목록·요청열의 candidate들을 비교한다. candidate마다 fresh fixture를 만든다. 같은 source/destination map을 이어서 재사용하지 않는다.
- `cases.csv`는 N, key/value 크기, B, rho, 분포·theta, SET/DELETE 비율, 사전 삭제, 장애·취소 경계, max ticks와 variant별 설정을 정의한다. S/E/M은 필요한 열을 suite별로 명시한다.
- workload는 protocol 실행 전에 정의한다. `(seed, tick, 요청 순번)`에서 key·연산·value를 정하고, BUSY·성공·복사 진행에 따라 다음 난수나 요청 수가 달라지지 않게 한다. 대용량은 결정론적 생성기로 재생해도 되지만 입력 hash·생성기 버전·최대 tick을 manifest에 남긴다.
- 두 방식에 같은 tick당 요청 시도를 공급한다. 먼저 완료한 방식에는 완료 시점까지의 요청만 집계한다. 집계 구간·시도 수·성공 수·BUSY 수를 함께 기록해 서로 다른 전체 시간에서 나온 요청 수 차이를 해석할 수 있게 한다.
- 정합성 oracle은 **각 variant에서 실제 성공한 SET·DELETE**를 반영한다. 같은 요청열이라도 Baseline의 BUSY와 pre-copy의 성공 때문에 두 방식의 최종 map은 달라질 수 있다. 각 결과를 자기 oracle과 비교한다.
- timeout은 `cases.csv`의 공통 `max_ticks`를 사용한다. pre-copy의 라운드 상한·수렴 종료는 timeout과 다르며, 최종 freeze 단계로 넘어가는 사유로 기록한다.

#### 공통 Runner 진입점과 연구 backend

**구현된 경로: `experiment-runner`의 `ResearchConfig`·`ResearchLauncher`가 설정·반복·실제 제한 시간·결과 수집을 맡고, core test의 `BaselineResearchMain`이 Baseline을 실행한다. `BaselineJson`으로 조건·seed별 원본을 저장한 뒤 다시 읽어 CSV·Markdown을 만든다. 알고리즘은 공통 Runner에 넣지 않는다.**

현재 공통 Runner의 `study`, `variant`, `scenario`는 metadata이며 B3 main을 선택하는 설정이 아니다. 기존 TCP 실행에는 서버 주소·요청 수·실제 시간 지표가 필요하므로 B3 tick 실험을 그 설정에 억지로 끼워 넣지 않는다.

공통 config는 `execution.mode=research`, `study`, `experiment`, `repository.root`, `repeat.count`, `run.timeout.ms`, `execution.timeout.ms`, `results.dir`와 `backend.main`·`backend.classpath`·번호별 인자·source 경로·결과 schema·기대 개수를 지원한다. 연구별 옵션은 `backend.option.*`에 두며 공통 Runner의 분기나 등록 없이 JVM main을 선택한다. [공통 backend 계약](../research-backends.md)을 따른다. 기본/`tcp` 모드는 기존 TCP 동작을 유지한다. 모르는 key·치환자·누락된 main은 실행 전에 거절한다. 저장소 경로는 config 기준, 출력과 classpath는 저장소 기준이다. B3 config가 `backend.option.profile=full|quick`를 main 인자로 넘기고 기대 개수를 130/125로 지정한다. 이 값의 의미는 B3에만 있으며 Runner는 해석하지 않는다. N·rho·fault 목록과 seed 1~5는 Baseline suite의 고정 조건이며 repeat는 전체 묶음의 반복이다. 개별 조건 설정과 S/E/M config는 후속으로 추가한다.

현재 B3 호출 순서는 `experiment-runner(config) → 별도 JVM의 BaselineResearchMain → JSON 원본 → 다시 읽기 → CSV·보고서`다. 연구 클래스는 core JAR에 포함되지 않으므로 먼저 `gradlew build :experiment-runner:installDist`를 수행하고 config에 core main/test classpath를 지정한다. main이 없으면 빌드와 classpath 확인을 요청하는 설정 오류로 종료한다. config는 실행할 Java main과 인자를 지정하며 shell command는 실행하지 않는다. 다른 연구도 같은 완료 계약의 backend와 config를 제공하면 Runner 수정 없이 실행한다. test fixture를 공통 Runner production 타입으로 참조하지 않는다.

현재 suite 폴더에는 config·manifest·반복 요약·입구 보고서를 두고 `repeat-001/` 등 하위에 JSON 원본·long CSV·상세 보고서·backend 로그를 둔다. 새 suite ID는 공통 `run-<시각>-<UUID 일부>` 형식이다. manifest는 source hash와 산출물별 SHA-256, 기대 반복·원본 개수를 기록한다. backend는 연구별 검증 후 status·schema·cases를 완료 파일에 기록하고 Runner는 이를 파일 개수와 대조한다. 기존 공식 run은 덮어쓰지 않는다. 설정 예시와 실행 명령은 [migration-comparison/README.md](migration-comparison/README.md)를 따른다.

Git commit은 Runner가 수집해 manifest와 원본 JSON이 공유한다. source hash는 텍스트 CRLF/CR을 LF로 정규화하는 `sha256-lf-text-v1`이며, 실제 backend classpath·Runner 클래스의 hash는 별도 기록한다. `backend.build.receipt`가 빌드 시 소스와 실행 클래스 hash를 함께 보관하고 실행 전 불일치·기록 누락을 거절한다. 빌드 기록의 자동 생성과 검증 범위는 [공통 backend 계약](../research-backends.md)을 따른다. 원본 JSON은 계속 Git에서 제외한다.

공통 Runner는 backend 프로세스 실패·실제 시간 제한·시작하지 못한 반복을 수집한다. 제한 시간이 끝나면 backend 프로세스와 자식을 종료하고 성공 산출물이 없는 반복을 SUCCESS로 표시하지 않는다. B3는 모델 안의 tick timeout·migration 취소·정합성 결과를 보고한다. 의도된 migration 취소가 정합성을 만족하면 실험 실행 자체는 성공일 수 있으며 두 상태를 혼합하지 않는다. backend 중단 시 부분 JSON·오류 상태·미실행 항목을 보존하고 complete 보고서를 만들지 않는다.

현재 `ExperimentResult`·`ResultWriter`는 TCP의 처리량·p99 등 고정 필드를 사용한다. B3 adapter는 동일한 공통 metadata 의미를 가진 별도의 연구 JSON 원본을 생성하고 TCP 고정 지표 record를 사용하지 않는다. 모든 연구를 공유하는 result API로 추출하는 일은 후속이며, B3는 p99·초당 처리량을 0으로 채우거나 tick을 ns로 환산하지 않는다. JSON의 공통 metadata 이름과 의미는 기존 Runner에 맞추고 연구 세부 지표만 별도 영역에 둔다.

#### B3 backend의 공통 실행기와 variant별 adapter

S 비교를 위한 후속 구현은 test의 experiment 내부에서 먼저 일반화한다. B1·공통 API·실제 KvEngine 계약은 유지한다. 공통 Runner에 추가할 부분은 위의 backend 선택·실행·수집과 결과 계약이며, B3 algorithm·fixture의 직접 의존성을 공통 Runner에 추가하지 않는다.

| 계층 | 책임 | 계획된 위치(test experiment 아래) |
|---|---|---|
| 조건·입력 | case 검증, 초기 데이터, tick별 요청 시도 재생 | common/ExperimentCase, WorkloadTrace |
| protocol adapter | 시작·tick 진행·취소·상태/진행도·commit 연결, 성공 write 관찰 hook | common/MigrationDriver + variant별 adapter |
| 관측·검증 | 쓰기 차단 구간, 요청 결과, 전송량, 실제 종료 사유, oracle·단일 owner·epoch·MOVED 검사 | common/RunObservation 및 검증 코드 |
| 출력 adapter | 공통 Runner의 결과 계약으로 metric/detail 전달, 파생 CSV·보고서 연결 | common/ExperimentOutput |
| 단일 이동 suite | S1~S6 × 조건 × seed × variant | comparison/MigrationComparison |
| 노드 증감 suite(후속) | 선정한 선행 알고리즘·관측·provisioner/membership | 재현 범위 확정 후 결정 |
| 여러 job suite | 다중 노드·Tablet fixture, job별 예산·scheduler 진행 | MovementExperiment·MultiMigrationFixture·MovementResearchMain (구현 완료) |

이는 클래스 책임·배치 계획이며 빈 Java 클래스나 공용 framework를 선행 생성하지 않는다. `MigrationDriver`의 최소 연산은 begin, 요청 처리, step, cancel, 불변 관측 snapshot이다. commit의 readiness·CAS·완료 통지는 adapter가 연결한다. 비교 보고서가 구체 protocol을 직접 조회하지 않게 한다.

현재 `MigrationFixture`·`TickRunner`·`BaselineResults`는 Baseline 전용으로 유지한다. pre-copy를 붙일 때 실제 중복을 기준으로 위 책임을 추출하고 기존 Baseline 회귀 결과를 확인한다. `Source`, `Destination`, ACK를 production `migration` 아래로 옮길지는 두 구현의 요구가 확인된 시점에 §7을 개정하며 결정한다.

stop-and-copy와 pre-copy는 source-owned 계약을 공유한다. post-copy는 공통 입력·측정·출력 API를 사용하되, 소유권 선전환·미도착 key 요청·commit 이후 실패 계약을 별도 adapter·계약 테스트로 구현한다. 모든 복사가 끝난 뒤에만 commit한다는 검사를 post-copy에 적용하지 않는다.

#### 시간·종료·정합성 관측

- `blocked_start_tick`은 최초 BUSY, `blocked_end_tick`은 현재 owner가 제어 write를 처리한 시점이다. tick 전후에 같은 값의 제어 write를 수행하고 variant의 freeze·재개 이벤트와 대조한다. 제어 요청은 workload 통계와 dirty 추적에서 제외한다. 같은 tick의 이벤트 순서는 별도로 보존해 경계를 판단한다.
- `writes_blocked_ticks`는 관측된 차단 구간의 합이다. 한 번도 차단되지 않았음을 확인한 완료 실행만 0으로 기록한다. 재개를 관측하지 못한 실행을 -1이나 0으로 대체하지 않고 미완료 구간과 관측 종료 tick을 남긴다.
- `total_ticks`는 begin부터 DONE/ABORTED까지, `commit_tick`은 실제 metadata 변경 시점이다. 후속 post-copy의 read 대기·미도착 key 지표는 write 차단과 분리한다.
- `outcome`은 완료·취소(timeout)·취소(요청)·취소 거부를 기록한다. runner 오류 등으로 lifecycle 결과를 관측할 수 없으면 outcome은 빈 값으로 두고 validation_status와 오류 사유를 남긴다. `termination_reason`은 실제 종료 이벤트, `stop_phase_reason`은 pre-copy가 freeze로 넘어간 이유(threshold/round-limit/non-converging 등)다. 이 둘을 섞지 않는다.
- 예상 밖 abort·관측 실패·정합성 위반·미지원 조건·runner 오류는 별도의 `validation_status`(valid/invalid/unsupported/error)와 상세 사유로 남긴다. 이를 정상적인 timeout 취소로 바꾸지 않는다. 현재 Baseline은 예상 밖 abort 시 예외 종료하는 기존 동작을 유지한다.
- `valid`인 의도된 취소도 복구 비용 비교에 포함한다. 미지원·관측 누락·오류는 성능 차이를 계산하지 않고 보고서에서 건수와 이유를 드러낸다.

#### JSON 원본과 파생 요약 · B3 후속 형식

2026-11-04 보완: Pre-copy generation별 dirty·실전송·ACK·종료 사유를 S 비교와 M/Scheduler의 개별 schema 2에 구현했다. [관측 계약과 호환성](migration-comparison/dirty-rounds.md)을 따른다. 아래 b3-research-v2는 별도의 통합 형식 제안이며 전체 구현 완료를 뜻하지 않는다. 기존 run의 관측을 소급 생성하지 않는다.

공통 Runner의 **JSON 원본·CSV 요약** 원칙을 따른다. 후속 B3 결과도 JSON이 재분석의 기준이며, CSV·Markdown은 JSON에서 다시 만들 수 있어야 한다. `b3-research-v2`는 비교용 후속 형식의 제안 식별자이며 프로젝트 전체의 schema v2 합의를 의미하지 않는다. 현재 구현된 Baseline JSON은 `b3-baseline-json-1`로 구별하며, M은 별도 b3-movement-json-1로 cases/runs/jobs/budgets/phase-rounds와 원본 재검증을 구현했다. 아래 통합 v2의 dirty generation 등 전체 상세 schema는 후속이다.

과거 SC v1은 CSV가 원본인 형식이었다. 기존 원본과 보고서는 모두 삭제했다. 당시 관측하지 않은 코드·환경 metadata를 추정해 채운 JSON을 사후 원본으로 만들지 않는다. 새 형식은 새 실행부터 적용하며, 공식 run을 대체할 때 공통 조건의 수치·정합성을 비교한다.

| 파일 | 역할 | 작성 기준 |
|---|---|---|
| config.properties | suite 실행 설정 | 공통 Runner 진입점과 연구 설정 참조. backend 미구현 동안 실행 가능한 예시를 가장하지 않는다 |
| manifest.json | suite ID·schema 식별자, 예상 case/seed/candidate, 입력·설정 hash, 생성 파일 목록·수집 상태 | 실행 시작 시 확정하고 관측된 상태로 갱신. 지표 원본의 대체물이 아님 |
| results/<run_id>.json | **개별 실행의 원본** | 공통 metadata, 실제 설정, outcome·validation·종료 사유, 단위가 있는 metrics와 rounds/decisions/jobs/budgets 세부 관측 |
| run-summary-<suite_id>.json | 반복 실행의 수집 결과 | 성공·실패·실제 시간 초과·미실행과 개별 원본 경로. 공통 Runner 요약과 의미를 맞춤 |
| cases.csv / runs.csv | 조건·실행 목록의 파생 요약 | JSON 설정·원본·수집 상태에서 생성 |
| summary.csv | 조건당 1행의 요약. seed 간 값이 다르면 최솟값~최댓값, 해당 없음은 `–` | JSON에서 생성. Git 보관 대상. 공통 TCP Runner의 wide CSV와 같은 schema로 append하지 않음 |
| metrics.csv | run ID·metric·value·unit의 숫자 long 형식 | JSON의 숫자 지표에서 생성. 로컬 분석용이며 Git에 올리지 않음 |
| rounds.csv | pre-copy 라운드별 dirty·전송 bytes·tick·전환 사유 | JSON 세부 관측의 파생 파일. 해당 suite만 생성하고 M은 job ID 추가 |
| decisions.csv | E 전용 window별 입력·현재/원하는 노드 수·결정·사유 | JSON 세부 관측의 파생 파일 |
| jobs.csv / budgets.csv | M 전용 job별 lifecycle·정합성 / tick별 node 예산 | JSON 세부 관측의 파생 파일 |
| results.md | 같은 suite의 비교표·해석·검증·누락 현황 | 원본 JSON 또는 그 원본에서 검증한 파생 요약으로 자동 생성 |

개별 원본은 기존 공통 Runner의 `executionId`, `resultSchemaVersion`, `study`, `variant`, `scenario`, `seed`, `runNumber`, `repeatCount`, `configSnapshot`, `invocation`, Git·JVM/OS metadata의 의미를 따른다. 공통 Runner의 `executionId`는 **개별 실행 ID**다. B3 폴더 경로의 `<execution_id>`는 suite 묶음 ID이므로 JSON에서는 `suiteExecutionId`로 구별하고, 기존 논리 필드 `run_id`가 JSON의 `executionId`에 대응한다. `caseId`, `candidateId`, `timeDomain=virtual`, metric/detail 영역을 연구 확장으로 정의한다. 공통 타입에 실제로 반영할 필드·호환성은 Runner 구현과 함께 검증한다.

`manifest.json`만 있고 metrics가 CSV에만 있는 형식은 JSON 원본 형식으로 인정하지 않는다. 파생 파일만 갱신해 원본과 값이 달라지지 않게 하고, 보고서 생성 전에 ID 참조·metric 단위·행 중복·원본과의 값 일치를 확인한다. 대용량 detail을 JSON 파일로 분리하면 원본 JSON에 상대 경로·hash·범위를 남긴다.

`results/<run_id>.json`은 공통 원본 결과 정책에 따라 Git에 올리지 않는다. 현재 `.gitignore`의 `experiments/**/results/*.json` 규칙이 이 경로를 포함한다. `runs/`에서 Git에 보관하는 것은 각 반복의 `repeat-*/summary.csv`·`repeat-*/results.md`뿐이다(2026-10-22 결정). config snapshot·manifest·launch·로그·완료 표시와 원본 JSON은 `.gitignore`로 제외해 실행한 PC에 남기고, 원본 JSON의 별도 보관 위치·접근 방법·hash는 실행 manifest에 기록한다. 현재 보관된 SC v1 CSV와 보고서는 없다.

해당 없는 설정·metric은 0으로 채우지 않는다. JSON에서는 해당 없음과 관측 누락을 null 및 상태·사유로 구별하고, 적용 자체가 없는 metric은 생략한다. CSV에서는 해당 없음 `–`, 관측 누락은 빈 값과 validation 사유로 구별한다. 관측된 0은 숫자 0으로 보존한다. 숫자 원본의 정밀도를 유지하고 파생 CSV·표의 비율만 소수 6자리로 반올림한다.

JSON key·방법론·코드 식별자는 영어를 유지한다. CSV 열 이름·지표명·상태 설명과 보고서는 한국어로 쓴다. 공통 CSV 열의 대응은 다음과 같다.

| 의미 | CSV 열 이름 |
|---|---|
| suiteExecutionId / caseId / candidateId / executionId | 실행 묶음 ID / 조건 ID / 비교 대상 ID / 실행 ID |
| seed / variant / timeDomain | 난수 초깃값 / 방식 / 시간 기준 |
| outcome / validationStatus | 결과 / 검증 상태 |
| terminationReason / stopPhaseReason | 종료 사유 / 쓰기 차단 전환 사유 |
| metric / value / unit | 측정 항목 / 측정값 / 단위 |

E의 방식은 정책 이름, S/M의 방식은 복사 variant다. 검증 상태는 정상·위반·미지원·오류로 표시한다. suite별 추가 열·단위도 backend 구현 시 명시적으로 고정한다.

- 새 suite마다 `runs/<execution_id>/`를 만들고 기존 폴더에는 쓰지 않는다. 공통 Runner의 누적 CSV와 별도로, B3 suite별 요약은 해당 suite의 JSON에서 생성한다.
- manifest의 예상 case·seed·candidate와 반복 수로 중복·누락을 확인한다. 중단되면 partial·오류·미실행을 원본/실행 요약에 남긴다. complete는 수집 완료이며 정합성 성공을 뜻하지 않는다.
- 서로 다른 commit/source hash·입력 hash·budget·time_domain의 실행을 자동으로 짝짓지 않는다. 한쪽 실패·미지원·관측 누락은 차이 대신 상태로 표시한다. valid인 의도된 migration 취소는 복구 비용 비교에 포함한다.
- S 비교는 같은 case·seed·반복의 `pre-copy - stop-and-copy` 차이를 먼저 계산한다. 해당 없는 metric이나 정의가 다른 metric은 차이를 계산하지 않는다. seed별 범위와 같은 seed의 반복을 구별하고 성능 신뢰구간으로 과장하지 않는다.
- CSV는 UTF-8 BOM·LF, JSON/Markdown은 UTF-8·LF다. 실제 제어 시간 제한은 ms, 모델 시간은 tick으로 명시하고 혼합 집계하지 않는다.

#### 결과 문서 계층

결과 문서는 세 단계로 둔다. 숫자는 아래 단계의 공식 run에서만 위로 옮기며, 위 단계에서 새로 계산하지 않는다.

| 단계 | 위치 | 내용 | 작성 |
|---|---|---|---|
| 실행별 | `<실험 폴더>/runs/<execution_id>/results.md` | 그 실행의 전체 표·검증·누락 현황 | runner가 같은 폴더의 원본에서 자동 생성 |
| 실험별 입구 | `<실험 폴더>/results.md` | 공식 run 지정(시나리오별), 핵심 결과 몇 줄과 출처 run 링크 | 공식 run을 정하거나 바꿀 때 사람이 갱신 |
| B3 통합 | `experiments/b3-live-migration/results.md` | 실험별 진행 상태, 결론 2~3줄, 실험별 입구 링크, 실험 사이의 해석 | 사람이 작성 |

- 한 실험 폴더 안에서도 시나리오 묶음마다 공식 run이 다를 수 있다(예: SC run과 S run). 입구 문서의 공식 run 표에 시나리오·run·형식(v1/v2)·상태를 적는다.
- 실험별 입구 `results.md`는 그 실험의 첫 공식 run이 생길 때 만든다. 그 전에는 B3 통합 문서가 폴더 README를 가리킨다. 빈 결과 문서를 미리 만들지 않는다.
- 공식 run을 바꾸면 실험별 입구와 B3 통합 문서를 같은 변경에서 함께 갱신한다.

#### 구현 순서와 완료 기준

1. 구현된 공통 Runner의 Baseline config 선택·실행·JSON 원본·파생 출력을 유지하면서 S 비교용 case·요청열·관측을 일반화한다. 새 경로에서 stop-and-copy를 실행하고 기존 공통 조건의 정합성·tick·byte가 유지되는지 확인한다.
2. pre-copy의 dirty generation·snapshot barrier·delta seq·tombstone·종료 조건과 adapter를 구현한다. S1~S3·S5·공통 S6 경계를 같은 입력의 두 variant로 비교하고, CATCHING_UP은 pre-copy 전용 검사로 남긴다.
3. Zipf를 구현하고 S4·S3 민감도·라운드 추적·비교 보고서 및 중단된 실행/한쪽 누락 처리를 검증한다. 미구현 시나리오를 포함해 전체 비교 완료라고 표시하지 않는다.
4. 선행 노드 증감 알고리즘의 재현 대상·입력·생략 범위를 확정한 뒤 E1~E7 평가와 runner를 구현한다. 동일 외부 workload를 사용하며 가상 모델과 실환경 실행을 구분한다.
5. 다중 노드/Tablet fixture와 scheduler로 M1~M5를 구현한다. 매 tick node·cluster 예산 합계와 job별 진행·전체 결과를 함께 검증한다. M5는 단일 이동 비교의 variant·pair 규칙을 재사용한다.
6. post-copy는 실패·미도착 key 계약을 결정한 뒤 같은 비교 입력·출력 체계에 추가한다. source-owned 계약을 만족하는 척 표시하지 않는다.

S1~S5의 1차 비교 backend는 ComparisonExperiment/ComparisonResearchMain이며 b3-comparison-json-1을 사용한다. 16조건 × seed 5개 × 세 방식의 원본과 phaseEvents를 생성한다. 실행 config는 migration-comparison/comparison.properties와 comparison-quick.properties다. 별도의 sensitivity.properties와 sensitivity-quick.properties는 S3 임계·라운드 상한 45조건 × seed 5개 × 세 방식 = 675회를 실행한다. workloadHash로 알고리즘 설정을 제외한 요청 생성 규격의 동일성을 검증하고 실제 라운드 수·종료 사유별 건수를 요약한다. S6는 별도 InterruptionResearchMain과 interruption.properties/interruption-quick.properties로 실행하며 b3-s6-json-1에 예상 취소·완료·미복구 실패·미지원을 구별한다. v2 dirty 라운드 상세는 후속이다.

현재 Baseline adapter의 실제 원본은 `repeat-NNN/results/<run_id>.json`에 있으며, 같은 반복 폴더의 CSV·보고서를 JSON에서 다시 생성한다. `BaselineResearchMain --rebuild <반복 폴더>`로 재생성할 수 있다. 수집 상태는 suite의 `run-summary.json`과 manifest에 남긴다. 아래 §7의 기존 v1 run 및 이 JSON run을 비교용 v2 결과로 간주하지 않는다.

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
├── stopcopy/StopAndCopyProtocol.java   stop-and-copy Baseline
├── precopy/
│   ├── PreCopyProtocol.java       dirty generation·라운드·final ACK·외부 commit
│   ├── PreCopyReceiver.java       session·epoch·snapshot barrier·seq 검증 staging
│   └── DeltaBatch.java            PUT/DELETE·seq·CRC·metadata ACK
└── postcopy/
    ├── PostCopyProtocol.java      소유권 선전환·priority/background pull·실패/재개
    ├── PostCopyState.java         source-owned와 분리된 상태
    └── PullReply.java             pull metadata·payload·CRC

(후속) scaling/{ClusterMetrics, ScalingDecision, ScalingState,
       ElasticScalingPolicy, ScalingController, MigrationScheduler}.java

cache-core/src/test/java/io/openkache/core/migration/
├── SourceOwnedMigrationContract.java  §5의 1단계 불변식. 우선 stop-copy만 사용
├── stopcopy/StopAndCopyProtocolTest.java
├── precopy/PreCopyProtocolTest.java
├── postcopy/PostCopyProtocolTest.java
├── experiment/LiveMigrationFixture.java   pre/post-copy source·전송·routing adapter
└── experiment/                        현재 Baseline 러너·fixture·tick·JSON codec·Runner용 BaselineResearchMain
    ├── common/                       (후속) 입력·adapter·관측·출력
    ├── comparison/                   (후속) MigrationComparison
    ├── decision/                     (후속) 선행 알고리즘 재현 범위 확정 후 구성
    └── movement/                     (후속) MovementScalingExperiment

(후속) scaling policy·scheduler 실험, v2 dirty 라운드 상세 원본

experiments/b3-live-migration/
├── README.md                      variant 목록
├── design.md                      이 문서
├── protocols.md                   구현된 pre/post-copy lifecycle·요청·실패 계약
├── references/                    논문·시스템별 정리
├── results.md                     (공식 run 선정 후 작성) B3 통합 상태·결론·링크
├── migration-comparison/
│   ├── README.md                   SC1~SC6 방법·지표, S1~S6 비교 계획
│   ├── results.md                  (공식 run 선정 후 작성) 실험별 입구
│   └── runs/                      (재실행 시 생성)
│       ├── <suite_execution_id>/   JSON Baseline: config·manifest·반복 요약·repeat-NNN/
│       └── <execution_id>/         (후속) S 비교 v2: manifest·results/*.json·파생 CSV·results.md
├── scaling-decision/
│   ├── README.md                   E1~E7 안내(준비됨)
│   ├── results.md                  (첫 공식 run 후 생성) 입구
│   └── runs/<execution_id>/        (후속 생성) 정책·설정 비교 원본과 보고서
└── movement-scaling-comparison/
    ├── README.md                   M1~M5 안내(구현·실험 완료)
    ├── results.md                  (첫 공식 run 후 생성) 입구
    └── runs/<execution_id>/        job·scheduler·variant 정식 결과
```

규칙:

1단계 구현 연결(아래 제한은 현재 Baseline에 적용; 후속 일반화의 범위·순서는 §6.6):

- `MigrationJob`은 Change·variant·상태·진행도의 불변 스냅샷이다. protocol은 상태 전이마다 새 스냅샷을 만들고 원래 begin 입력은 session 식별에 사용한다.
- `MigrationProtocol`의 공통 다섯 연산은 유지한다. `StopAndCopyProtocol`은 외부 실행기용 `job`, `state`, `expectedTable`, `readyToCommit`, `committed`를 추가 제공한다.
- 저장소·전송 경계는 `StopAndCopyProtocol`의 내부 `Source`, `Destination`, `Acknowledgement`로 둔다. 별도 production 파일이나 공통 계약을 추가하지 않는다. 최소 adapter와 조건부 CAS coordinator, tick 실행기는 test의 `experiment`에 둔다.
- protocol은 SWITCHING에서 기다린다. 외부 실행기가 freeze·ACK·staging 완료를 확인하고 `MigrationCommitter.commit(job, expectedTable)`을 호출한다. 성공 뒤 게시된 table을 `committed`에 전달하면 cleanup을 진행한다. commit은 복사 완료 다음 tick, cleanup은 그 다음 tick부터다.
- batch는 기존 검증 metadata에 마지막 표시와 불변 record payload를 더했다. CRC32C와 실험 framing(헤더 29 bytes, record 길이 필드 8 bytes)을 사용한다. 실제 peer wire 계약은 아니다.
- 전송은 부분 batch도 byte 예산을 소비하며 미사용 예산은 이월하지 않는다. 마지막 batch도 ACK가 있어야 SWITCHING으로 간다. 전달과 ACK polling을 분리하며 누락·지속적인 잘못된 ACK는 실행기의 pre-commit timeout으로 취소한다.

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
| 실제 KV의 key별 version과 post-copy resolved/tombstone adapter의 연결 | B3, A1 | in-process는 job별 resolved 집합으로 구현. 실제 KV 연계 전 version 합의 |
| `post-copy` 미도착 GET | B3 | 구현: priority pull 예약 + BUSY 재시도. source 장애 후 UNAVAILABLE. [구현 계약](protocols.md) |
| 늦은 pull과 destination write·DELETE 순서 | B3, A1 | 구현: resolved/tombstone이 baseline 적용을 차단. 실제 KV adapter 연계는 후속 |
| `post-copy` source 장애 | B3 | 구현: FAILED, 소유권·성공 쓰기 보존, 동일 source/fence 복구 후 resume. 미도착을 MISS/DONE으로 처리하지 않음 |
| endpoint별 동시 job 수와 전송률 기본값. MongoDB의 참여 shard당 1개는 M2·M3의 baseline일 뿐 OpenKache 결정값은 아님 | B3 | Scaling 실험 후 |
| 실험 결과 JSON 형식 | Metrics, 팀 합의 | 결과 Schema 합의 후 |
| `runs/` 커밋 범위: 파일은 각 반복의 `summary.csv`·`results.md`로 결정됨. 어느 run을 커밋할지(공식 run만 / 전부)는 미정 | B3 | S 비교 첫 실행 전 |
| suite ID와 개별 실행 ID의 구분. 현재 공통 연구 Runner의 suite ID는 run-시각-UUID 일부 형식이다 | B3, Experiment Runner | 공통 runner 구현 시 |
| SC1~SC6을 공통 runner로 재실행할 때 S1~S6과 한 조건 표로 합칠지, 시나리오 ID 체계(예: 성능 P / 검증 V) | B3 | 공통 runner 구현 시 |

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
| scaling 방법 선정 | 기존 target tracking 우선 초안은 보류. 선행 시스템별 알고리즘·전제를 조사한 뒤 재현 범위 선정 | §4.3·§6.3 |
| 1단계 variant | `stop-and-copy` Baseline만 구현. `pre-copy`, `post-copy`, scaling은 후속 단계 | §1 |
| commit point | source freeze와 final ACK 뒤 coordinator가 expected table/version을 조건부로 원자 교체 | §3 |
| freeze 중 요청 | read는 source가 처리, write는 `BUSY`로 즉시 거절 | §3 |
| 변경 추적 | collecting/sending 두 dirty generation + 전송 `seq`. 삭제는 tombstone | §4.2 |
| `pre-copy` 종료 조건 | 남은 dirty 직렬화 예상량 ≤ 256 KiB, 라운드 상한 10, dirty bytes가 줄지 않으면 종료 | §4.2 |
| 전송 검증 | 1단계는 snapshot batch의 checksum·record 수·ACK를 확인한다. 후속 Pre-Copy는 delta batch와 final `seq` 검증을 추가한다. 계약 테스트는 전체 map exact compare | §3·§4.1·§4.2 |
| 이동 scaling 범위 | 노드 증감 판단과 movement 실행을 분리. movement 단계는 고정된 `Change` 목록에서 job 수·동시성·전송 예산·endpoint 경합만 변경 | §1·§6.3·§6.4 |


## 독립 prototype 실행 범위 보정 (2026-11-04)

사용자 지시에 따라 개인 B3 실험의 필수 완료 기준에서 공통 서비스 통합을 제외한다. 자체 저장소·라우팅 fixture와 세 migration 프로토콜을 독립 Java B3ResearchLauncher로 직접 실행한다. 공통 자료형은 모델로 재사용하되 B1/B2 controller·실제 KvEngine·Network·공통 Runner 기동을 요구하지 않는다. 증감 판단과 이동 실행의 연결·검증은 독립 B3 prototype 안에서 진행할 수 있다. 이전 절의 공통 연계는 호환·선택 사항이며 개인의 남은 필수 작업으로 해석하지 않는다.
