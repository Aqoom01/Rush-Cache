# A1 설계

상태(2026-10-28): crash-only Primary-Backup 기준형과 고정 3노드 VR을 구현했다. 초기 설계와 실제 구현의 차이는 §9~10 및 [현재 실행 문서](protocol-comparison/README.md)를 따른다. 아래 구현 계획 중 이번 범위를 벗어난 항목은 완료로 보지 않는다.

## 1. 목표와 범위

A1은 "Primary가 죽었을 때 Backup이 얼마나 빨리, 얼마나 잃지 않고 이어받는가"를 복제 방식별로 같은 조건에서 잰다.

| 기능 | 결정 내용 | 단계 |
|---|---|---|
| 복제 전달 | 쓰기는 Primary만 받고, Primary가 Backup에 순서대로 전달한다 | 1단계 (지금) |
| 쓰기 확정 조건 | 언제 클라이언트에 OK를 보내는가: 전원 ack(동기) / 즉시(비동기) / 과반수 / 전원 통과 / 지연 상한 | 1단계: 동기만. 비동기는 설정으로 2단계 |
| 장애 감지 | Primary↔Backup heartbeat와 timeout | 2단계. A3 SWIM 연동 포인트 (§2) |
| 승격 | Backup이 스스로 Primary가 되는 규칙. 선출 없이 승계 순서를 미리 고정 | 2단계 |
| fencing | 옛 Primary 차단. 모든 메시지에 `OwnershipEpoch`를 붙이고 낮은 epoch는 `STALE_EPOCH`로 거부 | 2단계 |
| 클라이언트 리다이렉트 | Backup이 쓰기를 받으면 `NOT_PRIMARY` + `Redirect(primary)` | 2단계 |
| VR 변형 | 과반수 commit + view change | 3단계 |
| Raft / Chain / Rosé 변형 | 조사 완료. 3단계 뒤 시간이 허용하면 | 4단계 |

1단계는 기준형(`primary-backup`)의 **동기 복제 경로**(전달 → ACK → 응답)만 만든다. 참고 기법은 [Primary-Backup](references/primary-backup.md)이고, 비교 대상은 [Redis Replication + Sentinel](references/redis-replication-sentinel.md)이다.

A1은 **노드 1대 = Tablet 1개**로 단순화한다. Tablet 여러 개를 노드마다 다른 Primary로 두는 것(B1·B3와의 결합)은 범위 밖이다. 죽은 노드가 **복귀해 따라잡는 것(recovery)** 도 범위 밖이며, 옛 Primary 복귀 시나리오에서는 강등과 fencing만 확인한다.

### 왜 "Fast"인가

| 조각 | 기준형의 선택 | 빨라지는 이유 |
|---|---|---|
| 동기 복제 | 클라이언트가 OK를 받은 쓰기는 Backup에 반드시 있다 | 승격 후 복구할 것이 없다 |
| 선출 없음 | 승계 순서가 미리 정해져 있다 | timeout ≈ failover 시간 |
| 짧은 timeout | 수백 ms | 감지가 빠르다. 대신 오판이 는다 (§6에서 측정) |

약점은 두 가지다. 네트워크 분단 시 Primary가 둘이 되는 split brain, 그리고 ACK 대기로 늘어나는 쓰기 지연. VR이 view 번호 + 과반수로 이 둘을 푸는 두 번째 변형이다.

## 2. 다른 연구와의 경계

| 대상 | 관계 | 현재 상태 | 1단계 대응 |
|---|---|---|---|
| A3 Failure Detection (`core.failure`) | **역할이 겹친다.** 기준형의 "heartbeat + timeout"은 SWIM이 하는 일이다 | `SwimFailureDetector`, `MembershipListener`가 `develop`에 있다 | 1단계는 감지를 넣지 않는다. 2단계에서 A1 자체 heartbeat를 가장 단순하게 넣되, 감지 결과를 받는 쪽을 `MembershipListener`(membership 변경 callback, detector owner thread에서 호출) 모양으로 열어 두고 5주 차에 SWIM을 끼운다. 자체 heartbeat와 SWIM 중 무엇을 썼는지 결과에 기록한다 |
| A2 Replication (`core.replication.ionia`) | 같은 상위 패키지를 쓴다 | `feat/workload` 브랜치에 골격 (미병합) | A1은 `core.replication.primarybackup`, `core.replication.vr` 하위 패키지만 쓴다. `core.replication/package-info.java`를 고칠 때 A2와 충돌하지 않게 먼저 병합된 쪽에 맞춘다 |
| KV Store (`core.kv`, 본인) | 로컬 저장소 | `KvEngine` 단일 노드 GET/SET/DELETE | 변경하지 않는다. A1 엔진이 `CacheEngine`으로 주입받아 Backup ACK 후 적용한다. EXISTS/COUNT는 별도 작업 |
| B1 Tablet (`core.tablet`) | `EpochStore`가 `TabletId` 단위로 epoch를 저장한다 | `fixed-slot`까지 구현 | 1단계는 Tablet 1개(고정 `TabletId`)로 쓴다. 다중 Tablet은 범위 밖 |
| Simulation (공통) | 3-node, crash/partition 재현 | **미정** (Java 대체 수단 없음) | 1·2단계 테스트는 in-memory `Transport`(메시지 큐, 지연·유실 주입)와 가상 `TimeSource`로 in-process 실행한다. 공통 Simulation이 정해지면 그쪽으로 옮긴다 |
| Experiment Runner / Workload (`experiment-runner`) | 실측 | 단일 노드, 반복 실행까지. 3-node와 장애 주입 **미구현** | §6의 failover 실측은 Runner에 "Primary kill 시각 기록 + 새 Primary로 재시도"가 필요하다. 담당(B3·김현호)과 협의 전까지는 in-process 측정만 한다 |
| Metrics | 결과 기록 | 결과 Schema 합의 전 | 1단계 결과는 테스트 출력. Schema 확정 후 JSON으로 옮긴다 |

## 3. 기준형(Primary-Backup)의 다섯 조각

Primary-Backup 논문(1993)은 "Primary가 받아 Backup에 전달한다"만 정하고 감지·승격은 비워 둔다. 그 빈칸을 아래처럼 채운다. 값과 규칙은 참고 자료에 없는 것이라 **프로젝트에서 직접 정했고** 근거를 각 항목에 적는다. 상세와 Redis 비교는 [references/primary-backup.md](references/primary-backup.md), [references/redis-replication-sentinel.md](references/redis-replication-sentinel.md)를 따른다.

| # | 조각 | 규칙 | 우리 코드 |
|---|---|---|---|
| 1 | 복제 전달 | Primary가 쓰기마다 op-number를 1 올려 로그에 쌓고 모든 Backup에 보낸다. Backup은 op-number 순서대로만 적용한다 | `PeerMessage`를 구현한 record (`Replicate(epoch, opNumber, command)`) + `Transport.send` |
| 2 | ACK 대기 (동기) | Backup이 로컬 KV에 적용한 뒤 ACK. Primary는 **모든** Backup의 ACK를 받은 뒤 클라이언트에 OK. deadline 안에 못 받으면 `TIMEOUT` | `ReplicateAck(epoch, opNumber)`. 대기는 owner worker에서 상태로 관리하고 `join()` 하지 않는다 |
| 3 | heartbeat | Primary가 주기적으로 Backup에 보낸다. 쓰기가 흐르는 동안은 `Replicate`가 heartbeat를 겸한다 | `Heartbeat(epoch, lastOpNumber)`. 주기는 `TimerEvent`로 |
| 4 | timeout 승격 | Backup이 timeout 동안 아무 메시지도 못 받으면 **승계 순서상 다음 노드**가 epoch를 1 올려 Primary가 된다. 투표 없음. 승계 순서는 `NodeId` 순으로 고정 | `OwnershipEpoch` 증가 → `EpochStore.save`. 모든 노드에 `Promoted(epoch)` 통지 |
| 5 | 클라이언트 리다이렉트 | Backup이 클라이언트 쓰기를 받으면 `NOT_PRIMARY` + `Redirect(primary)`. 낮은 epoch가 붙은 요청·메시지는 `STALE_EPOCH` | `Result.redirect(NOT_PRIMARY, …)`, `ResultStatus.STALE_EPOCH` |

조각 4의 "NodeId 순" 승계는 **비동기 설정에서는 덜 받은 노드가 올라갈 수 있다.** 이것이 기준형의 알려진 약점이고, VR의 "최신 로그 선택"과 비교하는 지점이다. 동기 설정에서는 모든 Backup이 같은 로그를 가지므로 문제가 없다.

조각 5의 fencing은 epoch를 **올리는 것**과 낮은 epoch를 **거부하는 것** 두 규칙으로 나뉜다. Redis는 전자(config epoch)만 있고 요청 단위 거부가 없어 split brain 창이 생긴다. 우리는 둘 다 넣고, "요청 단위 fencing 유무"를 §6에서 비교한다.

## 4. 변형 비교 축

5개 variant를 같은 표에 놓기 위한 축이다. 근거와 제외한 후보는 [references/survey.md](references/survey.md)를 따른다.

| 축 | `primary-backup` | `rose-bounded-lag` | `viewstamped-replication` | `raft` | `chain-replication` |
|---|---|---|---|---|---|
| 쓰기 확정 조건 | 전원 ack (동기) / 즉시 (비동기) | 즉시. 단 Backup 지연이 L 초과면 대기 | 과반수 | 과반수 | 전원 통과 (tail 도달) |
| 느린 Backup 1대 영향 | 동기면 전부 지연 | 지연이 L 안이면 없음, 넘으면 대기 | 없음 | 없음 | 전부 지연 |
| 유실 상한 | 동기: 0 / 비동기: 없음 | L개 이하 | 0 | 0 | 0 |
| Primary 교체 주체 | 외부 (heartbeat + NodeId 순) | `primary-backup`과 동일 | 프로토콜 내 (view mod N) | 프로토콜 내 (투표) | 외부 master |
| 교체 시 로그 선택 | 없음 (비동기면 유실 가능) | 불필요 (Backup이 항상 일관 prefix) | 최신 로그 채택 | 투표로 거름 | 불필요 (앞 ⊇ 뒤) |
| fencing | epoch 수동 부착 | epoch 수동 부착 | view 번호 | term | master 구성 |
| f 장애 견디는 노드 수 | f+1 | f+1 | 2f+1 | 2f+1 | f+1 |
| 읽기 처리 | Primary | Primary (Backup은 snapshot 읽기 가능) | Primary | Leader | tail (CRAQ: 전원) |

공통 대응: view-number / term / config epoch / 사슬 구성 버전은 모두 `core.model.OwnershipEpoch`로, 낮은 번호 거부는 `STALE_EPOCH`로, Primary가 아닌 노드의 응답은 `NOT_PRIMARY` + `Redirect`로, (client-id, request-number)는 `ClientId` + `OperationId`로 대응한다. 변형마다 다른 것은 `PeerMessage` 종류와 확정·교체 규칙뿐이어야 한다.

## 5. 불변식

variant 구현은 모두 아래를 지켜야 하며, in-memory `Transport`로 단위 테스트한다. 1·2는 variant마다 조건이 다르고 3~6은 공통이다.

1. **확정의 의미.** 클라이언트가 `OK`를 받은 쓰기는 그 variant의 확정 조건을 만족하는 노드 집합에 존재한다. 동기 `primary-backup`은 모든 Backup, VR·Raft는 과반수, Chain은 전원, Rosé는 "Primary + L개 이내로 뒤처진 Backup".
2. **유실 상한.** failover 뒤 살아남은 Primary에 없는 `OK` 쓰기의 개수가 variant의 상한(0 또는 L) 이하다. 비동기 `primary-backup`은 상한이 없으므로 개수를 **측정해 보고**한다.
3. **로그는 prefix.** Backup의 로그는 같은 epoch에서 Primary 로그의 앞부분과 같다. 순서가 바뀌거나 구멍이 나지 않는다.
4. **fencing.** 노드는 자기 epoch보다 낮은 epoch가 붙은 쓰기·복제 메시지를 적용하지 않고 `STALE_EPOCH`로 거부한다.
5. **결정성.** 같은 메시지 순서와 가상 시각이면 같은 상태가 된다. 난수와 실제 시계를 쓰지 않는다(Raft의 랜덤 timeout은 seed로 고정).
6. **thread 규칙.** 엔진 상태는 owner worker에서만 바꾼다. `Transport.send` 완료를 원격 commit으로 세지 않는다.

같은 epoch에 Primary가 둘이 되지 않는 것은 불변식이 **아니다.** 기준형은 분단 시 깨질 수 있고, 그것을 §6에서 측정한다.

## 6. 측정과 실험 계획

### 질문

1. 쓰기 확정 조건을 바꾸면 정상 상태 쓰기 지연과 failover 후 유실량이 어떻게 바뀌는가?
2. Primary 교체 방식(외부 승계 / view change / 투표)을 바꾸면 end-to-end failover 시간이 어떻게 바뀌는가?
3. heartbeat timeout을 줄이면 감지는 얼마나 빨라지고 오판은 얼마나 느는가?

### 지표

| 지표 | 정의 | 비교 대상 |
|---|---|---|
| 정상 상태 복제 비용 | 복제 없음 / 비동기 / 동기 / 과반수 각각 SET p50·p99, throughput | 모든 variant |
| end-to-end failover 시간 | Primary kill 시각 → 클라이언트가 새 Primary에 **첫 쓰기 성공** 시각. 감지 / 승격 / 클라이언트 재연결 구간으로 분리 | 모든 variant |
| failover 후 유실 | `OK`를 받은 쓰기 중 failover 뒤 사라진 개수 | 불변식 2 검증 |
| fencing 검증 | 옛 Primary의 쓰기가 `STALE_EPOCH`로 거절되는 비율. 요청 단위 fencing이 없을 때(Sentinel식) 받아들여진 쓰기 수 | 기준형 vs Sentinel식 |
| 느린 Backup 1대 | Backup 하나에 지연을 주입했을 때 SET p99 변화 | 동기 `primary-backup` vs VR vs Rosé |
| 오판 횟수 | 장애가 없는데 승격이 일어난 횟수 | timeout 값별 |

### 시나리오

노드 3대(VR·Raft는 f=1, 과반수 2). 모든 variant에 같은 workload(고정 seed)와 같은 장애 주입 시각을 쓴다.

| 시나리오 | 장애 | 보는 것 |
|---|---|---|
| S1 정상 | 없음 | 복제 비용 |
| S2 Primary crash | 측정 구간 중간에 Primary를 멈춘다 | failover 시간, 유실 |
| S3 느린 Backup | Backup 1대에 복제 지연 주입 | 쓰기 p99 |
| S4 옛 Primary 복귀 | Primary를 timeout보다 길게 멈췄다가 깨운다 (GC pause 모사) | fencing, split brain 창 |
| S5 timeout 민감도 | S2를 timeout 200 / 500 / 1000 ms로 반복, S1에 지연 jitter를 넣어 오판 측정 | 감지 속도 vs 오판 곡선 |

"예상"은 가설이다. 결과가 다르면 그대로 기록한다.

### 직접 정하는 값

| 파라미터 | 기본값 | 근거 |
|---|---|---|
| heartbeat 간격 | 100 ms | Raft 논문의 "election timeout 최솟값의 1/3 이하"와 VR의 COMMIT 주기를 참고 |
| timeout | 500 ms (200 / 1000 비교) | Sentinel 기본 30 s는 우리 규모에서 비교가 안 되므로 줄인다 |
| 승계 순서 | `NodeId` 오름차순 | 투표 없는 가장 단순한 규칙. VR의 view mod N과 비교 |
| 클라이언트 deadline | `RequestContext.deadline` | 동기 ACK 대기와 Rosé의 backpressure 대기가 이 안에 못 끝나면 `TIMEOUT` |
| GET 처리 | Primary만 | Backup 읽기(stale)는 variant별 선택 항목으로 뒤에 |

### 실행 환경

- 1·2단계는 in-process로 실행한다. 서버, 네트워크, 실제 시계를 쓰지 않는다. 시간은 가상 `TimeSource`, 결과의 time_domain은 virtual이다.
- 메시지 전송은 in-memory `Transport` 구현으로 하고, 지연·유실·분단을 테스트 코드에서 주입한다.
- `cache-server` 3대 + `experiment-runner`로 하는 실측(wall-clock)은 Runner에 3-node·장애 주입이 생긴 뒤에 한다(§2).

## 7. 코드 구조

설계 시점의 계획이다. `[구현]` 표시가 없는 것은 전부 **미구현**이다.

```
cache-core/src/main/java/io/openkache/core/replication/
├── package-info.java                  (있음) A1·A2 공통 설명
├── primarybackup/
│   ├── package-info.java
│   ├── PrimaryBackupEngine.java       CacheEngine 구현. 역할(Primary / Backup) 상태 기계, 동기 ACK 대기
│   ├── PrimaryBackupConfig.java       복제 모드(동기/비동기), heartbeat 간격, timeout, 승계 순서
│   ├── ReplicationLog.java            op-number 순 로그. Backup 적용 위치(appliedOp) 추적
│   └── PrimaryBackupMessage.java      PeerMessage를 구현한 sealed interface + record들
│                                      (Replicate, ReplicateAck, Heartbeat, Promoted)
└── vr/                                3단계. PREPARE / PREPAREOK / COMMIT / STARTVIEWCHANGE / DOVIEWCHANGE / STARTVIEW

cache-core/src/test/java/io/openkache/core/replication/
├── InMemoryTransport.java             메시지 큐. 지연·유실·분단 주입. 변형 공통
├── primarybackup/
│   ├── PrimaryBackupEngineTest.java   §5 불변식 1~4
│   ├── ReplicationLogTest.java
│   └── FailoverScenarioTest.java      §6 S2·S4 (in-process)
└── vr/

experiments/a1-active-passive-failover/
├── README.md                          variant 목록
├── design.md                          이 문서
├── references/                        방법론별 정리
└── failover-comparison/               variant 비교 실험 설정과 결과 요약 (실험 실행 방식 확정 후)
```

규칙:

- **패키지는 variant별로 나눈다** (`primarybackup`, `vr`, …). B2가 기능별로 나눈 것과 다른 이유는, A1의 variant는 "같은 기능의 다른 정책"이 아니라 메시지 종류와 상태 기계 자체가 다른 프로토콜이기 때문이다. 공통인 것은 `CacheEngine` 계약과 테스트용 `InMemoryTransport`뿐이다.
- **variant 하나는 `CacheEngine` 구현 하나**다. 클래스 Javadoc에 참고 문헌을 적고 `references/`의 문서 이름과 맞춘다.
- **A1 공통 인터페이스를 미리 만들지 않는다.** 두 번째 variant(VR)를 붙일 때 실제로 겹치는 것만 끌어올린다.
- **공통 계약(`core.api`, `core.model`, `core.spi`)은 바꾸지 않는다.** `Redirect`가 노드 하나만 가리키는 것처럼 부족한 점이 보이면 §8에 적고 팀 확인 후 바꾼다.
- **A1 엔진은 `KvEngine`을 소유하지 않는다.** 주입받은 `CacheEngine`에 적용만 위임하고, worker·transport 생명주기는 외부 조립부가 관리한다(A2 IONIA 골격과 같은 원칙).

## 8. 미결 사항

| 항목 | 결정할 사람 | 기한 |
|---|---|---|
| A3 SWIM 감지기를 A1 승격에 끼우는 방식 (`MembershipListener` 그대로 쓸지, A1이 받을 이벤트 종류) | A1·A3 | 2단계 시작 전 |
| `core.replication/package-info.java`의 A1·A2 분담 설명 | A1·A2 | A2 IONIA 병합 후 |
| Runner의 3-node 실행과 Primary kill 주입, failover 시각 기록 | B3·김현호와 협의 | 6주 차 Docker 실측 전 |
| Java Simulation 수단 (in-memory `Transport`를 공통화할지) | 팀 | 2주 차 |
| `Redirect`가 읽기·쓰기 대상을 따로 가리켜야 하는지 (Chain은 head와 tail이 다르다) | 공통 계약 변경이라 팀 확인 | Chain 구현 전 |
| 결과 JSON 형식 (failover 시간, 유실 개수, time_domain) | Metrics·팀 합의 | 결과 Schema 합의 후 |
| VR의 GET 처리 (논문대로 로그 경유 / Primary 로컬 읽기) | A1 | 3단계 |

## 9. 단일 crash 기준형 구현 갱신 (2026-10-28)

기존 고정 구성 엔진을 CrashOnlyPrimaryBackupEngine으로 조립하여 heartbeat timeout, 최소 Backup ID 승계, epoch 증가, 생존 Backup READY 이후 쓰기 재개를 구현했다. 자세한 상태 전환·실험 전제는 [실행 문서](failover-comparison/README.md)를 따른다. 이전 절의 전체 failover·fencing 계획과 달리 현재 구현은 단일 영구 crash와 고유 키 보존 검증으로 제한한다. VR·분단 안전성·recovery·실제 TCP 연결은 후속이다.

## 10. VR 및 분단·미완료 쓰기 비교 구현 (2026-10-28)

`core.replication.vr.VrEngine`에 고정 3노드 과반수 로그, view change, 미확정 suffix 선택·재확정, client table, 순서 있는 KV 적용을 구현했다.
읽기도 로그를 통과한다. 이전 §9의 후속 중 VR·분단·미완료 로그 복구는 이번 단계에서 구현했다. 분단 후 메모리를 유지한 노드가 따라잡는 것은 지원하지만 JVM 재시작 recovery와 membership 변경은 포함하지 않는다.

기준형은 비교를 위해 crash-only 범위를 유지한다. 기준형에 VR의 보장을 덧붙였다고 서술하지 않는다. 부분 복제 후 승격 시 기준형 복제본이 달라질 수 있음을 비교 실험에 남긴다.
같은 실행기로 PB/VR × 8시나리오 × 2지연 × 5seed를 실행하며, 모델·보장 전제·원본 지표는 [protocol-comparison](protocol-comparison/README.md)에 명시했다.

## 11. VR 전송 개선 (2026-10-28)

기본 전송을 INCREMENTAL로 바꿨다. 새 연산·누락 suffix만 전송하고 작은 COMMIT으로 heartbeat를 보낸다. 전체 상태 전달은 유지하며 원격 확인과 1~4초 backoff로 중복 재전송을 제한한다.
상태 전달이 증분 재전송에 밀리지 않도록 우선순위를 두고, 초기 감지와 별도로 view-change 대기 예산을 최소 3초로 잡았다.
FULL_LOG 모드는 기존 실험의 정확한 재현을 위해 유지했다. [같은 네트워크 비용 216조건의 개선 전후 결과](network-optimization/results.md)를 따른다.

## 12. 연속 부하와 후속 연구 범위 (2026-10-28)

고정 간격 open-loop 입력 30초, 10~20초 ACK 지연, 정상화 이후 추가 관측을 [48조건으로 실행](sustained-load/results.md)했다.
동시 쓰기 한 건이라는 구현 조건과 큰 값에서의 재동기화 비용을 확인했다. 동시 쓰기 pipeline·느린 노드 전송/지연 정책·checkpoint는 [멘토 질문](sustained-load/mentoring-questions.md)으로 남겨 후속 범위를 정한다.

## 13. 부하 중 crash (2026-10-28)

0~30초 연속 입력 중 11초에 실제 Primary를 종료하는 [48조건](load-crash/results.md)을 추가했다.
첫 post-crash OK와 crash 전 성공 요청의 생존 보존을 구분한다. VR은 로그 보존과 KV 적용을 따로 검사하고 죽은 노드 메모리를 보존 근거에서 제외한다.
이 실험은 위 멘토 질문의 추가 근거이며 pipeline·checkpoint·느린 노드 정책의 구현 범위를 늘리지 않았다.

## 14. Raft 추가 (2026-10-29)

`raft/RaftEngine`에 고정 3노드 투표·로그 최신성 검사·AppendEntries·충돌 suffix 교체·현재 term commit·리더 no-op을 구현했다. 읽기는 로그 경유, 단일 in-flight, 메모리 유지 분단/복구 범위다. term·votedFor·로그를 디스크에 저장하지 않으므로 프로세스 재시작 recovery는 지원하지 않는다.

기존 PB/VR 실험은 유지하고 [별도 공통 비교 216조건](election-comparison/results.md)을 추가했다. PB/VR 감지 200ms와 Raft 선거 [200,400)ms의 분포 차이를 명시한다. 실험 결과를 선출 자체의 속도 순위로 해석하지 않는다. Chain Replication·Rosé는 구현 대기이며 기존 세 멘토 질문의 정책 범위도 그대로다.

## 15. Chain Replication 추가 (2026-10-29)

`chain/ChainEngine`에 SET/DELETE 사슬 전달·tail 읽기·tail 확정·역방향 Sent Ack를 구현했다. CacheEngine future를 가진 head로 tail 결과를 한 번 중계하며 그 peer 홉도 비용에 포함한다. master는 정확한 영구 crash만 제거하는 실험 코디네이터다. 전 생존자 freeze 및 KV 적용 완료 → peer suffix 재전송 → Ack·prefix 일치 → 전 생존자 open 순서를 사용한다. 살아 있는 분단 노드는 제거하지 않는다.

[네 방식 공통 288조건과 Chain 위치별 crash 24조건](chain-comparison/results.md)을 추가했다. 기존 PB/VR/Raft 216조건은 CSV가 그대로 재현된다. CRAQ·신규 노드 합류·master 합의·재시작·checkpoint·pipeline은 포함하지 않았으며 기존 세 멘토 질문을 유지한다. 다음 주 variant는 Rosé다.

## 16. Rosé 기반 단일 파티션 변형 (2026-10-29)

`rose/RoseEngine`에 op 단위 lag 예약·상한 도달 시 한 요청 대기·deadline 경계·중복 응답·prefix 복구를 구현했다. 양수 L은 최소 Backup 적용 ACK와 예약 로그 길이 차이를 제한하며, 0은 비교용 전원 ACK 동기 모드, -1은 무제한 비동기 제어군이다.

외부 master는 정확한 원래 Primary crash만 제외하고 가장 긴 생존 적용 prefix를 선택한다. 기존 NodeId 고정 승계와 다르며 master 합의·통신 비용을 모델링하지 않는다. 다중 파티션 snapshot·coordinated apply·지역 복제·영속 WAL을 재현한 Rosé 전체 구현이 아니다. §4 조사 표의 일반적인 논문 보장을 실제 구현 결과로 혼동하지 않는다.

[240조건의 성공·유실·lag trade-off](rose-comparison/results.md)를 실행했다. 기존 네 방식 288조건은 CSV가 그대로 재현된다. 논문 재현 범위·동시성 통제·상태 전달 우선순위는 [최신 멘토 질문](mentoring/2026-10-29-questions.md)으로 정리했다.
