# IONIA O00 구현 및 검증

2026-10-27. 기준 커밋 `4ed4dcb24857b1ce007140204fc932371c3c30d7`의 프로토타입을 확장했다.

## 명세와 구현 경계

- [O00 기준선 v1.0](https://app.notion.com/p/4064ed9024378122a673e1b4a7467ca1)
- [복구 상세 v0.2](https://app.notion.com/p/4064ed9024378169a3eac3697244c9c3)
- [IONIA §4](https://www.usenix.org/system/files/fast24-xu.pdf)
- [Skyros §4.6, Figure 6](https://research.cs.wisc.edu/adsl/Publications/nilext.pdf)

정석 코드는 `cache-core/src/main/java/io/openkache/core/replication/ionia`에 둔다. 이후 두 번째·세 번째 방식은 별도 디렉터리에서 개발하고 이 원본을 유지한다. 이번 변경에는 변형 디렉터리가 없다. 별도의 연구 Gradle 모듈이나 외부 의존성을 추가하지 않았다.

원문 규칙(R), Java/TCP 적응(D), 미실증 사항(U)을 구분한다. 아래 구현은 Java에서의 기능 재현이며 SplinterDB/eRPC 또는 논문 장비의 성능 재현을 뜻하지 않는다.

## 내구성 계약

**O00 기본은 독립 Replica RAM 로그의 정족수 복제이다.** Fast ACK는 KV Apply나 노드별 fsync 완료를 의미하지 않는다. `n=2f+1`, `qFast=f+ceil(f/2)+1`, `qConsensus=f+1`, `qRecovery=ceil(f/2)+1`을 사용한다. Fast ACK에는 해당 View의 Leader가 반드시 포함된다.

- 3노드: Fast 3/3, Slow 2/3, Recovery 2.
- 5노드: Fast 4/5, Slow 3/5, Recovery 2.
- TCP flush는 원격 기록 ACK/PrepareAck로 세지 않는다.
- View 번호는 `view.meta`에 원자 교체와 force로 저장한다. 이 제어 메타데이터 영속화는 원문 장애 복구의 전제이며, 매 요청 Durability Log fsync와 다르다.
- `IoniaFileStorage`의 `syncKvWrites`는 **적용된 KV 데이터**의 force 옵션이다. Fast ACK 전 Durability Log WAL을 force하는 D-PERSIST가 아니다.
- D-PERSIST는 이번 구현에서 활성화하지 않았다. 모든 Replica가 동시에 RAM 로그를 잃으면 정상 복구를 가장하지 않고 RECOVERING을 유지한다.

## 구현 구성

| 구성 | 책임 |
|---|---|
| IoniaClient | SET fanout, Leader 포함 Fast 정족수, 타이머/실패 기반 동기 fallback, 요청 ID 유지 |
| IoniaEngine | owner 상태 머신, 기록/합의/적용 분리, Prepare/Commit 재전송·batch, 키별 FIFO/서로 다른 키 병렬 Apply |
| IoniaLog | 도착 순서의 미완료 Durability 기록, 별도 Consensus 순서, dedup, 연속 applied prefix |
| IoniaHistory | 최근 수정 index, 보수적 LTI, active follower 보고와 lazy trim |
| IoniaRecovery | highest-normal-view, 최대 op-number Consensus 선택, n1+n2 DAG, 위상 정렬과 중복 제거 |
| IoniaStorage | 기존 Apply 종료 후 reset, authoritative Consensus의 전체 재생 계약 |
| server.ionia | TCP codec/transport/node, 안정 View 저장, 디스크 KV 어댑터, 독립 프로세스·클라이언트 진입점 |

공통 Command/Result/RequestContext/Bytes/Mailbox/Transport/TimeSource를 재사용했다. 공통 KvEngine, WorkloadGenerator, 기존 Netty 서버 경로는 유지했다. O00는 별도 `IoniaMain`으로 실행한다.

### 쓰기와 읽기

SET은 모든 Replica에 동일 논리 ID로 전송한다. Fast 정족수를 못 모으면 같은 요청을 Leader의 동기 실행 경로로 보낸다. 2개의 기록 ACK를 Fast 성공으로 바꾸지 않는다. timeout/UNAVAILABLE 이후에는 이미 적용됐을 수 있으므로 재시도에 동일 ClientId/OperationId와 payload를 사용한다.

Leader는 owner mailbox에서 여러 순서 배정을 묶어 Prepare를 발송한다. 합의 전에 Apply하지 않는다. 같은 키의 다음 Apply는 이전 Apply가 완료된 후 시작하며, 서로 다른 키는 동시에 실행 가능하다. 완료 순서가 뒤집혀도 `appliedIndex`는 연속 완료 구간까지만 증가한다.

Leader GET은 그 키의 Pending이 적용될 때까지 기다린다. Follower GET은 값을 읽기 **전** applied prefix를 캡처한다. Client는 Follower GET과 Leader MetaQuery를 병렬 호출한다. Pending이면 Leader DATA(값 또는 MISS)를 사용하고, INDEX이면 Follower prefix가 충분할 때만 받아들인다. History에서 빠진 키는 LTI를 반환한다.

**D — 분할된 옛 Primary의 읽기 fencing:** 읽기 데이터 요청과 병렬로 Replica View를 조회하고, 동일 View/Leader의 NORMAL 응답을 과반수에서 확인한다. 옛 Primary만 접근 가능한 경우 읽기 성공을 허용하지 않는다. 정상 경로에서 병렬 1RTT이나 원문 Figure 7보다 제어 RPC 수가 증가하므로 이 비용을 숨겨 비교하지 않는다. 기본 Client는 이 검사를 켠다. `Options.certifyReads=false`는 View가 바뀌지 않는 시뮬레이션에만 사용한다.

### View Change와 재시작

NORMAL / VIEW_CHANGE / RECOVERING을 구분한다. 새 View의 STARTVIEWCHANGE를 받은 뒤 옛 View Prepare를 승인하지 않는다. 새 Leader는 자신을 포함한 정확히 f+1개의 DOVIEWCHANGE를 선택한다. 가장 높은 normal view의 Durability Logs에서 요청 집합을 복원하고, `a before b` 또는 `a present, b absent`를 합산해 DAG 간선을 결정한다. Cycle·payload 충돌·확정 prefix 충돌은 성공으로 진행하지 않고 fail-stop한다.

이미 확정된 Consensus 위치는 유지하고 복구 Durable 요청을 ID 중복 없이 덧붙인다. History/LTI를 재구성한다. 상태 교체는 기존 Apply가 모두 종료된 뒤 KV를 reset하고 확정 prefix를 재생하는 방식이다. 재생 완료 전 정상 서비스하지 않는다. 선택된 미확정 suffix는 새 View에서 다시 합의한 후 적용한다.

재시작 노드는 동일 nonce의 f+1 응답 및 관측한 최고 View Leader의 상태를 요구한다. 로그를 잃은 노드끼리 서로 정상 상태를 인정하지 않는다. Membership은 정적이며 View Change는 동적 join/leave 프로토콜이 아니다.

## 실행

JDK 21이 PATH에 있어야 한다. 저장소 루트에서:

```powershell
.\gradlew.bat build --rerun-tasks --console=plain
.\gradlew.bat :cache-server:installDist --console=plain
```

서로 다른 터미널 세 개에서 각각 실행한다:

```powershell
.\experiments\ionia-o00\start-node.ps1 -Node 1
.\experiments\ionia-o00\start-node.ps1 -Node 2
.\experiments\ionia-o00\start-node.ps1 -Node 3
```

클라이언트 터미널:

```powershell
.\experiments\ionia-o00\client.ps1 -Operation set -Key hello -Value world
.\experiments\ionia-o00\client.ps1 -Operation get -Key hello
.\experiments\ionia-o00\client.ps1 -Operation delete -Key hello
```

포트는 17101~17103, 데이터는 `build/ionia-o00/node-*`이다. 재시작 시 같은 디렉터리를 사용해야 한다. 실제 SSD에서 시험하려면 각 properties의 `data.directory`를 해당 장치 경로로 바꾼다. 세 노드가 모두 종료된 기존 디렉터리는 O00 RAM 로그만으로 새 클러스터를 만들 수 없다. 기존 데이터를 지워 이를 복구 성공으로 가장하지 않는다. 새로운 독립 실험은 새로운 데이터 디렉터리를 사용한다.

Linux에서는 같은 배포본으로 아래 진입점을 사용한다:

```bash
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia.IoniaMain experiments/ionia-o00/node-1.properties
java -cp 'cache-server/build/install/openkache-server/lib/*' io.openkache.server.ionia.IoniaClientMain experiments/ionia-o00/node-1.properties get hello
```

## 검증 및 한계

`IoniaO00Test`는 정족수, timeout 뒤 늦은 ACK, 키별 적용/연속 prefix, stale Value/MISS, LTI, 3·5노드 장애, Recovery nonce, finalized 위치와 n1+n2, cycle 및 저장 상태 교체를 결정적으로 검사한다. 기존 IONIA 테스트도 유지하며 Fast 실패 시 fallback, 읽기 View 확인에 필요한 테스트 어댑터만 갱신했다.

`IoniaLinearizabilityTest`는 seed 0~63, 각 10개 동시 GET/SET/DELETE 이력을 실제 완료 순서/실시간 선행 관계와 함께 수집하고 가능한 순차 실행을 전수 탐색한다. 잘못된 stale MISS 이력을 거부하는 대조 테스트도 있다. 이 유한 이력 검사는 전체 시스템의 형식적 증명이 아니다.

`IoniaProcessTest`는 실제 JVM 3개를 실행하고 Leader를 강제 종료한다. 새 View 읽기, Majority fallback 쓰기, 같은 디렉터리 재시작과 상태 재생/재합류, DELETE 결과를 확인한다. `IoniaTcpConcurrencyTest`는 실제 TCP로 같은 키에 32개 동시 쓰기를 보내 모든 디스크 Replica의 값·버전·prefix가 일치하는지 확인한다. `IoniaStorageWireTest`는 디스크 재열기, partial tail, 파일 잠금, View 메타데이터, 모든 peer 메시지와 잘못된 frame을 검사한다.

테스트 보고서는 각 모듈의 `build/reports/tests/test/index.html` 및 `build/test-results/test/TEST-*.xml`에 있다. 실패 시 seed/요청 이력/메시지·View·Log trace 또는 프로세스 로그를 제공한다.

U: 물리 SSD 성능·전원 차단·다중 머신 장애 도메인·무한 상태 모델 검사는 수행하지 않았다. Loopback의 독립 프로세스는 물리 호스트 분리를 대체하지 않는다. Snapshot은 전체 Consensus replay 방식으로, 최대 전송 frame 64 MiB 안에서 동작한다. 로그 checkpoint/compaction 및 chunk 전송, G7 성능 비교, 동적 Membership/TTL/CAS/트랜잭션은 이번 범위 밖이다.
