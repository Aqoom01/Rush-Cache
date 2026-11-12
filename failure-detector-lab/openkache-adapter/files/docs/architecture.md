# 아키텍처

OpenKache는 연구별 분산 캐시 알고리즘을 같은 서버 뼈대 위에서 실험하기 위한 Java 21 프로젝트입니다. 이 문서는 현재 뼈대의 구조와 thread 경계를 설명합니다. 모듈 의존성은 [module-boundaries.md](module-boundaries.md), wire 형식은 [protocol-v1.md](protocol-v1.md)를 참고하세요.

## 1. 현재 구현 범위

| 구분 | 상태 |
|---|---|
| Gradle 프로젝트 (`cache-core`, `cache-server`) | 구현 |
| core 공통 계약 (api, model, spi, runtime) | 구현 |
| TCP 연결, frame codec, PING | 구현 |
| SWIM UDP transport, direct/indirect probe, suspicion | 구현 |
| `/metrics` (요청 수, JVM 지표) | 구현 |
| GET/SET/DELETE 처리 | 구현. 단일 노드 인메모리 KV(`core.kv.KvEngine`)에 연결됨. 복제·라우팅·Tablet은 없음 |
| `CacheEngine` 구현체 | `core.kv.KvEngine` (단일 노드 전용) |
| TCP peer 프로토콜 (복제·마이그레이션 메시지) | 미구현. PING 외에는 `NOT_IMPLEMENTED`로 응답 |
| 연구 알고리즘 | SWIM baseline 구현. Lifeguard·복제·Tablet·부하 분산·마이그레이션은 미구현 |

## 2. 요청 흐름

```
client ──TCP──► [client listener]  FrameDecoder → ClientRequestHandler ──► 응답
peer   ──TCP──► [peer listener]    FrameDecoder → PeerMessageHandler   ──► 응답
peer   ──UDP──► [SWIM listener]    SwimDatagramHandler ──► SWIM owner mailbox
                       (Netty EventLoop)

GET/SET/DELETE:
ClientRequestHandler ─ Command, RequestContext ─► KvEngine.execute
                                                    └─► Mailbox.offer ─► OwnerWorker (KvStore 변경)
                                                    ◄── CompletionStage<Result> ──┘
```

PING은 EventLoop에서 바로 응답합니다. GET/SET/DELETE는 `ClientRequestHandler`가 frame body(`docs/protocol-v1.md` §2)를
`Command`와 `RequestContext`로 바꿔 `KvEngine.execute`를 호출합니다. `KvEngine`은 Key를 `Math.floorMod(key.hashCode(),
workers.size())`로 고정 worker에 배정하고, 그 worker의 mailbox로 실제 저장소 변경을 넘깁니다. 결과 stage는 owner worker
thread에서 완료되며, Netty `ChannelHandlerContext`에 다른 thread에서 쓰는 것은 안전하므로 EventLoop로 되돌아가지 않고 그
thread에서 바로 응답 frame을 씁니다. TCP peer listener는 아직 `CacheEngine.onPeerMessage`를 호출하지
않습니다(복제·마이그레이션 peer 프로토콜 미구현). SWIM UDP listener는 datagram을 검증한 뒤 전용 owner mailbox에
등록하며 EventLoop에서 detector 상태를 직접 변경하지 않습니다.

## 3. Thread 경계

| Thread | 이름 | 하는 일 | 금지 |
|---|---|---|---|
| main | `main` | 설정 로딩, 기동, 종료 대기 | |
| Netty accept | `openkache-accept-*` | client·peer TCP 연결 수락 | blocking |
| Netty I/O | `openkache-io-*` | TCP frame 및 SWIM UDP datagram 처리 | blocking I/O, `Future.get()`, `join()`, core 상태 직접 변경 |
| core worker | `openkache-worker-<id>` | 자기가 소유한 상태 변경 (엔진 연결 후) | 다른 worker 상태 변경 |
| SWIM owner | `openkache-worker-<id>` | detector 상태 변경, 메시지 처리, tick | EventLoop 상태 직접 변경 |
| SWIM scheduler | `openkache-swim-scheduler` | owner mailbox에 tick 등록 | detector 직접 호출, blocking |
| metrics HTTP | `openkache-metrics-http-*` | `/metrics` 응답 | |
| shutdown | `openkache-shutdown-hook` | 종료 순서 실행 | |

**핵심 규칙**
1. **단일 owner:** 변경 가능한 상태는 정확히 하나의 `OwnerWorker`가 소유합니다. 그 worker의 thread에서만 바꾸므로 lock을 쓰지 않습니다.
2. **EventLoop와 worker 분리:** Netty EventLoop와 core worker는 별개의 thread입니다. EventLoop는 `Mailbox.offer`로 작업을 넘기기만 하고, 결과를 기다리며 block하지 않습니다.
3. **크기 제한 mailbox:** worker당 1024개입니다. 가득 차면 대기하지 않고 즉시 `BUSY`로 응답합니다.
4. **snapshot:** `EngineSnapshot`은 불변 복사본이라 어느 thread에서나 읽을 수 있습니다.
5. **시간:** timeout과 경과 시간은 `TimeSource.monotonicNanos()`로, 기록용 시각은 `epochMillis()`로 다룹니다. 다른 노드에는 절대 시각이 아닌 상대 timeout을 보냅니다.

## 4. 기동과 종료

**기동 순서** (`ServerApplication.start`)
1. 설정 로딩과 검증
   - 잘못된 포트, worker 수, 알 수 없는 key가 있으면 서버를 띄우지 않고 종료 코드 2로 끝냅니다.
2. metrics endpoint를 엽니다.
3. core worker를 시작합니다.
4. Netty EventLoop를 시작합니다.
5. peer TCP listener와 client TCP listener를 bind합니다.
6. SWIM UDP listener를 bind합니다.
7. SWIM detector와 scheduler를 시작합니다.

중간에 실패하면 이미 시작한 구성 요소를 닫고 종료 코드 1로 끝납니다.

**종료 순서** (shutdown hook → `ServerApplication.close`)

SIGTERM이나 Ctrl+C를 받으면 아래 순서로 닫습니다.
1. client listener와 peer TCP listener를 닫습니다. 이 시점부터 신규 TCP 연결·요청을 받지 않습니다.
2. SWIM scheduler, UDP listener와 owner worker를 닫습니다.
3. EventLoop를 종료합니다. 이미 받아들인 연결의 처리를 끝냅니다.
4. `KvEngine.close()`를 호출합니다. 아직 완료되지 않은 요청(worker mailbox에서 대기 중이거나 처리 중인 것 모두)의
   `CompletionStage`를 `UNAVAILABLE`로 완료합니다. listener를 먼저 닫았으므로 이 시점 이후 새 요청은 들어오지 않습니다.
5. worker를 닫습니다(대기 작업은 버림). 4에서 이미 그 작업들의 응답을 처리했으므로 버려도 안전합니다.
6. metrics endpoint를 닫습니다.

`close()`는 여러 번 호출해도 안전합니다. 종료 후 `openkache-*` thread가 남지 않는다는 것을 테스트(`ServerApplicationTest`)로 확인합니다.

## 5. 관측과 배포

**Prometheus 지표**
- `openkache_requests_total{listener, opcode, status}`
- JVM 기본 지표 (`jvm_memory_used_bytes`, `jvm_threads_current` 등)
- label에는 Key나 request ID처럼 종류가 계속 늘어나는 값을 넣지 않습니다.

**로컬 실행 환경** (`deploy/compose.yml`)
- 구성: server, Prometheus, Grafana
- 공개 포트: 모두 `127.0.0.1`에만 bind합니다.
- server의 `/metrics`: 호스트에 공개하지 않고, compose 내부 네트워크에서 Prometheus만 접근합니다.
- 이미지: linux/amd64와 linux/arm64를 지원하는 Temurin 21 이미지를 씁니다.

**범위 밖:** Terraform과 AWS 자원 생성은 이번 범위가 아닙니다.

## 6. 연구 담당자가 확인해야 할 것 (아직 정해지지 않음)

아래는 이번 단계(단일 노드 KV)에서 구현하지 않았지만, 복제·Tablet·마이그레이션을 구현할 때 서로 다른 가정을 하지 않도록 미리
정리해 둔 결정 지점입니다. 담당자 간 합의 후 이 문서와 `module-boundaries.md`를 갱신하세요.

- **worker는 논리적 단위다.** `OwnerWorker`는 전용 Java thread 하나를 쓰지만, 특정 CPU core 고정이나 GC 격리를 보장하지
  않습니다. "owner thread에서만 상태를 바꾼다"는 규칙은 지키지만, 성능 격리를 전제로 설계하지 마세요.
- **복제의 commit 경계.** A1(Active-Passive)과 A2(Active-Active)가 "GET을 허용해도 되는 조건", "SET이 성공으로
  ACK하는 조건"을 각자 정의해야 합니다. `core.replication` 패키지 안에서 두 연구가 공유할 타입과, 연구별로 갈라지는
  타입을 먼저 나눠야 합니다. `Transport.send()`의 완료는 로컬 전송 계층이 메시지를 받아들였다는 뜻일 뿐 원격 commit이
  아니라는 점을 재확인하세요(`core.spi.Transport` Javadoc).
- **request identity와 중복 처리.** 재시도 식별은 이미 있는 `ClientId`+`OperationId`를 쓰세요(§1). 새 식별자를 만들지
  마세요. 어느 계층이 중복 실행을 걸러내는지(엔진? 복제 계층?)는 아직 정해지지 않았습니다. 단일 노드 KV는 중복을 걸러내지
  않습니다(재시도된 SET은 같은 값을 다시 쓸 뿐이라 단일 노드에서는 무해합니다).
- **epoch 증가와 fencing은 다른 문제입니다.** `OwnershipEpoch.next()`로 값을 올리는 것만으로 안전한 승격이 끝나지
  않습니다. "이전 owner가 자신이 더 이상 owner가 아님을 어떻게 알고 요청을 거절하는지"는 연구별로 정의해야 합니다.
- **의심(suspicion)과 승격(promotion)은 다른 책임입니다.** A3의 failure detector는 "의심"만 보고하고, 실제 승격·재구성
  결정은 A1/A2가 `core.failure`가 보낸 신호를 받아 자기 책임으로 내립니다. 이 경계를 `core.failure`/`core.replication`
  사이에서 섞지 마세요.
- **`EpochStore`의 보장 범위.** `save()`가 반환하면 내구성 있게 기록됐다는 뜻이지만, 전체 replica가 동시에 사라지는
  경우까지 보장하지는 않습니다(아직 영속 구현이 없으므로 이 보장 범위는 구현 시점에 다시 검증해야 합니다).
- **Tablet 메타데이터 구조.** `TabletId`는 있지만 Key Range·분할 방식·owner node/worker 구조는 B1·B2·B3 합의 전까지
  정의하지 않습니다. `OwnershipEpoch`(소유권 세대)와 `RoutingVersion`(라우팅 테이블 세대)은 서로 다른 개념이므로 혼용하지
  마세요 — 예를 들어 owner가 바뀌지 않아도 라우팅 테이블만 갱신되면 `RoutingVersion`만 올라갈 수 있습니다.
- **마이그레이션 중 쓰기 안전성.** B3는 snapshot 복사, 복사 중 변경분(delta) 반영, cutover, 이전 owner 차단의 순서를
  정의해야 합니다. 이동 중 성공으로 응답한 쓰기가 누락되지 않는지, 이전 owner에서 읽은 값이 stale `HIT`가 아닌지를 검증할
  방법(예: 테스트 시나리오)도 함께 정의하세요.
- **Simulation의 결정성 범위.** `TimeSource`는 시간 조회만 추상화합니다. 실제 `sleep`, 타이머, 난수, 메시지 전달 순서까지
  제어하지 않으므로, 지금 상태로는 "완전히 결정적인 시뮬레이션"을 주장할 수 없습니다. A3나 B2가 결정적 재현이 필요하면
  이 부분을 먼저 확장해야 합니다.
