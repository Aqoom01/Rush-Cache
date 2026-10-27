# 모듈 경계

## 1. Gradle 모듈과 의존성

```
cache-server       ──►  cache-core  ──►  JDK 표준 API
   │                        ▲
   └─► Netty, SLF4J/Logback,│Prometheus client
                             │
experiment-runner  ──►──────┘
   │  (test 전용) ──► cache-server (실제 서버를 띄워 end-to-end 검증)
```

- **의존 방향:** `cache-server → cache-core`, `experiment-runner → cache-core`만 production 의존성으로 허용합니다.
  `experiment-runner`의 테스트 코드만 `cache-server`에 의존합니다(실제 서버를 띄우는 통합 테스트용, production 코드가
  아니므로 순환이 아닙니다). core는 server·runner를 모릅니다.
- **cache-core의 production 의존성:** JDK 표준 API뿐입니다.
  - `./gradlew :cache-core:check`가 `verifyNoProductionDependencies` task로 외부 라이브러리 유입을 막습니다.
  - 로그가 필요하면 `java.lang.System.Logger`를 씁니다.
  - 테스트 라이브러리(JUnit)는 core에서도 쓸 수 있습니다.
- **외부 의존성 버전:** 모두 `gradle/libs.versions.toml`에서 정확한 버전으로 관리합니다.
- **연구별 Gradle 모듈:** 만들지 않습니다. 연구 코드는 cache-core의 연구 패키지에 둡니다. `experiment-runner`는 연구
  모듈이 아니라 공통 모듈(Workload·Experiment Runner)입니다.
- **`experiment-runner`가 Netty를 쓰지 않는 이유:** `cache-server`에 의존하면 Runner가 쓰지도 않는 Netty·Prometheus를
  끌고 옵니다. 대신 `protocol-v1`의 wire 형식을 `io.openkache.runner.WireClient`/`WireProtocol`에 독립적으로(JDK
  `Socket`만으로) 구현합니다. 프로토콜을 바꾸면 `server.netty.Protocol`/`Frame`과 `runner.WireProtocol`/`WireClient`
  양쪽, 그리고 `docs/protocol-v1.md`를 함께 갱신해야 합니다.

## 2. 패키지 책임

**cache-core** (`io.openkache.core`)

| 패키지 | 책임 | 상태 |
|---|---|---|
| `api` | `CacheEngine`, `Command`, `Result`, `ResultStatus`, `RequestContext`, `Deadline`, `EngineSnapshot` | 계약 정의 |
| `model` | `NodeId`, `TabletId`, `WorkerId`, `ClientId`, `OperationId`, `OwnershipEpoch`, `RoutingVersion`, `Bytes` | 구현 |
| `spi` | `Transport`, `PeerMessage`, `PeerEnvelope`, `MetricsSink`(+NoOp), `TimeSource`(+system), `EpochStore` | 계약 정의 |
| `runtime` | `Mailbox`, `OwnerWorker`, `TimerEvent`, 실행 원칙 | 구현 |
| `kv` | 로컬 KV | `KvEngine`(단일 노드 인메모리 GET/SET/DELETE) 구현. TTL·eviction·영속화는 미구현 |
| `replication` | A1 Active-Passive, A2 Active-Active | 미구현 |
| `failure` | A3 FailureDetector, membership | 미구현 |
| `tablet` | B1 routing, ownership | fixed-slot·hash-range-tablet·표 전파·소유권 판정(in-process) 구현, ring-vnode·wire 인코딩 미구현 |
| `balance` | B2 배치 계획, split/merge | 미구현 |
| `migration` | B3 snapshot, delta, cutover | 미구현 |

미구현 패키지에는 `package-info.java`로 책임만 적어 두었습니다. 인터페이스는 실제로 필요한 메서드가 확인된 뒤 추가합니다.

**cache-server** (`io.openkache.server`)

| 패키지 | 책임 |
|---|---|
| `Main` | 설정 로딩, 검증, 기동, shutdown hook 등록 |
| `config` | properties 로딩과 기동 전 검증 |
| `lifecycle` | 구성 요소 기동·종료 순서 |
| `netty` | wire `Frame`, codec, `CommandCodec`(body ↔ `Command` 변환), client·peer handler |
| `metrics` | Prometheus 지표, `/metrics` endpoint |

**experiment-runner** (`io.openkache.runner`)

| 클래스 | 책임 |
|---|---|
| `Main` | 설정 로딩, 실행, 결과 출력 |
| `RunnerConfig` | properties 로딩과 검증 (`ServerConfig`와 같은 패턴) |
| `WorkloadGenerator`, `Distribution` | 논리적 요청(`core.api.Command`) 생성. Zipfian은 미구현 |
| `WireProtocol`, `WireClient` | `protocol-v1` TCP client (Netty 없음, server와 별개로 유지) |
| `ExperimentRunner`, `ResultAccumulator`, `LatencyReservoir`, `Outcome` | warm-up/측정 실행, 결과 집계 |
| `ExperimentResult`, `ResultWriter`, `GitInfo` | 결과 JSON/CSV 기록, Git metadata |

## 3. 경계 규칙

- **Netty 타입:** `Channel`, `ByteBuf` 등은 `server.netty` 밖으로 나가지 않습니다. core에는 `Command`, `RequestContext`, `PeerMessage` 같은 core 타입만 넘깁니다.
- **wire 타입과 core 타입:** wire의 `Frame`과 core의 `Command`는 별개입니다. 변환은 server가 맡습니다.
- **Prometheus 의존성:** `server.metrics`에만 둡니다. core는 `MetricsSink` 계약만 압니다.
- **연구 패키지 간 참조:** 각 연구 패키지는 `api`, `model`, `spi`, `runtime`을 쓸 수 있습니다. 다른 연구 패키지를 참조해야 하면 담당자 간 합의 후 공통 타입을 `model`이나 `spi`로 올립니다.
- **공통 계약 변경:** `api`, `model`, `spi`, `runtime`을 바꿀 때는 영향받는 연구 담당자의 리뷰를 받습니다.
