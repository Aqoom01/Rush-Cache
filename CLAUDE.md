# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

OpenStd(OpenKache)는 고성능 NVMe SSD 기반 분산 캐시의 가용성과 확장성에 관한 선행연구 알고리즘을 Prototype으로 구현하고, 공통 실험 환경에서 동작 및 성능 특성을 비교·검증하는 프로젝트다. Java 21, Gradle(Kotlin DSL), Linux/WSL2 환경에서 개발한다. Rust에서 Java로 전환했으며 Rust 코드는 저장소에서 제거했다(Git 기록에만 남아 있음).

6명이 각자 개별 연구 1개와 공통 모듈 1개를 함께 맡는다.

| # | 연구 | 공통 모듈 | 1차 인수 기준 |
|---|---|---|---|
| A1 | Active-Passive Fast Failover | KV Store | GET/SET/DELETE 및 로컬 저장 단위 테스트 |
| A2 | Active-Active / SSD-aware Replication | Workload Generator | 고정 Seed로 동일 요청 생성 |
| A3 | SWIM / Lifeguard Failure Detection | Simulation | 3-node 및 crash/partition 재현 |
| B1 | Tablet-based Data Distribution | Network / Shared Types | NodeId 및 공통 모델, 기본 송수신 예제 |
| B2 | Load-aware Balancing + Split/Merge | Metrics | 가상/실제 시간 구분 JSON 지표 기록 |
| B3 | Live Migration + Elastic Scaling | Experiment Runner | N회 실행 및 실패/타임아웃 결과 수집 |

## 구조와 현재 상태

구현되지 않은 기능을 완료된 것으로 서술하지 않는다. 상세는 `README.md`, `docs/architecture.md`(thread 경계), `docs/module-boundaries.md`(의존성), `docs/protocol-v1.md`(wire 형식)를 따른다.

- **`cache-core`** (`io.openkache.core`): 순수 Java 라이브러리. production 의존성은 JDK 표준 API만 허용하며 `verifyNoProductionDependencies` task가 `check`에서 검사한다.
  - `api`, `model`, `spi`, `runtime`: 공통 계약 구현 완료 (`CacheEngine`, `Command`, `Result`, `Bytes`, ID 타입, `Transport`, `MetricsSink`, `TimeSource`, `EpochStore`, `OwnerWorker` 등)
  - `kv`: `KvEngine`이 단일 노드 인메모리 GET/SET/DELETE/EXISTS/COUNT를 구현한다(`CacheEngine` 구현체). Key마다 고정 worker로 샤딩하고 그 worker thread에서만 저장소에 접근한다. COUNT는 모든 worker에 보내 합산한다. SCAN/RANGE는 `Command`가 아닌 `KvEngine.scan`/`range` 메서드(`ScanResult` 반환)다. 오류 처리 규칙은 `kv/package-info.java`에 있다. TTL·eviction·영속화·복제·라우팅은 미구현이고, EXISTS/COUNT/SCAN/RANGE의 wire opcode도 아직 없다(core에서만 호출 가능).
  - `balance`: B2. Split 정책의 공통 입출력 타입(`KeyRange`, `TabletLoad`, `SplitPolicy`, `SplitDecision`)과 Baseline `MidpointSplitPolicy`만 구현했다. `split-for-heat`, 분할 시작 조건, Merge, 재배치는 미구현. 설계는 `experiments/b2-load-balancing/design.md`를 따른다.
  - `replication`, `failure`, `tablet`, `migration`: 연구 패키지. `package-info.java`로 책임만 적혀 있고 **미구현**이다.
- **`cache-server`** (`io.openkache.server`): Netty 실행 애플리케이션. 설정 검증, client·peer listener, TCP frame codec, PING, `/metrics`(Prometheus), GET/SET/DELETE의 `KvEngine` 연결까지 구현했다. peer 프로토콜(복제·장애 감지·마이그레이션 메시지)은 PING 외에는 `NOT_IMPLEMENTED`로 응답한다.
- **`experiment-runner`** (`io.openkache.runner`): Workload Generator + Experiment Runner 최소 구현. `cache-core`에만 의존하고(production), Netty 없이 `protocol-v1`을 직접 구현한 TCP client로 단일 노드 `cache-server`에 GET/SET/DELETE를 보낸다. warm-up/측정 구간 분리, HIT/MISS/실패 분리 집계, reservoir 기반 p50/p99, JSON(원본)+CSV(요약) 결과 기록까지 구현했다. 3-node, 장애 주입, Zipfian 분포는 미구현. 통합 테스트에서만(test 전용 의존성) `cache-server`를 띄워 end-to-end로 검증한다.
- 의존 방향은 `cache-server → cache-core`, `experiment-runner → cache-core`(+ 테스트 전용 `cache-server`)만 허용한다. Netty·Prometheus·logging 구현은 server에만 둔다. 연구별 Gradle 모듈은 만들지 않는다(`experiment-runner`는 연구가 아닌 공통 모듈이다).
- `monitoring/`(Prometheus, Grafana), `deploy/`(Dockerfile, compose): linux/amd64에서 이미지 빌드, compose 실행, PING, 호스트 `experiment-runner`로 GET/SET/DELETE 실행, Prometheus 수집, Grafana 대시보드까지 확인했다. 이미지에는 서버 배포본만 들어가지만 `settings.gradle.kts`가 `experiment-runner`를 include하므로 빌드 단계에서 그 디렉터리도 복사해야 한다. linux/arm64 실행은 아직 확인하지 않았다.
- `infra/`는 비어 있다.

## 공통 모듈 명세 요약

기준 문서는 「OpenKache 분산 캐시 선행연구 공통 모듈 개발 명세서 v1.0」이다. 원본 PDF는 저장소에 없으며 아래 요약이 기준이다. 명세서는 Rust 기준으로 작성되었으므로 아래 책임은 언어와 무관하게 유지하고, Java 배치가 정해지지 않은 항목은 임의로 정하지 말고 사용자에게 확인한다. "선택"으로 표시한 기능은 관련 연구의 요구가 확정되기 전에는 구현하지 않는다.

| 공통 모듈 | 현재 Java 배치 |
|---|---|
| Network / Shared Types | `core.model`(ID 타입, `Bytes`), `core.spi`(`Transport`, `PeerMessage`), `server.netty`(wire) |
| KV Store | `core.kv`(`KvEngine`, 단일 노드 인메모리 구현) |
| Metrics | `core.spi.MetricsSink`(계약), `server.metrics`(Prometheus), `experiment-runner`(실험 결과 JSON/CSV) |
| Workload, Experiment Runner | 새 Gradle 모듈 `experiment-runner`(`io.openkache.runner`) |
| Simulation | 미정 |

- **Network / Shared Types**
  - 노드 식별: `NodeId`와 `Address`. 시뮬레이션 호스트명과 실제 주소의 매핑 규약을 포함한다.
  - 통신: UDP와 TCP의 특성을 유지하는 최소 통신 경계. 하나의 범용 Transport로 강제하지 않는다.
  - Codec: 공통 Envelope가 필요할 때만 Version·Type·Length 수준으로 정의한다. 연구별 메시지는 각 연구에서 정의한다.
  - Tablet: `TabletId`, Tablet Metadata(Owner, Key Range, Version). 구조는 B1·B2·B3 담당자가 합의한 뒤 확정한다.
  - 실제 요구가 확인되기 전에는 타입과 추상화를 만들지 않는다.
- **KV Store** (GET/SET/DELETE는 `core.kv.KvEngine`으로 구현 완료, 단일 노드 전용)
  - Key와 Value는 바이트열(`Bytes`)이다.
  - GET: 값의 복사본을 반환하고, 없는 Key는 오류가 아닌 정상 MISS로 처리한다.
  - SET: 같은 Key면 덮어쓴다.
  - DELETE: 삭제 여부를 반환한다.
  - EXISTS, COUNT를 제공한다. `core.api.Command`에 `Exists`, `Count`를 추가하고 `KvEngine`이 구현했다(B3 확인 후 `WireClient` switch에 case 추가). COUNT 개수는 `Result.value()`에 8-byte big-endian으로 담는다. TCP wire opcode는 **미구현**(B1 영역, `protocol-v1`에 정의 필요).
  - SCAN/RANGE는 선택이다. B1은 불필요(해시 범위로 Tablet을 나눠 Key를 훑지 않음), B3 요구로 구현했다. `Command`/`Result`가 값 하나만 다루므로 `KvEngine.scan(Predicate<Bytes>, limit)`/`range(start, end, limit)` 메서드로 제공하고 `ScanResult`(상태 + `KvEntry` 목록, Key 바이트 순서 정렬)를 돌려준다. Tablet 해시 범위 선택은 호출자가 predicate로 넣는다(KV는 `core.tablet`을 참조하지 않는다). wire opcode는 **미구현**.
  - 오류 처리 규칙을 문서화한다.
- **Workload** (`experiment-runner`의 `WorkloadGenerator`/`RunnerConfig`로 구현 완료)
  - 요청: GET/SET/DELETE Operation 자료형(`core.api.Command`를 그대로 쓴다. 재사용). Key 개수·Key 길이·Value 크기를 조절한다.
  - 연산 비율(Mix): 합계 100%인지 검증한다.
  - 분포: Uniform, 고정 Hot-Key 구현. Zipfian은 **미구현**(설정값으로 쓰면 기동 전에 거절된다).
  - 재현성: 같은 설정과 Seed면 같은 요청열을 만든다. `concurrency`를 바꾸면 thread 간 끼어듦 순서까지는 보장하지 않는다.
  - 스케줄: 동시 클라이언트 수(`concurrency`)를 설정한다. 고정 요청 간격(open-loop)은 **미구현**이다(closed-loop만 지원).
    논리적 요청 생성(`WorkloadGenerator`)과 실제 발송(`WireClient`)은 분리돼 있다.
- **Simulation**
  - 명세서는 Rust Turmoil 기반이었다. Java에서 대체할 시뮬레이션 수단은 정해지지 않았다.
  - 3-host 메시지 교환 예제, Fault Helper(Crash/Restart, Partition/Repair, Delay/Loss)를 제공한다.
  - Scenario Config: 대상 호스트, 주입 시각, 파라미터, Seed, 실행 제한 시간.
  - Fault Event: 주입 직후의 가상 시각과 장애 대상을 기록한다.
  - 시뮬레이션 도구를 과도하게 추상화하지 않는다. 가상 시간은 `TimeSource` 구현으로 주입한다.
- **Metrics** (서버 쪽 Prometheus는 기존대로, 실험 결과 쪽은 `experiment-runner`의 `ExperimentResult`/`ResultWriter`로 일부 구현)
  - Event 필드: name, node_id, timestamp, time_domain. **미구현**(`ExperimentResult`는 요청 단위 event가 아니라 실행 단위 집계만 기록한다).
  - 측정값: Duration·Counter(누적)·Gauge를 단위와 함께 기록한다. 현재 Duration(p50/p99 latency, 나노초)·Counter(Outcome별 누적)만 구현했고 Gauge는 **미구현**.
  - 통계: Raw Sample로 mean, min/max, p50/p95/p99를 계산하고 계산 방식을 명시한다. 현재는 Raw Sample 전체가 아니라 8,192개 reservoir sampling(Algorithm R)으로 p50/p99만 근사한다. mean/min/max/p95는 **미구현**. reservoir 근사를 명세 충족으로 볼지는 정해지지 않았다.
  - Metadata: study, variant, scenario, seed, node_count, commit, config, execution_id. config를 제외한 항목은 `ExperimentResult`에 있다. config는 concurrency만 기록하고 key.count, key.length, value.size, mix, distribution 등 Workload 설정은 결과에 남지 않는다.
  - 출력: 원본 결과는 JSON(`<study>/<executionId>.json`), 분석용은 CSV(`<study>/summary.csv`, append) — 구현 완료.
  - Virtual Time과 Wall-clock Time: Runner는 실제 시계만 쓴다(가상 시간 Simulation과 결합하는 시나리오는 아직 없다).
  - Prometheus label에 Key, request ID처럼 종류가 계속 늘어나는 값을 넣지 않는다 — 기존 `server.metrics` 구현이 지킨다.
- **Experiment Runner** (`experiment-runner`로 최소 구현 완료, 단일 노드·단일 실행 전용)
  - Config 항목: 연구명(study), Variant, Scenario, Seed **하나**(목록 아님), 반복 횟수는 **미구현**(한 번 실행하면 한 결과를 쓴다), 제한 시간은 요청 단위 timeout만 있다.
  - 실행: `cache-server`를 TCP로 직접 호출한다. 실행 파일/Gradle 테스트를 대신 실행하는 기능은 **미구현**.
  - 반복(N회): **미구현**. Execution ID(UUID)는 실행마다 하나 발급한다.
  - 종료 판정: 요청 단위로 TIMEOUT/BUSY/UNAVAILABLE/CLIENT_FAILURE를 구분해 집계한다. Runner 프로세스 자체의 비정상 종료 판정(예: 반복 실행 중 일부 실행만 실패)은 반복 기능이 없어 **미구현**.
  - 결과 수집: JSON/CSV를 직접 쓴다. 여러 실행을 모아 형식 검증·누락 감지하는 것은 **미구현**(현재는 한 번에 한 실행).
  - Manifest: Git Commit과 dirty 여부는 JSON에 있다. Config 스냅샷(Workload 설정 전체)과 결과 경로는 JSON에 없다(결과 경로는 실행 시 stdout으로만 출력). "실행 명령"·"호스트 환경"은 Runner 쪽 JVM/OS만 기록하고 서버 쪽은 운영자가 `server.workers.hint`/`server.jvm.opts.note`로 적어 넣는 참고값이다(Runner가 서버를 직접 관측하지 않는다).

**연구별 공통 모듈 사용**:

| 연구 | 사용하는 공통 모듈 |
|---|---|
| A1, A2 | KV, Network, Simulation, Workload, Metrics, Runner |
| A3 | Network(선택), Simulation, Metrics, Runner |
| B1 | Shared Types, KV(선택), Workload, Simulation, Metrics, Runner |
| B2 | Shared Types, Workload, Simulation, Metrics, Runner |
| B3 | Shared Types, KV, Workload, Simulation, Metrics, Runner |

**일정 우선순위**

| 주차 | 공통 모듈 작업 |
|---|---|
| 1주 | NodeId 및 결과 Schema/API 합의, KV CRUD, 3-node 시뮬레이션 샘플, Runner 최소 동작 |
| 2-4주 | 실제 중복 확인 후 Network/Workload/Tablet 타입 보강, Metrics·Runner 안정화 |
| 5주 | 공통 인터페이스 통합, 다른 연구 담당자의 재사용 검증 |
| 6주 | 반복 실험, Docker 기반 실측 |
| 7주 | 재현 스크립트 확정, 보고서·발표용 그래프 |

## 명령어

저장소 루트에서 Gradle Wrapper로 실행한다. JDK 21이 필요하다. 컴파일은 `-Xlint:all -Werror`이므로 경고도 빌드 실패다.

```bash
./gradlew build                                             # 컴파일 + 전체 테스트 + core 의존성 검사
./gradlew test                                              # 전체 테스트
./gradlew :cache-core:test                                  # 모듈 테스트
./gradlew :cache-server:test --tests '*FrameCodecTest'      # 단일 테스트 클래스
./gradlew :cache-server:installDist                         # 실행 배포본 생성
cache-server/build/install/openkache-server/bin/openkache-server [config.properties]   # 서버 실행
./gradlew :experiment-runner:installDist                    # Runner 실행 배포본 생성
experiment-runner/build/install/openkache-experiment-runner/bin/openkache-experiment-runner \
    experiments/baseline-single-node/config.properties       # 서버가 떠 있어야 한다
```

`org.gradle.caching=true`이므로 테스트를 실제로 다시 돌렸는지 확인하려면 `--rerun-tasks`를 붙인다. 의존성 버전은 `gradle/libs.versions.toml`에서만 관리한다.

## 개발 원칙

- 모든 설명과 작업 결과는 한국어로 작성한다.
- 공통 모듈은 최소 API와 사용 예제를 먼저 제공한다. 다른 연구자 최소 1명이 사용해 본 뒤 공용 API를 확정한다.
- 공통 모듈에는 연구별 알고리즘을 넣지 않는다. 명세에 없는 기능을 임의로 추가하지 않는다.
- 불필요한 추상화, 범용 프레임워크(Spring 등), 외부 라이브러리를 추가하지 않는다.
- 공통 계약(`core.api`, `core.model`, `core.spi`, `core.runtime`)을 변경할 때는 수정 전에 영향 범위(호출부, 영향받는 담당자)를 먼저 설명한다.
- thread 규칙: core 상태는 owner worker에서만 변경하고, Netty EventLoop에서 blocking I/O나 `join()`을 하지 않는다. 예상 가능한 실패는 예외가 아닌 `Result` 상태로 표현한다.
- 공개 API에는 Javadoc으로 의미, 단위, thread 규칙을 적는다.
- 구현 범위와 완료 여부는 실제 코드와 테스트로 판단한다.
- Java 코드를 변경한 뒤 `./gradlew build`로 검증한다.
- commit, push, merge는 명시적으로 요청받은 경우에만 실행한다.

## Git / MR 컨벤션

상세 규칙은 `docs/git-convention.md`, `docs/mr-convention.md`를 따른다.

- 브랜치: `master`(검증된 결과물·마일스톤), `develop`(통합). 작업 브랜치는 `develop`에서 `<type>/<영문 요약>`으로 만들고 MR로 `develop`에 병합한다(Squash 권장). 마일스톤 완료 시 `develop` → `master` MR(Squash 금지).
- 커밋: `<type>: <한국어 설명>` (예: `feat: KV Store GET/SET 구현`)
- MR 제목: `[Type] 작업 내용 (S15P31D111-<번호>)`. 본문은 `.gitlab/merge_request_templates/Default.md`를 쓰고 `Closes S15P31D111-<번호>`(작업이 남으면 `Refs`)로 Jira 이슈를 연결한다.
