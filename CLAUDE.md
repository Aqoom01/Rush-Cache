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
  - `kv`: `KvEngine`이 단일 노드 GET/SET/DELETE/EXISTS/COUNT를 구현한다(`CacheEngine` 구현체). Key마다 고정 worker로 샤딩하고 그 worker thread에서만 저장소에 접근한다. COUNT는 모든 worker에 보내 합산한다. SCAN/RANGE는 `Command`가 아닌 `KvEngine.scan`/`range` 메서드(`ScanResult` 반환)다. 오류 처리 규칙은 `kv/package-info.java`에 있다. 저장소는 `KvStorage`로 고른다: 메모리(기본) 또는 NVMe(worker별 segment 로그 + 메모리 색인, O_DIRECT, 용량이 차면 오래된 segment부터 FIFO 축출, 재시작 시 비움). 서버는 `storage.*` 설정으로 고른다. 설계는 `docs/storage-nvme.md`(2026-11-04 구현, A1 리뷰 전). TTL·재시작 복구·복제·라우팅은 미구현이고, EXISTS/COUNT/SCAN/RANGE의 wire opcode도 아직 없다(core에서만 호출 가능).
  - `balance`: B2. B1 `TabletRange`(hash 구간) 위의 Split 정책 입출력 타입(`TabletLoad`, `SplitPolicy`, `SplitDecision`)과 정책 2개(Baseline `MidpointSplitPolicy`, `HeatSplitPolicy`)를 구현했다. 분할 시작 조건(`SplitTrigger`), 시나리오 실험, Merge, 재배치는 미구현. 설계는 `experiments/b2-load-balancing/design.md`를 따른다.
  - `migration`: B3 stop-and-copy baseline과 pre-copy·post-copy in-process 프로토콜을 구현했다. pre-copy는 dirty generation·delta seq/tombstone·수렴 조건·final ACK, post-copy는 별도 상태·소유권 선전환·priority/background pull·late-pull 쓰기 보호·실패 후 재개를 제공한다. 상세와 테스트 실행은 `experiments/b3-live-migration/protocols.md`를 따른다. S1~S5 세 방식 비교 backend·finite Zipf·JSON/요약 생성, S3 종료 설정 민감도, S6 취소·장애 비교를 구현했다. S6 extended profile은 delta 일부 적용·ACK 후 취소를 포함한 10경계로, 실제 90회·미지원 60행을 기록한다. 기존 full/quick 135행의 재생성도 유지한다. 다중 이동 Scheduler·M1~M5 backend도 구현·실행했다. 실제 KvEngine/peer 연계, 노드 증감 controller 연결과 schema v2 dirty 상세 생성기는 후속이다. 기존 SC1~SC6 실행·결과는 stop-and-copy 단독 결과로 유지한다. 후속 비교 계약은 `experiments/b3-live-migration/design.md` §6.6에 있다.
  - `replication`: A1·A2 연구 패키지. `primarybackup`(A1)은 SET 동기 복제와 `CrashOnlyPrimaryBackupEngine`의 단일 영구 crash 감지·고정 승계·쓰기 재개를 구현했다. `vr/VrEngine`은 고정 3노드 과반수 로그·view change·미완료 쓰기 복구·중복 요청 처리·읽기 로그 경유를 구현했다. 기본 INCREMENTAL 모드는 증분 PREPARE·작은 COMMIT·확인 기반 재전송을 사용하며 FULL_LOG는 개선 전 재현용이다. 전송 개선과 정책·216조건 비교는 `experiments/a1-active-passive-failover/network-optimization/README.md`, 기존 분단 비교는 `protocol-comparison/README.md`를 따른다. PB의 분단 안전성, 실제 TCP 연결, 재시작 recovery, membership 변경은 미구현이다. `ionia`(A2)는 골격만 있다. 테스트용 `InMemoryTransport`·`VirtualTimeSource`·`DirectMailbox`는 `core.replication` 테스트 패키지에 있다.
  - `failure`, `tablet`: 연구 패키지. 구현 현황은 각 `package-info.java`를 따른다.
- **`cache-server`** (`io.openkache.server`): Netty 실행 애플리케이션. 설정 검증, client·peer listener, TCP frame codec, PING, `/metrics`(Prometheus), GET/SET/DELETE의 `KvEngine` 연결까지 구현했다. peer 프로토콜(복제·장애 감지·마이그레이션 메시지)은 PING 외에는 `NOT_IMPLEMENTED`로 응답한다.
- **`experiment-runner`** (`io.openkache.runner`): Workload Generator + Experiment Runner 최소 구현. `cache-core`에만 의존하고(production), Netty 없이 `protocol-v1`을 직접 구현한 TCP client로 단일 노드 `cache-server`에 GET/SET/DELETE를 보낸다. warm-up/측정 구간 분리, HIT/MISS/실패 분리 집계, reservoir 기반 p50/p99, JSON(원본)+CSV(요약) 결과 기록까지 구현했다. 3-node, 장애 주입, Zipfian 분포는 미구현. 통합 테스트에서만(test 전용 의존성) `cache-server`를 띄워 end-to-end로 검증한다.
- 의존 방향은 `cache-server → cache-core`, `experiment-runner → cache-core`(+ 테스트 전용 `cache-server`)만 허용한다. Netty·Prometheus·logging 구현은 server에만 둔다. 연구별 Gradle 모듈은 만들지 않는다(`experiment-runner`는 연구가 아닌 공통 모듈이다).
- `monitoring/`(Prometheus, Grafana), `deploy/`(Dockerfile, compose): linux/amd64에서 이미지 빌드, compose 실행, PING, 호스트 `experiment-runner`로 GET/SET/DELETE 실행, Prometheus 수집, Grafana 대시보드까지 확인했다. 이미지에는 서버 배포본만 들어가지만 `settings.gradle.kts`가 `experiment-runner`를 include하므로 빌드 단계에서 그 디렉터리도 복사해야 한다. linux/arm64는 AWS m6gd.medium에서 이미지 빌드와 cache-server 기동(`/metrics` 응답)까지 확인했다(2026-11-03, 요청 처리는 미확인).
- `infra/`: Terraform 기반 AWS 실험 클러스터(실험할 때만 EC2 생성 → 결과 S3 → destroy). `bootstrap/`(state·결과 버킷, 최소 권한 IAM, 비용 알림)은 적용을 마쳤고, `experiment/`·`scripts/`로 노드 기동까지 확인했다. 팀원 실행용 GitLab CI(`.gitlab-ci.yml`, 웹 실행, 입력 `NODES`·`CONFIG`, Runner는 SSAFY 서버)는 cache-server N대 + 부하 생성 1대를 띄워 TCP 실험 config를 실행하고 결과를 S3에 올린 뒤 지운다(2026-11-04 첫 실행 성공). **m6gd.medium만, cache-server 8대 + 부하 생성 1대(전체 9대), 월 27만 원**은 교보재 조건이므로 절대 넘지 않는다. AWS 명령은 profile `openstd`로만 실행한다. 설계와 절차는 `infra/README.md`.

### A1 추가 구현 (2026-10-29)

`cache-core/.../replication/raft/RaftEngine`에 고정 3노드 Raft를 구현했다. seed별 선거 timeout·투표·AppendEntries·충돌 suffix 교체·현재 term commit·읽기 로그 경유를 제공한다. term/vote/log는 메모리에만 있고 재시작 recovery는 지원하지 않는다. PB/VR/Raft 216조건의 별도 비교·결과는 `experiments/a1-active-passive-failover/election-comparison/`에 있다. 기존 pipeline·느린 replica 정책·checkpoint의 멘토 질문은 유지한다.

`replication/chain/ChainEngine`은 head 쓰기·tail 읽기/확정·역방향 Sent Ack·미완료 suffix 복구를 제공한다. 신뢰하는 외부 master가 정확한 영구 crash만 제외하고 전 생존자를 freeze/복구/open한다. 분단된 생존자를 강제 제외하지 않는다. 네 방식 288조건과 위치별 crash 24조건은 `experiments/a1-active-passive-failover/chain-comparison/`에 있다. CRAQ·master 합의·신규 노드 추가·재시작은 미구현이다.

`replication/rose/RoseEngine`은 Rosé에서 착안한 **단일 파티션·op 상한 변형**이다. 다중 파티션 coordinated apply·snapshot·지역 복제·영속 WAL은 재현하지 않았다. L별 예약 lag·성공 쓰기 유실, 대기 deadline·prefix 복구 240조건은 `experiments/a1-active-passive-failover/rose-comparison/`에 있다. 외부 master가 정확한 원래 Primary crash를 알고 최장 생존 prefix를 선택하므로 PB의 고정 승계와 전제가 다르다. 최신 종합 보고·멘토 질문은 `experiments/a1-active-passive-failover/mentoring/2026-10-29-*.md`다.

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
  - Manifest: Git Commit은 JSON에 있다. Config 스냅샷(Workload 설정 전체)과 결과 경로는 JSON에 없다(결과 경로는 실행 시 stdout으로만 출력). "실행 명령"·"호스트 환경"은 Runner 쪽 JVM/OS만 기록하고 서버 쪽은 운영자가 `server.workers.hint`/`server.jvm.opts.note`로 적어 넣는 참고값이다(Runner가 서버를 직접 관측하지 않는다).

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

## B3 선행 증감·재배치 실험 추가 (2026-11-02)

선행 메커니즘 8개의 연구 prototype은 `experiments/b3-live-migration/scaling-decision/backend/`의 Python 코드로 구현했다. 공통 Runner가 test의 `LiteratureResearchMain`을 통해 실행한다. 가상 요청·대기열·준비·전송 모형이며 실제 OpenKache 노드 실행·원 논문 시스템의 재현은 아니다. 구현 변경점·실행·결과는 해당 폴더의 `README.md`·`results.md`를 따른다. 기존 자체 비교 정책 세 개는 사용하지 않는다. production 노드 증감 controller와 실제 migration 연계는 후속이다.

## B3 다중 이동 추가 (2026-11-02)

`core.migration.MigrationScheduler`에 FIFO·전체/노드 동시 상한·회전 byte 할당·timeout 수집을 구현했다. 세 protocol의 `committed(before, published)`와 `MigrationPublication`으로 다른 Tablet의 선행 commit을 보존하고 대상 Tablet 변경을 거절한다. 공통 `api/model/spi/runtime` 계약은 변경하지 않았다. test의 `MultiMigrationFixture`·`MovementExperiment`·`MovementResearchMain`은 실제 세 protocol을 다중 노드 메모리 저장소·공유 라우팅 CAS에서 실행한다. M1~M5 정식 210회/3,465 jobs·축소 두 반복 168회를 완료했고 정합성·매 tick 예산 위반은 0이다. 실행·schema·범위·결과는 `experiments/b3-live-migration/movement-scaling-comparison/README.md`와 `results.md`를 따른다. 실제 KvEngine/TCP/SSD·노드 증감 controller 연계, v2 dirty 상세는 후속이다. 상단 구조 요약의 과거 E/M 후속 표기보다 이 현황을 우선한다.

## B3 선행 Scheduler 비교 추가 (2026-11-04)

`MigrationScheduler.Policy`에 시작 후보와 예산의 주입 경계를 추가했다. Scheduler가 job ID·전체/노드 예산을 검증하며 기존 FIFO constructor는 유지한다. 공통 `api/model/spi/runtime`과 production 의존성은 변경하지 않았다. test의 `SchedulerAlgorithms`·`SchedulerComparisonExperiment`·`SchedulerResearchMain`이 서로 다른 Tablet 크기·교차 경합·dirty 편향·완료 기한에서 9개 정책을 실제 세 protocol로 비교한다. CAMIG는 같은 후보 목적지 집합의 계획 선택을 별도로 평가하며 실행기는 동일하다. 원 시스템의 전체 재현이 아닌 endpoint-only 연구 변형이고 TiDB AIMD, SLAMIG γ=0, VMbuddies 비동기 cutover, FPTA 단일 경로·4 slot, CAMIG의 제한된 목표를 구분한다. 설정·원문 대비 범위·실행·결과는 `experiments/b3-live-migration/scheduler-comparison/README.md`와 `results.md`를 따른다. 기존 M1~M5 결과는 FIFO의 조건 변화 실험으로 보존한다.

정식 855회/10,260 jobs·축소 342회×2반복=684회를 완료했고 정합성·예산·동시 상한 위반은 0이다. 전체 빌드·테스트 633개를 통과했다. source 공유·Pre-copy는 노드당 동시 1개가 FIFO 동시 4개보다 빨랐으며 CAMIG는 데이터량 균등 기준선과 전체 완료 평균이 같았다. 공식 run과 정확한 수치는 위 결과 문서를 따른다. commit/push는 별도 명시 요청이 있을 때만 한다.


## B3 Pre-copy dirty 상세 추가 (2026-11-04)

PreCopyProtocol.roundEvents()는 generation별 STARTED/ACKNOWLEDGED/ABORTED와 dirty key·tombstone 수·예상/직전 bytes·실제 전송 bytes·seq·종료 사유를 불변 관측으로 제공한다. payload를 보관하거나 실제 시계를 읽지 않는다. test의 PreCopyRoundTrace가 caller tick을 붙이고 JSON 원본 검증 및 dirty-rounds.csv/md 재생성을 맡는다. S1~S5/S3 민감도·M1~M5·Scheduler의 새 config는 각 backend schema 2를 사용하며 기존 schema 1을 읽는다. 통합 b3-research-v2 전체 구현이 아니며 실제 KV/peer·증감 controller 연결은 여전히 후속이다. 세부 의미와 검증은 experiments/b3-live-migration/migration-comparison/dirty-rounds.md를 따른다. 이 절을 과거 dirty 상세 미구현 표기보다 우선한다.


### B3 현재 작업 범위 보정

사용자는 공통 모듈을 사용할 계획이 없다고 명시했다(2026-11-04). 개인 B3의 필수 범위에 공통 KV/Network/B1/B2 controller/Experiment Runner 통합을 추가하지 않는다. experiments/b3-live-migration/run_experiment.py로 자체 fixture/backend를 직접 실행한다. 기존 공통 자료형은 모델로 재사용한다. 이전의 통합 후속 표기는 선택·호환 경로로 해석하며 독립 prototype의 증감 판단·이동 실행 연결과 실제 노드 실험을 우선한다.


## B3 배치·준비 지연·안전한 축소·부분 실패 추가 (2026-11-04)

개인 B3의 공통 서비스 통합 제외 지시를 유지한다. execution-policy-comparison/policy_lab.py는 Accordion MILP/CA식 배치 가능성과 AWS warm-up/ElastMan/P-Store의 준비·이동 지연을 기존 독립 Python 모형에서 실행한다. execution-safety-comparison은 실제 B3 세 protocol·MigrationScheduler를 자체 fixture에서 실행해 Scylla식 preflight/drain, MongoDB식 부분 drain/관리 metadata, PD식 job 격리/timeout, FDB식 seed 장애 재생을 검사한다. 원 제품 전체 재현이나 물리 장애 실험은 아니다. main에는 운영 의존성을 추가하지 않았다.

최종 정식 policy 820회×2와 safety 435회×2, 총 2,510회를 완료했다. 예상 거부·보류·HELD와 완료를 구분하며 검사한 위반은 0이다. 원본 재생성 파생 파일 12개와 반복별 요약·Java event·record evidence가 일치했다. Map의 표시 순서로 controller-reload 상세 로그가 달라진 문제를 수정 후 safety 정식을 재실행했다. B3 Java 113개·Python 5개와 운영 의존성 검사는 통과했다. 전체 build는 기존 Windows NvmeKvStoreTest 두 실패로 실패하며 성공 처리하지 않는다. 문제·선행 사례·지표·최종 run은 experiments/b3-live-migration/execution-validation.md를 따른다. 독립 판단→실제 protocol→drain의 전체 폐루프와 실제 서버 측정은 아직 완료가 아니다. commit/push는 별도 요청 때만 한다.

## B3 Java 이전·선택 모듈·결과 교체 (2026-11-05, 현재 기준)

과거 Python 구현·실행 안내보다 이 절을 우선한다. B3의 Python 소스와 실행기를 제거하고 Java21/JDK 전략 모듈(io.openkache.core.migration.strategy), B3Configuration/B3Workflow, CopyDrivers, B3ResearchLauncher, Java2D 그림·계획 생성기로 이식했다. 기존 cache-core에서 구현했고 새 Gradle 모듈·외부 production 의존성은 없다. 공통 KV/Network/B1/B2/Runner 서비스 통합은 필수가 아니다. 기본 b3/b3Research는 목록/도움말이며 build가 실험을 자동 실행하지 않는다.

사용자 요청으로 이전 Python 알고리즘·보조 실행 결과를 삭제하고 Java 가상 실험 9묶음 6,157회로 교체했다. 원본 재생성 79파일, 반복 비교 34파생 파일·1,971 scientific evidence가 일치했다. 보고서 반복 정보·실제 planning 시간은 scientific 비교에서 분리한다. 현재 결과·run·소스/클래스/manifest hash는 experiments/b3-live-migration/java/results.md와 run-audit.json을 따른다. 기존 순수 Java 정식 결과는 보존하며 중간 Java 실행은 제거했다. 기존 S1~S5 full CSV로 다섯 그림을 Java2D로 재생성해 Python 렌더링 이미지를 교체했다.

B3 Java 테스트 140개가 모두 통과했다. 전체 762개 중 성공 757개·기존 Windows NVMe 실패 2개·조건부 skip 3개다. 실패는 NvmeKvStoreTest의 scanReadsValuesFromStorage, recordsInOlderSegmentsRemainReadableAfterRotation이며 변경 전 기준에서도 재현했다. 전체 build는 실패 상태이며 core 운영 의존성 검사는 통과했다.

실노드 전체 흐름은 N/U/R 3종 계획만 사용한다. 사용자 지시로 실제 N/U/R 시나리오는 실행하지 않았다. B3Workflow는 자체 Environment/Binding을 통해 관측→판단→READY→계획→copy→drain→제거를 조립하며 단위 계약 검사까지다. 실제 TCP/SSD·프로세스 재시작·metadata 영속성·PAX native hint·실제 split/merge는 준비·검증됐다고 주장하지 않는다. commit/push는 별도 요청 때만 한다.

## B3 범위 보정: 판단만 비교 (2026-11-05, 최우선 현재 기준)

기존 상태를 063c817에 커밋한 뒤 B2가 섞인 실험을 수정했다. NodeCountPolicy.Decision은 수만 반환한다. 모든 판단에 FixedTabletPlacement의 fixed-tablet-lpt-v1을 적용하며 노드 수가 같으면 배치도 유지한다. SCADS/ElastMan/E-Store/Accordion/P-Store/TIRAMOLA/PAX 7판단과 고정 대조군을 비교한다. Slicer·CAMIG·Accordion MILP 재배치·E-Store 세분화·SCADS 동적 복제·PAX partition 선택/hint를 활성 코드·실험에서 제거했다. PAX는 ARIMA·평균 CPU 수 판단 변형이며 원형 재현이 아니다.

기존 결합형 증감·배치·CAMIG run을 삭제하고 새 Java 6묶음 4,684회(판단 full560/quick224, 지연 full1440/quick192, Scheduler full1620/quick648)로 교체했다. 원본 재생성 60파일·반복 evidence 2342개가 일치했다. 기존 순수 copy·safety·M은 이전 버전 기록으로 별도 유지한다. 과거 6,157회를 현재 B3 성과로 쓰지 않는다. 현재 run·수치·hash는 experiments/b3-live-migration/java/results.md와 run-audit.json을 따른다.

B3 테스트 138개 모두 통과. 전체 760개 중 성공 755개·기존 NVMe 실패 2개·조건부 skip 3개다. 실패는 NvmeKvStoreTest의 scanReadsValuesFromStorage와 recordsInOlderSegmentsRemainReadableAfterRotation이며 변경 전에도 재현한 항목이다. 전체 build는 실패 상태이며 production 외부 의존성 검사는 통과했다.

실노드 N/U/R은 7판단×고정 배치×9 Scheduler×3copy×2drain×3흐름×5seed=5,670회/환경의 계획만 생성했다. 실행은 금지 상태를 유지한다. 기존 21,870/25,380 조합 수는 폐기한다. 공통 서비스 통합은 필수가 아니고 실제 I/O·프로세스 복구·영속성·성능은 후속이다. push는 요청하지 않았으므로 실행하지 않는다.
