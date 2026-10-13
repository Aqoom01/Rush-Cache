# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

OpenStd(OpenKache)는 고성능 NVMe SSD 기반 분산 캐시의 가용성과 확장성에 관한 선행연구 알고리즘을 Prototype으로 구현하고, 공통 실험 환경에서 동작 및 성능 특성을 비교·검증하는 프로젝트다. Rust, Linux/WSL2 환경에서 개발한다.

6명이 각자 개별 연구 1개와 공통 모듈 1개를 함께 맡는다.

| # | 연구 | 공통 모듈 (크레이트) | 1차 인수 기준 |
|---|---|---|---|
| A1 | Active-Passive Fast Failover | KV Store (`kv-store`) | GET/SET/DELETE 및 로컬 저장 단위 테스트 |
| A2 | Active-Active / SSD-aware Replication | Workload Generator (`workload`) | 고정 Seed로 동일 요청 생성 |
| A3 | SWIM / Lifeguard Failure Detection | Simulation / Turmoil (`simulation`) | 3-node 및 crash/partition 재현 |
| B1 | Tablet-based Data Distribution | Network / Shared Types (`common`) | NodeId 및 공통 모델, 기본 송수신 예제 |
| B2 | Load-aware Balancing + Split/Merge | Metrics (`metrics`) | 가상/실제 시간 구분 JSON 지표 기록 |
| B3 | Live Migration + Elastic Scaling | Experiment Runner (`experiment-runner`) | N회 실행 및 실패/타임아웃 결과 수집 |

## 현재 상태

크레이트 골격만 있는 단계다. 6개 크레이트 모두 `cargo new --lib`가 생성한 placeholder(`add`, `it_works`)만 있고, 아래 책임은 모두 **구현 예정**이다. 구현되지 않은 기능을 완료된 것으로 서술하지 않는다. `infra/`는 빈 디렉터리이고 README는 없다.

## 공통 모듈 명세 요약

기준 문서는 「OpenKache 분산 캐시 선행연구 공통 모듈 개발 명세서 v1.0」이다. 원본 PDF는 저장소에 없으며 아래 요약이 기준이다. 요약에 없는 세부 사항은 임의로 정하지 말고 사용자에게 확인한다. "선택"으로 표시한 기능은 관련 연구의 요구가 확정되기 전에는 구현하지 않는다.

`cache/`가 Cargo workspace 루트다(`resolver = "3"`, `edition = "2024"`, Rust 1.85 이상). 크레이트는 `cache/crates/` 아래에 있다.

- **`common`** (Network / Shared Types)
  - 노드 식별: `NodeId`와 `Address`. 시뮬레이션 호스트명과 실제 주소의 매핑 규약을 포함한다.
  - 통신: UDP와 TCP의 특성을 유지하는 최소 통신 경계. 하나의 범용 Transport로 강제하지 않는다.
  - Codec: 공통 Envelope가 필요할 때만 Version·Type·Length 수준으로 정의한다. 연구별 메시지는 각 연구에서 정의한다.
  - Tablet: `TabletId`, Tablet Metadata(Owner, Key Range, Version). 구조는 B1·B2·B3 담당자가 합의한 뒤 확정한다.
  - 실제 요구가 확인되기 전에는 타입과 추상화를 만들지 않는다.
- **`kv-store`**
  - `KvStore` Trait과 HashMap 기반 `MemoryStore`를 만든다. Key와 Value는 바이트열이다.
  - GET: 값의 복사본을 반환하고, 없는 Key는 오류가 아닌 정상 MISS로 처리한다.
  - SET: 같은 Key면 덮어쓴다.
  - DELETE: 삭제 여부를 반환한다.
  - EXISTS, COUNT를 제공한다.
  - SCAN/RANGE는 선택이다. B1·B3 요구 확정 후 구현한다.
  - 오류 처리 규칙을 문서화한다.
- **`workload`**
  - 요청: GET/SET/DELETE Operation 자료형. Key 개수·Key 길이·Value 크기를 조절한다.
  - 연산 비율(Mix): 합계 100%인지 검증한다.
  - 분포: Uniform, 고정 Hot-Key. Zipfian은 선택이다.
  - 재현성: 같은 설정과 Seed면 같은 요청열을 만든다.
  - 스케줄: 동시 클라이언트 수와 요청 간격을 설정한다. 논리적 요청 생성과 실제 발송을 분리한다.
- **`simulation`**
  - Turmoil 3-host 예제(메시지 교환)를 만든다.
  - Fault Helper: Crash/Restart, Partition/Repair, Delay/Loss.
  - Scenario Config: 대상 호스트, 주입 시각, 파라미터, Seed, 실행 제한 시간.
  - Fault Event: 주입 직후의 가상 시각과 장애 대상을 기록한다.
  - Turmoil을 과도하게 추상화하지 않는다. 사용 중인 Turmoil 버전이 지원하지 않는 API를 가정하지 않는다.
- **`metrics`**
  - Event 필드: name, node_id, timestamp, time_domain.
  - 측정값: Duration·Counter(누적)·Gauge를 단위와 함께 기록한다.
  - 통계: Raw Sample로 mean, min/max, p50/p95/p99를 계산하고 계산 방식을 명시한다.
  - Metadata: study, variant, scenario, seed, node_count, commit, config, execution_id.
  - 출력: 원본 결과는 JSON, 분석용은 CSV.
  - Virtual Time과 Wall-clock Time 값을 섞어 통계 내지 않는다.
- **`experiment-runner`**
  - Config 항목: 연구명, Variant, Scenario, Seed 목록, 반복 횟수, 제한 시간.
  - 실행: 실행 파일이나 Cargo 테스트를 호출하고 stdout/stderr를 캡처한다.
  - 반복: N회 반복하고 실행마다 Execution ID를 발급한다.
  - 종료 판정: 비정상 종료와 시간 초과를 구분해 저장한다.
  - 결과 수집: JSON/CSV 결과를 수집하고 형식 검증과 누락 감지를 한다.
  - Manifest: Config, Git Commit, 실행 명령, 호스트 환경, 결과 경로를 기록한다.

**연구별 공통 모듈 사용** (크레이트 의존 관계를 연결할 때 참고):

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
| 1주 | NodeId 및 결과 Schema/API 합의, KV CRUD, Turmoil 3-node 샘플, Runner 최소 동작 |
| 2~4주 | 실제 중복 확인 후 Network/Workload/Tablet 타입 보강, Metrics·Runner 안정화 |
| 5주 | 공통 인터페이스 통합, 다른 연구 담당자의 재사용 검증 |
| 6주 | 반복 실험, Docker 기반 실측 |
| 7주 | 재현 스크립트 확정, 보고서·발표용 그래프 |

## 명령어

루트에는 `Cargo.toml`이 없으므로 모든 Cargo 명령은 `cache/`에서 실행한다.

```bash
cd cache
cargo build --workspace                                  # 전체 빌드
cargo test --workspace                                   # 전체 테스트
cargo test -p kv-store                                   # 특정 크레이트 테스트
cargo test -p kv-store it_works                          # 이름 필터로 단일 테스트 실행
cargo fmt --all                                          # 포맷 (검사만: -- --check)
cargo clippy --workspace --all-targets -- -D warnings    # 린트
```

## 개발 원칙

- 모든 설명과 작업 결과는 한국어로 작성한다.
- 공통 모듈은 최소 API와 사용 예제를 먼저 제공한다. 다른 연구자 최소 1명이 사용해 본 뒤 공용 API를 확정한다.
- 공통 모듈에는 연구별 알고리즘을 넣지 않는다. 명세에 없는 기능을 임의로 추가하지 않는다.
- 불필요한 추상화, 범용 프레임워크, 외부 라이브러리를 추가하지 않는다.
- 공통 인터페이스를 변경할 때는 수정 전에 영향 범위(의존 크레이트, 호출부, 영향받는 담당자)를 먼저 설명한다.
- 크레이트 간 의존성은 필요한 API가 정의될 때 연결하고, 순환 의존을 만들지 않는다. 코드 수정 전에 기존 구조와 의존 관계를 확인한다.
- 구현 범위와 완료 여부는 실제 코드와 테스트로 판단한다.
- Rust 코드를 변경한 뒤 위의 fmt, clippy, test 명령으로 검증한다.
- commit, push, merge는 명시적으로 요청받은 경우에만 실행한다.

## Git / MR 컨벤션

상세 규칙은 `docs/git-convention.md`, `docs/mr-convention.md`를 따른다.

- 브랜치: `master`(검증된 결과물·마일스톤), `develop`(통합). 작업 브랜치는 `develop`에서 `<type>/<영문 요약>`으로 만들고 MR로 `develop`에 병합한다(Squash 권장). 마일스톤 완료 시 `develop` → `master` MR(Squash 금지).
- 커밋: `<type>: <한국어 설명>` (예: `feat: KV Store GET/SET 구현`)
- MR 제목: `[Type] 작업 내용 (S15P31D111-<번호>)`. 본문은 `.gitlab/merge_request_templates/Default.md`를 쓰고 `Closes S15P31D111-<번호>`(작업이 남으면 `Refs`)로 Jira 이슈를 연결한다.
