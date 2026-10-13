# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트 개요

OpenStd는 고성능 NVMe SSD 기반 분산 캐시의 가용성(Availability)과 확장성(Scalability) 기술을 연구하고, 선행연구 알고리즘을 Prototype으로 구현해 공통 실험 환경에서 비교·검증하는 프로젝트다.

연구 주제:

- A1: Active-Passive Fast Failover
- A2: Active-Active / SSD-aware Replication
- A3: SWIM/Lifeguard Failure Detection
- B1: Tablet 기반 Partitioning 및 Routing
- B2: Load-aware Balancing 및 Split/Merge
- B3: Live Migration 및 Elastic Scale-out/in

## 현재 상태

초기 골격만 있는 단계다. 각 크레이트에는 `cargo new --lib`가 생성한 placeholder(`add` 함수와 `it_works` 테스트)만 있고, 아래 크레이트 역할은 **구현 예정**인 설계 의도다. 아직 구현되지 않은 기능을 구현된 것처럼 서술하거나 가정하지 않는다. `docs/`와 `infra/`는 빈 디렉터리(`.gitkeep`)이고 README는 없다.

## 구조

- `cache/`: Cargo workspace 루트다(`resolver = "3"`). 모든 크레이트가 `edition = "2024"`이므로 Rust 1.85 이상이 필요하다. 빌드 산출물은 `cache/target/`에 생성된다.
  - `crates/common`: 공통 자료형, KV 요청·응답 모델, Node ID 및 인터페이스
  - `crates/kv-store`: GET/SET 기반 In-Memory KV Store
  - `crates/simulation`: Turmoil 기반 분산 네트워크 및 장애 시뮬레이션 환경
- 새 크레이트는 `cache/Cargo.toml`의 `members`에 추가해야 한다.
- `infra/`는 인프라 설정용, `docs/`는 문서용으로 예약된 디렉터리다.

## 명령어

루트에는 `Cargo.toml`이 없으므로 모든 Cargo 명령은 `cache/`에서 실행한다.

```bash
cd cache
cargo build                         # 전체 빌드
cargo test                          # 전체 테스트
cargo test -p kv-store              # 특정 크레이트 테스트
cargo test -p kv-store it_works     # 이름 필터로 단일 테스트 실행
cargo fmt --all                     # 포맷
cargo clippy --all-targets          # 린트
```

## 개발 규칙

- 모든 설명과 작업 결과는 한국어로 작성한다.
- 코드를 수정하기 전에 기존 아키텍처와 크레이트 간 의존 관계를 확인한다.
- `common`의 공통 인터페이스를 변경할 때는 수정 전에 영향 범위(의존 크레이트, 호출부)를 먼저 설명한다.
- 불필요한 추상화나 외부 의존성을 추가하지 않는다.
- 코드를 변경한 뒤 `cargo fmt --all`, `cargo test`, `cargo clippy --all-targets`로 검증한다.
- commit, push, merge는 명시적으로 요청받은 경우에만 실행한다.

## Git / MR 컨벤션

- 커밋 메시지는 Conventional Commit 접두어와 한국어 설명으로 작성한다. 예: `chore: GitLab MR 공통 템플릿 추가`
- 브랜치 이름은 `<type>/<kebab-description>` 형식을 따른다. 예: `env/init-cargo-workspace`
- GitLab MR은 `.gitlab/merge_request_templates/Default.md` 템플릿을 사용하고, Jira 이슈는 `Closes S15P31D111-<번호>`로 닫는다. 실험 관련 MR에는 실험 환경, 워크로드, 장애 시나리오, Throughput, p50/p99 Latency 측정 결과를 적는다.
