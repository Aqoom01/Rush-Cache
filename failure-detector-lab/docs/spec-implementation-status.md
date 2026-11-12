# 모듈형 Failure Detector v2 명세 구현 상태 (2026-10-27)

## Gate 0 — 기존 기반 확인

- 완료: deterministic scheduler, ground truth와 detector event 분리, nullable latency와 censoring.
- 완료: 기존 Fixed Deadline, Phi+Gossip, SWIM base/Lifeguard 계약 보존.
- 완료: 기존 결과 디렉터리 보존과 출력 덮어쓰기 거부.

## Gate 1 — A/B/C 구조와 호환성

- 완료: `DecisionKind`, `ObservationKind`, `CorrectionKind`, `ProfileModules`.
- 완료: F0~F7c 10개 선언 프로필과 code/ID 양쪽 CLI 선택.
- 완료: 의미 없는 조합을 capability validation으로 거부.
- 완료: `compatibility.csv` 96개 조합과 거부 사유 출력.
- 제한: 모든 내부 구현을 억지로 하나의 generic detector interface로 분해하지 않고,
  프로토콜 고유 결합은 profile adapter 안에 보존했다.

## Gate 2 — 판단·관측 모듈

- 완료: F4 EWMA 평균/편차 기반 bounded adaptive deadline.
- 완료: F5 deterministic ring assignment와 view 변경 계약.
- 완료: 같은 관측 모듈끼리 공통 stateless network stream 사용.
- 제한: F4는 논문 완전 재현이 아닌 inspired baseline이다.
- 제한: wire-budget 정규화 비교는 미구현이다.

## Gate 3 — 보정 모듈

- 완료: F6 독립 helper 경로의 Verify Suspect와 연속 실패 threshold.
- 완료: F7a LHA-Probe, F7b LHA-Suspicion, F7c Buddy 분리 프로필.
- 완료: Buddy refutation을 독립 DEAD 증거로 세지 않는 계약.
- 제한: F7a-c는 contribution 실험용이며 full memberlist wire compatibility를 주장하지 않는다.

## Gate 4 — 공통 실험과 리포트

- 완료: 12 scenarios × 5/25/100 nodes × 30 seeds × 10 profiles = 10,800 runs.
- 완료: profile code와 A/B/C를 manifest, runs CSV/JSONL, summary에 기록.
- 완료: paired `module-contributions.csv`와 `compatibility.csv`.
- 완료: simulation-observed 지연/오판과 model-estimated 비용을 분리 표기.
- 제한: 평균 CI는 bootstrap이 아닌 normal approximation.

## Gate 5 — 실제 UDP + Docker

- 완료: 3·5·8노드 Compose와 F0~F6, F7a, F7b 실제 UDP 경로.
- 완료: F5/F6 5·8노드 crash smoke에서 SUSPECT/DEAD 관측.
- 완료: observer-local monotonic time과 실제 Datagram frame/byte 기록.
- 수정: 최초 F5/F6 ring smoke에서 heartbeat 방향 오류를 발견해 수정했고, 해당 결과는
  삭제하지 않고 `INVALID_RUN.md`로 제외했다.
- 제한: F7c UDP adapter, 반복 통계, 제품 GET/SET health, CPU/GC, 자동 failover는 미구현.

Hybrid, Rapid multi-observer cut, Geode/JGroups 전체 복제, 자동 replica 승격은 구현하지 않았다.
상세 실행 증거는 `docs/verification.md`에 있다.
