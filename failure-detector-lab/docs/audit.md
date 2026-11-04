# Failure Detector Lab v2 저장소 감사

감사일: 2026-10-27
브랜치: `feat/swim-network`
감사 시점 revision: `97a4ba8fea509138f12da84eebc703a74aef6bad`

`failure-detector-lab/`과 두 실험 명세는 작업 시작 전부터 untracked 상태였다. 기존 결과
디렉터리는 삭제하거나 덮어쓰지 않았고, 새 실행은 항상 새 이름을 사용했다.

## 기존 구현과 A/B/C 대응

| 코드 | A: 판단 | B: 관측·전파 | C: 보정 | v2 상태 |
|---|---|---|---|---|
| `FixedDeadlineFailureDetector` | 고정 deadline | periodic heartbeat | 없음 | F0 |
| `PhiAccrualFailureDetector` | bounded-window Phi | gossip heartbeat | 없음 | F1 |
| `SwimLifeguardDetector` base 설정 | SWIM suspicion | direct/indirect probe | 없음 | F2 |
| `SwimLifeguardDetector` 기본 설정 | SWIM suspicion | F2와 같음 | 기존 Lifeguard 묶음 | F3 |
| `AdaptiveDeadlineFailureDetector` | inter-arrival EWMA deadline | F0 stream | 없음 | F4 추가 |
| `RingMonitorAssignment` + fixed | F0와 같음 | successor ring | 없음 | F5 추가 |
| fixed verified mode | F5와 같음 | ring + helper | Verify Suspect | F6 추가 |
| SWIM correction 설정 | F2와 같음 | F2와 같음 | LHA-Probe/Suspicion/Buddy 각각 | F7a-c 추가 |

## 재사용한 공통 기반

- `DeterministicEventScheduler`: monotonic 가상 시간과 고정 event 우선순위.
- `ExperimentRunner.NetworkTrace`: seed/time/leg 기반 stateless draw와 관측 모듈 namespace.
- `GroundTruthEvent`: process alive, observer reachability, service health 분리.
- `MetricsEvaluator`: nullable SUSPECT/DEAD latency, false state, unreachable duration, censoring.
- `ResultWriter`: manifest, CSV/JSONL, 통계, source label, overwrite refusal.
- `UdpExperimentNodeMain`: observer-local monotonic clock과 실제 UDP frame/byte 계측.

## 의도적으로 보존한 결합

SWIM probe correlation, incarnation, suspicion, Lifeguard 조정은 generic heartbeat policy로
왜곡하지 않고 기존 detector 안에 유지했다. 모듈 계층은 capability와 실험 metadata를
정의하고 runner는 profile adapter를 사용한다. F6 검증은 별도 helper를 쓰며, F7 Buddy는
추가 refutation probe일 뿐 독립 DEAD 증거가 아니다.

## 결과 보존과 제외

- 기존 v1 및 corrected 4-profile 결과는 그대로 보존했다.
- v2 이전의 speculative broad 결과는 `PRE_SPEC_V2.md`로 범위 밖임을 표시했다.
- Docker 준비 실패와 ring 방향 오류 결과는 `INVALID_RUN.md`로 표시하고 삭제하지 않았다.
- 최종 형식 simulation은 `results-modular-v2-formal-20261027-v2/`다.

## 남은 위험

- F5 simulation은 node-0이 successor node-1을 관측하는 경로 중심이며 전체 membership view
  수렴 시간을 별도 관측자별로 출력하지 않는다.
- F6 verification timing은 simulation-observed이나 비용은 model-estimated다.
- F7 correction 분리는 controlled prototype이며 production protocol 동일성을 주장하지 않는다.
- CPU/GC, 제품 직렬화 byte, GET/SET health, 자동 failover는 범위 밖이다.
