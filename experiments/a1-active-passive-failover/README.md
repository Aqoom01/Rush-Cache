# A1 Active-Passive Fast Failover

노드 여러 대 중 한 대(Primary)만 쓰기를 받고 나머지(Backup)는 복사본을 들고 대기하다가, Primary가 죽으면 Backup이 빠르게 이어받는 복제 방식 여러 개를 같은 인터페이스로 구현하고, 같은 workload와 장애 시나리오에서 쓰기 지연·failover 시간·데이터 유실·fencing을 비교한다.

## 문서

| 파일 | 내용 |
|---|---|
| [design.md](design.md) | A1 전체 설계: 범위, 경계, 기준형의 다섯 조각, 변형 비교 축, 불변식, 지표, 실험 계획, 코드 구조 |
| [references/](references/) | 방법론별 정리. 읽은 내용과 프로젝트 적용 방식("실험을 위해 직접 정해야 할 것", "우리 코드와의 대응")을 분리해 적는다 |
| [references/survey.md](references/survey.md) | 후보 15개 + 최근 논문 7개 조사와 선정 근거 |
| [replication-comparison/](replication-comparison/README.md) | 고정 3노드 동기 SET의 지연·deadline 27조건 실험 및 공통 Runner 실행 안내 |
| [failover-comparison/](failover-comparison/README.md) | crash-only 기준형의 장애 감지·승격·쓰기 재개 27조건 |
| [protocol-comparison/](protocol-comparison/README.md) | PB / VR의 정상·분단·미완료 쓰기 160조건 비교 |
| [network-cost-comparison/](network-cost-comparison/README.md) | 메시지 크기·방향별 대역폭·FIFO 큐를 반영한 쓰기·crash 비용 216조건 |
| [network-optimization/](network-optimization/README.md) | 증분 PREPARE·작은 COMMIT·재전송 제어의 개선 전후 216조건 비교 |
| [sustained-load/](sustained-load/README.md) | 연속 쓰기·ACK 지연·정상화 48조건, 시계열과 멘토 질문 |
| [load-crash/](load-crash/README.md) | 연속 부하 중 Primary 종료 48조건, 첫 OK와 생존 로그/KV 보존 |
| [election-comparison/](election-comparison/README.md) | Raft 추가: PB/VR/Raft의 정상·crash·분단·재합류 216조건 |
| [chain-comparison/](chain-comparison/README.md) | Chain 추가: 네 방식 288조건 + head/middle/tail 위치별 crash 24조건 |
| [rose-comparison/](rose-comparison/README.md) | Rosé 기반 단일 파티션 bounded-lag 변형 240조건, L별 성공·유실·lag 시계열 |
| [최신 종합 보고](mentoring/2026-10-29-report.md) | 다섯 접근의 구현 범위·최신 핵심 552조건·Notion 보고용 자료 |
| [멘토링 질문](mentoring/2026-10-29-questions.md) | 목표 환경의 지연·복구 예산, 캐시 유실 허용, 후순위 Rosé 검증 범위 |

## 변형(variant) 목록

variant별 참고 문헌과 실제 구현 범위를 구분한다. 실험의 variant ID는 각 backend 문서를 따르며 Rosé의 단순화 변형은 `ROSE_ADAPTATION`으로 명시한다.

| variant | 쓰기 확정 조건 | Primary 교체 | 근거 | 우선순위 | 상태 |
|---|---|---|---|---|---|
| `primary-backup` | 전원 ack (동기 구현) / 즉시 (비동기 계획) | heartbeat + 승계 순서 고정 (단일 crash 구현) | [Primary-Backup](references/primary-backup.md) (Budhiraja et al. 1993) | **1 (기준형)** | 동기 SET·단일 Primary crash 승격 구현. 지연·복구 실험 |
| `viewstamped-replication` | 과반수 | 프로토콜 내: view change (view mod N) | [VR Revisited](references/viewstamped-replication.md) (Liskov & Cowling 2012) | **1** | 고정 3노드 로그·view change·미완료 쓰기 복구·분단 검증 구현 |
| `raft` | 과반수 | 프로토콜 내: 랜덤 timeout + 투표 | [Raft](references/raft.md) (Ongaro & Ousterhout 2014) | 2 | 고정 3노드 메모리 구현·투표·충돌 로그 정리·비교 backend 구현. 재시작 영속화 제외 |
| `chain-replication` (+ CRAQ) | 전원 통과 (tail 도달) | 외부 master | [Chain Replication / CRAQ](references/chain-replication-craq.md) (2004 / 2009) | 2 | Chain 쓰기·tail 읽기·Sent 복구·위치별 crash 구현. CRAQ 미구현 |
| `rose-bounded-lag` (실험 `ROSE_ADAPTATION`) | 로컬 적용 + op lag 상한 L | 외부 master·최장 생존 prefix | [Rosé](references/rose-bounded-lag.md) (CIDR 2026)에서 착안 | 2 | 단일 파티션 bounded-lag 변형 구현·240조건. 다중 파티션 coordinated apply 미구현 |

- 우선순위 1 두 개가 팀 과제 카드의 인수 기준이다. 기준형(`primary-backup`)을 먼저 만들고 그 위에서 공통 인터페이스·실험 틀을 검증한 뒤 `viewstamped-replication`을 붙인다.
- Raft·Chain과 Rosé 기반 단일 파티션 변형까지 추가했다. Raft는 선출 방식, Chain은 사슬 구조와 tail 확정, Rosé 변형은 L별 응답·유실 trade-off를 비교한다. 논문 전체의 구현 완료를 뜻하지 않으며 CRAQ·Rosé 다중 파티션 coordinated apply 등은 별도 범위다.
- **Redis Replication + Sentinel**은 variant가 아니라 **비교 대상**이다. 기준형의 빈칸(감지·승격)을 실제 제품이 어떻게 채웠는지 보여주며, 정리는 [references/redis-replication-sentinel.md](references/redis-replication-sentinel.md)에 있다. 기준형의 heartbeat timeout, 승격 대상 선택 규칙은 Sentinel 값을 우리 규모로 줄여 쓴다.

## 코드 위치

알고리즘 코드는 `cache-core`의 `io.openkache.core.replication` 하위 패키지에 둔다(A2 IONIA와 같은 상위 패키지를 쓰고 하위 패키지로 나눈다). 이 디렉터리에는 설계, 참고 문헌 정리, 실험 설정과 결과 요약만 둔다. 상세는 [design.md](design.md) §7을 따른다.

기준형의 분단·미완료 쓰기 한계와 VR의 로그 복구를 [같은 실행기에서 비교](protocol-comparison/README.md)한다. 실제 서버 연결·프로세스 재시작 recovery·동적 구성 변경은 이번 구현에 포함하지 않는다.
