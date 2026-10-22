# A1 Active-Passive Fast Failover

노드 여러 대 중 한 대(Primary)만 쓰기를 받고 나머지(Backup)는 복사본을 들고 대기하다가, Primary가 죽으면 Backup이 빠르게 이어받는 복제 방식 여러 개를 같은 인터페이스로 구현하고, 같은 workload와 장애 시나리오에서 쓰기 지연·failover 시간·데이터 유실·fencing을 비교한다.

## 문서

| 파일 | 내용 |
|---|---|
| [design.md](design.md) | A1 전체 설계: 범위, 경계, 기준형의 다섯 조각, 변형 비교 축, 불변식, 지표, 실험 계획, 코드 구조 |
| [references/](references/) | 방법론별 정리. 읽은 내용과 프로젝트 적용 방식("실험을 위해 직접 정해야 할 것", "우리 코드와의 대응")을 분리해 적는다 |
| [references/survey.md](references/survey.md) | 후보 15개 + 최근 논문 7개 조사와 선정 근거 |

## 변형(variant) 목록

variant 하나는 참고 문헌 하나에 대응한다. 실험 결과의 `variant` 값으로 이 이름을 쓴다.

| variant | 쓰기 확정 조건 | Primary 교체 | 근거 | 우선순위 | 상태 |
|---|---|---|---|---|---|
| `primary-backup` | 전원 ack (동기) / 즉시 (비동기, 설정) | 외부: heartbeat + 승계 순서 고정 | [Primary-Backup](references/primary-backup.md) (Budhiraja et al. 1993) | **1 (기준형)** | 설계 |
| `viewstamped-replication` | 과반수 | 프로토콜 내: view change (view mod N) | [VR Revisited](references/viewstamped-replication.md) (Liskov & Cowling 2012) | **1** | 설계 |
| `raft` | 과반수 | 프로토콜 내: 랜덤 timeout + 투표 | [Raft](references/raft.md) (Ongaro & Ousterhout 2014) | 2 | 조사 완료 |
| `chain-replication` (+ CRAQ) | 전원 통과 (tail 도달) | 외부 master | [Chain Replication / CRAQ](references/chain-replication-craq.md) (2004 / 2009) | 2 | 조사 완료 |
| `rose-bounded-lag` | 즉시. 단 Backup 지연 ≤ L일 때만 | `primary-backup`과 동일 | [Rosé](references/rose-bounded-lag.md) (Zarkadas et al., CIDR 2026) | 2 | 조사 완료 |

- 우선순위 1 두 개가 팀 과제 카드의 인수 기준이다. 기준형(`primary-backup`)을 먼저 만들고 그 위에서 공통 인터페이스·실험 틀을 검증한 뒤 `viewstamped-replication`을 붙인다.
- 우선순위 2 세 개는 조사는 끝났고 1이 끝난 뒤 시간이 허용하면 구현한다. 각각 비교 축 하나씩을 더한다: Raft는 "선출 방식", Chain은 "확정 조건(전원)과 구조(사슬)", Rosé는 "동기와 비동기 사이의 제3의 확정 조건".
- **Redis Replication + Sentinel**은 variant가 아니라 **비교 대상**이다. 기준형의 빈칸(감지·승격)을 실제 제품이 어떻게 채웠는지 보여주며, 정리는 [references/redis-replication-sentinel.md](references/redis-replication-sentinel.md)에 있다. 기준형의 heartbeat timeout, 승격 대상 선택 규칙은 Sentinel 값을 우리 규모로 줄여 쓴다.

## 코드 위치

알고리즘 코드는 `cache-core`의 `io.openkache.core.replication` 하위 패키지에 둔다(A2 IONIA와 같은 상위 패키지를 쓰고 하위 패키지로 나눈다). 이 디렉터리에는 설계, 참고 문헌 정리, 실험 설정과 결과 요약만 둔다. 상세는 [design.md](design.md) §7을 따른다.
