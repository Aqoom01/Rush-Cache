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
| [real-server/](real-server/README.md) | PB 실제 EC2 3대, 유휴 crash12회·연속 crash12회·느린 Backup/ACK 경로18회. 보존·재시도·TIMEOUT·버전 한계 |
| [protocol-comparison/](protocol-comparison/README.md) | PB / VR의 정상·분단·미완료 쓰기 160조건 비교 |
| [network-cost-comparison/](network-cost-comparison/README.md) | 메시지 크기·방향별 대역폭·FIFO 큐를 반영한 쓰기·crash 비용 216조건 |
| [network-optimization/](network-optimization/README.md) | 증분 PREPARE·작은 COMMIT·재전송 제어의 개선 전후 216조건 비교 |
| [sustained-load/](sustained-load/README.md) | 연속 쓰기·ACK 지연·정상화 48조건, 시계열과 멘토 질문 |
| [load-crash/](load-crash/README.md) | 연속 부하 중 Primary 종료 48조건, 첫 OK와 생존 로그/KV 보존 |
| [election-comparison/](election-comparison/README.md) | Raft 추가: PB/VR/Raft의 정상·crash·분단·재합류 216조건 |
| [chain-comparison/](chain-comparison/README.md) | Chain 추가: 네 방식 288조건 + head/middle/tail 위치별 crash 24조건 |
| [craq-comparison/](craq-comparison/README.md) | CRAQ clean/dirty GET·EXISTS와 Chain 읽기 경로 32조건 비교 |
| [craq-mixed/](craq-mixed/README.md) | 읽기·쓰기 혼합 부하 288조건, key 충돌 범위·지연별 로컬 읽기/Tail 조회·성공/거절 비교 |
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
| `chain-replication` (+ CRAQ) | 전원 통과 (tail 도달) | 외부 master | [Chain Replication / CRAQ](references/chain-replication-craq.md) (2004 / 2009) | 2 | Chain 쓰기·Sent 복구·위치별 crash 및 선택적 CRAQ clean/dirty 다중 버전 읽기 구현 |
| `rose-bounded-lag` (실험 `ROSE_ADAPTATION`) | 로컬 적용 + op lag 상한 L | 외부 master·최장 생존 prefix | [Rosé](references/rose-bounded-lag.md) (CIDR 2026)에서 착안 | 2 | 단일 파티션 bounded-lag 변형 구현·240조건. 다중 파티션 coordinated apply 미구현 |

- 우선순위 1 두 개가 팀 과제 카드의 인수 기준이다. 기준형(`primary-backup`)을 먼저 만들고 그 위에서 공통 인터페이스·실험 틀을 검증한 뒤 `viewstamped-replication`을 붙인다.
- Raft·Chain과 Rosé 기반 단일 파티션 변형까지 추가했다. Raft는 선출 방식, Chain은 사슬 구조와 tail 확정, Rosé 변형은 L별 응답·유실 trade-off를 비교한다. CRAQ는 clean/dirty 읽기 경로를 별도 비교한다. 논문 전체의 구현 완료를 뜻하지 않으며 Rosé 다중 파티션 coordinated apply 등은 별도 범위다.
- **Redis Replication + Sentinel**은 variant가 아니라 **비교 대상**이다. 기준형의 빈칸(감지·승격)을 실제 제품이 어떻게 채웠는지 보여주며, 정리는 [references/redis-replication-sentinel.md](references/redis-replication-sentinel.md)에 있다. 기준형의 heartbeat timeout, 승격 대상 선택 규칙은 Sentinel 값을 우리 규모로 줄여 쓴다.

## 코드 위치

실서버에서는 [PB·VR 동일 조건 24회 비교](real-server/compare-results.md)를 추가했다. PB 전원 ACK·고정 승계와 VR 과반수·view change를 같은 3대 EC2, warmup30/측정128건, 값64B/8KiB, 감지500/1000ms에서 실행했다. 성공 쓰기3072건과 장애 후6192개 값 복사본을 확인했다. 기존 PB 단독42회 결과는 별도로 보존한다. 최대 처리량·SSD·분단·재시작 검증은 아니다.

이후 [PB·VR·Raft 새36회 실측](real-server/election-results.md)에서 같은 배포본으로 세 방식을 비교했다. Raft는 실제 선출 리더 호스트에서 해당 JVM을 종료하고 생존 리더의 새 쓰기를 확인한다. 성공 쓰기4608건·장애 후9288개 값 복사본이 일치했다. Raft 난수 선거 timeout 범위·선거별 진단·초기/이후 리더와 term을 함께 남기며 앞선 결과와 섞지 않는다.

[VR 정상 전송량 계측12회](real-server/traffic-results.md)에서는 COMMIT이 Backup 로그보다 앞서는 상태 요청67건과 전체 로그 전송23회를 확인했다. 전체 로그가 전체 frame byte 합계의52.2%였으나8회는 전체 전송이 없었다. ACK만 늦춘 결정론적 재현에서도 같은 경로가 발생했다. 알고리즘 최적화 없이 원인 계측만 추가한 결과다.

[VR 전송 경로 개선 전후 비교24회](real-server/catchup-results.md)는 `INCREMENTAL_CATCHUP` 모드(Backup별 전송 범위 안의 COMMIT 통지·같은 view 정상 Backup의 증분 catch-up)를 기존 INCREMENTAL과 같은 배포본·부하로 조건마다 번갈아 측정한 결과다. 개선 전12회는 전체 로그118회·GET_STATE380건·전체 peer byte54,930,865B, 개선 후12회는 전체 로그0·GET_STATE0·13,536,680B(75.4% 감소)였고 24회 모두 값 복사본이 일치했다. 과반수 확정·view change·재전송 backoff는 바꾸지 않았으며 유실 복구 지연은 재전송 timer를 따른다.

[Chain 실서버 위치별 crash36회](real-server/chain-results.md)는 `ChainEngine`을 실제 TCP에 연결하고 정확한 영구 crash를 아는 외부 master 역할을 실험 controller가 맡은 결과다. head/middle/tail × 값64B/8KiB × 고정 감지 대기500/1000ms × 3회에서 정상4608건·tail 읽기4608건·값 복사본 전부 일치했고, 감지 대기 이후 freeze/repair/open 비용은 30~40ms(repair 약20ms)였다. controller의 고정 대기는 실제 failure detector가 아니며 head는 역방향 Ack 전 새 쓰기에 BUSY를 돌려준다.

[CRAQ 읽기 경로 실서버12회](real-server/craq-results.md)는 원래 브랜치의 CRAQ 구현을 실서버 worktree에 통합해 TAIL_ONLY와 번갈아 측정한 결과다. CRAQ는 head/middle clean GET을 로컬에서 응답해 redirect 두 왕복(p50 1.3~1.9ms)을 0.9~1.0ms로 줄였고, hot key 동시 쓰기 중 middle 읽기의 dirty 비율은 실행당 약 140/550건이며 순번 역행은 없었다. 대신 혼합 구간 head SET p99는 높아졌다.

[Rosé 변형 실서버30회](real-server/rose-results.md)는 단일 파티션 bounded-lag 변형을 실제 TCP에 연결해 L ∈ {0,1,4,16,∞} × 값 × 3회로 client 8개 동시 쓰기와 쓰기 진행 중 Primary crash를 측정했다. OK 받은 쓰기의 유실은 L=0에서 0, L=4에서 최대 2, L=16에서 최대 9로 모두 상한 안이었고, L=∞는 실행당 168~188건이 사라졌다. backpressure·peakLag는 L에 정확히 닿았다. 외부 master 역할은 controller가 맡았다.

[실서버 종합 비교](real-server/overall-results.md)는 PB/VR/Raft 선출 비교, VR 전송 비교, Chain, CRAQ, Rosé cohort의 요약을 같은 축으로 모은 표와 보장·전제 비교, 재현 절차다. cohort마다 감지 모델·장애 모델·부하가 다르므로 순위로 읽지 않는다.

알고리즘 코드는 `cache-core`의 `io.openkache.core.replication` 하위 패키지에 둔다(A2 IONIA와 같은 상위 패키지를 쓰고 하위 패키지로 나눈다). 이 디렉터리에는 설계, 참고 문헌 정리, 실험 설정과 결과 요약만 둔다. 상세는 [design.md](design.md) §7을 따른다.

기준형의 분단·미완료 쓰기 한계와 VR의 로그 복구를 [같은 실행기에서 비교](protocol-comparison/README.md)한다. 실제 서버 실험은 별도 실측을 참고한다. 프로세스 재시작 recovery와 동적 구성 변경은 범위 밖이다.
