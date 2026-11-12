# 개인 B3: 처음부터 끝까지의 현재 범위

2026-11-05. B3는 노드 수 판단과 안전한 이동 실행을 맡는다. B2의 분할·병합·일반 부하 균등화·목적지 선택 알고리즘을 연구 과제로 추가하지 않는다. 이전 결합형 증감·배치 결과를 삭제하고 [고정 배치 재실행](java/results.md)으로 교체했다.

| 처리 순서 | 문제 | B3에서 비교·확인한 것 | 결과 |
|---|---|---|---|
| 관측→수 판단 | 부하 변화·spike·hotspot·예측 오차 | SCADS/ElastMan/E-Store/Accordion/P-Store/TIRAMOLA/PAX의 수 판단 | [7개 비교](scaling-decision/results.md) |
| 준비 | READY 전 용량을 쓰거나 재판단 | 공통 준비 지연·직렬 실행·warm-up·예측 비용 | [준비 지연](execution-validation.md) |
| 계획 입력 | 정해진 Tablet을 목표 노드 수에 담을 수 없음 | 같은 fixed-tablet-lpt adapter, 메모리 실패 시 유지·거부 | [고정 조건](scaling-decision/README.md) |
| schedule | endpoint·대역폭 경합·기아·기한 | 목적지 고정 실행 정책 9개 | [Scheduler](scheduler-comparison/results.md) |
| copy | 쓰기 차단·재전송·미도착 조회 | 실제 B3 Stop/Pre/Post-copy | [S1~S5](migration-comparison/results.md), [M1~M5](movement-scaling-comparison/results.md) |
| Pre-copy 종료 | dirty 감소 후 증가·수렴 실패 | 임계→라운드 상한→NOT_SHRINKING | [민감도](migration-comparison/sensitivity-results.md), [상세](migration-comparison/dirty-rounds.md) |
| publish·복구 | 쓰기 유실·중복 owner·잘못된 rollback | CAS·epoch·전환 전 source 유지·전환 후 owner 보존 | [S6](migration-comparison/interruption-results.md) |
| 부분 실패 | 한 job 장애가 전체에 전파 | PD식 격리·timeout·재시도, FDB식 seed 재생 | [안전성](execution-validation.md) |
| drain→제거 | 데이터·cleanup·metadata가 남음 | preflight-all / incremental-drain, ACK·잔여 작업 guard | [안전성](execution-validation.md) |
| 재관측 | 완료 반영·cooldown·다음 판단 | B3Workflow 단위 계약 | [Java 연결](java/README.md) |

노드 증감 비교는 7판단+고정 대조군 full560/quick224회, 준비 지연 full1440/quick192회, 고정 계획 Scheduler full1620/quick648회로 새로 실행했다. 나머지 순수 Java copy·safety 기록은 이전 검증 버전임을 구분한다. 원본과 각 실행의 hash는 [감사 기록](java/results.md)을 따른다. 반복·profile·미지원 행을 독립 표본 수로 합치지 않는다.

Slicer는 노드 수 판단이 없어 제외했다. Accordion의 후보 수 선택은 남기되 MILP 배치를 없앴다. PAX는 ARIMA·평균 CPU만 남긴 변형이다. 원문 전체와 동등한 성능 비교로 주장하지 않는다. split/merge를 고정 입력으로 두는 것과 실제 분할 알고리즘을 구현·평가하는 일을 구분한다.

현재는 가상 FCFS 모형과 실제 protocol의 인메모리 검증·단위 계약까지다. 실제 I/O adapter·네트워크 RTT·SSD/EBS·프로세스 재시작·metadata 영속성·실제 요청 p95/p99·downtime·비용 측정이 남는다. 공통 KV/Network/B1/B2 controller/Runner 통합을 필수로 추가하지 않는다.

[실서버 N/U/R 계획](real-node-plan/README.md)은 환경당 5,670회로 수정했고 **실험은 실행하지 않았다**. 각 흐름에 배정한 문제는 모두 포함하며 문제 부분집합을 다시 조합하지 않는다.
