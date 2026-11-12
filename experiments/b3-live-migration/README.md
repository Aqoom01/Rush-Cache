# B3 Live Migration + Elastic Scaling

**현재 B3 비교는 노드 수 판단과 이동 실행이다.** 2026-11-05에 B2의 split/merge·일반 부하 균등화·목적지 선택을 제외하고 배치를 하나로 고정했다. 기존 작업을 063c817에 커밋한 뒤 수정했고, 결합형 증감·배치·CAMIG 결과는 삭제했다.

[현재 Java 결과·감사](java/results.md) · [처음부터 끝까지](end-to-end-review.md) · [실서버 N/U/R 계획](real-node-plan/README.md)

| 비교·검증 | 현재 범위 | 안내 |
|---|---|---|
| 증감 판단 | 선행 판단 7개+고정 4노드 대조군, 배치는 동일 | [명세](scaling-decision/README.md), [결과](scaling-decision/results.md) |
| 준비 지연 | AWS warm-up/ElastMan/P-Store, 같은 배치·전송 모형 | [안내](execution-policy-comparison/README.md), [결과](execution-validation.md) |
| Scheduler | 입력 이동 계획의 목적지를 고정한 9실행 정책 | [안내](scheduler-comparison/README.md), [결과](scheduler-comparison/results.md) |
| 단일 이동 | Stop/Pre/Post-copy·종료 민감도·취소/장애 | [안내](migration-comparison/README.md), [결과](migration-comparison/results.md) |
| 다중 이동 | M1~M5 FIFO의 job 수·동시 수·endpoint·쓰기 영향 | [안내](movement-scaling-comparison/README.md), [결과](movement-scaling-comparison/results.md) |
| 안전한 축소·실패 | drain·metadata·cleanup·job 격리·seed 장애 | [안내](execution-safety-comparison/README.md), [종합](execution-validation.md) |
| 구현·설정 | Java21/JDK, NodeCountPolicy·고정 adapter·B3Workflow | [모듈](java/README.md), [protocol](protocols.md) |

공통 서비스 통합을 전제로 하지 않는 독립 B3 prototype이다. 자체 fixture/저장/전송 port와 독립 Java 연구 실행기를 사용하며 공통 api/model/spi/runtime 계약은 변경하지 않았다. 팀 B1/B2의 이동 계획을 입력받는 경계와 개인 실험의 고정 adapter를 구분한다. B2의 배치 알고리즘을 B3 필수 실험으로 평가하지 않는다.

실제 노드 생성·종료·membership·실제 I/O는 별도 adapter 책임이다. 가상 노드 수 변경을 실제 서버 조작 완료로 표시하지 않는다. 실제 N/U/R 시나리오는 계획만 작성했으며 실행하지 않았다.

[초기 설계](design.md)는 현재 모듈·실험 명세가 우선한다. [선행 자료](references/)는 원문 설명이며 문서에 등장하는 모든 기능을 B3에서 구현·실행했다는 뜻이 아니다.
