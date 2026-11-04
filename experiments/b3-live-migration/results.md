# B3 실험 진행과 결과

2026-11-04. stop-and-copy·pre-copy·post-copy의 in-process 구현과 단일/다중 이동·선행 Scheduler 비교를 완료했다.

| 실험 | 상태 | 결과 |
|---|---|---|
| 단일 이동 S1~S5 | 16조건 × seed 5개 × 세 방식, 240회 완료 | [참조 run·핵심 수치·재현](migration-comparison/results.md) |
| stop-and-copy SC1~SC6 | 기존 130회 Baseline 보관 | [실행과 결과 안내](migration-comparison/README.md) |
| S3 민감도 | 45조건 × seed 5개 × 세 방식, 675회 완료 | [설정별 결과·재현](migration-comparison/sensitivity-results.md) |
| S6 | 10경계 × seed 5개 × 세 방식, 실제 90회·미지원 60행 검증 | [취소·장애·복구 결과](migration-comparison/interruption-results.md) |
| 선행 증감·재배치 메커니즘 | 8개 연구 prototype의 가상 모형 비교. 실제 서버·원 논문 제품 재현 아님 | [결과](scaling-decision/results.md) · [구현 범위](scaling-decision/README.md) |
| 여러 job 이동 M1~M5 | Scheduler·다중 노드 fixture, 42조건 × seed 5개 = 210회·3,465 jobs 완료 | [결과와 재현](movement-scaling-comparison/results.md) |
| 선행 Scheduler 방법 비교 | 고정 계획 9정책 810회 + CAMIG 목적지 선택 45회 = 855회·10,260 jobs 완료 | [정책별 결과와 원문 대비 범위](scheduler-comparison/results.md) |

S1~S5의 정합성 위반은 0건이며 최종 데이터 240/240건이 일치했다. 같은 코드의 축소 반복 실험 480회도 모두 성공했고 두 반복의 요약 hash가 일치했다.

10만 key·쓰기 없음에서 쓰기 차단은 stop-and-copy/pre-copy/post-copy가 234/2/1 tick이었다. post-copy의 미도착 조회 BUSY는 48.24~50.23%였다. 균등 쓰기 rho=0.5에서 pre-copy는 차단을 3~4 tick으로 유지하는 대신 전송량이 약 1.51배로 증가했다. 각 수치와 조건은 위 단일 이동 결과의 참조 run에서 가져왔다.

이는 가상 시간·인메모리 모델 결과다. 실제 서버 중단 시간, 네트워크 latency, 실제 노드 증감 성능은 측정하지 않았다. 다중 이동 Scheduler는 구현했으며 실제 KV/peer 연계와 노드 증감 controller의 이동 연결은 후속이다.

2026-10-26 S3 민감도 비교도 완료했다. 정식 675/675회 정합성 검증과 축소 1,350회 반복 재현 검증을 통과했다. rho=1.2·임계 256 KiB에서 라운드 상한 3→30은 쓰기 차단을 56~57→11~12 tick으로 줄였지만 전송량은 약 2.49→4.06~4.32배로 늘었다. [상세 결과](migration-comparison/sensitivity-results.md)를 따른다.

2026-10-28 S6 취소·장애 비교를 완료했다. 부분 delta 적용·ACK 뒤 취소를 포함한 정식 90개 시나리오에서 전환 전 취소 45회, 전환 후 취소 거부·복구 뒤 이동 완료 40회, source 미복구 FAILED 유지 5회를 검증했고 정합성 위반은 0건이다. 미지원 60행에는 수치를 채우지 않았다. 축소 두 반복의 실제 실행 180회도 통과했고 요약 hash가 일치한다. [상세 결과](migration-comparison/interruption-results.md)를 따른다. v2 dirty 라운드 상세·실제 KV/peer 통합은 남아 있다.

2026-11-02 M1~M5 다중 이동을 완료했다. 정식 210회·3,465 jobs와 축소 반복 168회에서 데이터 정합성·소유권·매 tick 예산 위반은 0이다. Stop-copy 동시 상한 1→8은 112.0→79.8 tick으로 줄었으나 쓰기 BUSY는 증가했다. 공유 source/destination은 노드 예산이 병목이었고, Pre-copy rho 0→1.2는 92.0→264.2 tick과 초기 대비 전송 비율 1.002→2.920을 보였다. 세 복사 방식의 차이·모형 제한·공식 run은 [다중 이동 결과](movement-scaling-comparison/results.md)를 따른다.

2026-11-04 선행 Scheduler의 핵심을 이식한 연구 비교를 완료했다. 정식 855회와 축소 두 반복 684회에서 위반 0이다. 공유 source·Pre-copy에서 FIFO 동시 4개는 3,625.4 tick, MongoDB식 노드당 1개는 1,114.8 tick이었다. 교차 경합의 single-MCs는 FIFO 대비 전체 완료 시간을 약 7.2% 줄였다. CAMIG 목적지 선택은 데이터량 균등 기준선과 전체 완료 평균이 같고 평균 job 완료는 약 11.6% 빨랐다. 원 논문 시스템 자체의 완전 재현이 아니며 각 변형의 설정·제한과 전체 결과는 [선행 Scheduler 비교](scheduler-comparison/results.md)를 따른다.
