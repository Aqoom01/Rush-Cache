# B3 실험 진행과 결과

2026-10-23. stop-and-copy·pre-copy·post-copy의 in-process 구현과 S1~S5 비교를 완료했다.

| 실험 | 상태 | 결과 |
|---|---|---|
| 단일 이동 S1~S5 | 16조건 × seed 5개 × 세 방식, 240회 완료 | [참조 run·핵심 수치·재현](migration-comparison/results.md) |
| stop-and-copy SC1~SC6 | 기존 130회 Baseline 보관 | [실행과 결과 안내](migration-comparison/README.md) |
| S3 민감도 | 45조건 × seed 5개 × 세 방식, 675회 완료 | [설정별 결과·재현](migration-comparison/sensitivity-results.md) |
| S6 | 10경계 × seed 5개 × 세 방식, 실제 90회·미지원 60행 검증 | [취소·장애·복구 결과](migration-comparison/interruption-results.md) |
| 노드 증감 E1~E7 | 설계 단계 | [계획](scaling-decision/README.md) |
| 여러 job 이동 M1~M5 | 설계 단계 | [계획](movement-scaling-comparison/README.md) |

S1~S5의 정합성 위반은 0건이며 최종 데이터 240/240건이 일치했다. 같은 코드의 축소 반복 실험 480회도 모두 성공했고 두 반복의 요약 hash가 일치했다.

10만 key·쓰기 없음에서 쓰기 차단은 stop-and-copy/pre-copy/post-copy가 234/2/1 tick이었다. post-copy의 미도착 조회 BUSY는 48.24~50.23%였다. 균등 쓰기 rho=0.5에서 pre-copy는 차단을 3~4 tick으로 유지하는 대신 전송량이 약 1.51배로 증가했다. 각 수치와 조건은 위 단일 이동 결과의 참조 run에서 가져왔다.

이는 가상 시간·인메모리 모델 결과다. 실제 서버 중단 시간, 네트워크 latency, E/M scaling 성능은 측정하지 않았다. 실제 KV/peer 연계와 정책·scheduler 구현은 후속이다.

2026-10-26 S3 민감도 비교도 완료했다. 정식 675/675회 정합성 검증과 축소 1,350회 반복 재현 검증을 통과했다. rho=1.2·임계 256 KiB에서 라운드 상한 3→30은 쓰기 차단을 56~57→11~12 tick으로 줄였지만 전송량은 약 2.49→4.06~4.32배로 늘었다. [상세 결과](migration-comparison/sensitivity-results.md)를 따른다.

2026-10-28 S6 취소·장애 비교를 완료했다. 부분 delta 적용·ACK 뒤 취소를 포함한 정식 90개 시나리오에서 전환 전 취소 45회, 전환 후 취소 거부·복구 뒤 이동 완료 40회, source 미복구 FAILED 유지 5회를 검증했고 정합성 위반은 0건이다. 미지원 60행에는 수치를 채우지 않았다. 축소 두 반복의 실제 실행 180회도 통과했고 요약 hash가 일치한다. [상세 결과](migration-comparison/interruption-results.md)를 따른다. v2 dirty 라운드 상세·E/M scaling·실제 KV/peer 통합은 남아 있다.
