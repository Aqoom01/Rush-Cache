# B3 실험 진행과 결과

2026-10-23. stop-and-copy·pre-copy·post-copy의 in-process 구현과 S1~S5 비교를 완료했다.

| 실험 | 상태 | 결과 |
|---|---|---|
| 단일 이동 S1~S5 | 16조건 × seed 5개 × 세 방식, 240회 완료 | [참조 run·핵심 수치·재현](migration-comparison/results.md) |
| stop-and-copy SC1~SC6 | 기존 130회 Baseline 보관 | [실행과 결과 안내](migration-comparison/README.md) |
| S6·S3 민감도 | 프로토콜 경계 테스트는 통과, 비교 suite는 후속 | [범위](migration-comparison/README.md) |
| 노드 증감 E1~E7 | 설계 단계 | [계획](scaling-decision/README.md) |
| 여러 job 이동 M1~M5 | 설계 단계 | [계획](movement-scaling-comparison/README.md) |

S1~S5의 정합성 위반은 0건이며 최종 데이터 240/240건이 일치했다. 같은 코드의 축소 반복 실험 480회도 모두 성공했고 두 반복의 요약 hash가 일치했다.

10만 key·쓰기 없음에서 쓰기 차단은 stop-and-copy/pre-copy/post-copy가 234/2/1 tick이었다. post-copy의 미도착 조회 BUSY는 48.24~50.23%였다. 균등 쓰기 rho=0.5에서 pre-copy는 차단을 3~4 tick으로 유지하는 대신 전송량이 약 1.51배로 증가했다. 각 수치와 조건은 위 단일 이동 결과의 참조 run에서 가져왔다.

이는 가상 시간·인메모리 모델 결과다. 실제 서버 중단 시간, 네트워크 latency, E/M scaling 성능은 측정하지 않았다. 실제 KV/peer 연계와 정책·scheduler 구현은 후속이다.
