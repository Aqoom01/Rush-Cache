# A1 단일 Primary crash 결과

실행일: 2026-10-28 (Asia/Seoul). run ID의 시각은 UTC다.

- [27조건 상세 결과](runs/run-2026-10-28T00-11-10.048660600Z-d2daff5c/repeat-001/results.md)
- [조건별 CSV](runs/run-2026-10-28T00-11-10.048660600Z-d2daff5c/repeat-001/summary.csv)
- [실행 설정](baseline.properties) · [모델·재현 방법](README.md)

## 확인한 동작

Primary 0 종료 → Backup 1 heartbeat timeout → epoch 2 구성 설치 → Backup 2 READY → 쓰기 재개.
승격 후에는 Backup 2 하나의 ACK를 기다린다. 종료된 Primary 0의 ACK를 요구하지 않는다.

| 항목 | 결과 |
|---|---:|
| 독립 조건 | 27 / 27 완료 |
| 조건별 장애 이전 OK 쓰기 | 고유 키 10개 |
| 장애 이전 쓰기의 생존 노드 복사본 검증 | 540 / 540 일치 (27 × 10 × 2) |
| 복구 후 첫 OK 쓰기의 생존 노드 검증 | 54 / 54 일치 (27 × 1 × 2) |
| 전체 failover 범위 | 가상 47~295ms |
| 감지 이후 준비 완료 | 가상 2~20ms (INSTALL/READY 왕복) |

전체 시간은 crash부터 첫 OK callback까지다. callback을 나중에 관측하는 polling 지연은 추가하지 않는다.
이 범위는 선택한 27조건의 최솟값·최댓값이며 실제 서버 성능이나 p99가 아니다.

## 클라이언트 정책에 따른 시간 차이

편도 지연 5ms, heartbeat timeout 100ms를 고정했다.

| 재시도 간격 | 감지 | 전환 | 준비 이후 재탐색·대기·첫 복제 | 전체 |
|---|---:|---:|---:|---:|
| 1ms | 95ms | 10ms | 10ms | 115ms |
| 10ms | 95ms | 10ms | 35ms | 140ms |
| 25ms | 95ms | 10ms | 80ms | 185ms |

감지와 전환이 같아도 클라이언트 재시도가 전체 복구 시간을 늘렸다. 클라이언트는 옛 Primary 실패 후 Backup을 조회하고 Redirect를 따르며, stale epoch 응답에서는 epoch를 갱신한다. 각 단계가 재시도 간격을 소비한다. 전환 전에는 Backup이 옛 Primary로 Redirect할 수 있어 왕복 탐색이 반복된다. 따라서 준비 이후 구간은 단 한 번의 재시도 대기만을 뜻하지 않는다.

heartbeat timeout 100ms인데 감지가 95ms인 것은 crash 직전 마지막 heartbeat 수신부터 시간이 이미 경과했기 때문이다. 감지 timeout과 장애 발생 이후 감지 지연은 동일하지 않다.

## 검증 근거

- 전체 Gradle build 성공. 결과 XML 기준 397 tests, failures/errors 0. 이번 단계의 새 테스트 9개 포함.
- 정상 heartbeat 유지, READY 누락 시 쓰기 차단과 재확인, 로컬 적용 barrier 대기, stale epoch·옛 메시지 거부, Redirect, 생존 Backup ACK 누락, 복제 중 crash를 별도 검증했다.
- 원본 재생성, 누락·중복·schema·보존 개수 위반 검출을 확인했다.
- quick run `run-2026-10-28T00-11-08.735522200Z-c175c0f6`은 27조건을 두 번 실행했고 두 CSV SHA-256이 동일했다: `C68B0854953DB36949232F5822BAE6EC1918948766CF3F79FAC572162074D4E1`.
- 기준 commit df5716b 위 미커밋 변경을 포함한다. 실제 source/backend hash는 상세 결과와 Runner launch/manifest에 기록했다.

## 해석 범위

한 프로세스의 가상 시간 모델이다. heartbeat 10ms, timer tick 1ms, 노드 간 고정 지연, 즉시 로컬 KV 처리를 사용한다. 접속 실패도 즉시 통지하므로 실제 TCP timeout 비용은 없다.

성공한 고유 키 쓰기를 완료한 뒤 Primary 한 대를 영구 종료했다. 미완료 쓰기의 일부 적용·동일 키 덮어쓰기 이력은 이번 보존 결과로 보장하지 않는다. 별도의 복제 중 crash 테스트도 로그 합의나 상태 복구를 입증하지 않는다.

분단·오판 시 옛 Primary 차단, recovery, 두 번째 장애, 실제 서버 TCP/SSD 연결은 미구현이다. VR 변형 비교는 후속이다. 반복 횟수를 성능의 독립 표본 수로 해석하지 않는다.
