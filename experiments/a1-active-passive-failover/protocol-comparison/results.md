# PB / VR 비교 결과

2026-10-28 (Asia/Seoul). 아래 run을 이번 구현의 참조 실행으로 사용한다. run ID의 시각은 UTC다.

- [조건별 전체 보고서](runs/run-2026-10-28T00-58-09.202711100Z-8480a12e/repeat-001/results.md)
- [CSV 원본 요약](runs/run-2026-10-28T00-58-09.202711100Z-8480a12e/repeat-001/summary.csv)
- [실행 설정](comparison.properties) · [모델·구현 범위·재현](README.md)

## 실행 범위

PB / VR × 8시나리오 × 편도 지연 1·5ms × seed 1~5 = 160조건을 모두 완료했다.
네트워크 jitter 0~3ms, heartbeat 10ms, timeout 100ms, 주 요청 deadline 60ms, 클라이언트 재시도 간격 5ms다.
구현 대상은 메모리 상태를 유지하는 고정 3노드다. 실제 서버·SSD 성능은 측정하지 않았다.

| 항목 | PB (80조건) | VR (80조건) |
|---|---:|---:|
| 주 요청 또는 복구 후 첫 요청의 OK | 60조건 | 80조건 |
| audit key의 복제본 값이 다른 조건 | 40조건 | 0조건 |
| 준비 구간에서 성공한 marker 복사본 보존 | 360 / 360 | 360 / 360 |

이 비율은 의도적으로 선택한 조건의 집계이며 일반적인 성공률·장애 확률이 아니다.
marker는 영향을 받지 않는 고유 키 두 개다. marker 보존과 일부 미완료 쓰기의 불일치를 구분한다.
복제본 불일치만으로 성공한 쓰기 유실이나 전체 선형화 위반이 증명되는 것은 아니다.

## 미완료 쓰기를 이어받는 방식의 차이

두 방식 모두 x=old 성공 후 x=pending을 Backup 2에만 전달하고 ACK를 버렸다. 그 후 Primary를 종료하거나 격리했다.

| 장애 이후 | PB | VR |
|---|---|---|
| 새 Primary / 남은 Backup의 x | old / pending | pending / pending |
| 장애 후 다른 키의 첫 SET | 성공 | 성공 |
| 해석 | 새 쓰기가 성공해도 기존 미완료 쓰기의 불일치는 남을 수 있음 | 로그를 모아 선택·재확정한 후 상태 일치 |

PARTIAL_CRASH와 PARTIAL_PARTITION 각각 10조건에서 같은 결과를 확인했다.
이전 Primary만 가진 suffix를 버리는 경우, 후보 노드에는 없지만 다른 Backup이 가진 성공 연산을 이어받는 경우도 별도 JUnit으로 검사했다.
응답을 못 받은 x=pending은 채택될 수도 버려질 수도 있다. 재시도는 같은 ClientId/OperationId를 사용해야 하며 VR은 이미 실행한 요청의 응답을 재사용한다.

## 느린 Backup·분단에서의 쓰기 진행

| 시나리오 | PB (각 10조건) | VR (각 10조건) |
|---|---|---|
| 정상 | 전부 OK, 응답 5~15ms | 전부 OK, 응답 2~12ms |
| Backup 2 ACK +100ms | 전부 60ms TIMEOUT | 전부 OK, 응답 2~14ms |
| Backup 2 분단 | 전부 60ms TIMEOUT | 전부 OK, 응답 2~15ms |
| Primary 0 분단 | 생존 쪽에서 전부 쓰기 재개 | 생존 과반수 쪽에서 전부 쓰기 재개 |

PB의 전원 ACK 조건과 VR의 과반수 조건 차이를 확인했다. PB의 Primary 분단 결과는 지원 범위 밖의 관측이며 분단 안전성 보장이 아니다.
VR의 옛 Primary에 분단 이후 새 쓰기를 제출해 성공하지 못하는 것, 과반수 쪽의 성공 쓰기, 재연결 후 따라잡기는 별도 테스트로 확인했다.
분단 전에 이미 과반수가 받은 진행 중 쓰기의 늦은 확정은 허용한다.

VR이 메시지별 지연 모델에서 더 빨리 응답한 것은 ACK 조건 차이와 일치한다. 전체 로그 크기에 비례하는 전송 시간은 반영하지 않으므로 실제 시스템에서의 속도 우위를 뜻하지 않는다.

## Primary crash 복구 시간

CRASH 10조건에서 PB는 115~133ms, VR은 113~134ms였다. 두 방식 모두 실제 callback을 끝점으로 기록했다.
감지 timeout 100ms, 장애 시점의 heartbeat 위상, 클라이언트 재탐색 정책이 포함된 시간이다. 이 범위를 승격 프로토콜 자체의 우열로 해석하지 않는다.
감지/준비/첫 쓰기 구간을 나눈 기준형 27조건 결과는 [별도 실험](../failover-comparison/results.md)에 있다. 그 결과는 jitter와 클라이언트 정책이 달라 이번 수치와 합산하지 않는다.

## 실행·검증 근거

- 전체 `gradlew build :experiment-runner:installDist` 성공. 테스트 XML 기준 419 tests, failures/errors 0.
- 성공 쓰기 보존, COMMIT 통지 유실, 후보의 로그 누락, 단일 Backup의 미확정 로그, primary-only suffix 폐기, client 중복 식별, 과반수 읽기, 비동기 적용 callback을 검사했다.
- seed 10개에서 6회씩 반복 분단·치유를 하며 유실·중복·순서 역전을 섞었고, 성공한 동일 키 덮어쓰기 결과가 모든 노드에 이어짐을 확인했다.
- 작은 단일 키 읽기/쓰기 이력은 시간 선후 관계를 만족하는 순열을 탐색했다. TIMEOUT 쓰기는 적용/미적용 가능성을 모두 허용했다. 이는 유한 테스트이며 형식 증명이 아니다.
- JSON 원본의 누락·중복·schema 변경·지표 위반을 검출하고 완료 표식을 제거한다.
- quick `run-2026-10-28T00-58-07.813451900Z-6bc51c99`의 32조건을 두 번 실행했다. 두 CSV SHA-256: `291A2FF1CCCA16640BD6FCB692869B9DE131F1DFE3B1DFDCA6B7D06B30EA345C`로 동일하다.
- full CSV·Markdown을 JSON 원본에서 재생성한 뒤 두 파일 모두 기존 hash와 같았다.
- full CSV SHA-256: `CF4DA912D25D4D76CCF1CF80DFC2B7A8F5AFF5D623E4F2032E4F59881BBF17F8`.
- full 보고서 SHA-256: `37CACA2BE6AEA1BD71DCF34DA8159463DD61A0CE45470D6016924BCCF364887B`.
- source SHA-256: `b63c5a0352ebad140691aeee8564070632c07a5c42ff398016abdd3e3b2826c0`.
- backend SHA-256: `6060c49a29ae72aaedf0c3f12b3e4e7283fb1a53116de231228ea64fbf8dba90`.
- 기준 commit은 df5716b이며 미커밋 구현을 포함한다. 실제 실행 소스는 source/class hash로 식별한다.

원본 160개와 실행 설정·manifest·로그는 해당 run 폴더에 로컬 보관한다. Git 보관 대상은 기존 정책대로 반복별 CSV·보고서다.
