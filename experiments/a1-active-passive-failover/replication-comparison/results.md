# A1 고정 구성 동기 복제 실험 결과

## 실행 근거

- 실행일: 2026-10-28 (Asia/Seoul). run ID의 시각은 UTC다.
- full: [27조건 상세 표](runs/run-2026-10-27T23-57-44.354058800Z-531d5fb3/repeat-001/results.md), [CSV](runs/run-2026-10-27T23-57-44.354058800Z-531d5fb3/repeat-001/summary.csv).
- 설정: [baseline.properties](baseline.properties), 27조건 × 100 SET = 2,700요청.
- 기준 commit df5716b 위의 미커밋 A1 실험 변경을 포함한다. 실제 source/backend hash는 상세 표에 기록돼 있다.
- quick: `cache-core/build/a1-replication-quick/run-2026-10-27T23-57-42.899675600Z-a8cdb6e1`, 27조건 × 10요청을 2회 실행.

## 관측 결과

|항목|결과|
|---|---:|
|전체 조건|27|
|모든 요청이 성공한 조건|23|
|모든 요청이 timeout인 조건|4|
|SET 성공|2,300|
|SET timeout|400|
|성공한 쓰기의 복제본 값 검증|6,900 / 6,900 일치|
|timeout 이후 일부 복제본에 값이 남은 요청|400|

이 성공 비율은 선택한 조건 조합의 집계이며 실제 시스템의 성공 확률·신뢰도가 아니다.

### 느린 Backup이 성공 응답을 지연시킴

기본 단방향 지연 5ms, deadline 200ms를 고정했다.

|Backup 2 추가 ACK 지연|성공 응답 시간(가상 ms)|
|---:|---:|
|0|10|
|20|30|
|100|110|

전원 ACK 조건에서 늦은 Backup의 추가 지연이 그대로 반영됐다. 예상식 `2 × 기본 단방향 지연 + 추가 ACK 지연`과 실제 엔진의 응답 callback 관측값이 일치했다. 이는 단순 모델의 확인이며 새로운 성능 우위를 입증하는 결과는 아니다.

### deadline보다 늦은 ACK는 성공으로 바꾸지 않음

|단방향 지연|추가 ACK 지연|deadline|예상 복제 완료|관측 응답|
|---:|---:|---:|---:|---|
|1|100|50|102|50ms TIMEOUT|
|5|100|50|110|50ms TIMEOUT|
|20|20|50|60|50ms TIMEOUT|
|20|100|50|140|50ms TIMEOUT|

각 조건에서 100건 모두 TIMEOUT이었다. 늦은 메시지를 배출한 뒤에도 응답은 TIMEOUT이며 일부 Backup의 데이터는 남았다. 따라서 timeout을 쓰기 미실행 또는 롤백으로 해석할 수 없다. deadline과 완료 시점이 정확히 같은 경계는 별도의 JUnit 테스트에서 TIMEOUT으로 확인했다.

## 재현 및 검증

- 전체 Gradle build 및 Runner 배포본 생성 성공. 테스트 결과 XML 기준 388 tests, failures/errors 0. 새 실험 테스트 7개 포함.
- quick 두 반복의 summary.csv SHA-256이 동일: `F823D512AA7A3303B83E58888CFC7F7B5E5E1481B33B334718CA63C0899F32F5`.
- full JSON에서 CSV를 재생성한 뒤 파일 hash 동일 확인.
- 원본 누락·지표 변조·schema 변경·중복 필드는 테스트에서 검출하고 완료 표식을 제거한다.

## 해석 범위와 다음 단계

가상 시간·고정 지연·로컬 즉시 처리 모델이며 실제 thread 스케줄링과 네트워크/SSD 비용은 없다. 요청 사이에 늦은 메시지를 배출하므로 지속 부하나 timeout 직후 재시도 실험도 아니다. 같은 입력의 반복은 결정성 점검이며 독립적인 성능 표본이 아니다.

현재 확인한 것은 고정 구성의 전원 ACK 대기와 deadline 동작이다. 장애 감지·승격·분단·VR 비교는 포함하지 않는다. 다음 단계에서는 A1 메시지/timer 실행기에 crash와 역할 전환을 추가하고, 요청·응답 이력을 기반으로 전체 failover 시간과 성공한 쓰기의 보존을 확인한다.
